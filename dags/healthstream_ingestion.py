"""Orchestration horaire de la chaîne HealthStream (séance 3).

Airflow ne remplace rien : il déclenche une production bornée, mesure le lag du
consommateur de la séance 2, contrôle l'intégrité de vitals_raw sur la fenêtre
du run et écrit un rapport d'ingestion idempotent (base + fichier JSON).

Règle d'or : toutes les tâches travaillent sur l'INTERVALLE DE DONNÉES du run
(data_interval_start / data_interval_end), jamais sur l'heure courante.
Ce module est ré-exécuté par le scheduler toutes les 30 s : aucun appel réseau
ni import lourd au niveau du module.
"""

import json
import logging
import time
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# --- Constantes de module ---
CONN_PG = "healthstream_pg"  # déclarée par AIRFLOW_CONN_HEALTHSTREAM_PG
DOSSIER_RAPPORTS = Path("/opt/airflow/data/rapports")
DOSSIER_ALERTES = Path("/opt/airflow/data/alertes")
ATTENTE_LAG_MAX_S = 120
PAS_LAG_S = 10

logger = logging.getLogger("airflow.task")

SQL_TABLE_RAPPORT = """
CREATE TABLE IF NOT EXISTS ingestion_report (
    dag_id         VARCHAR(250) NOT NULL,
    window_start   TIMESTAMPTZ  NOT NULL,
    window_end     TIMESTAMPTZ  NOT NULL,
    rows_ingested  INTEGER      NOT NULL,
    patients       INTEGER      NOT NULL,
    null_values    INTEGER      NOT NULL,
    unknown_types  INTEGER      NOT NULL,
    lag_before     BIGINT,
    lag_after      BIGINT,
    run_count      INTEGER      NOT NULL DEFAULT 1,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_ingestion_report PRIMARY KEY (dag_id, window_start)
);
"""

SQL_INTEGRITE = """
SELECT
    count(*)                                                        AS lignes,
    count(DISTINCT patient_id)                                      AS patients,
    count(*) FILTER (WHERE value IS NULL)                           AS valeurs_nulles,
    count(*) FILTER (WHERE vital_type NOT IN ('HR', 'SPO2', 'ABP_SYS')) AS types_inconnus,
    count(*) FILTER (WHERE vital_type = 'SPO2' AND value > 100)     AS spo2_impossibles
FROM vitals_raw
WHERE ingested_at >= %s AND ingested_at < %s;
"""

SQL_UPSERT_RAPPORT = """
INSERT INTO ingestion_report (dag_id, window_start, window_end, rows_ingested, patients,
                              null_values, unknown_types, lag_before, lag_after)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dag_id, window_start) DO UPDATE SET
    window_end    = EXCLUDED.window_end,
    rows_ingested = EXCLUDED.rows_ingested,
    patients      = EXCLUDED.patients,
    null_values   = EXCLUDED.null_values,
    unknown_types = EXCLUDED.unknown_types,
    lag_before    = EXCLUDED.lag_before,
    lag_after     = EXCLUDED.lag_after,
    run_count     = ingestion_report.run_count + 1,
    updated_at    = now()
RETURNING run_count, updated_at;
"""


# --- Outil partagé ---
def _lag_du_groupe(bootstrap: str, topic: str, groupe: str):
    """Lag total du consumer group et détail par partition.

    N'appelle JAMAIS subscribe() : committed() envoie un OffsetFetch sans
    rejoindre le groupe, donc sans rééquilibrage du consommateur de la séance 2.
    """
    from confluent_kafka import Consumer, TopicPartition  # import lourd : dans la fonction

    consommateur = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": groupe,
        "enable.auto.commit": False,
        "client.id": "airflow-lag-probe",
    })
    try:
        meta = consommateur.list_topics(topic, timeout=10)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            raise RuntimeError(f"topic {topic} introuvable sur {bootstrap}")
        partitions = [TopicPartition(topic, p) for p in sorted(meta.topics[topic].partitions)]
        commits = consommateur.committed(partitions, timeout=10)
        detail = {}
        for tp in commits:
            bas, haut = consommateur.get_watermark_offsets(
                TopicPartition(topic, tp.partition), timeout=10, cached=False)
            # offset négatif = aucun commit : tout ce qui est retenu est en retard
            detail[tp.partition] = haut - (tp.offset if tp.offset >= 0 else bas)
        return sum(detail.values()), detail
    finally:
        consommateur.close()


# --- Fonctions de tâche ---
def verifier_broker(**context):
    from confluent_kafka.admin import AdminClient

    bootstrap = Variable.get("kafka_bootstrap")
    topic = Variable.get("vitals_topic")
    meta = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
    if topic not in meta.topics:
        raise RuntimeError(f"le topic {topic} n'existe pas sur {bootstrap}")
    n = len(meta.topics[topic].partitions)
    logger.info("broker %s joignable, topic %s : %d partitions", bootstrap, topic, n)
    return n


def relever_offsets_avant(**context):
    lag, detail = _lag_du_groupe(Variable.get("kafka_bootstrap"),
                                 Variable.get("vitals_topic"),
                                 Variable.get("vitals_group"))
    logger.info("lag_avant=%d detail=%s", lag, detail)
    context["ti"].xcom_push(key="lag_avant", value=lag)


def controler_lag(**context):
    bootstrap = Variable.get("kafka_bootstrap")
    topic = Variable.get("vitals_topic")
    groupe = Variable.get("vitals_group")
    seuil = int(Variable.get("lag_max"))

    debut = time.monotonic()
    lag_max_vu = 0
    while True:
        lag, detail = _lag_du_groupe(bootstrap, topic, groupe)
        lag_max_vu = max(lag_max_vu, lag)
        ecoule = time.monotonic() - debut
        logger.info("lag=%d seuil=%d max_vu=%d attente=%.0fs detail=%s",
                    lag, seuil, lag_max_vu, ecoule, detail)
        if lag <= seuil:
            break
        if ecoule >= ATTENTE_LAG_MAX_S:
            raise RuntimeError(
                f"lag {lag} > seuil {seuil} après {ATTENTE_LAG_MAX_S}s "
                f"(groupe {groupe}) ; détail par partition : {detail}")
        time.sleep(PAS_LAG_S)

    ti = context["ti"]
    ti.xcom_push(key="lag_apres", value=lag)
    ti.xcom_push(key="lag_detail", value={str(p): v for p, v in detail.items()})
    ti.xcom_push(key="lag_max_vu", value=lag_max_vu)


def verifier_integrite(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # L'intervalle du RUN, pas l'heure courante : c'est ce qui rend le run rejouable.
    debut = context["data_interval_start"]        # et NON pendulum.now()
    fin = context["data_interval_end"]
    hook = PostgresHook(postgres_conn_id=CONN_PG)
    lignes, patients, nulles, types_inconnus, spo2 = hook.get_first(
        SQL_INTEGRITE, parameters=(debut, fin))

    integrite = {
        "lignes": lignes, "patients": patients, "valeurs_nulles": nulles,
        "types_inconnus": types_inconnus, "spo2_impossibles": spo2,
    }
    logger.info("fenêtre [%s, %s[ : %s", debut, fin, integrite)
    context["ti"].xcom_push(key="integrite", value=integrite)

    if lignes == 0:
        raise ValueError(f"aucune ligne ingérée dans la fenêtre [{debut}, {fin}[")
    anomalies = nulles + types_inconnus + spo2
    if anomalies:
        raise ValueError(f"{anomalies} anomalie(s) dans la fenêtre [{debut}, {fin}[ : {integrite}")


def ecrire_rapport(**context):
    try:
        from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
    except ImportError:  # provider common-sql plus ancien
        from airflow.providers.common.sql.hooks.sql import fetch_one_handler
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = context["ti"]
    debut = context["data_interval_start"]
    fin = context["data_interval_end"]
    dag_id = context["dag"].dag_id
    lag_avant = ti.xcom_pull(task_ids="relever_offsets_avant", key="lag_avant")
    lag_apres = ti.xcom_pull(task_ids="controler_lag", key="lag_apres")
    integrite = ti.xcom_pull(task_ids="verifier_integrite", key="integrite")

    # En base : la clé primaire (dag_id, window_start) absorbe le rejeu.
    hook = PostgresHook(postgres_conn_id=CONN_PG)
    run_count, maj = hook.run(
        SQL_UPSERT_RAPPORT,
        parameters=(dag_id, debut, fin, integrite["lignes"], integrite["patients"],
                    integrite["valeurs_nulles"], integrite["types_inconnus"],
                    lag_avant, lag_apres),
        handler=fetch_one_handler,
    )

    # Sur disque : nom dérivé du DÉBUT DE FENÊTRE, jamais de l'heure courante.
    # Un rejeu ÉCRASE le fichier, il n'en crée pas un second.
    DOSSIER_RAPPORTS.mkdir(parents=True, exist_ok=True)
    chemin = DOSSIER_RAPPORTS / f"ingestion_{debut.format('YYYYMMDD_HH')}.json"
    rapport = {
        "dag_id": dag_id,
        "window_start": debut.isoformat(),
        "window_end": fin.isoformat(),
        "rows_ingested": integrite["lignes"],
        "patients": integrite["patients"],
        "null_values": integrite["valeurs_nulles"],
        "unknown_types": integrite["types_inconnus"],
        "spo2_impossibles": integrite["spo2_impossibles"],
        "lag_before": lag_avant,
        "lag_after": lag_apres,
        "run_count": run_count,
        "updated_at": maj.isoformat(),
    }
    chemin.write_text(json.dumps(rapport, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("rapport écrit : %s (run_count=%d)", chemin, run_count)


# --- Alerte et traçage des nouvelles tentatives ---
def alerter_echec(context):
    """Canal d'alerte : un fichier JSON par échec définitif (Slack/PagerDuty en prod)."""
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    charge = {
        "gravite": "CRITIQUE",
        "dag_id": dag_id,                                        # quoi
        "task_id": ti.task_id,
        "run_id": context["run_id"],                             # quand (fenêtre)
        "fenetre": str(context["data_interval_start"]),
        "tentative": ti.try_number,                              # laquelle
        "erreur": str(context.get("exception", "inconnue")),     # pourquoi
        "journal": f"/opt/airflow/logs/dag_id={dag_id}"          # où regarder
                   f"/run_id={context['run_id']}/task_id={ti.task_id}"
                   f"/attempt={ti.try_number}.log",
        "horodatage": pendulum.now("Europe/Paris").isoformat(),  # quand (réel)
    }
    DOSSIER_ALERTES.mkdir(parents=True, exist_ok=True)
    nom = f"alerte_{context['run_id']}_{ti.task_id}.json"
    for interdit in ':+/\\':  # run_id contient ':' et '+', interdits sous Windows
        nom = nom.replace(interdit, "-")
    (DOSSIER_ALERTES / nom).write_text(json.dumps(charge, indent=2, ensure_ascii=False),
                                       encoding="utf-8")
    logger.error("ALERTE %s", json.dumps(charge, ensure_ascii=False))


def tracer_tentative(context):
    ti = context["ti"]
    logger.warning("nouvelle tentative : %s.%s, tentative %s échouée (%s)",
                   context["dag"].dag_id, ti.task_id, ti.try_number,
                   context.get("exception", "?"))


default_args = {
    "owner": "icu-ops",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": alerter_echec,
    "on_retry_callback": tracer_tentative,
}


with DAG(
    dag_id="healthstream_ingestion",
    default_args=default_args,
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    tags=["s3", "healthstream"],
) as dag:
    t_creer_table = SQLExecuteQueryOperator(
        task_id="creer_table_rapport", conn_id=CONN_PG, sql=SQL_TABLE_RAPPORT)

    t_broker = PythonOperator(task_id="verifier_broker", python_callable=verifier_broker)

    t_avant = PythonOperator(task_id="relever_offsets_avant",
                             python_callable=relever_offsets_avant)

    t_produire = BashOperator(
        task_id="produire_fenetre",
        bash_command=(
            "timeout --preserve-status --signal=TERM {{ var.value.fenetre_duree_s }} "
            "python /opt/airflow/producer/producer.py"
        ),
        append_env=True,
        env={
            "KAFKA_BOOTSTRAP": "{{ var.value.kafka_bootstrap }}",
            "VITALS_TOPIC": "{{ var.value.vitals_topic }}",
            "NB_PATIENTS": "12",
            "INTERVAL_S": "1.0",
        },
    )

    t_lag = PythonOperator(task_id="controler_lag", python_callable=controler_lag)

    t_integrite = PythonOperator(task_id="verifier_integrite",
                                 python_callable=verifier_integrite)

    t_rapport = PythonOperator(task_id="ecrire_rapport", python_callable=ecrire_rapport)

    t_fin = EmptyOperator(task_id="fin_pipeline")

    t_creer_table >> t_broker >> t_avant >> t_produire
    t_produire >> t_lag >> t_integrite >> t_rapport >> t_fin
