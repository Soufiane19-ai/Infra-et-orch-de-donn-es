"""Consommateur de persistance HealthStream (séance 2, partie 4).

Lit mimic-vitals, accumule un lot en mémoire, l'écrit en UNE instruction SQL
idempotente dans vitals_raw, puis — seulement ensuite — acquitte les offsets.
Garantie : at-least-once sur le flux, exactly-once dans la table (clé primaire
sur les coordonnées Kafka).
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError, KafkaException

# --- Configuration par l'environnement ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "vitals-persister")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "5.0"))
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "55432"))
PG_DB = os.getenv("PG_DB", "healthstream")
PG_USER = os.getenv("PG_USER", "hs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "hs_password")

SQL_INSERT = (
    "INSERT INTO vitals_raw (kafka_partition, kafka_offset, patient_id, "
    "event_time, vital_type, value, unit) VALUES %s "
    "ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING"
)

ERREURS_PG = (psycopg2.OperationalError, psycopg2.InterfaceError)

_arret_demande = False


def gerer_signal(signum, frame):
    global _arret_demande
    _arret_demande = True


def log(texte: str, erreur: bool = False) -> None:
    print(f"{time.strftime('%H:%M:%S')} {texte}",
          file=sys.stderr if erreur else sys.stdout, flush=True)


def creer_consommateur() -> Consumer:
    """Acquittement manuel, attribution coopérative."""
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "client.id": f"persister-{os.getenv('HOSTNAME', 'hote')}",
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        # Doit rester supérieur au temps d'écriture d'un lot, retentatives comprises.
        "max.poll.interval.ms": 300000,
    })


def connecter_postgres(tentatives: int = 30, delai_s: float = 2.0):
    """Connexion avec retentatives : la base peut tomber en cours de route."""
    derniere = None
    for essai in range(1, tentatives + 1):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASSWORD,
                                    connect_timeout=5)
            conn.autocommit = False  # transactions explicites
            if essai > 1:
                log(f"PostgreSQL de nouveau joignable (tentative {essai})")
            return conn
        except psycopg2.OperationalError as exc:
            derniere = exc
            log(f"PostgreSQL injoignable (tentative {essai}/{tentatives}) : "
                f"{str(exc).strip().splitlines()[0]}", erreur=True)
            time.sleep(delai_s)
    raise derniere


def ligne_depuis_message(msg) -> tuple:
    """Message Kafka -> tuple d'insertion. Laisse remonter JSONDecodeError,
    KeyError, ValueError, TypeError sur un message malformé."""
    d = json.loads(msg.value())
    event_time = datetime.fromtimestamp(int(d["event_time"]) / 1000, tz=timezone.utc)
    return (
        msg.partition(),
        msg.offset(),
        str(d["patient_id"]),
        event_time,
        str(d["vital_type"]),
        float(d["value"]),
        str(d["unit"]),
    )


def inserer_lot(conn, lignes: list) -> int:
    """Écrit le lot en une instruction et valide. Retourne les lignes réellement
    insérées (les doublons absorbés par ON CONFLICT n'y comptent pas)."""
    if not lignes:
        return 0
    try:
        with conn.cursor() as cur:
            execute_values(cur, SQL_INSERT, lignes, page_size=len(lignes))
            n = cur.rowcount
        conn.commit()
        return n
    except ERREURS_PG:
        raise
    except Exception:
        conn.rollback()
        raise


def main() -> int:
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    conn = connecter_postgres()
    consommateur = creer_consommateur()
    etat = {"conn": conn}
    lot = []
    lus = inseres = ignores = rejets = 0
    a_acquitter = False  # messages lus (même rejetés) non encore acquittés

    def ecrire_et_acquitter() -> None:
        """Ordre critique : la base d'abord, l'acquittement ENSUITE."""
        nonlocal lot, inseres, ignores, a_acquitter
        n = inserer_lot(etat["conn"], lot)                 # 1. la base d'abord
        try:
            consommateur.commit(asynchronous=False)        # 2. l'acquittement ENSUITE
        except KafkaException as exc:
            # Lot déjà en base : un échec d'acquittement ne coûte qu'une relecture,
            # absorbée par la clé primaire. _NO_OFFSET = rien à acquitter.
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                log(f"acquittement refusé (relecture possible) : {exc}", erreur=True)
        inseres += n
        ignores += len(lot) - n
        if lot:
            log(f"LOT taille={len(lot)} inseres={n} doublons={len(lot) - n} "
                f"total_lus={lus}")
        lot = []
        a_acquitter = False

    def sur_retrait(c, partitions):
        # Avant de rendre des partitions, on écrit ce qui a été lu dessus :
        # le prochain propriétaire repartira de l'offset acquitté.
        liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
        log(f"RETRAIT partitions=[{liste}]")
        if a_acquitter:
            try:
                ecrire_et_acquitter()
            except Exception as exc:  # le lot sera relu par le prochain propriétaire
                log(f"écriture avant retrait impossible : {exc}", erreur=True)

    def sur_attribution(c, partitions):
        liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
        log(f"ATTRIBUTION partitions=[{liste}]")

    consommateur.subscribe([VITALS_TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)
    log(f"démarrage groupe={CONSUMER_GROUP} topic={VITALS_TOPIC} batch={BATCH_SIZE} "
        f"timeout={BATCH_TIMEOUT_S}s pg={PG_HOST}:{PG_PORT}/{PG_DB}")

    debut = time.monotonic()
    dernier_envoi = time.time()
    try:
        while not _arret_demande:
            msg = consommateur.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log(f"erreur Kafka : {msg.error()}", erreur=True)
                else:
                    lus += 1
                    a_acquitter = True
                    try:
                        lot.append(ligne_depuis_message(msg))
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
                        rejets += 1  # compté, ignoré, mais acquitté avec le lot
                        log(f"message rejeté p={msg.partition()} o={msg.offset()} : {exc!r}",
                            erreur=True)

            lot_pret = len(lot) >= BATCH_SIZE or (
                a_acquitter and time.time() - dernier_envoi >= BATCH_TIMEOUT_S)
            if lot_pret:
                try:
                    ecrire_et_acquitter()
                except ERREURS_PG as exc:
                    log(f"ERREUR PostgreSQL, lot de {len(lot)} conservé, offsets NON "
                        f"acquittés : {str(exc).strip().splitlines()[0]}", erreur=True)
                    etat["conn"] = connecter_postgres()  # lot CONSERVÉ
                    continue
                dernier_envoi = time.time()
            elif not a_acquitter:
                dernier_envoi = time.time()
    finally:
        # Arrêt propre : écrire le lot résiduel, acquitter, quitter le groupe.
        if a_acquitter:
            try:
                ecrire_et_acquitter()
            except Exception as exc:
                log(f"lot résiduel non écrit (sera relu au redémarrage) : {exc}", erreur=True)
        consommateur.close()
        try:
            etat["conn"].close()
        except Exception:
            pass
        duree = time.monotonic() - debut
        debit = lus / duree if duree > 0 else 0.0
        log(f"Arret. lus={lus} inseres={inseres} ignores_doublons={ignores} "
            f"rejets={rejets} duree={duree:.1f}s debit={debit:.2f} msg/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
