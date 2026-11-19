"""Premier DAG de la séance 3 : prise en main (tâches, dépendances, contexte d'un run)."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "icu-ops",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def afficher_contexte(**context):
    """Affiche ce qui distingue ce run de tous les autres."""
    print(f"logical_date        = {context['logical_date']}")
    print(f"data_interval_start = {context['data_interval_start']}")
    print(f"data_interval_end   = {context['data_interval_end']}")
    print(f"run_id              = {context['run_id']}")
    print(f"try_number          = {context['task_instance'].try_number}")


with DAG(
    dag_id="premier_dag_s3",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["s3", "prise-en-main"],
) as dag:
    debut = EmptyOperator(task_id="debut")

    horodater = BashOperator(
        task_id="horodater",
        bash_command='echo "run={{ run_id }} debut_intervalle={{ data_interval_start }}"; date -u',
    )

    contexte = PythonOperator(
        task_id="contexte",
        python_callable=afficher_contexte,
    )

    debut >> horodater >> contexte
