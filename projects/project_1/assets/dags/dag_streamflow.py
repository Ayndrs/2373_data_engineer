"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from streamflow_python_tasks import (
    run_ingest_kafka,
    run_spark_etl,
    run_validate_gold,
)


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Log SLA misses (Airflow also records them in the UI)."""
    logging.getLogger(__name__).warning(
        "SLA miss: dag_id=%s task_ids=%s",
        dag.dag_id,
        [t.task_id for t in task_list],
    )


default_args = {
    'owner': 'Student',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(minutes=4),
    catchup=False,
    sla_miss_callback=_sla_miss_callback,
    params={
        'batch_duration_sec': 10,
        'landing_path': '/opt/spark-data/landing',
        'gold_path': '/opt/spark-data/gold',
    },
) as dag:

    ingest_kafka = PythonOperator(
        task_id='ingest_kafka',
        python_callable=run_ingest_kafka,
        sla=timedelta(minutes=10),
    )

    spark_etl = PythonOperator(
        task_id='spark_etl',
        python_callable=run_spark_etl,
        sla=timedelta(minutes=10),
    )

    validate_gold = PythonOperator(
        task_id='validate_gold_outputs',
        python_callable=run_validate_gold,
        sla=timedelta(minutes=10),
    )

    ingest_kafka >> spark_etl >> validate_gold
