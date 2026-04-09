"""
Python callables for streamflow_main DAG.
"""

import os
import subprocess
import sys

from airflow.operators.python import get_current_context

SPARK_MASTER = "spark://spark-master:7077"
SPARK_HOME_DEFAULT = "/opt/spark"
JOBS_DIR = "/opt/spark-jobs"


def run_ingest_kafka() -> None:
    context = get_current_context()
    params = context["params"]
    env = {**os.environ, "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}
    for topic in ("user_events", "transaction_events"):
        cmd = [
            sys.executable,
            os.path.join(JOBS_DIR, "ingest_kafka_to_landing.py"),
            "--topic",
            topic,
            "--batch_duration_sec",
            str(params["batch_duration_sec"]),
            "--output_path",
            params["landing_path"],
        ]
        subprocess.run(cmd, check=True, env=env)


def run_spark_etl() -> None:
    """spark-submit etl_job.py (latest landing file per topic)."""
    context = get_current_context()
    params = context["params"]
    spark_home = os.environ.get("SPARK_HOME", SPARK_HOME_DEFAULT)
    submit = os.path.join(spark_home, "bin", "spark-submit")
    cmd: list[str] = [
        submit,
        "--master",
        SPARK_MASTER,
        "--deploy-mode",
        "client",
        os.path.join(JOBS_DIR, "etl_job.py"),
        "--master",
        SPARK_MASTER,
        "--input_path",
        params["landing_path"],
        "--output_path",
        params["gold_path"],
    ]
    env = {**os.environ, "PATH": os.pathsep.join([os.path.join(spark_home, "bin"), os.environ.get("PATH", "")])}
    subprocess.run(cmd, check=True, env=env)


def run_validate_gold() -> None:
    context = get_current_context()
    params = context["params"]
    cmd = [
        sys.executable,
        os.path.join(JOBS_DIR, "validate_data.py"),
        "--gold_path",
        params["gold_path"],
    ]
    subprocess.run(cmd, check=True)
