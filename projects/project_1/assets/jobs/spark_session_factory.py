"""
SparkSession Factory Module

Provides factory functions for creating SparkSession instances.
"""
from pyspark.sql import SparkSession
from typing import Optional
import os


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    config_overrides: Optional[dict] = None
) -> SparkSession:
    """
    Create and return a configured SparkSession.
    
    Args:
        app_name: Name for the Spark application
        master: Spark master URL ("local[*]" or "spark://spark-master:7077")
        config_overrides: Optional dict of Spark configurations
        
    Returns:
        Configured SparkSession instance
    """
    master_from_env = os.getenv("SPARK_MASTER")
    effective_master = master_from_env if master_from_env else master

    builder = SparkSession.builder.appName(app_name).master(effective_master)

    default_configs = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"),
    }

    for k, v in default_configs.items():
        builder = builder.config(str(k), str(v))

    if config_overrides:
        for k, v in config_overrides.items():
            builder = builder.config(str(k), str(v))

    return builder.getOrCreate()