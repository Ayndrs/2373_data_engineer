"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes curated datasets to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from spark_session_factory import create_spark_session
import glob
import os
import argparse
from typing import Dict

def _file_ts(path: str) -> int:
    return int(os.path.basename(path).split("_")[2].split(".")[0])


def get_latest_file(landing_path: str, topic: str) -> str:
    files = glob.glob(os.path.join(landing_path, f"{topic}*.json"))

    if not files:
        raise FileNotFoundError(f"No files found for topic: {topic}")

    latest_file = max(files, key=_file_ts)
    return latest_file


def read_landing_json_array(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Landing zone files are written as a single JSON array: [ {...}, {...}, ... ].
    Spark can read these reliably with multiLine=True.
    """
    return spark.read.option("multiLine", "true").json(file_path)


def standardize_user_events(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("event_id").cast("string"),
            col("user_id").cast("string"),
            col("session_id").cast("string"),
            col("event_type").cast("string"),
            F.to_timestamp("timestamp").alias("event_ts"),
            F.to_date(F.to_timestamp("timestamp")).alias("event_date"),
            col("page").cast("string"),
            col("device").cast("string"),
            col("browser").cast("string"),
            col("ip_address").cast("string"),
            col("country").cast("string"),
            col("city").cast("string"),
            col("search_query").cast("string"),
            col("element_id").cast("string"),
            col("product_id").cast("string"),
            col("quantity").cast("int").alias("event_quantity"),
        )
        .withColumn(
            "event_category",
            F.when(col("event_type").isin("add_to_cart", "remove_from_cart"), F.lit("cart"))
            .otherwise(F.lit("engagement")),
        )
    )


def flatten_transaction_events(df: DataFrame) -> DataFrame:
    tx = (
        df.withColumn("tx_ts", F.to_timestamp("timestamp"))
        .withColumn("tx_date", F.to_date(F.to_timestamp("timestamp")))
        .withColumn("product", F.explode_outer(col("products")))
    )

    return tx.select(
        col("transaction_id").cast("string"),
        col("original_transaction_id").cast("string"),
        col("user_id").cast("string"),
        col("transaction_type").cast("string"),
        col("status").cast("string"),
        col("payment_method").cast("string"),
        col("currency").cast("string"),
        col("tx_ts"),
        col("tx_date"),
        col("subtotal").cast("double"),
        col("tax").cast("double"),
        col("total").cast("double"),
        col("billing_address.street").cast("string").alias("billing_street"),
        col("billing_address.city").cast("string").alias("billing_city"),
        col("billing_address.state").cast("string").alias("billing_state"),
        col("billing_address.zip_code").cast("string").alias("billing_zip_code"),
        col("billing_address.country").cast("string").alias("billing_country"),
        col("shipping_address.street").cast("string").alias("shipping_street"),
        col("shipping_address.city").cast("string").alias("shipping_city"),
        col("shipping_address.state").cast("string").alias("shipping_state"),
        col("shipping_address.zip_code").cast("string").alias("shipping_zip_code"),
        col("shipping_address.country").cast("string").alias("shipping_country"),
        col("product.product_id").cast("string").alias("product_id"),
        col("product.product_name").cast("string").alias("product_name"),
        col("product.category").cast("string").alias("product_category"),
        col("product.quantity").cast("int").alias("product_quantity"),
        col("product.unit_price").cast("double").alias("unit_price"),
        F.round(col("product.quantity") * col("product.unit_price"), 2).alias("line_total"),
    )


def build_gold_tables(user_events: DataFrame, tx_lines: DataFrame) -> Dict[str, DataFrame]:
    """
    Builds "gold" tables in a star-schema-friendly format:
    - conformed dimensions: users, products, dates
    - facts: user_events, transaction_lines
    """

    user_events_cached = user_events.cache()
    tx_lines_cached = tx_lines.cache()

    # ---------- Conformed date dimension ----------
    # Use an integer date key (YYYYMMDD) to make joins fast and tool-friendly.
    user_dates = user_events_cached.select(col("event_date").alias("date")).where(col("date").isNotNull()).distinct()
    tx_dates = tx_lines_cached.select(col("tx_date").alias("date")).where(col("date").isNotNull()).distinct()
    dim_dates = (
        user_dates.unionByName(tx_dates)
        .distinct()
        .withColumn("date_key", F.date_format(col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year(col("date")).cast("int"))
        .withColumn("month", F.month(col("date")).cast("int"))
        .withColumn("day", F.dayofmonth(col("date")).cast("int"))
        .withColumn("day_of_week", F.date_format(col("date"), "E"))
        .withColumn("week_of_year", F.weekofyear(col("date")).cast("int"))
        .select("date_key", "date", "year", "month", "day", "day_of_week", "week_of_year")
    )

    # ---------- User dimension ----------
    # Keep a single row per user with "current" attributes observed from events.
    w_last_user = Window.partitionBy("user_id").orderBy(col("event_ts").desc_nulls_last())
    dim_users = (
        user_events_cached
        .select(
            "user_id",
            "event_ts",
            "device",
            "browser",
            "city",
            "country",
        )
        .withColumn("rn", F.row_number().over(w_last_user))
        .where(col("rn") == 1)
        .drop("rn", "event_ts")
        .withColumnRenamed("device", "current_device")
        .withColumnRenamed("browser", "current_browser")
        .withColumnRenamed("city", "current_city")
        .withColumnRenamed("country", "current_country")
    )

    user_bounds = (
        user_events_cached
        .groupBy("user_id")
        .agg(
            F.min("event_ts").alias("first_seen_ts"),
            F.max("event_ts").alias("last_seen_ts"),
        )
    )
    dim_users = dim_users.join(user_bounds, "user_id", "left")

    # ---------- Product dimension ----------
    # Derive products from transaction lines (conformed across facts via product_id).
    w_last_product = Window.partitionBy("product_id").orderBy(col("tx_ts").desc_nulls_last())
    dim_products = (
        tx_lines_cached
        .select("product_id", "product_name", "product_category", "tx_ts")
        .where(col("product_id").isNotNull())
        .withColumn("rn", F.row_number().over(w_last_product))
        .where(col("rn") == 1)
        .drop("rn", "tx_ts")
    )

    # ---------- Facts ----------
    fact_user_events = (
        user_events_cached
        .drop("ip_address")
        .withColumn("event_date_key", F.date_format(col("event_date"), "yyyyMMdd").cast("int"))
        .select(
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "event_category",
            "event_ts",
            "event_date",
            "event_date_key",
            "page",
            "device",
            "browser",
            "country",
            "city",
            "search_query",
            "element_id",
            "product_id",
            "event_quantity",
        )
    )

    fact_transaction_lines = (
        tx_lines_cached
        .withColumn("tx_date_key", F.date_format(col("tx_date"), "yyyyMMdd").cast("int"))
        .select(
            "transaction_id",
            "original_transaction_id",
            "user_id",
            "transaction_type",
            "status",
            "payment_method",
            "currency",
            "tx_ts",
            "tx_date",
            "tx_date_key",
            "subtotal",
            "tax",
            "total",
            "billing_street",
            "billing_city",
            "billing_state",
            "billing_zip_code",
            "billing_country",
            "shipping_street",
            "shipping_city",
            "shipping_state",
            "shipping_zip_code",
            "shipping_country",
            "product_id",
            "product_name",
            "product_category",
            "product_quantity",
            "unit_price",
            "line_total",
        )
    )

    return {
        "dim_dates": dim_dates,
        "dim_users": dim_users,
        "dim_products": dim_products,
        "fact_user_events": fact_user_events,
        "fact_transaction_lines": fact_transaction_lines,
    }


def write_gold_tables(tables: Dict[str, DataFrame], output_path: str) -> None:
    os.makedirs(output_path, exist_ok=True)
    for name, df in tables.items():
        out = os.path.join(output_path, name)
        df.repartition(8).write.mode("overwrite").parquet(out)

def run_etl(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Main ETL pipeline: read -> transform -> write.

    Uses the latest landing file per topic (by filename timestamp).
    """
    user_file = get_latest_file(input_path, "user_events")
    tx_file = get_latest_file(input_path, "transaction_events")
    raw_user = read_landing_json_array(spark, user_file)
    raw_tx = read_landing_json_array(spark, tx_file)

    user_events = standardize_user_events(raw_user)
    tx_lines = flatten_transaction_events(raw_tx)

    tables = build_gold_tables(user_events=user_events, tx_lines=tx_lines)
    write_gold_tables(tables, output_path=output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StreamFlow ETL Job (Module B) - Landing JSON -> Gold Parquet")
    parser.add_argument("--app_name", type=str, default=os.getenv("SPARK_APP_NAME", "ETL_Job"))
    parser.add_argument("--master", type=str, default=os.getenv("SPARK_MASTER", "local[*]"))
    parser.add_argument("--input_path", type=str, default=os.getenv("LANDING_ZONE_PATH", "./data/landing"))
    parser.add_argument("--output_path", type=str, default=os.getenv("GOLD_ZONE_PATH", "./data/gold"))
    parser.add_argument(
        "--shuffle_partitions",
        type=int,
        default=int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200")),
        help="spark.sql.shuffle.partitions",
    )
    args = parser.parse_args()

    spark = create_spark_session(
        app_name=args.app_name,
        master=args.master,
        config_overrides={"spark.sql.shuffle.partitions": str(args.shuffle_partitions)},
    )
    try:
        run_etl(spark, args.input_path, args.output_path)
    finally:
        spark.stop()