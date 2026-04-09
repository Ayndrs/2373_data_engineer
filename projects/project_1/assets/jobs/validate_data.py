"""
Validation of gold Parquet outputs from etl_job.

Checks: each expected table folder exists, Parquet is readable, row count > 0.
Exits 1 on failure so Airflow can fail the task.
"""
import argparse
import os
import sys

from spark_session_factory import create_spark_session

# Must match etl_job.build_gold_tables() output names.
TABLES = (
    "dim_dates",
    "dim_users",
    "dim_products",
    "fact_user_events",
    "fact_transaction_lines",
)

# One or two columns that must exist if the ETL schema is correct (sanity only).
MIN_COLUMNS = {
    "dim_dates": ("date_key",),
    "dim_users": ("user_id",),
    "dim_products": ("product_id",),
    "fact_user_events": ("event_id", "user_id"),
    "fact_transaction_lines": ("transaction_id", "line_total"),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate gold Parquet under a root path.")
    parser.add_argument(
        "--gold_path",
        default=os.getenv("GOLD_ZONE_PATH", "/opt/spark-data/gold"),
        help="Directory containing table subfolders (default: GOLD_ZONE_PATH or /opt/spark-data/gold).",
    )
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER", "local[*]"),
        help="Spark master for reading Parquet (default: local[*]).",
    )
    args = parser.parse_args()

    root = os.path.abspath(args.gold_path)
    if not os.path.isdir(root):
        print(f"VALIDATION ERROR: not a directory: {root}", file=sys.stderr)
        return 1

    spark = create_spark_session(
        app_name=os.getenv("SPARK_APP_NAME", "ValidateGold"),
        master=args.master,
        config_overrides={"spark.sql.shuffle.partitions": "8"},
    )
    failed = False
    try:
        print(f"Validating gold: {root}")
        for name in TABLES:
            path = os.path.join(root, name)
            if not os.path.isdir(path):
                print(f"VALIDATION ERROR: missing {path}", file=sys.stderr)
                failed = True
                continue
            try:
                df = spark.read.parquet(path)
            except Exception as e:
                print(f"VALIDATION ERROR: cannot read {name}: {e}", file=sys.stderr)
                failed = True
                continue

            cols = set(df.columns)
            missing = [c for c in MIN_COLUMNS.get(name, ()) if c not in cols]
            if missing:
                print(
                    f"VALIDATION ERROR: {name} missing columns {missing}",
                    file=sys.stderr,
                )
                failed = True
                continue

            n = df.count()
            if n == 0:
                print(f"VALIDATION ERROR: {name} is empty", file=sys.stderr)
                failed = True
                continue

            print(f"  OK {name}: {n} rows")
    finally:
        spark.stop()

    if failed:
        print("Validation failed.", file=sys.stderr)
        return 1
    print("Validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
