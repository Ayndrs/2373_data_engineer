"""
Analytics over StreamFlow gold Parquet (star-schema tables).

Used by the Streamlit dashboard in projects/project_1/assets/jobs/visualization.py.
Reads each table folder with pandas/pyarrow (same layout as etl_job output).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import pandas as pd

TABLE_NAMES = (
    "dim_dates",
    "dim_users",
    "dim_products",
    "fact_user_events",
    "fact_transaction_lines",
)


def read_parquet_table(gold_root: str | Path, name: str) -> pd.DataFrame:
    path = Path(gold_root) / name
    if not path.is_dir():
        raise FileNotFoundError(f"Missing gold table directory: {path}")
    return pd.read_parquet(path)


def load_gold(gold_root: str | Path) -> dict[str, pd.DataFrame]:
    """Load all gold tables. Raises if any folder is missing or unreadable."""
    root = Path(gold_root)
    return {name: read_parquet_table(root, name) for name in TABLE_NAMES}


def analyze(gold_root: str | Path) -> dict[str, Any]:
    """
    Run aggregations suitable for dashboards / Streamlit.

    Returns a dict with:
      - summary: scalars (counts, totals)
      - events_by_type, events_by_date, revenue_by_category, revenue_by_day: small DataFrames
    """
    tables = load_gold(gold_root)
    events = tables["fact_user_events"]
    tx = tables["fact_transaction_lines"]
    users = tables["dim_users"]
    products = tables["dim_products"]

    event_day = pd.to_datetime(events["event_date"], errors="coerce").dt.date
    # Only count "completed" transactions for revenue/sales metrics.
    # (Other statuses like failed/pending/cancelled/chargeback are excluded.)
    if "status" in tx.columns:
        tx_completed = tx[tx["status"].astype(str).str.lower() == "completed"].copy()
    else:
        tx_completed = tx.copy()

    tx_day = pd.to_datetime(tx_completed["tx_date"], errors="coerce").dt.date

    events_by_type = (
        events.groupby("event_type", dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values("event_count", ascending=False)
    )

    events_by_date = (
        events.assign(day=event_day)
        .groupby("day", dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values("day")
    )

    revenue_by_category = (
        tx_completed.groupby("product_category", dropna=False)["line_total"]
        .sum()
        .reset_index(name="revenue")
        .sort_values("revenue", ascending=False)
    )

    revenue_by_day = (
        tx_completed.assign(day=tx_day)
        .groupby("day", dropna=False)["line_total"]
        .sum()
        .reset_index(name="revenue")
        .sort_values("day")
    )

    # Product sales (most/least sold). "Sold" is interpreted as total quantity across tx lines.
    sales = (
        tx_completed.dropna(subset=["product_id"])
        .groupby(["product_id", "product_name", "product_category"], dropna=False)
        .agg(
            units_sold=("product_quantity", "sum"),
            revenue=("line_total", "sum"),
        )
        .reset_index()
    )
    if not sales.empty:
        sales["units_sold"] = pd.to_numeric(sales["units_sold"], errors="coerce").fillna(0).astype(int)
        sales["revenue"] = pd.to_numeric(sales["revenue"], errors="coerce").fillna(0.0).astype(float)
        sales_sorted = sales.sort_values(["units_sold", "revenue"], ascending=[False, False])
        most_sold = sales_sorted.iloc[0].to_dict()
        least_sold = sales_sorted.iloc[-1].to_dict()
    else:
        most_sold = None
        least_sold = None

    summary = {
        "dim_user_count": len(users),
        "dim_product_count": len(products),
        "fact_event_count": len(events),
        "fact_transaction_line_count": len(tx_completed),
        "total_line_revenue": float(tx_completed["line_total"].sum()) if len(tx_completed) else 0.0,
        "most_sold_product": most_sold,
        "least_sold_product": least_sold,
    }

    return {
        "summary": summary,
        "events_by_type": events_by_type,
        "events_by_date": events_by_date,
        "revenue_by_category": revenue_by_category,
        "revenue_by_day": revenue_by_day,
    }


def _default_gold_path() -> str:
    return os.getenv(
        "GOLD_ZONE_PATH",
        str(Path(__file__).resolve().parent.parent / "data" / "gold"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Print gold analysis summary to stdout.")
    parser.add_argument(
        "--gold_path",
        default=_default_gold_path(),
        help="Root folder containing dim_*/fact_* Parquet directories.",
    )
    args = parser.parse_args()
    result = analyze(args.gold_path)
    print("Summary:", result["summary"])


if __name__ == "__main__":
    main()
