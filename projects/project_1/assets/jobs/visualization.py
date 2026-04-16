"""
Streamlit dashboard for StreamFlow: visualizes aggregates from analyze_data.py (same folder).

Run locally (from repo or jobs folder):
  pip install streamlit pandas pyarrow
  streamlit run visualization.py

Optional: set GOLD_ZONE_PATH to your gold root (folder containing dim_* / fact_*).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

_JOBS_DIR = Path(__file__).resolve().parent
if str(_JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(_JOBS_DIR))

import analyze_data

# Gold Parquet folders live next to landing: .../project_1/assets/data/gold
# (not project_1/data/gold — jobs/ is under assets/, so use parents[1] == assets)
_DEFAULT_GOLD = Path(__file__).resolve().parents[1] / "data" / "gold"


@st.cache_data(ttl=60)
def _run_analysis(gold_path: str):
    return analyze_data.analyze(gold_path)


def main() -> None:
    st.set_page_config(page_title="Streamlit - Gold analytics", layout="wide")
    st.title("Streamlit — Gold analytics")

    default = os.getenv("GOLD_ZONE_PATH", str(_DEFAULT_GOLD))
    # gold_path = st.sidebar.text_input("Gold root", value=default)

    if not Path(default).is_dir():
        st.error(f"Not a directory: `{default}`")
        return

    try:
        result = _run_analysis(default)
    except Exception as e:
        st.error(f"Could not load or analyze gold data: {e}")
        return

    s = result["summary"]
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Users", f"{s['dim_user_count']:,}")
    c2.metric("Products", f"{s['dim_product_count']:,}")
    c3.metric("User events", f"{s['fact_event_count']:,}")
    c4.metric("Transactions", f"{s['fact_transaction_line_count']:,}")
    c5.metric("Line revenue", f"${s['total_line_revenue']:,.2f}")

    def _product_label(p):
        if not p:
            return "—", None
        # Prefer a human-friendly product name; fall back to the id/key if absent.
        pname = p.get("product_name")
        pid = p.get("product_id") or "?"
        units = p.get("units_sold", 0)
        rev = p.get("revenue", 0.0)
        label = str(pname).strip() if pname is not None and str(pname).strip() else str(pid)
        return label, f"{int(units):,} units • ${float(rev):,.2f}"

    most_label, most_delta = _product_label(s.get("most_sold_product"))
    least_label, least_delta = _product_label(s.get("least_sold_product"))
    c6.metric("Most sold product", most_label, delta=most_delta)
    c7.metric("Least sold product", least_label, delta=least_delta)

    left, right = st.columns(2)
    with left:
        st.subheader("Events by type")
        st.bar_chart(result["events_by_type"].set_index("event_type")["event_count"])
    with right:
        st.subheader("Events over time (by day)")
        ed = result["events_by_date"].copy()
        if not ed.empty:
            ed = ed.set_index("day")
        st.line_chart(ed["event_count"])

    left2, right2 = st.columns(2)
    with left2:
        st.subheader("Revenue by product category")
        rc = result["revenue_by_category"].copy()
        if not rc.empty:
            rc = rc.set_index("product_category")
        st.bar_chart(rc["revenue"])
    with right2:
        st.subheader("Revenue over time (by day)")
        rd = result["revenue_by_day"].copy()
        if not rd.empty:
            rd = rd.set_index("day")
        st.line_chart(rd["revenue"])

    with st.expander("Raw aggregate tables (from analyze_data)"):
        st.dataframe(result["events_by_type"], use_container_width=True)
        st.dataframe(result["events_by_date"], use_container_width=True)
        st.dataframe(result["revenue_by_category"], use_container_width=True)
        st.dataframe(result["revenue_by_day"], use_container_width=True)


if __name__ == "__main__":
    main()
