#!/usr/bin/env python3
"""
elt_pipeline.py
Production-ready ETL/ELT pipeline for Smart Budget car sales.
- Connects to PostgreSQL using SQLAlchemy
- Loads views produced by SQL pipeline (schema: analytics)
- Calculates KPIs
- Exports CSVs to ./outputs with timestamped filenames

Run:
  pip install -r requirements.txt
  python elt_pipeline.py

requirements (suggested):
  python-dotenv
  sqlalchemy
  psycopg2-binary
  pandas
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("elt_pipeline")

# -------------------------
# 1) Load environment
# -------------------------
load_dotenv()  # loads .env from current dir

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "smart_budget")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./outputs")
SCHEMA = os.getenv("DB_SCHEMA", "analytics")

# Basic validation (do not run with missing credentials)
if not PG_USER or not PG_PASS:
    log.error("PG_USER and PG_PASS must be set in your .env file. See .env.example.")
    raise SystemExit(1)

# Ensure output directory exists
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# -------------------------
# 2) DB connection
# -------------------------
def get_engine():
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    log.info("🔗 Connecting to PostgreSQL...")
    engine = create_engine(conn_str, pool_pre_ping=True, connect_args={"options": f"-csearch_path={SCHEMA}"})
    log.info("✅ DB engine created.")
    return engine

# -------------------------
# 3) helpers: load view
# -------------------------
def load_view(engine, view_name, schema=SCHEMA):
    qualified = f"{schema}.{view_name}"
    log.info(f"📥 Loading view: {qualified}")
    try:
        df = pd.read_sql_table(view_name, con=engine, schema=schema)
        log.info(f"   → Loaded {len(df):,} rows from {qualified}")
        return df
    except Exception as e:
        # fallback to direct SELECT if read_sql_table fails
        log.warning(f"read_sql_table failed for {qualified}: {e!s} — trying SELECT * FROM ...")
        q = text(f"SELECT * FROM {schema}.{view_name};")
        df = pd.read_sql(q, con=engine)
        log.info(f"   → Loaded {len(df):,} rows from {qualified} (via SELECT)")
        return df

# -------------------------
# 4) export CSV helper
# -------------------------
def export_csv(df: pd.DataFrame, name: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{ts}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    log.info(f"💾 Exported: {path}")
    return path

# -------------------------
# 5) KPI calculations
# -------------------------
def compute_kpis(sales_df: pd.DataFrame, top_customers_df: pd.DataFrame, monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Expected columns in sales_df: price, customer_name
    Expected columns in top_customers_df: customer_name, total_revenue (or total_revenue)
    Expected columns in monthly_df: total_revenue (aggregate)
    """

    # Defensive renames (handle common variants)
    df = sales_df.copy()
    if "price" not in df.columns and "total_revenue" in df.columns:
        df["price"] = df["total_revenue"]
    if "customer_name" not in df.columns and "customer" in df.columns:
        df["customer_name"] = df["customer"]

    total_revenue = float(df["price"].sum()) if not df.empty else 0.0
    total_orders = int(len(df))
    avg_order_value = float(total_revenue / total_orders) if total_orders > 0 else 0.0
    avg_revenue_per_customer = 0.0
    if "customer_name" in df.columns and not df.empty:
        avg_revenue_per_customer = float(df.groupby("customer_name")["price"].sum().mean())

    # top customer
    top_customer_name = None
    top_customer_revenue = 0.0
    if not top_customers_df.empty:
        # try to fetch a revenue field if present
        rcol = None
        for c in ["total_revenue", "total_spent", "total_revenue"]:
            if c in top_customers_df.columns:
                rcol = c
                break
        # fallback use last numeric column
        if rcol is None:
            numeric_cols = top_customers_df.select_dtypes(include="number").columns
            rcol = numeric_cols[0] if len(numeric_cols) > 0 else None

        top_customer_name = top_customers_df.iloc[0]["customer_name"] if "customer_name" in top_customers_df.columns else top_customers_df.iloc[0].iloc[0]
        top_customer_revenue = float(top_customers_df.iloc[0][rcol]) if rcol is not None else 0.0

    # monthly growth (compare latest two months)
    monthly_growth_pct = 0.0
    try:
        mdf = monthly_df.sort_values(["year", "month"]) if ("year" in monthly_df.columns and "month" in monthly_df.columns) else monthly_df
        if not mdf.empty:
            last = float(mdf.iloc[-1]["total_revenue"])
            prev = float(mdf.iloc[-2]["total_revenue"]) if len(mdf) > 1 else last
            if prev != 0:
                monthly_growth_pct = (last - prev) / prev * 100
            else:
                monthly_growth_pct = 0.0
    except Exception as e:
        log.warning("Error computing monthly growth: %s", e)

    kpis = {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "avg_order_value": round(avg_order_value, 2),
        "avg_revenue_per_customer": round(avg_revenue_per_customer, 2),
        "top_customer_name": top_customer_name,
        "top_customer_revenue": round(top_customer_revenue, 2),
        "monthly_growth_pct": round(monthly_growth_pct, 2)
    }

    return pd.DataFrame([kpis])

# -------------------------
# 6) Run pipeline
# -------------------------
def run_pipeline():
    engine = get_engine()

    # Views to fetch (must match SQL pipeline)
    views = {
        "vw_sales_export": "vw_sales_export",
        "summary_revenue_country": "summary_revenue_country",
        "summary_top_customers": "summary_top_customers",
        "summary_deal_size": "summary_deal_size",
        "vw_monthly_revenue": "vw_monthly_revenue",
        "vw_null_summary": "vw_null_summary"
    }

    dfs = {}
    for key, view in views.items():
        try:
            dfs[key] = load_view(engine, view)
        except Exception as e:
            log.error("Failed to load view %s: %s", view, e)
            dfs[key] = pd.DataFrame()  # continue with empty DF

    # Export raw views to CSV
    for key, df in dfs.items():
        if df is None or df.empty:
            log.warning("View %s is empty. Exporting empty CSV.", key)
        export_csv(df, key)

    # Compute KPIs (sales: vw_sales_export, top customers: summary_top_customers, monthly: vw_monthly_revenue)
    kpi_df = compute_kpis(
        sales_df=dfs.get("vw_sales_export", pd.DataFrame()),
        top_customers_df=dfs.get("summary_top_customers", pd.DataFrame()),
        monthly_df=dfs.get("vw_monthly_revenue", pd.DataFrame())
    )
    export_csv(kpi_df, "kpi_summary")

    log.info("✅ ETL run complete. All outputs in: %s", os.path.abspath(OUTPUT_DIR))


if __name__ == "__main__":
    run_pipeline()