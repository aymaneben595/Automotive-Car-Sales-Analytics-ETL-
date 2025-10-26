#!/usr/bin/env python3
"""
elt_pipeline.py
Production-ready ETL/ELT pipeline for Smart Budget car sales.

This script is the AUTOMATION ENGINE for our analytics.
It runs automatically to pull the clean data, calculate key business metrics (KPIs),
and save the final reports for management dashboards.
"""

# We import the necessary professional tools for data work, logging, and database connection.
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd              # Tool for powerful data manipulation (like Excel, but automated)
from sqlalchemy import create_engine, text # Tool to connect to and communicate with the PostgreSQL database
from dotenv import load_dotenv     # Tool to securely load login credentials

# -------------------------
# Logging (The Script's Internal Reporter)
# -------------------------
# This section sets up a system to tell us when the script starts, finishes, or encounters a problem.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("elt_pipeline")

# -------------------------
# 1) Load environment (The Security & Setup Check)
# -------------------------
load_dotenv()  # Loads sensitive login details (username, password) from a secure .env file.

# We define where to find our database login details and settings.
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "smart_budget")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./outputs")
SCHEMA = os.getenv("DB_SCHEMA", "analytics") # Tells the script the clean data lives in the 'analytics' schema

# Security and Setup Validation
# If we are missing login credentials, the script stops immediately for security.
if not PG_USER or not PG_PASS:
    log.error("PG_USER and PG_PASS must be set in your .env file. See .env.example.")
    raise SystemExit(1)

# Makes sure the folder where we save our reports actually exists.
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# -------------------------
# 2) DB connection (Building the Bridge to the Data)
# -------------------------
def get_engine():
    # Builds the secure connection string using the hidden login details.
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    log.info("🔗 Connecting to PostgreSQL...")
    # Creates the 'engine'—the main tool Python uses to talk to the database.
    engine = create_engine(conn_str, pool_pre_ping=True, connect_args={"options": f"-csearch_path={SCHEMA}"})
    log.info("✅ DB engine created.")
    return engine

# -------------------------
# 3) helpers: load view (Fetching the Clean Reports)
# -------------------------
# This function is responsible for retrieving the clean data tables produced by the SQL script.
def load_view(engine, view_name, schema=SCHEMA):
    qualified = f"{schema}.{view_name}"
    log.info(f"📥 Loading view: {qualified}")
    try:
        # Tries to pull the clean data report directly into a Pandas DataFrame.
        df = pd.read_sql_table(view_name, con=engine, schema=schema)
        log.info(f"   -> Loaded {len(df):,} rows from {qualified}")
        return df
    except Exception as e:
        # If the quick fetch fails, it uses a safe 'SELECT *' as a backup plan.
        log.warning(f"read_sql_table failed for {qualified}: {e!s} — trying SELECT * FROM ...")
        q = text(f"SELECT * FROM {schema}.{view_name};")
        df = pd.read_sql(q, con=engine)
        log.info(f"   -> Loaded {len(df):,} rows from {qualified} (via SELECT)")
        return df

# -------------------------
# 4) export CSV helper (The File Packer)
# -------------------------
# This function takes any report (DataFrame) and saves it as a finished .csv file.
def export_csv(df: pd.DataFrame, name: str) -> str:
    # Creates a unique timestamp (e.g., 20251027_115900) to ensure version control.
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{ts}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    # Saves the file without the index column (index=False).
    df.to_csv(path, index=False)
    log.info(f"💾 Exported: {path}")
    return path

# -------------------------
# 5) KPI calculations (The Business Calculator)
# -------------------------
# This section computes the critical Key Performance Indicators (KPIs) needed for executive summaries.
def compute_kpis(sales_df: pd.DataFrame, top_customers_df: pd.DataFrame, monthly_df: pd.DataFrame) -> pd.DataFrame:
    
    # Ensures key columns (like 'price' and 'customer_name') are present, even if they have slight variations.
    df = sales_df.copy()
    if "price" not in df.columns and "total_revenue" in df.columns:
        df["price"] = df["total_revenue"]
    if "customer_name" not in df.columns and "customer" in df.columns:
        df["customer_name"] = df["customer"]

    # 1. Total Revenue: Sums up all sales prices.
    total_revenue = float(df["price"].sum()) if not df.empty else 0.0
    # 2. Total Orders: Counts the total number of transactions.
    total_orders = int(len(df))
    # 3. Average Order Value (AOV): Divides total revenue by total orders.
    avg_order_value = float(total_revenue / total_orders) if total_orders > 0 else 0.0
    
    # 4. Average Revenue Per Customer: Calculates the average amount spent per unique customer.
    avg_revenue_per_customer = 0.0
    if "customer_name" in df.columns and not df.empty:
        avg_revenue_per_customer = float(df.groupby("customer_name")["price"].sum().mean())

    # 5. Top Customer Identification
    top_customer_name = None
    top_customer_revenue = 0.0
    if not top_customers_df.empty:
        # Pulls the name and revenue of the highest-spending customer (always the first row of the summary_top_customers view).
        rcol = None
        for c in ["total_revenue", "total_spent", "total_revenue"]:
             if c in top_customers_df.columns:
                 rcol = c
                 break
        if rcol is None:
             numeric_cols = top_customers_df.select_dtypes(include="number").columns
             rcol = numeric_cols[0] if len(numeric_cols) > 0 else None

        top_customer_name = top_customers_df.iloc[0]["customer_name"] if "customer_name" in top_customers_df.columns else top_customers_df.iloc[0].iloc[0]
        top_customer_revenue = float(top_customers_df.iloc[0][rcol]) if rcol is not None else 0.0

    # 6. Monthly Growth: Calculates sales momentum by comparing the last two full months.
    monthly_growth_pct = 0.0
    try:
        # Sorts data to ensure we are comparing the correct 'latest' and 'previous' month.
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

    # Packages all the calculated KPIs into a single, clean report structure.
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
# 6) Run pipeline (The Orchestrator)
# -------------------------
def run_pipeline():
    # Step 1: Connect to the database.
    engine = get_engine()

    # Step 2: Define the reports we need to fetch (must match the views in the SQL pipeline).
    views = {
        "vw_sales_export": "vw_sales_export",                # The full, clean sales ledger
        "summary_revenue_country": "summary_revenue_country",  # Revenue breakdown by region
        "summary_top_customers": "summary_top_customers",      # Top 20 customers
        "summary_deal_size": "summary_deal_size",            # Low/Medium/High price segment summary
        "vw_monthly_revenue": "vw_monthly_revenue",            # Monthly sales trend data
        "vw_null_summary": "vw_null_summary"                  # Data quality report
    }

    dfs = {}
    for key, view in views.items():
        try:
            # Step 3: Load each required report from the database.
            dfs[key] = load_view(engine, view)
        except Exception as e:
            log.error("Failed to load view %s: %s", view, e)
            dfs[key] = pd.DataFrame()  # If a report fails, we continue with a blank table.

    # Step 4: Save all fetched reports as timestamped CSV files.
    for key, df in dfs.items():
        if df is None or df.empty:
            log.warning("View %s is empty. Exporting empty CSV.", key)
        export_csv(df, key)

    # Step 5: Calculate the final executive KPIs using the fetched data.
    kpi_df = compute_kpis(
        sales_df=dfs.get("vw_sales_export", pd.DataFrame()),
        top_customers_df=dfs.get("summary_top_customers", pd.DataFrame()),
        monthly_df=dfs.get("vw_monthly_revenue", pd.DataFrame())
    )
    # Step 6: Save the final KPI summary report.
    export_csv(kpi_df, "kpi_summary")

    log.info("✅ ETL run complete. All outputs in: %s", os.path.abspath(OUTPUT_DIR))


if __name__ == "__main__":
    # This ensures the pipeline runs when the script is executed.
    run_pipeline()
