-- ===============================================================
-- CAR SALES ANALYTICS PIPELINE (PostgreSQL)
-- Author: Aïmane Benkhadda
-- Date: 2025-10-22
--
-- Description:
-- This SQL file transforms raw car sales data into a clean and 
-- analytics-ready structure. It follows a standard ELT flow:
--   1. Load raw CSV → staging table
--   2. Clean and normalize → core fact table
--   3. Derive metrics and categories → analytics views
--   4. Expose clean views → Python / Power BI integration
-- ===============================================================


/* ===============================================================
   OPTIONAL: Database setup
   ---------------------------------------------------------------
   You can uncomment this part to create your database manually.
   Otherwise, connect to your existing PostgreSQL database.
   =============================================================== */

-- CREATE DATABASE db;
-- \c db;


-- ===============================================================
-- 1. SCHEMA CREATION
-- ---------------------------------------------------------------
-- Use a dedicated schema to isolate analytics tables and views.
-- ===============================================================
CREATE SCHEMA IF NOT EXISTS analytics;



-- ===============================================================
-- 2. STAGING TABLE
-- ---------------------------------------------------------------
-- This temporary table holds the raw CSV data exactly as imported.
-- It mirrors the dataset columns before any cleaning or validation.
-- ===============================================================
DROP TABLE IF EXISTS analytics.staging_car_sales;
CREATE TABLE analytics.staging_car_sales (
    raw_car_id         TEXT,
    raw_date           TEXT,
    raw_customer       TEXT,
    raw_gender         TEXT,
    raw_annual_inc     TEXT,
    raw_dealer_name    TEXT,
    raw_company        TEXT,
    raw_model          TEXT,
    raw_engine         TEXT,
    raw_transmission   TEXT,
    raw_color          TEXT,
    raw_price          TEXT,
    raw_dealer_no      TEXT,
    raw_body_style     TEXT,
    raw_phone          TEXT,
    raw_dealer_region  TEXT
);

-- Example COPY (uncomment and adjust path to load your CSV)
-- COPY analytics.staging_car_sales
-- FROM 'C:\path\to\your\car_sales.csv'
-- DELIMITER ',' CSV HEADER;



-- ===============================================================
-- 3. CLEAN TABLE (TARGET)
-- ---------------------------------------------------------------
-- This is the main structured table that stores clean, typed data.
-- Data will be inserted here after transformations and validation.
-- ===============================================================
DROP TABLE IF EXISTS analytics.car_sales;
CREATE TABLE analytics.car_sales (
    car_id          VARCHAR PRIMARY KEY,
    sale_date       DATE,
    customer_name   VARCHAR,
    gender          VARCHAR(16),
    annual_income   NUMERIC,
    dealer_name     VARCHAR,
    company         VARCHAR,
    model           VARCHAR,
    engine          VARCHAR,
    transmission    VARCHAR,
    color           VARCHAR,
    price           NUMERIC,
    dealer_no       VARCHAR,
    body_style      VARCHAR,
    phone           VARCHAR,
    dealer_region   VARCHAR
);



-- ===============================================================
-- 4. LOAD & CLEAN DATA FROM STAGING
-- ---------------------------------------------------------------
-- This step:
--   • Cleans encodings and removes unwanted characters
--   • Converts data types (dates, numbers)
--   • Normalizes gender and capitalization
--   • Handles nulls and empty values safely
-- ===============================================================
TRUNCATE analytics.car_sales;

INSERT INTO analytics.car_sales (
    car_id, sale_date, customer_name, gender, annual_income,
    dealer_name, company, model, engine, transmission, color,
    price, dealer_no, body_style, phone, dealer_region
)
SELECT
    NULLIF(TRIM(raw_car_id), '') AS car_id,

    -- Parse various date formats (MM/DD/YYYY or YYYY-MM-DD)
    CASE
        WHEN raw_date IS NULL OR TRIM(raw_date) = '' THEN NULL
        ELSE COALESCE(
            TO_DATE(raw_date, 'MM/DD/YYYY'),
            TO_DATE(raw_date, 'YYYY-MM-DD')
        )
    END AS sale_date,

    INITCAP(NULLIF(TRIM(raw_customer), '')) AS customer_name,

    -- Normalize gender to standard labels
    CASE
        WHEN LOWER(TRIM(raw_gender)) IN ('m','male','man') THEN 'Male'
        WHEN LOWER(TRIM(raw_gender)) IN ('f','female','woman') THEN 'Female'
        WHEN raw_gender IS NULL OR TRIM(raw_gender) = '' THEN NULL
        ELSE INITCAP(TRIM(raw_gender))
    END AS gender,

    -- Remove non-numeric characters from annual income
    CASE
        WHEN raw_annual_inc IS NULL OR TRIM(raw_annual_inc) = '' THEN NULL
        ELSE (REGEXP_REPLACE(raw_annual_inc, '[^0-9.-]', '', 'g'))::NUMERIC
    END AS annual_income,

    INITCAP(NULLIF(TRIM(raw_dealer_name), '')) AS dealer_name,
    INITCAP(NULLIF(TRIM(raw_company), '')) AS company,
    INITCAP(NULLIF(TRIM(raw_model), '')) AS model,

    -- Fix common encoding issues (e.g., "Ã‚Â") in engine field
    INITCAP(
        REGEXP_REPLACE(
            REGEXP_REPLACE(NULLIF(TRIM(raw_engine), ''), 'Ã‚Â|Â', ' ', 'g'),
        '\s+', ' ', 'g')
    ) AS engine,

    INITCAP(NULLIF(TRIM(raw_transmission), '')) AS transmission,
    INITCAP(NULLIF(TRIM(raw_color), '')) AS color,

    -- Clean price field
    CASE
        WHEN raw_price IS NULL OR TRIM(raw_price) = '' THEN NULL
        ELSE (REGEXP_REPLACE(raw_price, '[^0-9.-]', '', 'g'))::NUMERIC
    END AS price,

    NULLIF(TRIM(raw_dealer_no), '') AS dealer_no,
    INITCAP(NULLIF(TRIM(raw_body_style), '')) AS body_style,
    NULLIF(TRIM(raw_phone), '') AS phone,
    INITCAP(NULLIF(TRIM(raw_dealer_region), '')) AS dealer_region
FROM analytics.staging_car_sales
WHERE raw_car_id IS NOT NULL AND TRIM(raw_car_id) <> '';



-- ===============================================================
-- 5. CLEAN VIEW: car_sales_clean
-- ---------------------------------------------------------------
-- Adds derived columns (year, month, labels) and segmentation:
--   • price_segment = static price band (Low, Medium, High)
--   • price_category = dynamic percentile ranking
-- ===============================================================
CREATE OR REPLACE VIEW analytics.car_sales_clean AS
SELECT
    car_id,
    sale_date,
    customer_name,
    gender,
    COALESCE(annual_income, 0) AS annual_income,
    dealer_name,
    company,
    model,
    engine,
    transmission,
    color,
    COALESCE(price, 0) AS price,
    dealer_no,
    body_style,
    phone,
    dealer_region,

    EXTRACT(YEAR FROM sale_date)::INT AS year,
    EXTRACT(MONTH FROM sale_date)::INT AS month,
    TO_CHAR(sale_date, 'Mon YYYY') AS month_label,

    -- Static segmentation
    CASE
        WHEN price >= 40000 THEN 'High'
        WHEN price BETWEEN 20000 AND 39999.99 THEN 'Medium'
        WHEN price > 0 AND price < 20000 THEN 'Low'
        ELSE 'Unknown'
    END AS price_segment,

    -- Dynamic percentile-based segmentation
    CASE
        WHEN price IS NULL OR price = 0 THEN 'Unknown'
        WHEN price >= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) FROM analytics.car_sales) THEN 'Top 25%'
        WHEN price >= (SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) FROM analytics.car_sales) THEN '50–75%'
        ELSE 'Bottom 50%'
    END AS price_category
FROM analytics.car_sales
WHERE car_id IS NOT NULL;



-- ===============================================================
-- 6. ANALYTICAL VIEWS
-- ---------------------------------------------------------------
-- These ready-to-use views are consumed by Python & Power BI.
-- They summarize revenue, customers, deal size, and quality.
-- ===============================================================

-- A. Regional revenue summary
CREATE OR REPLACE VIEW analytics.summary_revenue_country AS
SELECT
    dealer_region AS region,
    COUNT(*) AS total_cars,
    SUM(price) AS total_revenue,
    ROUND(AVG(price)::NUMERIC, 2) AS avg_price
FROM analytics.car_sales_clean
GROUP BY dealer_region
ORDER BY total_revenue DESC;

-- B. Top 20 customers
CREATE OR REPLACE VIEW analytics.summary_top_customers AS
SELECT
    customer_name,
    gender,
    COALESCE(annual_income,0) AS annual_income,
    COUNT(*) AS purchases,
    SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY customer_name, gender, annual_income
ORDER BY total_revenue DESC
LIMIT 20;

-- C. Deal size distribution
CREATE OR REPLACE VIEW analytics.summary_deal_size AS
SELECT
    price_segment AS deal_size_category,
    COUNT(*) AS total_cars,
    SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY price_segment
ORDER BY total_revenue DESC;

-- D. Monthly revenue trend
CREATE OR REPLACE VIEW analytics.vw_monthly_revenue AS
SELECT
    year,
    month,
    month_label,
    SUM(price) AS total_revenue,
    COUNT(*) AS total_cars
FROM analytics.car_sales_clean
GROUP BY year, month, month_label
ORDER BY year, month;

-- E. Data quality summary
CREATE OR REPLACE VIEW analytics.vw_null_summary AS
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE customer_name IS NULL OR customer_name = '') AS missing_customer_name,
    COUNT(*) FILTER (WHERE company IS NULL OR company = '') AS missing_company,
    COUNT(*) FILTER (WHERE dealer_region IS NULL OR dealer_region = '') AS missing_region,
    COUNT(*) FILTER (WHERE price IS NULL OR price = 0) AS missing_price,
    COUNT(*) FILTER (WHERE gender IS NULL OR gender = '') AS missing_gender
FROM analytics.car_sales;

-- F. Revenue by company and region
CREATE OR REPLACE VIEW analytics.vw_sales_company_region AS
SELECT
    company,
    dealer_region,
    SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY company, dealer_region
ORDER BY total_revenue DESC;



-- ===============================================================
-- 7. EXPORT VIEW
-- ---------------------------------------------------------------
-- Final clean dataset used by Python ETL & Power BI dashboards.
-- ===============================================================
CREATE OR REPLACE VIEW analytics.vw_sales_export AS
SELECT *
FROM analytics.car_sales_clean;



-- ===============================================================
-- 8. FINAL NOTES
-- ---------------------------------------------------------------
-- Once this script runs:
--   - The dataset is clean, normalized, and analytics-ready
--   - Python ETL and Power BI can query analytics.* views
--   - You can adapt thresholds or segments for new datasets
-- ===============================================================
