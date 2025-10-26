--
-- car_sales_pipeline.sql
-- Smart Budget — Car Sales analytics pipeline (PostgreSQL)
-- Author: ChatGPT → adapted for Aïmane
-- Date: 2025-10-22
--
-- Instructions:
-- 1) Create or connect to the target database (smart_budget).
-- 2) Update COPY path below or use your ETL to load staging_raw.
-- 3) Run the entire script as a single SQL file (psql or pgAdmin).
--

/* ============================
   OPTIONAL: create database (uncomment if you can)
   ============================ */

CREATE DATABASE database;
-- \c smart_budget   -- if using psql and you created it


-- Use a schema for isolation (optional)
CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------
-- 1) STAGING TABLE (raw CSV import)
-- -----------------------------
DROP TABLE IF EXISTS analytics.staging_car_sales;
CREATE TABLE analytics.staging_car_sales (
    raw_car_id       TEXT,
    raw_date         TEXT,
    raw_customer     TEXT,
    raw_gender       TEXT,
    raw_annual_inc   TEXT,
    raw_dealer_name  TEXT,
    raw_company      TEXT,
    raw_model        TEXT,
    raw_engine       TEXT,
    raw_transmission TEXT,
    raw_color        TEXT,
    raw_price        TEXT,
    raw_dealer_no    TEXT,
    raw_body_style   TEXT,
    raw_phone        TEXT,
    raw_dealer_region TEXT
);

-- Example COPY (commented). Replace path with your CSV file path and run:
COPY analytics.staging_car_sales
FROM 'C:\Users\ayman\OneDrive\Desktop\First Project\VSCode, SQL & Python\Python'
DELIMITER ',' CSV HEADER;

-- -----------------------------
-- 2) CLEAN TARGET TABLE
-- -----------------------------
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

-- -----------------------------
-- 3) LOAD CLEAN DATA FROM STAGING
--    - Robust parsing & cleaning
--    - Handles broken encodings (e.g. "DoubleÃ‚Â Overhead")
--    - Non-numeric characters removed for price and income
-- -----------------------------
TRUNCATE analytics.car_sales;

INSERT INTO analytics.car_sales (
    car_id, sale_date, customer_name, gender, annual_income,
    dealer_name, company, model, engine, transmission, color,
    price, dealer_no, body_style, phone, dealer_region
)
SELECT
    NULLIF(TRIM(raw_car_id), '') AS car_id,

    -- parse date in multiple common formats (try MM/DD/YYYY first)
    -- if parse fails, set to NULL.
    CASE
      WHEN raw_date IS NULL OR TRIM(raw_date) = '' THEN NULL
      ELSE
        -- Try common formats: M/D/YYYY or YYYY-MM-DD
        COALESCE(
          -- try M/D/YYYY or MM/DD/YYYY
          to_date(NULLIF(TRIM(raw_date), ''), 'MM/DD/YYYY') 
            -- to_date will error if invalid format; use safe approach with to_timestamp + cast
            -- We'll try with explicit try-cast using to_timestamp via NULLIF(..., '')
        , (CASE
             WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(raw_date,'YYYY-MM-DD')
             ELSE NULL
           END))
    END AS sale_date,

    initcap(NULLIF(TRIM(raw_customer), '')) AS customer_name,

    -- Normalize gender values (male/female/other)
    (CASE
       WHEN lower(trim(raw_gender)) IN ('m','male','man') THEN 'Male'
       WHEN lower(trim(raw_gender)) IN ('f','female','woman') THEN 'Female'
       WHEN raw_gender IS NULL OR trim(raw_gender) = '' THEN NULL
       ELSE initcap(trim(raw_gender))
     END) AS gender,

    -- Clean annual income: remove non-digits and parse numeric
    (CASE
       WHEN raw_annual_inc IS NULL OR trim(raw_annual_inc) = '' THEN NULL
       ELSE (regexp_replace(raw_annual_inc, '[^0-9.-]', '', 'g'))::NUMERIC
     END) AS annual_income,

    initcap(NULLIF(TRIM(raw_dealer_name), '')) AS dealer_name,
    initcap(NULLIF(TRIM(raw_company), '')) AS company,
    initcap(NULLIF(TRIM(raw_model), '')) AS model,

    -- Fix known encoding artefacts (common sequences like Ã‚Â) and normalize spaces
    initcap(
      regexp_replace(
        regexp_replace(NULLIF(TRIM(raw_engine), ''), 'Ã‚Â|Â', ' ', 'g'),
      '\s+', ' ', 'g')
    ) AS engine,

    initcap(NULLIF(TRIM(raw_transmission), '')) AS transmission,
    initcap(NULLIF(TRIM(raw_color), '')) AS color,

    -- Clean price: remove non-digit characters and parse as numeric
    (CASE
       WHEN raw_price IS NULL OR trim(raw_price) = '' THEN NULL
       ELSE (regexp_replace(raw_price, '[^0-9.-]', '', 'g'))::NUMERIC
     END) AS price,

    NULLIF(TRIM(raw_dealer_no), '') AS dealer_no,
    initcap(NULLIF(TRIM(raw_body_style), '')) AS body_style,
    NULLIF(TRIM(raw_phone), '') AS phone,
    initcap(NULLIF(TRIM(raw_dealer_region), '')) AS dealer_region

FROM analytics.staging_car_sales
WHERE raw_car_id IS NOT NULL AND TRIM(raw_car_id) <> '';

-- -----------------------------
-- 4) CLEAN VIEW: car_sales_clean
--    - Add derived columns: year, month, month_label
--    - Two segmentation logics: price_segment (sample scale) and price_category (percentile)
-- -----------------------------
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

    -- Static thresholds (adjust to your business scale)
    CASE
      WHEN price >= 40000 THEN 'High'
      WHEN price BETWEEN 20000 AND 39999.99 THEN 'Medium'
      WHEN price > 0 AND price < 20000 THEN 'Low'
      ELSE 'Unknown'
    END AS price_segment,

    -- Dynamic percentile-based category (approx)
    CASE
      WHEN price IS NULL OR price = 0 THEN 'Unknown'
      WHEN price >= (SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY price) FROM analytics.car_sales) THEN 'Top 25%'
      WHEN price >= (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY price) FROM analytics.car_sales) THEN '50-75%'
      ELSE 'Bottom 50%'
    END AS price_category

FROM analytics.car_sales
-- keep only rows with a region and price for many analyses
WHERE car_id IS NOT NULL;


-- -----------------------------
-- 5) EXPORT VIEW (the one Python & PowerBI should use)
-- -----------------------------
CREATE OR REPLACE VIEW analytics.vw_sales_export AS
SELECT
  car_id,
  sale_date,
  customer_name,
  gender,
  annual_income,
  dealer_name,
  company,
  model,
  engine,
  transmission,
  color,
  price,
  dealer_no,
  body_style,
  phone,
  dealer_region,
  year,
  month,
  month_label,
  price_segment,
  price_category
FROM analytics.car_sales_clean;


-- -----------------------------
-- 6) SUMMARY VIEWS (names used by Python)
-- -----------------------------

-- revenue by country / region (python expects summary_revenue_country)
CREATE OR REPLACE VIEW analytics.summary_revenue_country AS
SELECT
  dealer_region AS region,
  COUNT(*) AS total_cars,
  SUM(price) AS total_revenue,
  ROUND(AVG(price)::numeric,2) AS avg_price
FROM analytics.car_sales_clean
GROUP BY dealer_region
ORDER BY total_revenue DESC;

-- top customers
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

-- deal size summary
CREATE OR REPLACE VIEW analytics.summary_deal_size AS
SELECT
  price_segment AS deal_size_category,
  COUNT(*) AS total_cars,
  SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY price_segment
ORDER BY total_revenue DESC;

-- monthly revenue (python expects vw_monthly_revenue)
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

-- data quality / null summary
CREATE OR REPLACE VIEW analytics.vw_null_summary AS
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE customer_name IS NULL OR customer_name = '') AS missing_customer_name,
  COUNT(*) FILTER (WHERE company IS NULL OR company = '') AS missing_company,
  COUNT(*) FILTER (WHERE dealer_region IS NULL OR dealer_region = '') AS missing_region,
  COUNT(*) FILTER (WHERE price IS NULL OR price = 0) AS missing_price,
  COUNT(*) FILTER (WHERE gender IS NULL OR gender = '') AS missing_gender
FROM analytics.car_sales;

-- revenue heatmap (company x region)
CREATE OR REPLACE VIEW analytics.vw_sales_company_region AS
SELECT
  company,
  dealer_region,
  SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY company, dealer_region
ORDER BY total_revenue DESC;


-- -----------------------------
-- 7) FINAL NOTES
-- -----------------------------
-- After running this script:
-- - Ensure you loaded raw CSV into analytics.staging_car_sales via COPY or other method.
-- - The Python pipeline will query the analytics.* views (vw_sales_export, summary_revenue_country, summary_top_customers, summary_deal_size, vw_monthly_revenue, vw_null_summary)
-- - If you want a different schema name, replace analytics.* with your schema.