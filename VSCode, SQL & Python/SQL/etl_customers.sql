/*
-- car_sales_pipeline.sql
-- Smart Budget — Car Sales analytics pipeline (PostgreSQL)
-- Author: Aïmane Benkhadda
-- Date: 2025-10-22
--
-- Instructions: This file cleans, standardizes, and prepares raw car sales data for reporting.
-- The comments are written for a non-technical stakeholder to explain the business purpose of each step.
--
*/

/* ============================
   OPTIONAL: DATABASE SETUP
   (Creates the workspace for our data)
   ============================ */

-- CREATE DATABASE database; -- Uncomment this if you need to create the main database
-- \c smart_budget   -- Connects to the database for use (psql command)


-- We create a separate area (schema) named 'analytics' to keep our clean tables
-- isolated from any raw or source data. This is our dedicated "Analytics Factory Floor."
CREATE SCHEMA IF NOT EXISTS analytics;

/* -----------------------------
   1) STAGING TABLE (The Data Receiving Dock)
   ----------------------------- */
-- This temporary table is the holding zone for the raw data, just as it was imported from the CSV.
-- It's messy, but we need it as a safe staging area before we clean it.
DROP TABLE IF EXISTS analytics.staging_car_sales;
CREATE TABLE analytics.staging_car_sales (
	raw_car_id       TEXT,
	raw_date         TEXT,
	raw_customer     TEXT,
	raw_gender       TEXT,
	raw_annual_inc   TEXT,
	raw_dealer_name  TEXT,
	raw_company      TEXT,
	raw_model        TEXT,
	raw_engine       TEXT,
	raw_transmission TEXT,
	raw_color        TEXT,
	raw_price        TEXT,
	raw_dealer_no    TEXT,
	raw_body_style   TEXT,
	raw_phone        TEXT,
	raw_dealer_region TEXT
);

-- Example COPY (commented). This command loads the raw data from the CSV file into the staging table.
-- COPY analytics.staging_car_sales
-- FROM 'C:\Users\ayman\OneDrive\Desktop\First Project\VSCode, SQL & Python\Python'
-- DELIMITER ',' CSV HEADER;

/* -----------------------------
   2) CLEAN TARGET TABLE (The Final Data Warehouse)
   ----------------------------- */
-- This table defines the structure and data types for our FINAL, trusted analytical data.
-- Fields like 'price' must be set to NUMERIC here to ensure accurate math in reports.
DROP TABLE IF EXISTS analytics.car_sales;
CREATE TABLE analytics.car_sales (
	car_id          VARCHAR PRIMARY KEY, -- Ensures every car sale is counted only once
	sale_date       DATE,                -- Must be a standard Date format
	customer_name   VARCHAR,
	gender          VARCHAR(16),
	annual_income   NUMERIC,             -- Must be a clean number (no '$' or text)
	dealer_name     VARCHAR,
	company         VARCHAR,
	model           VARCHAR,
	engine          VARCHAR,
	transmission    VARCHAR,
	color           VARCHAR,
	price           NUMERIC,             -- Must be a clean number for financial reporting
	dealer_no       VARCHAR,
	body_style      VARCHAR,
	phone           VARCHAR,
	dealer_region   VARCHAR
);

/* -----------------------------
   3) LOAD CLEAN DATA FROM STAGING (The Quality Control Checkpoint)
   ----------------------------- */
-- This step moves data from the raw staging table into the clean final table,
-- performing all necessary transformations along the way.
TRUNCATE analytics.car_sales;

INSERT INTO analytics.car_sales (
	car_id, sale_date, customer_name, gender, annual_income,
	dealer_name, company, model, engine, transmission, color,
	price, dealer_no, body_style, phone, dealer_region
)
SELECT
	NULLIF(TRIM(raw_car_id), '') AS car_id, -- Removing empty spaces/nulls from the ID

	-- STANDARDIZING DATES: Converts multiple date formats (like M/D/YYYY or YYYY-MM-DD)
	-- into a single, reliable DATE format for accurate time series analysis.
	CASE
	  WHEN raw_date IS NULL OR TRIM(raw_date) = '' THEN NULL
	  ELSE
		COALESCE(
		  to_date(NULLIF(TRIM(raw_date), ''), 'MM/DD/YYYY') 
		, (CASE
			 WHEN raw_date ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(raw_date,'YYYY-MM-DD')
			 ELSE NULL
		   END))
	END AS sale_date,

	initcap(NULLIF(TRIM(raw_customer), '')) AS customer_name,

	-- NORMALIZING GENDER: Ensures 'm', 'male', and 'Man' are all recorded as 'Male'.
	-- This makes customer segmentation accurate and prevents miscounts.
	(CASE
		WHEN lower(trim(raw_gender)) IN ('m','male','man') THEN 'Male'
		WHEN lower(trim(raw_gender)) IN ('f','female','woman') THEN 'Female'
		WHEN raw_gender IS NULL OR trim(raw_gender) = '' THEN NULL
		ELSE initcap(trim(raw_gender))
	  END) AS gender,

	-- CLEANING INCOME: Removes currency symbols, commas, and text from the income field
	-- so we can correctly calculate customer value segments.
	(CASE
		WHEN raw_annual_inc IS NULL OR trim(raw_annual_inc) = '' THEN NULL
		ELSE (regexp_replace(raw_annual_inc, '[^0-9.-]', '', 'g'))::NUMERIC
	  END) AS annual_income,

	initcap(NULLIF(TRIM(raw_dealer_name), '')) AS dealer_name,
	initcap(NULLIF(TRIM(raw_company), '')) AS company,
	initcap(NULLIF(TRIM(raw_model), '')) AS model,

	-- FIXING ENCODING: Cleans up garbled characters (like "Ã‚Â") in product descriptions
	-- to make product reporting clean and readable.
	initcap(
	  regexp_replace(
		regexp_replace(NULLIF(TRIM(raw_engine), ''), 'Ã‚Â|Â', ' ', 'g'),
	  '\s+', ' ', 'g')
	) AS engine,

	initcap(NULLIF(TRIM(raw_transmission), '')) AS transmission,
	initcap(NULLIF(TRIM(raw_color), '')) AS color,

	-- CLEANING PRICE: Same as income—removes symbols to allow accurate SUMS and AVERAGES.
	(CASE
		WHEN raw_price IS NULL OR trim(raw_price) = '' THEN NULL
		ELSE (regexp_replace(raw_price, '[^0-9.-]', '', 'g'))::NUMERIC
	  END) AS price,

	NULLIF(TRIM(raw_dealer_no), '') AS dealer_no,
	initcap(NULLIF(TRIM(raw_body_style), '')) AS body_style,
	NULLIF(TRIM(raw_phone), '') AS phone,
	initcap(NULLIF(TRIM(raw_dealer_region), '')) AS dealer_region

FROM analytics.staging_car_sales
-- FINAL FILTER: We only keep records that have a valid car ID, ensuring we only analyze complete transactions.
WHERE raw_car_id IS NOT NULL AND TRIM(raw_car_id) <> '';

/* -----------------------------
   4) CLEAN VIEW: car_sales_clean (Adding Business Value Metrics)
   ----------------------------- */
-- This view takes the clean table and calculates new fields that are useful for analysis,
-- such as defining high-value sales or extracting the month/year.
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

	-- TIME ATTRIBUTES: Extracts year and month to enable easy filtering and trend plotting.
	EXTRACT(YEAR FROM sale_date)::INT AS year,
	EXTRACT(MONTH FROM sale_date)::INT AS month,
	TO_CHAR(sale_date, 'Mon YYYY') AS month_label,

	-- STATIC PRICE SEGMENTATION: Creates simple categories (Low/Medium/High) based on fixed dollar thresholds
	-- defined by the sales leadership team.
	CASE
	  WHEN price >= 40000 THEN 'High'
	  WHEN price BETWEEN 20000 AND 39999.99 THEN 'Medium'
	  WHEN price > 0 AND price < 20000 THEN 'Low'
	  ELSE 'Unknown'
	END AS price_segment,

	-- DYNAMIC PRICE CATEGORIZATION: Uses statistical percentiles to determine if a sale is in the
	-- Top 25% of all transactions, providing a relative benchmark regardless of market inflation.
	CASE
	  WHEN price IS NULL OR price = 0 THEN 'Unknown'
	  WHEN price >= (SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY price) FROM analytics.car_sales) THEN 'Top 25%'
	  WHEN price >= (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY price) FROM analytics.car_sales) THEN '50-75%'
	  ELSE 'Bottom 50%'
	END AS price_category

FROM analytics.car_sales
-- We filter out any transactions that somehow lost their ID, just for safety.
WHERE car_id IS NOT NULL;


/* -----------------------------
   5) EXPORT VIEW (The Dashboard Feed)
   ----------------------------- */
-- This is the single, unified data source that Power BI and Python will use for all reports.
CREATE OR REPLACE VIEW analytics.vw_sales_export AS
SELECT
	car_id, sale_date, customer_name, gender, annual_income, dealer_name, company, model, engine,
	transmission, color, price, dealer_no, body_style, phone, dealer_region, year, month,
	month_label, price_segment, price_category
FROM analytics.car_sales_clean;


/* -----------------------------
   6) SUMMARY VIEWS (Pre-Calculated Reports for Speed)
   ----------------------------- */
-- These are pre-calculated tables that answer specific business questions instantly,
-- speeding up dashboards and Python scripts that rely on them.

-- REVENUE BY REGION: Gives us Total Revenue, Car Count, and Average Price by region.
CREATE OR REPLACE VIEW analytics.summary_revenue_country AS
SELECT
	dealer_region AS region,
	COUNT(*) AS total_cars,
	SUM(price) AS total_revenue,
	ROUND(AVG(price)::numeric,2) AS avg_price
FROM analytics.car_sales_clean
GROUP BY dealer_region
ORDER BY total_revenue DESC;

-- TOP 20 CUSTOMERS: Identifies the highest-spending customers for targeted marketing/loyalty programs.
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

-- DEAL SIZE SUMMARY: Breaks down total sales by the 'Low/Medium/High' category we defined earlier.
CREATE OR REPLACE VIEW analytics.summary_deal_size AS
SELECT
	price_segment AS deal_size_category,
	COUNT(*) AS total_cars,
	SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY price_segment
ORDER BY total_revenue DESC;

-- MONTHLY REVENUE TREND: Aggregates total revenue and car count by month for time-series charts.
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

-- DATA QUALITY CHECK: Audits the data, showing us how many transactions are missing crucial information (e.g., gender, price).
CREATE OR REPLACE VIEW analytics.vw_null_summary AS
SELECT
	COUNT(*) AS total_rows,
	COUNT(*) FILTER (WHERE customer_name IS NULL OR customer_name = '') AS missing_customer_name,
	COUNT(*) FILTER (WHERE company IS NULL OR company = '') AS missing_company,
	COUNT(*) FILTER (WHERE dealer_region IS NULL OR dealer_region = '') AS missing_region,
	COUNT(*) FILTER (WHERE price IS NULL OR price = 0) AS missing_price,
	COUNT(*) FILTER (WHERE gender IS NULL OR gender = '') AS missing_gender
FROM analytics.car_sales;

-- REVENUE HEATMAP: Calculates revenue contribution by company and dealer region.
CREATE OR REPLACE VIEW analytics.vw_sales_company_region AS
SELECT
	company,
	dealer_region,
	SUM(price) AS total_revenue
FROM analytics.car_sales_clean
GROUP BY company, dealer_region
ORDER BY total_revenue DESC;


/* -----------------------------
   7) FINAL NOTES
   ----------------------------- */
-- The pipeline is complete! All reporting tools should now query the analytics.* views.
