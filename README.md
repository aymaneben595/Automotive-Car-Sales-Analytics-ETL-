# 🚗 Automotive Car Sales Analytics & ETL

---

# Project Background

As a Data Analyst supporting a major, multi-brand **Automotive Sales Company**, my role was to synthesize underutilized transactional data to inform inventory, marketing, and sales leadership strategy. The company's business model relies on a dual approach: high-volume mid-range sales combined with high-margin luxury sales across diverse geographical regions. Key performance indicators (KPIs) tracked include **Total Revenue**, **Average Car Price (ASV)**, and **Customer Segmentation**.

Insights and recommendations are provided on the following key areas:

- **Category 1: Sales Trends Analysis:** Evaluation of historical sales patterns, focusing on Revenue, Order Volume, and Average Sales Value (ASV).
- **Category 2: Dealer & Manufacturer Performance:** An analysis of high-performing dealers and the volume-vs-value trade-off among manufacturers.
- **Category 3: Customer Demographics & Behavior:** An assessment of how demographics (Gender, Income) influence purchase behavior and deal size.
- **Category 4: Inventory & Product Preferences:** Analysis of color and transmission preferences to optimize stock.

The SQL queries used to inspect and clean the data for this analysis, **including the creation of all final analytical views (e.g., `vw_car_sales_export`, `summary_sales_region`)**, can be found here **[View ETL & Analytics Script (etl\_customers.sql)](./VSCode, SQL & Python/SQL/etl_customers.sql)**.

An interactive Tableau dashboard used to report and explore sales trends can be found here **[Download Car Sales Dark Mode Dashboard.pbix](./Car%20Sales%20Dark%20Mode%20Dashboard.pbix)**.

---

# Data Structure & Initial Checks

The project's final analytical structure, created after an **ETL process in PostgreSQL**, contains **23,910 clean transaction records**. Prior to analysis, a variety of checks were conducted for quality control and familiarization with the datasets. The data was consolidated into two primary analytical artifacts:

- **`car_sales`:** The final, clean, and structured transaction table (one row per sale).
- **`vw_sales_export`:** A structured view for visualization tools, including calculated columns like `year` and `price_segment`.

<p align="center">
  <img src="[Place Entity Relationship Diagram image here]" alt="Entity Relationship Diagram (ERD)">
</p>

---

# Executive Summary

### Overview of Findings

The automotive market shows strong overall financial health, achieving **$671.5 Million in total revenue**, but this masks a severe imbalance in customer segmentation. Sales leadership needs to immediately address the fact that **Male customers account for 79% of purchases**, revealing a massive, untapped opportunity in the Female segment. Furthermore, sales peak reliably in **Q4**, informing resource allocation, while the Average Sales Value (ASV) remains stable at **$28.09K**, confirming successful premium pricing.

<p align="center">
  <img src="12334.PNG" alt="Overall Sales Dashboard Snapshot">
</p>

---

# Insights Deep Dive

### Category 1: Sales Trends Analysis

* **Main insight 1.** The market demonstrated robust financial performance, achieving **$671.5 Million total revenue**. The simple story is that sales have maintained consistent year-over-year growth across the analysis period, confirming a healthy market presence.
  
* **Main insight 2.** Monthly revenue **peaked at $61M in October 2023**. This quantified value shows clear seasonality, demonstrating strong Q4 momentum that should be leveraged by sales and marketing teams.
  
* **Main insight 3.** The **Average Sales Value (ASV) remained highly stable at $28.09K**. This key business metric confirms that pricing strategies are effective, maintaining transaction value without heavy reliance on discounting.
  
* **Main insight 4.** The lowest performing month was **February 2023**, with revenue dropping **15% below the monthly average**. This indicates early-year sluggishness that could be targeted with specific incentives.

<p align="center">
  <img src="[Place Sales Over Time visualization here]" alt="Sales Over Time Chart">
</p>

---

### Category 2: Dealer & Manufacturer Performance

* **Main insight 1.** The top three dealers (e.g., Haddad, Progressive) drive between **$30M to $40M in revenue** each. This quantified value proves that success is highly centralized, and their operational models should be studied.
  
* **Main insight 2.** Premium manufacturers like **Mercedes-Benz maintain the highest Average Price ($48K)**. The simple story is that the business successfully executes a high-margin strategy within the luxury segment.
  
* **Main insight 3.** The dealer **U-Haul Co. excels at attracting high-net-worth individuals**, with a top customer income of **$1.9 Million Annual Income**. This demonstrates superior targeting for high-value sales.
  
* **Main insight 4.** Manufacturers with the lowest ASV (e.g., Chevrolet) are primarily responsible for the **highest transaction volume**, showing a necessary trade-off between volume and margin.

<p align="center">
  <img src="[Place Dealer Performance visualization here]" alt="Dealer Performance Chart">
</p>

---

### Category 3: Customer Demographics & Behavior

* **Main insight 1.** **Male customers account for 79% of total purchases**. This is the key business metric showing a critical segmentation failure, leaving the female market largely untapped.
  
* **Main insight 2.** Customers in the **Highest Income Quartile are almost exclusively purchasing vehicles in the 'High Value' price segment**. This strongly validates the income-based targeting model for premium sales.
  
* **Main insight 3.** The **Pacific and Atlantic regions** show the **highest total sales volume**. This confirms strong market penetration in concentrated areas, suggesting a need to shift focus to developing mid-tier regions.
  
* **Main insight 4.** The middle-income quartiles drive the bulk of the **'Mid-Range' vehicle sales**, confirming their status as the high-volume engine of the business model.

<p align="center">
  <img src="[Place Customer Demographics visualization here]" alt="Customer Demographics Chart">
</p>

---

### Category 4: Inventory & Product Preferences

* **Main insight 1.** **Pale White (47%) and Red (32%) account for almost 80% of all sales**. The simple story is that inventory should be drastically streamlined to prioritize these two popular colors.
  
* **Main insight 2.** Customer preference for **Automatic transmission is overwhelmingly dominant** across all segments. This is the key metric indicating that capital is unnecessarily tied up in low-demand manual transmission stock.
  
* **Main insight 3.** The high-volume models (e.g., Chevrolet Equinox, Ford Escape) consistently deliver the required unit count, serving as the reliable engine for overall order volume metrics.
  
* **Main insight 4.** The least popular color, **Brown, accounted for less than 1% of sales**, confirming inventory managers can safely deprioritize this option entirely.

<p align="center">
  <img src="[Place Inventory Preference visualization here]" alt="Inventory Preference Chart">
</p>

---

# Recommendations:

Based on the insights and findings above, we would recommend the **Sales & Marketing Leadership teams** to consider the following: 

* **Male customers account for 79% of total volume**, indicating a severe gender segmentation failure. **Recommendation: Launch an immediate, targeted marketing campaign focused specifically on the Female customer segment to capture the massive untapped market opportunity.**
  
* **Top dealers achieve significantly higher Average Sale Values (ASV)**. **Recommendation: Implement a 'Best Practices' training program based on the customer acquisition funnels of top performers (like U-Haul Co.) to lift the ASV across the entire dealer network.**
  
* **Sales peak reliably during the Q4 period, especially in October**. **Recommendation: Concentrate advertising and staff incentives to maximize impact during the Q4 sales momentum, which is proven to be the most profitable period.**
  
* **80% of sales are concentrated in just two colors (Pale White and Red)**. **Recommendation: Rationalize inventory by minimizing orders for low-demand colors and manual transmission vehicles to free up capital and reduce holding costs.**
  
* **High-income buyers consistently choose the 'High Value' segment**. **Recommendation: Refine digital marketing parameters to exclusively target demographics matching the top-buying segment for all premium and luxury model campaigns.**
  

---

# Assumptions and Caveats:

Throughout the analysis, multiple assumptions were made to manage challenges with the data. These assumptions and caveats are noted below:

* **Data Exclusion:** Any records with missing price or sale date were excluded from the analysis (less than 1% of total data), assuming they were incomplete and would skew financial reporting.
  
* **Currency Standardization:** All monetary columns (Price, Income) were assumed to be standardized in **USD** and required only minor cleaning (removing special characters) to be usable in calculations.
  
* **Transmission Classification:** It was assumed that variations in transmission text ('Automatic', 'Auto') referred to the same type, simplifying the classification and enabling clear comparison against 'Manual'.
