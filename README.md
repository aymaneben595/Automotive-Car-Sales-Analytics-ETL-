# 🚗 Automotive Car Sales Analytics & ETL

---

## 📘 Project Background

As a Data Analyst supporting a major multi-brand **Automotive Sales Company**, my role was to synthesize underutilized transactional data to inform **inventory**, **marketing**, and **sales leadership strategy**.

The company’s business model relies on a dual approach:
* **High-volume mid-range sales**, and
* **High-margin luxury sales**
across diverse geographical regions.

Key performance indicators (KPIs) tracked include:
* **Total Revenue**
* **Average Car Price (ASV)**
* **Customer Segmentation**

Insights and recommendations are provided across the following areas:

1.  **Sales Trends Analysis:** Revenue, Order Volume, and Average Sales Value (ASV) patterns.
2.  **Dealer & Manufacturer Performance:** Identifying top-performing dealers and volume-vs-value dynamics.
3.  **Customer Demographics & Behavior:** How gender and income influence purchases.
4.  **Inventory & Product Preferences:** Popular colors, transmissions, and model trends.

🔗 **SQL ETL Script:**
**[View ETL & Analytics Script (etl\_customers.sql)](https://github.com/aymaneben595/Automotive-Car-Sales-Analytics-ETL-/blob/b95acd9c102d59b0a3d4ecc820e2744f814c451d/Car%20Sales%20Project/VSCode%2C%20SQL%20%26%20Python/SQL/etl_customers.sql)**

📊 **Dashboard:**
**[⬇️ Download Car Sales Dark Mode Dashboard.pbix](https://github.com/aymaneben595/Automotive-Car-Sales-Analytics-ETL-/raw/ff5795cbf8b7f39ac0d61cc357a358097059f840/Power%20Bi/Car%20Sales%20Dark%20Mode%20Dashboard.pbix)**

---

## 🧩 Data Structure & Initial Checks

After a complete **ETL process in PostgreSQL**, the dataset contained **23,910 clean transaction records**.

Two main analytical artifacts were produced:
* **`car_sales`** → Final structured transaction table (one row per sale)
* **`vw_sales_export`** → Analytical view for BI tools (with calculated fields like `year` and `price_segment`)

<p align="center">
  <img src="Images/car_sales_erd.png" alt="Entity Relationship Diagram (ERD)">
</p>

---

## 📈 Executive Summary

### Overview of Findings

The automotive market achieved **$671.5 Million in total revenue**, demonstrating strong performance — yet revealing key imbalances:

* **79% of purchases were made by male customers**, highlighting a massive untapped female segment.
* **Q4 consistently peaks in sales** (October 2023 is the highest month at $61M), indicating clear seasonality.
* **Average Sales Value (ASV):** **$28.09K** — stable and healthy, validating consistent pricing strategies.

<p align="center">
  <img src="Images/dash.PNG" alt="Overall Sales Dashboard Snapshot">
</p>

---

## 🔍 Insights Deep Dive

### **Category 1: Sales Trends Analysis**

1.  **Total revenue reached $671.5M**, confirming a robust financial position based on **23.91K cars sold**.
2.  **October 2023 peaked at $61M**, showing clear seasonality and strong Q4 momentum.
3.  **ASV remained steady at $28.09K**, validating consistent pricing strategies across the analyzed period.
4.  Sales bottomed out in **April 2023 at $12M**, revealing a significant mid-year dip.

<p align="center">
  <img src="Images/over.PNG" alt="Sales Over Time Chart">
</p>

---

### **Category 2: Dealer & Manufacturer Performance**

1.  **Haddad Used Car Sales** is the top dealer, followed closely by **Progressive Shoppers C.** and **U-Haul Co.**.
2.  The top three dealers each generate revenue **between $30M and $40M**—success is highly concentrated.
3.  **U-Haul Co.** excels at high-value transactions (filtered only by dealer), with **Total Sales of $35.9M** and attracting a customer with a **$1.91 Million Annual Income**.
4.  **Chevrolet** leads in **Total Cars Sold (10.9K)** but maintains a lower **Avg. Price ($26K)**, contrasting with **Mercedes-Benz's $48K Avg. Price**—illustrating the volume–margin trade-off.

<p align="center">
  <img src="[Place Dealer Performance visualization here]" alt="Dealer Performance Chart">
</p>

---

### **Category 3: Customer Demographics & Behavior**

1.  **Male customers account for 79%** of total sales—a critical imbalance showing massive underpenetration in the female market (21% share).
2.  **U-Haul Co.'s top customers** include individuals with high annual incomes, such as **$1.91M** and **$2.01M**.
3.  Sales volume is highest in the **Pacific and Atlantic regions**, indicating successful market concentration in coastal areas.
4.  The dealer **Race Car Help (filtered by BMW)** shows a smaller total sales volume of **$1.1M from 44 cars**, providing specific insights into that brand/dealer combination.

<p align="center">
  <img src="[Place Customer Demographics visualization here]" alt="Customer Demographics Chart">
</p>

---

### **Category 4: Inventory & Product Preferences**

1.  Across the entire dataset, **Red (21%) and Pale White (47%)** are dominant colors, and inventory should prioritize these.
2.  **Automatic transmissions are overwhelmingly preferred** by customers across all dealers, as evidenced by both the high-volume dealer **U-Haul Co.** (approx. 650 Auto vs. 60 Manual) and **Race Car Help** (approx. 30 Auto vs. 15 Manual).
3.  The top models sold by **U-Haul Co.** by revenue are the **LS400, Silhouette, and Jetta**.
4.  The dealer **Race Car Help (BMW)** sees **Black** as the most common color sold (61%), followed by **Pale White (20%)** and **Red (18%)**—showing brand-specific color trends.

<p align="center">
  <img src="[Place Inventory Preference visualization here]" alt="Inventory Preference Chart">
</p>

---

## 💡 Recommendations

To support **Sales & Marketing Leadership** decisions:

1.  **Target Female Customers:**
    Launch an immediate campaign to engage the female segment; the **79% Male / 21% Female split** represents a critical untapped market opportunity.

2.  **Replicate Top Dealer Practices:**
    Study and train other dealers based on the high-value acquisition strategy of **U-Haul Co.**, which successfully attracts high-net-worth clients.

3.  **Capitalize on Q4 Momentum:**
    Concentrate advertising and staff incentives to maximize impact during the **Q4 sales momentum**, which peaked at **$61M in October**.

4.  **Streamline Inventory:**
    Reduce low-demand manual stock and **double down on popular colors (like Pale White and Red)** and Automatic transmissions to free up capital.

5.  **Address Seasonal Lulls:**
    Investigate the cause of the **April 2023 sales dip ($12M)** and introduce specific, targeted incentives to smooth out early-to-mid-year performance.

---

## ⚙️ Assumptions & Caveats

* **Drill Through Context:** It is assumed that the **U-Haul Co.** analysis is filtered **only by dealer name**, while the **Race Car Help** analysis is filtered by **Dealer Name and Company (BMW)**.
* **Data Exclusion:** Less than 1% of records with missing `price` or `sale_date` were excluded.
* **Currency:** All prices and income standardized to **USD** after cleaning.
* **Transmission Classification:** Variants like “Auto” and “Automatic” were unified for clarity.

---

<p align="center">
  <i>Created by Aïmane Benkhadda — Data Analyst (Excel, SQL, Power BI, Python)</i>
  <br>
  <a href="mailto:aymanebenkhadda5959@gmail.com">aymanebenkhadda5959@gmail.com</a>
</p>
