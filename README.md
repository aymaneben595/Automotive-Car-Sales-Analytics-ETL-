# 🚗 Automotive Car Sales Analytics & ETL

---

## 📘 Project Background

As a Data Analyst supporting a major multi-brand **Automotive Sales Company**, my role was to synthesize underutilized transactional data to inform **inventory**, **marketing**, and **sales leadership strategy**.  

The company’s business model relies on a dual approach:
- **High-volume mid-range sales**, and  
- **High-margin luxury sales**  
across diverse geographical regions.

Key performance indicators (KPIs) tracked include:
- **Total Revenue**
- **Average Car Price (ASV)**
- **Customer Segmentation**

Insights and recommendations are provided across the following areas:

1. **Sales Trends Analysis:** Revenue, Order Volume, and Average Sales Value (ASV) patterns.  
2. **Dealer & Manufacturer Performance:** Identifying top-performing dealers and volume-vs-value dynamics.  
3. **Customer Demographics & Behavior:** How gender and income influence purchases.  
4. **Inventory & Product Preferences:** Popular colors, transmissions, and model trends.

🔗 **SQL ETL Script:**  
**[View ETL & Analytics Script (etl_customers.sql)](https://github.com/aymaneben595/Automotive-Car-Sales-Analytics-ETL-/blob/b95acd9c102d59b0a3d4ecc820e2744f814c451d/Car%20Sales%20Project/VSCode%2C%20SQL%20%26%20Python/SQL/etl_customers.sql)**

📊 **Dashboard:**  
**[⬇️ Download Car Sales Dark Mode Dashboard.pbix](https://github.com/aymaneben595/Automotive-Car-Sales-Analytics-ETL-/raw/ff5795cbf8b7f39ac0d61cc357a358097059f840/Power%20Bi/Car%20Sales%20Dark%20Mode%20Dashboard.pbix
)**

---

## 🧩 Data Structure & Initial Checks

After a complete **ETL process in PostgreSQL**, the dataset contained **23,910 clean transaction records**.  

Two main analytical artifacts were produced:
- **`car_sales`** → Final structured transaction table (one row per sale)  
- **`vw_sales_export`** → Analytical view for BI tools (with calculated fields like `year` and `price_segment`)

<p align="center">
  <img src="Images/car_sales_erd.png" alt="Entity Relationship Diagram (ERD)">
</p>

---

## 📈 Executive Summary

### Overview of Findings

The automotive market achieved **$671.5 Million in total revenue**, demonstrating strong performance — yet revealing key imbalances:

- **79% of purchases were made by male customers**, highlighting a massive untapped female segment.  
- **Q4 consistently peaks in sales**, indicating clear seasonality.  
- **Average Sales Value (ASV):** $28.09K — stable and healthy, validating premium pricing strategies.

<p align="center">
  <img src="12334.PNG" alt="Overall Sales Dashboard Snapshot">
</p>

---

## 🔍 Insights Deep Dive

### **Category 1: Sales Trends Analysis**

1. **Total revenue reached $671.5M**, confirming a robust financial position.  
2. **October 2023 peaked at $61M**, showing clear seasonality and strong Q4 momentum.  
3. **ASV remained steady at $28.09K**, validating consistent pricing strategies.  
4. **February 2023 was 15% below average**, revealing early-year slowdowns.

<p align="center">
  <img src="[Place Sales Over Time visualization here]" alt="Sales Over Time Chart">
</p>

---

### **Category 2: Dealer & Manufacturer Performance**

1. Top dealers (e.g., **Haddad**, **Progressive**) generate **$30–40M** each — success is highly concentrated.  
2. **Mercedes-Benz** maintains the **highest ASV at $48K**, confirming luxury segment strength.  
3. **U-Haul Co.** attracts the **wealthiest clients (up to $1.9M income)** — a benchmark for high-value targeting.  
4. **Chevrolet** leads in **volume** but with lower ASV — showing the volume–margin trade-off.

<p align="center">
  <img src="[Place Dealer Performance visualization here]" alt="Dealer Performance Chart">
</p>

---

### **Category 3: Customer Demographics & Behavior**

1. **Male customers = 79%** of sales — massive underpenetration in female market.  
2. **High-income buyers** dominate **High Value** car purchases — validating premium targeting.  
3. **Pacific & Atlantic regions** lead in volume — mid-tier regions underperform.  
4. **Middle-income quartiles** fuel **mid-range vehicle** sales — core growth segment.

<p align="center">
  <img src="[Place Customer Demographics visualization here]" alt="Customer Demographics Chart">
</p>

---

### **Category 4: Inventory & Product Preferences**

1. **Pale White (47%)** and **Red (32%)** = ~80% of all sales — inventory should prioritize these.  
2. **Automatic transmissions dominate** — low manual demand ties up unnecessary capital.  
3. **Chevrolet Equinox** and **Ford Escape** consistently lead in volume.  
4. **Brown cars = <1% of sales** — should be deprioritized in procurement.

<p align="center">
  <img src="[Place Inventory Preference visualization here]" alt="Inventory Preference Chart">
</p>

---

## 💡 Recommendations

To support **Sales & Marketing Leadership** decisions:

1. **Target Female Customers:**  
   Launch a campaign to engage women buyers — a 21% share gap is a major untapped market.

2. **Replicate Top Dealer Practices:**  
   Study and train others based on top-performing dealers like **U-Haul Co.** to lift ASV across all regions.

3. **Capitalize on Q4 Momentum:**  
   Focus staff incentives and advertising in **Q4**, the historically most profitable quarter.

4. **Streamline Inventory:**  
   Reduce low-demand stock (manual, brown cars) and double down on **Pale White & Red automatics**.

5. **Refine Premium Targeting:**  
   Enhance digital campaigns for high-income demographics who dominate high-value purchases.

---

## ⚙️ Assumptions & Caveats

- **Data Exclusion:** Less than 1% of records with missing `price` or `sale_date` were excluded.  
- **Currency:** All prices and income standardized to **USD** after cleaning.  
- **Transmission Classification:** Variants like “Auto” and “Automatic” were unified for clarity.  

---

<p align="center">
  <i>Created by Aïmane Benkhadda — Data Analyst (Excel, SQL, Power BI, Python)</i>  
  <br>
  <a href="mailto:aymanebenkhadda5959@gmail.com">aymanebenkhadda5959@gmail.com</a>
</p>
