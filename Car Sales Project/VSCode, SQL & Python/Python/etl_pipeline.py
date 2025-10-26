# ==========================================================
# 🚗 Car Sales Data Analysis – Python Workflow
# ==========================================================
# Author: Aïmane Benkhadda
# Project: Car Sales Dashboard (Data Analytics Portfolio)
# Description:
# This script connects to a PostgreSQL database, loads car sales data,
# performs cleaning and analysis, and exports the results for visualization.
# The code is structured and commented so that even a non-technical recruiter
# can follow the logic behind the workflow.
# ==========================================================


# ----------------------------------------------------------
# 1. Import Required Libraries
# ----------------------------------------------------------
# pandas and numpy for data manipulation
# psycopg2 for database connection
# dotenv to load credentials from a secure .env file
# matplotlib for optional visual validation
# ----------------------------------------------------------

import os
import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import matplotlib.pyplot as plt


# ----------------------------------------------------------
# 2. Load Environment Variables
# ----------------------------------------------------------
# The .env file securely stores your database credentials.
# This avoids hardcoding sensitive information in the script.
# Example .env file:
#   DB_NAME=car_sales
#   DB_USER=postgres
#   DB_PASSWORD=your_password
#   DB_HOST=localhost
#   DB_PORT=5432
# ----------------------------------------------------------

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


# ----------------------------------------------------------
# 3. Connect to PostgreSQL Database
# ----------------------------------------------------------
# Establish a connection to your local or hosted database.
# If the connection fails, an error message will be printed.
# ----------------------------------------------------------

try:
    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("✅ Database connection successful!")
except Exception as e:
    print("❌ Database connection failed:", e)


# ----------------------------------------------------------
# 4. Load Data from SQL Tables or Views
# ----------------------------------------------------------
# You can use SQL queries directly in pandas to fetch clean data.
# Example: pulling from a cleaned view like car_sales_cleaned_view.
# ----------------------------------------------------------

query = """
SELECT *
FROM car_sales_cleaned_view;
"""

df = pd.read_sql(query, connection)
print(f"✅ Loaded {len(df)} rows from database")


# ----------------------------------------------------------
# 5. Data Cleaning & Preparation
# ----------------------------------------------------------
# Clean missing values, fix data types, and handle inconsistencies.
# ----------------------------------------------------------

# Remove leading/trailing spaces
df.columns = df.columns.str.strip()

# Fill missing numeric values with 0
numeric_cols = df.select_dtypes(include=np.number).columns
df[numeric_cols] = df[numeric_cols].fillna(0)

# Fill missing text fields with "Unknown"
df = df.fillna("Unknown")

# Convert date columns to datetime
if 'Date' in df.columns:
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

# Verify final data types
print("✅ Data cleaning complete. Data types:")
print(df.dtypes.head())


# ----------------------------------------------------------
# 6. Feature Engineering
# ----------------------------------------------------------
# Create useful columns for dashboard metrics (e.g., profit margin, year, month).
# ----------------------------------------------------------

# Extract year and month from Date
if 'Date' in df.columns:
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month

# Example: calculate profit margin if Price and Cost exist
if 'Price' in df.columns and 'Cost' in df.columns:
    df['Profit_Margin'] = ((df['Price'] - df['Cost']) / df['Cost']) * 100

print("✅ Feature engineering complete.")


# ----------------------------------------------------------
# 7. Key Performance Indicators (KPI) Summary
# ----------------------------------------------------------
# Calculate main metrics: total sales, average price, top models, etc.
# ----------------------------------------------------------

total_revenue = df['Price'].sum()
average_price = df['Price'].mean()
top_model = df['Model'].value_counts().idxmax()

print("\n🚀 KPI Summary:")
print(f"Total Revenue: ${total_revenue:,.2f}")
print(f"Average Price: ${average_price:,.2f}")
print(f"Top-Selling Model: {top_model}")


# ----------------------------------------------------------
# 8. Optional: Quick Data Visualization (Matplotlib)
# ----------------------------------------------------------
# These visuals help validate data before exporting to Power BI.
# ----------------------------------------------------------

plt.figure(figsize=(8, 5))
df.groupby('Year')['Price'].sum().plot(kind='bar')
plt.title('Total Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue ($)')
plt.tight_layout()
plt.show()


# ----------------------------------------------------------
# 9. Export Cleaned Data for Power BI / Excel
# ----------------------------------------------------------
# The cleaned data can be exported to a CSV for visualization.
# ----------------------------------------------------------

output_path = "cleaned_car_sales.csv"
df.to_csv(output_path, index=False)
print(f"✅ Cleaned dataset exported to {output_path}")


# ----------------------------------------------------------
# 10. Close Database Connection
# ----------------------------------------------------------

connection.close()
print("🔒 Database connection closed successfully.")
