import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# Streamlit app title
st.title("Walmart Inventory Dashboard")

# PostgreSQL connection details
pg_config = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
    "dbname": "walmartinventory"
}

# Initialize PostgreSQL connection
try:
    engine = create_engine(
        f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}"
    )
    st.success("Connected to PostgreSQL successfully!")
except Exception as e:
    st.error(f"Error connecting to PostgreSQL: {e}")
    st.stop()

# Business Use Case 1: Total Sales Across Time and Regions
st.subheader("1. Total Sales Across Time and Regions")
query_total_sales = """
    SELECT 
        d.year, d.month, s.store_location, SUM(f.total_sales) AS total_sales
    FROM 
        fact_sales f
    JOIN 
        dim_date d ON f.date = d.date_id
    JOIN 
        dim_store s ON f.store_id = s.store_id
    GROUP BY 
        d.year, d.month, s.store_location
    ORDER BY 
        d.year, d.month, s.store_location;
"""
try:
    df_total_sales = pd.read_sql(query_total_sales, engine)
    st.write("Total Sales Across Time and Regions:")
    st.dataframe(df_total_sales)
except Exception as e:
    st.error(f"Error fetching Total Sales data: {e}")

# Business Use Case 2: Most and Least Sold Products Across the Year
st.subheader("2. Most and Least Sold Products Across the Year")
year_filter = st.number_input("Enter Year (e.g., 2025):", value=2025)
query_product_sales = f"""
    SELECT 
        p.product_id, p.product_category, SUM(f.quantity_sold) AS total_quantity_sold
    FROM 
        fact_sales f
    JOIN 
        dim_product p ON f.product_id = p.product_id
    JOIN 
        dim_date d ON f.date = d.date_id
    WHERE 
        d.year = {year_filter}
    GROUP BY 
        p.product_id, p.product_category
    ORDER BY 
        total_quantity_sold DESC;
"""
try:
    df_product_sales = pd.read_sql(query_product_sales, engine)
    st.write("Most and Least Sold Products (Descending by Quantity Sold):")
    st.dataframe(df_product_sales)
except Exception as e:
    st.error(f"Error fetching Product Sales data: {e}")

# Business Use Case 3: Inventory Turnover
st.subheader("3. Inventory Turnover Across Stores and Products")
query_inventory_turnover = """
    SELECT 
        s.store_location, p.product_id, p.product_category, 
        SUM(f.quantity_sold) AS total_sold, AVG(f.stock_level) AS avg_stock_level
    FROM 
        fact_sales f
    JOIN 
        dim_store s ON f.store_id = s.store_id
    JOIN 
        dim_product p ON f.product_id = p.product_id
    GROUP BY 
        s.store_location, p.product_id, p.product_category
    ORDER BY 
        avg_stock_level DESC;
"""
try:
    df_inventory_turnover = pd.read_sql(query_inventory_turnover, engine)
    st.write("Inventory Turnover Across Stores and Products:")
    st.dataframe(df_inventory_turnover)
except Exception as e:
    st.error(f"Error fetching Inventory Turnover data: {e}")

# Business Use Case 4: Product Performance Across Regions
st.subheader("4. Product Performance Across Regions")
query_product_performance = """
    SELECT 
        s.store_location, p.product_id, p.product_category, 
        SUM(f.quantity_sold) AS total_quantity_sold, SUM(f.total_sales) AS total_sales
    FROM 
        fact_sales f
    JOIN 
        dim_store s ON f.store_id = s.store_id
    JOIN 
        dim_product p ON f.product_id = p.product_id
    GROUP BY 
        s.store_location, p.product_id, p.product_category
    ORDER BY 
        total_sales DESC;
"""
try:
    df_product_performance = pd.read_sql(query_product_performance, engine)
    st.write("Product Performance Across Regions:")
    st.dataframe(df_product_performance)
except Exception as e:
    st.error(f"Error fetching Product Performance data: {e}")

# Close PostgreSQL connection
# conn.close()
st.success("Data visualization completed successfully!")
