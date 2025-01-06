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

# PostgreSQL table names
table_names = ["dim_product", "dim_store", "dim_date", "fact_sales"]

# Initialize PostgreSQL connection
try:
    engine = create_engine(
        f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}"
    )
    conn = engine.connect()
    st.success("Connected to PostgreSQL successfully!")
except Exception as e:
    st.error(f"Error connecting to PostgreSQL: {e}")
    st.stop()

# Fetch and display data from PostgreSQL
st.subheader("Data Preview: PostgreSQL")

for table_name in table_names:
    try:
        # Fetch data from PostgreSQL
        st.info(f"Fetching data for {table_name} from PostgreSQL...")
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)

        # Display table preview
        st.success(f"Data for {table_name} successfully fetched from PostgreSQL!")
        st.write(f"{table_name} Table Preview:")
        st.dataframe(df.head())

    except Exception as e:
        st.error(f"Error fetching data for {table_name}: {e}")

# Close PostgreSQL connection
conn.close()
st.success("Data fetching completed successfully!")
