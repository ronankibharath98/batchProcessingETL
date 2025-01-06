import pandas as pd
from pyarrow import fs
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

# PostgreSQL connection details
pg_config = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
    "dbname": "walmartinventory"
}

# HDFS file paths
curated_base_path = "hdfs://localhost:9000/curated/walmart_inventory/"
paths = {
    "dim_product": f"{curated_base_path}dim_product",
    "dim_store": f"{curated_base_path}dim_store",
    "dim_date": f"{curated_base_path}dim_date",
    "fact_sales": f"{curated_base_path}fact_sales"
}

# Initialize PostgreSQL connection
try:
    engine = create_engine(
        f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}"
    )
    conn = engine.connect()
    print("Connected to PostgreSQL successfully!")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()

# Initialize HDFS connection
try:
    hdfs = fs.HadoopFileSystem('localhost', 9000)
    print("Connected to HDFS successfully!")
except Exception as e:
    print(f"Error connecting to HDFS: {e}")
    exit()

# Load and write data to PostgreSQL
for table_name, table_path in paths.items():
    try:
        # Read data from HDFS
        print(f"Reading data for {table_name} from HDFS...")
        df = pd.read_parquet(table_path, filesystem=hdfs)

        # Write data to PostgreSQL
        print(f"Writing data for {table_name} to PostgreSQL...")
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        # Display table preview
        print(f"Data for {table_name} successfully written to PostgreSQL!")
        print(f"{table_name} Table Preview:")
        print(df.head())

    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Close PostgreSQL connection
conn.close()
print("Data transfer completed successfully!")

# Visualization using Matplotlib and Seaborn
try:
    conn = engine.connect()

    # Example Visualization 1: Sales performance by store
    query = """
        SELECT s.store_name, SUM(f.sales_amount) AS total_sales
        FROM fact_sales f
        JOIN dim_store s ON f.store_id = s.store_id
        GROUP BY s.store_name
        ORDER BY total_sales DESC
        LIMIT 10
    """
    sales_by_store = pd.read_sql(query, conn)
    plt.figure(figsize=(12, 6))
    sns.barplot(x='total_sales', y='store_name', data=sales_by_store, palette='viridis')
    plt.title('Top 10 Stores by Sales')
    plt.xlabel('Total Sales')
    plt.ylabel('Store Name')
    plt.show()

    # Example Visualization 2: Product performance
    query = """
        SELECT p.product_name, SUM(f.sales_quantity) AS total_quantity
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_name
        ORDER BY total_quantity DESC
        LIMIT 10
    """
    product_performance = pd.read_sql(query, conn)
    plt.figure(figsize=(12, 6))
    sns.barplot(x='total_quantity', y='product_name', data=product_performance, palette='mako')
    plt.title('Top 10 Products by Quantity Sold')
    plt.xlabel('Total Quantity Sold')
    plt.ylabel('Product Name')
    plt.show()

    # Close the PostgreSQL connection
    conn.close()

except Exception as e:
    print(f"Error during visualization: {e}")
