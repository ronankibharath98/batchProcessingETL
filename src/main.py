import psycopg2

def create_table_and_load_data():
    connection = psycopg2.connect(
        "host=localhost dbname=walmartInventory user=postgres password=postgres"
    )
    cursor = connection.cursor()

    # Create the updated table schema
    create_table_query = """
    CREATE TABLE IF NOT EXISTS inventory (
        transaction_id VARCHAR(50) PRIMARY KEY,
        date TIMESTAMP,
        store_id VARCHAR(50),
        store_location VARCHAR(100),
        product_id VARCHAR(50),
        product_category VARCHAR(100),
        quantity_sold INT,
        unit_price FLOAT,
        total_sales FLOAT,
        stock_level INT,
        reorder_point INT,
        lead_time_days INT,
        carrying_cost FLOAT,
        stock_out_risk FLOAT,
        inventory_turnover FLOAT
    )
    """
    cursor.execute(create_table_query)

    # Load data from CSV
    csv_file_path = r"data\walmart_inventory_data.csv"  
    with open(csv_file_path, 'r') as f:
        next(f)  # Skip the header row
        cursor.copy_from(f, 'inventory', sep=',', null='')

    connection.commit()
    cursor.close()
    connection.close()
    print("Table created and CSV data loaded successfully!")

if __name__ == "__main__":
    create_table_and_load_data()
