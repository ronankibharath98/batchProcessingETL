from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.utils import AnalysisException

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Curated Layer - Walmart Inventory") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# HDFS Paths
STAGING_DATA_PATH = "hdfs://localhost:9000/staging/walmart_inventory"
CURATED_FACT_PATH = "hdfs://localhost:9000/curated/walmart_inventory/fact_sales"
CURATED_DIM_PRODUCT_PATH = "hdfs://localhost:9000/curated/walmart_inventory/dim_product"
CURATED_DIM_STORE_PATH = "hdfs://localhost:9000/curated/walmart_inventory/dim_store"
CURATED_DIM_DATE_PATH = "hdfs://localhost:9000/curated/walmart_inventory/dim_date"

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/walmartinventory"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Read data from Staging Layer
print("Reading data from staging layer...")
staging_df = spark.read.parquet(STAGING_DATA_PATH)

# Helper Function to Read Existing Data from PostgreSQL
def read_existing_data(table_name):
    try:
        return spark.read.jdbc(
            url=POSTGRES_URL,
            table=table_name,
            properties=POSTGRES_PROPERTIES
        )
    except Exception as e:
        print(f"No existing data found in table {table_name}. {e}")
        return None

# Helper Function to Apply SCD Type 2
def apply_scd_type_2(new_data, existing_data, primary_key_columns, update_columns):
    if existing_data is not None:
        # Alias for clarity
        new_data = new_data.alias("new")
        existing_data = existing_data.alias("existing")

        # Add required columns if they don't exist
        if "end_date" not in existing_data.columns:
            existing_data = existing_data.withColumn("end_date", lit(None).cast("timestamp"))
        if "start_date" not in new_data.columns:
            new_data = new_data.withColumn("start_date", lit(None).cast("timestamp"))
        if "is_current" not in existing_data.columns:
            existing_data = existing_data.withColumn("is_current", lit(True))
        
        print("Length of new data: ", new_data.count())
        print("Length of existing data: ", existing_data.count())

        # Create dynamic change detection conditions
        change_conditions = []
        for update_col in update_columns:
            change_conditions.append(
                col(f"new.{update_col}") != col(f"existing.{update_col}")
            )
        
        # Find changed records
        changed_records = new_data.join(
            existing_data,
            primary_key_columns,
            "left_outer"
        )
        
        if change_conditions:
            changed_records = changed_records.filter(
                reduce(lambda x, y: x | y, change_conditions)
            )
        
        changed_records = changed_records.select("new.*")

        print("Number of changed records: ", changed_records.count())

        # Mark existing records as inactive
        updated_existing = existing_data.join(
            changed_records.select(primary_key_columns),
            primary_key_columns,
            "left_outer"
        )

        # Update records with changed values
        updated_existing = updated_existing.withColumn(
            "is_current",
            when(col(primary_key_columns[0]).isNotNull(), lit(False)).otherwise(col("is_current"))
        ).withColumn(
            "end_date",
            when(col(primary_key_columns[0]).isNotNull(), current_timestamp()).otherwise(col("end_date"))
        )

        # Select only the columns we need
        required_columns = primary_key_columns + update_columns + ["is_current", "end_date"]
        updated_existing = updated_existing.select(*[col(c).alias(c) for c in required_columns])

        # Prepare new records
        new_records = changed_records.withColumn("is_current", lit(True)) \
                                   .withColumn("start_date", current_timestamp()) \
                                   .withColumn("end_date", lit(None).cast("timestamp"))
        
        print("Schema of updated_existing:")
        updated_existing.printSchema()

        print("Schema of new_records:")
        new_records.printSchema()

        # Ensure both DataFrames have the same schema
        all_columns = set(updated_existing.columns + new_records.columns)
        
        for column in all_columns:
            if column not in updated_existing.columns:
                updated_existing = updated_existing.withColumn(column, lit(None))
            if column not in new_records.columns:
                new_records = new_records.withColumn(column, lit(None))

        # Sort columns to ensure they match
        sorted_columns = sorted(all_columns)
        updated_existing = updated_existing.select(sorted_columns)
        new_records = new_records.select(sorted_columns)

        print("Columns in updated_existing:", updated_existing.columns)
        print("Columns in new_records:", new_records.columns)

        # Union the updated existing records with new records
        final_data = updated_existing.unionByName(new_records)

    else:
        # If no existing data, treat everything as new data
        final_data = new_data.withColumn("is_current", lit(True)) \
                            .withColumn("start_date", current_timestamp()) \
                            .withColumn("end_date", lit(None).cast("timestamp"))

    return final_data

# Handle DimProduct with SCD Type 2
print("Handling DimProduct with SCD Type 2...")
existing_dim_product = read_existing_data("dim_product")
dim_product = staging_df.select("product_id", "product_category", "unit_price").distinct()
dim_product_scd = apply_scd_type_2(
    dim_product,
    existing_dim_product,
    primary_key_columns=["product_id"],
    update_columns=["product_category", "unit_price"]
)

# Handle DimStore with SCD Type 2
print("Handling DimStore with SCD Type 2...")
existing_dim_store = read_existing_data("dim_store")
dim_store = staging_df.select("store_id", "store_location").distinct()
dim_store_scd = apply_scd_type_2(
    dim_store,
    existing_dim_store,
    primary_key_columns=["store_id"],
    update_columns=["store_location"]
)

# Handle DimDate (no SCD required as dates don't change)
print("Handling DimDate...")
dim_date = staging_df.select("date").distinct().withColumnRenamed("date", "date_id")
existing_dim_date = read_existing_data("dim_date")
dim_date_merged = dim_date.subtract(existing_dim_date) if existing_dim_date is not None else dim_date

# Handle FactSales (no SCD, but ensure deduplication)
print("Handling FactSales...")
fact_sales = staging_df.select(
    "transaction_id",
    "date",
    "store_id",
    "product_id",
    "quantity_sold",
    "total_sales",
    "stock_level"
)
existing_fact_sales = read_existing_data("fact_sales")
fact_sales_merged = fact_sales.subtract(existing_fact_sales) if existing_fact_sales is not None else fact_sales

print("Schema of dim_product_scd:")
dim_product_scd.printSchema()

# DimProduct
dim_product_scd.write.mode("overwrite").parquet(CURATED_DIM_PRODUCT_PATH)
# Write Dimension Tables to PostgreSQL
print("Writing Dimension Tables to PostgreSQL...")
dim_product_scd.write.jdbc(
    url=POSTGRES_URL,
    table="dim_product",
    mode="append",
    properties=POSTGRES_PROPERTIES
)

# DimStore
dim_store_scd.write.mode("overwrite").parquet(CURATED_DIM_STORE_PATH)
dim_store_scd.write.jdbc(
    url=POSTGRES_URL,
    table="dim_store",
    mode="append",
    properties=POSTGRES_PROPERTIES
)

# DimDate
dim_date_merged.write.mode("overwrite").parquet(CURATED_DIM_DATE_PATH)
dim_date_merged.write.jdbc(
    url=POSTGRES_URL,
    table="dim_date",
    mode="append",
    properties=POSTGRES_PROPERTIES
)

# FactSales
fact_sales_merged.write.mode("overwrite").parquet(CURATED_FACT_PATH)
# Write Fact Table to PostgreSQL
print("Writing Fact Table to PostgreSQL...")
fact_sales_merged.write.jdbc(
    url=POSTGRES_URL,
    table="fact_sales",
    mode="append",
    properties=POSTGRES_PROPERTIES
)

print("Curated layer processing with SCD Type 2 completed successfully!")
