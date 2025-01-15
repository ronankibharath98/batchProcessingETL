from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth, when
from pyspark.sql.types import IntegerType
from pyspark.sql.utils import AnalysisException
from functools import reduce

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Data Modeling with SCD Type 2") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.4") \
    .getOrCreate()

# Paths
STAGING_DATA_PATH = "hdfs://localhost:9000/staging/walmart_inventory"
CURATED_FACT_PATH = "hdfs://localhost:9000/curated/walmart_inventory/fact_sales"
CURATED_DIM_DATE_PATH = "hdfs://localhost:9000/curated/walmart_inventory/dim_date"
CURATED_DIM_STORE_PATH = "hdfs://localhost:9000/curated/walmart_inventory/dim_store"
CURATED_DIM_PRODUCT_PATH = "hdfs://localhost:9000/curated/walmart_inventory/dim_product"

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/walmartinventory"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Read Staging Data
staging_df = spark.read.parquet(STAGING_DATA_PATH).repartition(100)

# Helper Function to Read Existing Data
def read_existing_data(path):
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        print(f"No existing data found at path: {path}. Starting fresh...")
        return None

# Helper Function for SCD Type 2
def apply_scd_type_2(new_data, existing_data, primary_key, update_columns):
    if existing_data is not None:
        # Detect changes in attributes
        changes_detected = new_data.alias("new").join(
            existing_data.alias("existing"),
            primary_key,
            "left"
        ).filter(
            reduce(lambda x, y: x | y, [col(f"new.{col_name}") != col(f"existing.{col_name}") for col_name in update_columns])
        ).select("new.*")
        
        # Expire existing records
        expired_existing = existing_data.join(
            changes_detected.select(primary_key),
            primary_key,
            "left_anti"
        ).withColumn("is_current", lit(False)) \
          .withColumn("end_date", current_timestamp())
        
        # Add effective date for new records
        new_records = changes_detected.withColumn("start_date", current_timestamp()) \
                                      .withColumn("end_date", lit(None).cast("timestamp")) \
                                      .withColumn("is_current", lit(True))
        
        # Combine updated existing and new records
        updated_data = expired_existing.unionByName(new_records)
    else:
        # If no existing data, treat all as new data
        updated_data = new_data.withColumn("start_date", current_timestamp()) \
                               .withColumn("end_date", lit(None).cast("timestamp")) \
                               .withColumn("is_current", lit(True))
    return updated_data

# Step 1: Create `DimDate`
dim_date = staging_df.select("date").distinct() \
    .withColumnRenamed("date", "date_id") \
    .withColumn("year", year(col("date_id")).cast(IntegerType())) \
    .withColumn("month", month(col("date_id")).cast(IntegerType())) \
    .withColumn("day", dayofmonth(col("date_id")).cast(IntegerType()))

# Write to HDFS and PostgreSQL
dim_date.repartition(10).write.mode("overwrite").parquet(CURATED_DIM_DATE_PATH)
dim_date.write.jdbc(url=POSTGRES_URL, table="dim_date", mode="overwrite", properties=POSTGRES_PROPERTIES)

# Step 2: Create `DimStore` with SCD Type 2
existing_dim_store = read_existing_data(CURATED_DIM_STORE_PATH)
dim_store = staging_df.select(
    "store_id",
    "store_location",
    "reorder_point",
    "lead_time_days",
    "carrying_cost",
    "stock_out_risk"
).distinct()

dim_store_scd = apply_scd_type_2(
    dim_store,
    existing_dim_store,
    primary_key=["store_id"],
    update_columns=["store_location", "reorder_point", "lead_time_days", "carrying_cost", "stock_out_risk"]
)

# Write to HDFS and PostgreSQL
dim_store_scd.repartition(10).write.mode("overwrite").parquet(CURATED_DIM_STORE_PATH)
dim_store_scd.write.jdbc(url=POSTGRES_URL, table="dim_store", mode="overwrite", properties=POSTGRES_PROPERTIES)

# Step 3: Create `DimProduct` with SCD Type 2
existing_dim_product = read_existing_data(CURATED_DIM_PRODUCT_PATH)
dim_product = staging_df.select(
    "product_id",
    "product_category",
    "unit_price"
).distinct()

dim_product_scd = apply_scd_type_2(
    dim_product,
    existing_dim_product,
    primary_key=["product_id"],
    update_columns=["product_category", "unit_price"]
)

# Write to HDFS and PostgreSQL
dim_product_scd.repartition(10).write.mode("overwrite").parquet(CURATED_DIM_PRODUCT_PATH)
dim_product_scd.write.jdbc(url=POSTGRES_URL, table="dim_product", mode="overwrite", properties=POSTGRES_PROPERTIES)

# Step 4: Create `FactSales`
fact_sales = staging_df.select(
    "transaction_id",
    "date",
    "store_id",
    "product_id",
    "quantity_sold",
    "total_sales",
    "stock_level"
)

# Write to HDFS and PostgreSQL
fact_sales.repartition(20).write.mode("overwrite").parquet(CURATED_FACT_PATH)
fact_sales.write.jdbc(url=POSTGRES_URL, table="fact_sales", mode="overwrite", properties=POSTGRES_PROPERTIES)

print("Curated layer processing with SCD Type 2 completed successfully!")
