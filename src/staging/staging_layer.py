import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import DecimalType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Staging Layer - Walmart Inventory") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# HDFS Paths
RAW_DATA_PATH = "hdfs://localhost:9000/raw/walmart_inventory"
STAGING_DATA_PATH = "hdfs://localhost:9000/staging/walmart_inventory"

# Step 1: Read Raw Data
print("Reading raw data from HDFS...")
raw_data = spark.read.parquet(RAW_DATA_PATH)
raw_data_count = raw_data.count()
print(f"Raw data row count: {raw_data_count}")

# Step 2: Check for Duplicates
duplicate_count = raw_data.groupBy(*raw_data.columns).count().filter(col("count") > 1).count()
print(f"Duplicate rows in raw data: {duplicate_count}")

# Step 3: Data Cleaning
print("Cleaning the raw data...")
cleaned_data = raw_data \
    .dropDuplicates() \
    .filter(col("date").isNotNull()) \
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2))) \
    .withColumn("total_sales", col("total_sales").cast(DecimalType(15, 2))) \
    .withColumn("quantity_sold", col("quantity_sold").cast(IntegerType()))

# Handle missing values
cleaned_data = cleaned_data.fillna({
    "stock_level": 0,
    "reorder_point": 0,
    "lead_time_days": 0,
    "carrying_cost": 0.0,
    "stock_out_risk": 0.0,
    "inventory_turnover": 0.0
})

cleaned_data_count = cleaned_data.count()
print(f"Cleaned data row count: {cleaned_data_count}")

# Step 4: Transformation
print("Applying transformations...")
transformed_data = cleaned_data \
    .withColumn("profit_margin", col("total_sales") - (col("quantity_sold") * col("unit_price"))) \
    .withColumn("is_restock_needed", when(col("stock_level") < col("reorder_point"), lit("Yes")).otherwise(lit("No")))

transformed_data_count = transformed_data.count()
print(f"Transformed data row count: {transformed_data_count}")

# Step 5: Write to Staging Zone
print("Writing cleaned and transformed data to HDFS staging zone...")
transformed_data.write.mode("overwrite").parquet(STAGING_DATA_PATH)

print("Staging layer processing completed successfully!")
