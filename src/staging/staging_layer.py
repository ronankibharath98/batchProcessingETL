from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count
from pyspark.sql.types import DecimalType, IntegerType
import logging

# Initialize Logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Staging Layer - Walmart Inventory") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# HDFS Paths
RAW_DATA_PATH = "hdfs://localhost:9000/raw/walmart_inventory"
STAGING_DATA_PATH = "hdfs://localhost:9000/staging/walmart_inventory"

# Function to Check if HDFS Path Exists
def hdfs_path_exists(path):
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
    except Exception as e:
        logger.error(f"Error checking HDFS path: {path}. Error: {e}")
        return False

try:
    # Step 1: Read Raw Data
    logger.info("Reading raw data from HDFS...")
    if not hdfs_path_exists(RAW_DATA_PATH):
        raise FileNotFoundError(f"Raw data path does not exist: {RAW_DATA_PATH}")
    
    raw_data = spark.read.parquet(RAW_DATA_PATH)
    raw_data_count = raw_data.count()
    logger.info(f"Raw data row count: {raw_data_count}")

    # Step 2: Check for Duplicates
    logger.info("Checking for duplicates in raw data...")
    duplicate_rows = raw_data.groupBy(*raw_data.columns).count().filter(col("count") > 1)
    duplicate_count = duplicate_rows.count()
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate rows in raw data.")
        logger.info("Duplicate rows:")
        duplicate_rows.show(truncate=False)
    else:
        logger.info("No duplicate rows found.")

    # Step 3: Data Cleaning
    logger.info("Cleaning the raw data...")
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
    logger.info(f"Cleaned data row count: {cleaned_data_count}")

    # Step 4: Transformation
    logger.info("Applying transformations...")
    transformed_data = cleaned_data \
        .withColumn("profit_margin", when(
            (col("quantity_sold").isNotNull()) & (col("unit_price").isNotNull()), 
            col("total_sales") - (col("quantity_sold") * col("unit_price"))
        ).otherwise(lit(None))) \
        .withColumn("is_restock_needed", when(
            (col("stock_level") < col("reorder_point")) & col("stock_level").isNotNull() & col("reorder_point").isNotNull(),
            lit("Yes")
        ).otherwise(lit("No")))
    
    transformed_data_count = transformed_data.count()
    logger.info(f"Transformed data row count: {transformed_data_count}")

    # Step 5: Write to Staging Zone
    logger.info("Writing cleaned and transformed data to HDFS staging zone...")
    transformed_data.write.mode("overwrite").parquet(STAGING_DATA_PATH)

    logger.info("Staging layer processing completed successfully!")

except FileNotFoundError as fnfe:
    logger.error(f"File not found error: {fnfe}")
except Exception as e:
    logger.error(f"An error occurred during processing: {e}")
