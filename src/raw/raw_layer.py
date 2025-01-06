import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Raw Layer - Walmart Inventory with HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.local.dir", "C:/Users/Bharath Ronanki/temp/spark-temp") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true") \
    .getOrCreate()

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "host": "localhost",
    "dbname": "walmartinventory",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

# HDFS Paths
RAW_DATA_PATH = "hdfs://localhost:9000/raw/walmart_inventory"
METADATA_PATH = "hdfs://localhost:9000/raw/metadata"

# Metadata Schema
METADATA_SCHEMA = StructType([StructField("last_processed", StringType(), True)])

# Function to Check if HDFS Path Exists
def hdfs_path_exists(path):
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
    except Exception as e:
        logger.error(f"Error checking HDFS path: {path}. Error: {e}")
        return False

# Function to Extract Data from PostgreSQL
def extract_from_postgres(query):
    jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
    properties = {
        "user": POSTGRES_CONFIG['user'],
        "password": POSTGRES_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }
    try:
        logger.info(f"Extracting data from PostgreSQL with query: {query}")
        df = spark.read.jdbc(url=jdbc_url, table=f"({query}) as subquery", properties=properties)
        logger.info(f"Extracted {df.count()} rows from PostgreSQL.")
        df.show(truncate=False)
        return df
    except Exception as e:
        logger.error(f"Error extracting data from PostgreSQL: {e}")
        raise

# Save Last Processed Timestamp to Metadata
def save_last_processed_timestamp(timestamp):
    try:
        timestamp_str = timestamp.strftime("%Y-%m-%d")
        metadata = spark.createDataFrame([{"last_processed": timestamp_str}], schema=METADATA_SCHEMA)
        metadata.write.mode("overwrite").json(METADATA_PATH)
        logger.info(f"Metadata saved successfully: {timestamp_str}")
    except Exception as e:
        logger.error(f"Error saving metadata: {e}")
        raise

# Load Last Processed Timestamp from Metadata
def get_last_processed_timestamp():
    try:
        if not hdfs_path_exists(METADATA_PATH):
            logger.info("Metadata path does not exist. Performing full load.")
            return None
        metadata = spark.read.schema(METADATA_SCHEMA).json(METADATA_PATH)
        last_processed = metadata.select("last_processed").collect()[0]["last_processed"]
        logger.info(f"Last processed timestamp retrieved: {last_processed}")
        return last_processed
    except Exception as e:
        logger.warning(f"No metadata found. Starting full load. Error: {e}")
        return None

# Perform Full Load
def perform_full_load():
    try:
        logger.info("Performing full load from PostgreSQL...")
        raw_data = extract_from_postgres("SELECT * FROM inventory_data")

        if raw_data.count() == 0:
            logger.warning("No data found in PostgreSQL for full load.")
            return

        # Write data to HDFS
        logger.info(f"Writing {raw_data.count()} rows to HDFS at {RAW_DATA_PATH}.")
        raw_data.write.mode("overwrite").parquet(RAW_DATA_PATH)

        # Update metadata
        max_timestamp = raw_data.agg(spark_max("date")).collect()[0][0]
        if max_timestamp:
            save_last_processed_timestamp(max_timestamp)
        logger.info("Full load completed successfully.")
    except Exception as e:
        logger.error(f"Error during full load: {e}")
        raise

# Perform Incremental Load
def perform_incremental_load():
    try:
        logger.info("Performing incremental load from PostgreSQL...")
        last_processed = get_last_processed_timestamp()
        if not last_processed:
            logger.info("No metadata found, performing full load...")
            perform_full_load()
            return

        query = f"SELECT * FROM inventory_data WHERE date > '{last_processed}'"
        logger.info(f"Running query: {query}")
        incremental_data = extract_from_postgres(query)

        if incremental_data.count() == 0:
            logger.info("No new data available for incremental load.")
            return

        # Load existing data from HDFS
        logger.info("Loading existing data from HDFS...")
        if hdfs_path_exists(RAW_DATA_PATH):
            existing_data = spark.read.parquet(RAW_DATA_PATH)
            logger.info("Merging new data with existing data...")
            updated_data = existing_data.union(incremental_data).dropDuplicates()
        else:
            logger.info("No existing data found, using only incremental data...")
            updated_data = incremental_data

        # Write updated data back to HDFS
        logger.info(f"Writing updated data to HDFS at {RAW_DATA_PATH}.")
        updated_data.write.mode("overwrite").parquet(RAW_DATA_PATH)

        # Update metadata
        max_timestamp = incremental_data.agg(spark_max("date")).collect()[0][0]
        if max_timestamp:
            save_last_processed_timestamp(max_timestamp)

        logger.info("Incremental load completed successfully.")
    except Exception as e:
        logger.error(f"Error during incremental load: {e}")
        raise

# Debug HDFS Metadata File
def debug_hdfs_metadata():
    try:
        if not hdfs_path_exists(METADATA_PATH):
            logger.info("Metadata path does not exist.")
            return
        metadata_content = spark.read.json(METADATA_PATH).collect()
        logger.info("HDFS Metadata Content:")
        for row in metadata_content:
            logger.info(row)
    except Exception as e:
        logger.error(f"Error reading metadata for debugging: {e}")

# Main Execution
if __name__ == "__main__":
    try:
        logger.info("Starting Raw Layer Processing...")
        last_processed = get_last_processed_timestamp()
        if last_processed:
            perform_incremental_load()
        else:
            perform_full_load()

        debug_hdfs_metadata()
        logger.info("Raw layer data extraction and processing completed successfully.")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
