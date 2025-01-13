import logging
import time
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

# Polling Interval (in seconds)
POLL_INTERVAL = 5  # Check for new data every 60 seconds


def hdfs_path_exists(path):
    """Check if an HDFS path exists."""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
    except Exception as e:
        logger.error(f"Error checking HDFS path: {path}. Error: {e}")
        return False


def extract_from_postgres(query):
    """Extract data from PostgreSQL."""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
    properties = {
        "user": POSTGRES_CONFIG['user'],
        "password": POSTGRES_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }
    try:
        logger.info(f"Extracting data from PostgreSQL with query: {query}")
        return spark.read.jdbc(url=jdbc_url, table=f"({query}) as subquery", properties=properties)
    except Exception as e:
        logger.error(f"Error extracting data from PostgreSQL: {e}")
        raise


def save_last_processed_timestamp(timestamp):
    """Save the last processed timestamp to HDFS metadata."""
    try:
        metadata = spark.createDataFrame([{"last_processed": timestamp.strftime("%Y-%m-%d")}], schema=METADATA_SCHEMA)
        metadata.write.mode("overwrite").json(METADATA_PATH)
        logger.info(f"Metadata saved successfully: {timestamp}")
    except Exception as e:
        logger.error(f"Error saving metadata: {e}")
        raise


def get_last_processed_timestamp():
    """Get the last processed timestamp from HDFS metadata."""
    try:
        if not hdfs_path_exists(METADATA_PATH):
            logger.info("Metadata path does not exist. Performing full load.")
            return None
        metadata = spark.read.schema(METADATA_SCHEMA).json(METADATA_PATH)
        return metadata.select("last_processed").collect()[0]["last_processed"]
    except Exception as e:
        logger.warning(f"Metadata retrieval failed. Error: {e}")
        return None


def perform_full_load():
    """Perform a full data load."""
    logger.info("Performing full load from PostgreSQL...")
    raw_data = extract_from_postgres("SELECT * FROM inventory_data")
    if raw_data.count() == 0:
        logger.warning("No data found in PostgreSQL for full load.")
        return

    raw_data.write.mode("overwrite").parquet(RAW_DATA_PATH)
    max_timestamp = raw_data.agg(spark_max("date")).collect()[0][0]
    if max_timestamp:
        save_last_processed_timestamp(max_timestamp)
    logger.info("Full load completed successfully.")


def perform_incremental_load():
    """Perform an incremental data load."""
    logger.info("Performing incremental load from PostgreSQL...")
    last_processed = get_last_processed_timestamp()
    if not last_processed:
        logger.info("No metadata found. Performing full load.")
        perform_full_load()
        return

    query = f"SELECT * FROM inventory_data WHERE date > '{last_processed}'"
    incremental_data = extract_from_postgres(query)
    if incremental_data.count() == 0:
        logger.info("No new data available for incremental load.")
        return

    if hdfs_path_exists(RAW_DATA_PATH):
        existing_data = spark.read.parquet(RAW_DATA_PATH)
        updated_data = existing_data.union(incremental_data).dropDuplicates()
    else:
        updated_data = incremental_data

    updated_data.write.mode("overwrite").parquet(RAW_DATA_PATH)
    max_timestamp = incremental_data.agg(spark_max("date")).collect()[0][0]
    if max_timestamp:
        save_last_processed_timestamp(max_timestamp)

    logger.info("Incremental load completed successfully.")


def main():
    """Main function to keep the pipeline running."""
    try:
        logger.info("Starting Raw Layer Processing...")
        while True:
            last_processed = get_last_processed_timestamp()
            print(last_processed)
            if last_processed:
                logger.info("Performing Incremental load")
                perform_incremental_load()
            else:
                logger.info("Performing full load")
                perform_full_load()

            logger.info(f"Waiting for {POLL_INTERVAL} seconds before checking for new data...")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        logger.info("Raw layer processing stopped.")


if __name__ == "__main__":
    main()
