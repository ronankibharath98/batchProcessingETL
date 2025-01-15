# batchProcessingETL
# Walmart Inventory Optimization Pipeline

=======================================

Overview

--------

The **Walmart Inventory Optimization Pipeline** is an ETL (Extract, Transform, Load) pipeline designed to process inventory data for Walmart. The pipeline leverages Apache Spark, PostgreSQL, and Hadoop HDFS to ensure efficient data processing across multiple stages (Raw, Staging, and Curated). It also includes a Streamlit-based dashboard for real-time visualization of key metrics.

Features

--------

-   **ETL Workflow**: Implements a multi-stage data processing pipeline using Spark.

-   **Incremental Load**: Supports incremental data updates by tracking metadata.

-   **Data Modeling**: Uses a star schema with fact and dimension tables for optimized analytics.

-   **Real-Time Visualization**: Interactive dashboard built using Streamlit.

-   **Scalable Storage**: Data stored in HDFS, ensuring high availability and fault tolerance.

* * * * *

Project Structure

-----------------

batchProcessingETL/

│

├── src/

│   ├── raw_layer.py         # Handles data extraction and incremental load from PostgreSQL to HDFS

│   ├── staging_layer.py     # Transforms raw data into cleaned, structured format

│   ├── curated_layer.py     # Performs data modeling (SCD Type 2) and loads data into the curated layer

│   ├── main.py              # Orchestrates the execution of raw, staging, and curated layers

│

├── dashboard/

│   ├── dashboard.py         # Streamlit application for visualizing processed data

│

├── config/

│   ├── hdfs-site.xml        # Hadoop configuration for HDFS

│   ├── core-site.xml        # Hadoop core configuration

│

├── data/

│   ├── initial_data.sql     # Initial dataset to populate PostgreSQL

│

├── scripts/

│   ├── run_hadoop.bat       # Batch script to start Hadoop services

│

├── README.md                # Project documentation

└── requirements.txt         # Python dependencies for the project

Prerequisites

-------------

### Software

1\.  **Hadoop**: Version 3.4.0

2\.  **Apache Spark**: Version 3.5.4

3\.  **PostgreSQL**: Version 14 or higher

4\.  **Python**: Version 3.9 or higher

5\.  **Streamlit**: Version 1.19.0 or higher

### Python Libraries

Install required Python dependencies using:

pip install -r requirements.txt

Setup and Configuration

-----------------------

### 1\. Configure Hadoop

Update the following files under the `config/` directory with the appropriate paths:

-   `hdfs-site.xml`

-   `core-site.xml`

### 2\. Initialize HDFS

Format and start the NameNode and DataNode:

### 3\. Setup PostgreSQL

-   Create the database:

-   Populate initial data:

### 4\. Run the ETL Pipeline

Run the `main.py` to orchestrate the pipeline:

### 5\. Start the Dashboard

Launch the Streamlit dashboard:

Data Pipeline Overview

----------------------

### Stages

1\.  **Raw Layer**:

    -   Extracts data from PostgreSQL and loads it into HDFS.

    -   Supports full and incremental loads.

2\.  **Staging Layer**:

    -   Cleanses and transforms data.

    -   Handles deduplication and ensures data consistency.

3\.  **Curated Layer**:

    -   Performs data modeling using a star schema.

    -   Implements Slowly Changing Dimensions (SCD Type 2).

    -   Loads modeled data into PostgreSQL for analytics.

### Data Flow Diagram

The data flows from the source PostgreSQL database through the pipeline and ends in the Streamlit dashboard for visualization

Troubleshooting

---------------

### Common Issues

-   **DataNode Error**: Ensure the `dfs.datanode.data.dir` path exists and has the correct permissions.

-   **Metadata Issues**: Check the metadata path in HDFS for inconsistencies.

-   **Streamlit Errors**: Verify PostgreSQL connection settings in `dashboard.py`.

### Logs

-   Logs are generated during pipeline execution to help debug issues.

License

-------

This project is licensed under the MIT License.
