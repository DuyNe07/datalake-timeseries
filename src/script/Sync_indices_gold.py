#!/usr/bin/env python
# Sync_indices_gold.py - Synchronize prediction data with Gold Indices table
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from functools import reduce

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col, lit, year, month, to_date, to_timestamp, date_format, row_number, monotonically_increasing_id, min as min_func, max as max_func  # type: ignore
from pyspark.sql.types import StringType, DoubleType, TimestampType, DateType, LongType  # type: ignore
from pyspark.sql.window import Window  # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SyncIndicesGold")

# Constants
SOURCE_CATALOG = "datalake"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.gold"
SOURCE_TABLE = f"{SOURCE_NAMESPACE}.indices"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
TARGET_TABLE = f"{TARGET_NAMESPACE}.indices_predict"
DATE_COLUMN = "date"
ID_COLUMN = "id"

# CSV file paths
CSV_BASE_DIR = "/src/data"
FUTURE_SRVAR_PATH = f"{CSV_BASE_DIR}/future/SrVAR/Indices_SrVAR_future.csv"
FUTURE_VARNN_PATH = f"{CSV_BASE_DIR}/future/VARNN/Indices_VARNN_future.csv"
PREDICT_SRVAR_PATH = f"{CSV_BASE_DIR}/predict/SrVAR/Indices_SrVAR_predict.csv"
PREDICT_VARNN_PATH = f"{CSV_BASE_DIR}/predict/VARNN/Indices_VARNN_predict.csv"

# Start dates
PREDICT_START_DATE = "2019-02-24"
FUTURE_START_DATE = "2025-03-08"


def create_spark_session(app_name: str) -> SparkSession:
    """Initialize Spark session with required configurations"""
    logger.info("Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    
    logger.info(f"Spark Session initialized successfully. Spark version: {spark.version}")
    
    # Log important configs
    conf = spark.sparkContext.getConf()
    s3_endpoint = conf.get("spark.hadoop.fs.s3a.endpoint", "N/A")
    catalog_type = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.type", "N/A")
    catalog_uri = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.uri", "N/A")
    warehouse = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.warehouse", "N/A")
    
    logger.info(f"Source Table: {SOURCE_TABLE}")
    logger.info(f"Target Table: {TARGET_TABLE}")
    logger.info(f"S3 Endpoint (Hadoop): {s3_endpoint}")
    logger.info(f"Catalog Type: {catalog_type}")
    logger.info(f"Catalog URI: {catalog_uri}")
    logger.info(f"Catalog Warehouse: {warehouse}")
    
    return spark


def create_namespace_if_not_exists(spark: SparkSession, namespace: str):
    """Create Iceberg namespace if it doesn't exist"""
    try:
        logger.info(f"Checking/Creating namespace: {namespace}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        logger.info(f"Namespace {namespace} ensured.")
    except Exception as e:
        logger.error(f"Failed to create namespace {namespace}: {str(e)}")
        raise


def generate_date_range(start_date: str, num_days: int) -> List[str]:
    """Generate a list of date strings starting from start_date for num_days"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    return [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days)]


def read_csv_file(spark: SparkSession, file_path: str, model_type: str, prediction_type: str, 
                  start_date: str) -> Optional[DataFrame]:
    """Read CSV file, add date column based on row count, and rename columns with model and prediction type"""
    logger.info(f"Reading {model_type}_{prediction_type} data from {file_path}")
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        # Read the CSV file into a pandas DataFrame first to handle the processing
        pdf = pd.read_csv(file_path)
        num_rows = len(pdf)
        
        # Generate dates based on the number of rows
        dates = generate_date_range(start_date, num_rows)
        
        # Add date column to pandas DataFrame
        pdf['date'] = dates
        
        logger.info(f"Added date column to {file_path}, starting from {start_date} for {num_rows} rows")
        
        # Convert pandas DataFrame to Spark DataFrame without specifying schema
        df = spark.createDataFrame(pdf)
        
        # Convert all numeric columns to float
        for column in df.columns:
            if column != DATE_COLUMN:
                df = df.withColumn(column, col(column).cast("float"))
        
        # Convert date string to timestamp with proper formatting - important for Cube compatibility
        df = df.withColumn(DATE_COLUMN, 
                          to_timestamp(
                              date_format(to_date(col(DATE_COLUMN), "yyyy-MM-dd"), "yyyy-MM-dd HH:mm:ss")
                          )
                         )
        
        # Rename columns - append model_type and prediction_type
        for column in df.columns:
            if column != DATE_COLUMN:
                df = df.withColumnRenamed(column, f"{column}_{model_type}_{prediction_type}")
        
        logger.info(f"Successfully processed {file_path}, resulting in {df.count()} rows")
        logger.info(f"Schema after processing {model_type}_{prediction_type} data:")
        df.printSchema()
        logger.info(f"Sample data from {model_type}_{prediction_type}:")
        df.show(5, truncate=False)
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading or processing {file_path}: {str(e)}")
        return None


def read_gold_indices(spark: SparkSession) -> Optional[DataFrame]:
    """Read existing indices data from gold layer"""
    logger.info(f"Reading existing gold indices from {SOURCE_TABLE}")
    
    try:
        df = spark.table(SOURCE_TABLE)
        if df.rdd.isEmpty():
            logger.warning(f"No data found in gold table {SOURCE_TABLE}")
            return None
        
        # Ensure date column is in timestamp format with proper formatting - important for Cube compatibility
        df = df.withColumn(DATE_COLUMN, 
                          to_timestamp(
                              date_format(col(DATE_COLUMN), "yyyy-MM-dd HH:mm:ss")
                          )
                         )
        
        logger.info(f"Successfully read {df.count()} rows from {SOURCE_TABLE}")
        logger.info("Schema of source gold table:")
        df.printSchema()
        logger.info("Sample data from source gold table:")
        df.show(5, truncate=False)
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading table {SOURCE_TABLE}: {str(e)}")
        return None


def join_dataframes(base_df: DataFrame, dataframes: List[DataFrame]) -> Optional[DataFrame]:
    """Join all prediction dataframes to the base dataframe"""
    if not dataframes:
        logger.error("No prediction dataframes to join")
        return base_df
    
    logger.info(f"Joining {len(dataframes)} prediction dataframes to base data")
    try:
        final_df = base_df
        
        # Join each prediction dataframe to the base
        for i, df in enumerate(dataframes):
            logger.info(f"Joining dataframe {i+1} of {len(dataframes)} to base data")
            
            # Ensure date columns have exactly the same format before joining
            df = df.withColumn(DATE_COLUMN, 
                              to_timestamp(
                                  date_format(col(DATE_COLUMN), "yyyy-MM-dd HH:mm:ss")
                              )
                             )
            final_df = final_df.withColumn(DATE_COLUMN, 
                                          to_timestamp(
                                              date_format(col(DATE_COLUMN), "yyyy-MM-dd HH:mm:ss")
                                          )
                                         )
            
            # Log the column names before join for debugging
            logger.info(f"Columns in base dataframe: {final_df.columns}")
            logger.info(f"Columns in joining dataframe: {df.columns}")
            
            # Perform the join
            final_df = final_df.join(df, on=DATE_COLUMN, how="full")
            
            # Log the count after each join
            logger.info(f"Row count after join {i+1}: {final_df.count()}")
        
        # Order by Date
        final_df = final_df.orderBy(DATE_COLUMN)
        
        # Add ID column starting from 1
        windowSpec = Window.orderBy(DATE_COLUMN)
        final_df = final_df.withColumn(ID_COLUMN, row_number().over(windowSpec))
        
        logger.info(f"Successfully joined all dataframes and added ID column, resulting in {final_df.count()} rows")
        logger.info("Final schema after joining:")
        final_df.printSchema()
        
        # Show sample with ID column
        logger.info("Sample data with ID column:")
        final_df.select(ID_COLUMN, DATE_COLUMN).show(5, truncate=False)
        
        return final_df
        
    except Exception as e:
        logger.error(f"Error joining dataframes: {str(e)}")
        return None


def write_to_iceberg(df: DataFrame, table_name: str, partition_by: List[str]):
    """Write DataFrame to Iceberg table"""
    logger.info(f"Starting write operation to Iceberg table: {table_name}")
    logger.info(f"Partitioning by: {partition_by}")
    logger.info(f"Write mode: overwrite")
    logger.info(f"DataFrame contains {df.count()} rows to write")
    
    try:
        # Ensure timestamp column is properly formatted for storage
        df = df.withColumn(DATE_COLUMN, 
                          to_timestamp(
                              date_format(col(DATE_COLUMN), "yyyy-MM-dd HH:mm:ss")
                          )
                         )
        
        # Ensure ID column is of proper type
        df = df.withColumn(ID_COLUMN, col(ID_COLUMN).cast(LongType()))
        
        # Create a temp view to facilitate SQL transformations if needed
        df.createOrReplaceTempView("pre_write_data")
        
        # Execute SQL to verify timestamp format and ID column
        result = df.sparkSession.sql(f"""
        SELECT 
            {ID_COLUMN},
            {DATE_COLUMN}, 
            COUNT(*) as count 
        FROM 
            pre_write_data 
        GROUP BY 
            {ID_COLUMN}, {DATE_COLUMN} 
        ORDER BY 
            {ID_COLUMN} 
        LIMIT 10
        """)
        
        logger.info("ID and timestamp format verification:")
        result.show(truncate=False)
        
        writer = (
            df.write
            .format("iceberg")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        
        if partition_by:
            # Ensure partition columns exist
            missing_columns = [p for p in partition_by if p not in df.columns]
            if not missing_columns:
                writer = writer.partitionBy(*partition_by)
                logger.info(f"Partitioning by {partition_by}")
            else:
                logger.error(f"Partition columns {missing_columns} not found in DataFrame. Skipping partitioning.")
        
        writer.saveAsTable(table_name)
        logger.info(f"Successfully wrote data to Iceberg table: {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Iceberg table {table_name}: {str(e)}")
        raise


def main():
    """Main function to orchestrate the ETL process for indices predictions"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Sync Indices Gold ETL Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Sync_Indices_Gold_ETL")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # Read the baseline gold indices data
        base_df = read_gold_indices(spark)
        if base_df is None:
            logger.error("Failed to read baseline indices data. Exiting.")
            return
        
        # Read and process prediction files
        dataframes = []
        
        # Future predictions - with dates starting from FUTURE_START_DATE
        srvar_future_df = read_csv_file(
            spark, FUTURE_SRVAR_PATH, "SrVAR", "future", FUTURE_START_DATE)
        if srvar_future_df is not None:
            dataframes.append(srvar_future_df)
            logger.info(f"Added SrVAR future data with {srvar_future_df.count()} rows")
        
        varnn_future_df = read_csv_file(
            spark, FUTURE_VARNN_PATH, "VARNN", "future", FUTURE_START_DATE)
        if varnn_future_df is not None:
            dataframes.append(varnn_future_df)
            logger.info(f"Added VARNN future data with {varnn_future_df.count()} rows")
        
        # Historical predictions - with dates starting from PREDICT_START_DATE
        srvar_predict_df = read_csv_file(
            spark, PREDICT_SRVAR_PATH, "SrVAR", "predict", PREDICT_START_DATE)
        if srvar_predict_df is not None:
            dataframes.append(srvar_predict_df)
            logger.info(f"Added SrVAR predict data with {srvar_predict_df.count()} rows")
        
        varnn_predict_df = read_csv_file(
            spark, PREDICT_VARNN_PATH, "VARNN", "predict", PREDICT_START_DATE)
        if varnn_predict_df is not None:
            dataframes.append(varnn_predict_df)
            logger.info(f"Added VARNN predict data with {varnn_predict_df.count()} rows")
        
        if not dataframes:
            logger.error("No prediction data was successfully loaded. Exiting.")
            return
        
        # Join all dataframes (this will also add the ID column)
        final_df = join_dataframes(base_df, dataframes)
        if final_df is None:
            logger.error("Failed to join dataframes. Exiting.")
            return
        
        # Convert all columns with null values to float type
        for column in final_df.columns:
            if column != DATE_COLUMN and column not in ["year", "month", ID_COLUMN]:
                final_df = final_df.withColumn(column, col(column).cast("float"))
        
        # Add partition columns
        final_df = (
            final_df
            .withColumn("year", year(col(DATE_COLUMN)))
            .withColumn("month", month(col(DATE_COLUMN)))
        )
        
        # Ensure timestamp is correctly formatted for Cube compatibility
        final_df = final_df.withColumn(DATE_COLUMN, 
                                      to_timestamp(
                                          date_format(col(DATE_COLUMN), "yyyy-MM-dd HH:mm:ss")
                                      )
                                     )
        
        # Verify timestamp values and ID column
        logger.info("Verifying timestamp values and ID column before writing:")
        final_df.select(ID_COLUMN, DATE_COLUMN).distinct().orderBy(ID_COLUMN).show(10, truncate=False)
        
        # Log sample of final dataframe before writing
        logger.info("Sample data from final dataframe before writing:")
        final_df.show(5, truncate=False)
        
        # Write to gold layer
        write_to_iceberg(final_df, TARGET_TABLE, partition_by=["year", "month"])
        
        # Verify data can be read back
        try:
            verify_df = spark.table(TARGET_TABLE)
            count = verify_df.count()
            logger.info(f"Verification check: Successfully read {count} rows from {TARGET_TABLE}")
            
            # Show sample data for verification with focus on the ID and date columns
            logger.info("Sample IDs and dates from target table to verify proper storage:")
            verify_df.select(ID_COLUMN, DATE_COLUMN).distinct().orderBy(ID_COLUMN).show(20, truncate=False)
            
            logger.info("Sample complete rows from target table:")
            verify_df.orderBy(col(ID_COLUMN)).limit(5).show(truncate=False)
            
            # Verify ID range - FIX: Using proper syntax for min and max aggregations
            min_id_df = verify_df.select(min_func(col(ID_COLUMN)).alias("min_id"))
            max_id_df = verify_df.select(max_func(col(ID_COLUMN)).alias("max_id"))
            
            min_id = min_id_df.collect()[0]["min_id"]
            max_id = max_id_df.collect()[0]["max_id"]
            
            logger.info(f"ID column range: Min ID = {min_id}, Max ID = {max_id}")
            
        except Exception as e:
            logger.error(f"Verification failed: Cannot read from {TARGET_TABLE}: {str(e)}")
        
        end_time = datetime.now()
        logger.info(f"Job completed successfully in {end_time - start_time}")
        
    except Exception as e:
        logger.critical(f"Job failed with critical error: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            end_time = datetime.now()
            logger.info(f"Spark Session stopped. Total job duration: {end_time - start_time}")


if __name__ == "__main__":
    main()