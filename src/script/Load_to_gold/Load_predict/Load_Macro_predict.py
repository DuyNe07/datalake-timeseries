#!/usr/bin/env python
# process_prediction_data.py - Process prediction data and write to Iceberg

import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, year, month, to_date, to_timestamp, 
    date_format, row_number, monotonically_increasing_id, lit
)
from pyspark.sql.types import LongType, StringType, StructType, StructField  # type: ignore
from pyspark.sql.window import Window  # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PredictionDataProcessor")

# Constants
CSV_BASE_DIR = "/src/data/predict"
NAME_TABLE = "macro"  # This can be changed as needed
SrVAR_DIR = "SrVAR"
VARNN_DIR = "VARNN"
DATE_COLUMN = "date"
ID_COLUMN = "id"
START_DATE = "2019-03-03"
P = 2203
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
today = datetime.today().date()
today = today.strftime('%d_%m_%Y')


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


def read_csv_with_model_suffix(spark: SparkSession, file_path: str, model_type: str) -> Optional[DataFrame]:
    """Read CSV file and rename columns with model type suffix"""
    logger.info(f"Reading {model_type} data from {file_path}")
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        # Read the CSV file directly with Spark
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        # Rename columns with model_type suffix
        for column in df.columns:
            df = df.withColumnRenamed(column, f"{column}_{model_type}")
        
        # Add monotonically increasing ID for joining later
        df = df.withColumn("row_id", monotonically_increasing_id())
        
        logger.info(f"Successfully read {file_path} and renamed columns with {model_type} suffix")
        logger.info(f"Schema after processing {model_type} data:")
        df.printSchema()
        logger.info(f"Sample data from {model_type}:")
        df.show(5, truncate=False)
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading or processing {file_path}: {str(e)}")
        return None


def merge_prediction_data(srvar_df: DataFrame, varnn_df: DataFrame) -> DataFrame:
    """Merge prediction dataframes with date dataframe using row index"""
    logger.info("Merging prediction dataframes")

    try:
        # Instead of using window function, use a simpler approach
        # Add row index to the date dataframe
        srvar_df = srvar_df.withColumn("row_id", monotonically_increasing_id())
        
        # First join SrVAR data
        windowSpec = Window.orderBy("row_id")
        srvar_with_rownum = srvar_df.withColumn("row_num", row_number().over(windowSpec))

        # Then join VARNN data
        varnn_with_rownum = varnn_df.withColumn("row_num", row_number().over(windowSpec))
        
        merged_df = srvar_with_rownum.join(
            varnn_with_rownum.drop("row_id"),
            "row_num",
            "left"
        )
        
        # Drop temporary columns
        merged_df = (
            merged_df
            .withColumn(ID_COLUMN, col("row_id"))
            .withColumn(DATE_COLUMN, col("date_VARNN"))
            .drop("date_SrVAR", "date_VARNN", "row_id", "row_num")
        )
        
        logger.info(f"Successfully merged all dataframes, resulting in {merged_df.count()} rows")
        logger.info("Final schema after merging:")
        merged_df.printSchema()
        logger.info("Sample data after merging (showing all columns):")
        merged_df.show(5, truncate=False)
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Error merging dataframes: {str(e)}")
        raise


def write_to_iceberg(df: DataFrame, table_name: str, partition_by: List[str]):
    """Write DataFrame to Iceberg table"""
    logger.info(f"Starting write operation to Iceberg table: {table_name}")
    logger.info(f"Partitioning by: {partition_by}")
    
    try:
        # Convert date to timestamp for Iceberg compatibility
        df = df.withColumn(DATE_COLUMN, col(DATE_COLUMN).cast("timestamp"))
        
        # Ensure ID column is of proper type
        df = df.withColumn(ID_COLUMN, col(ID_COLUMN).cast(LongType()))
        
        # Add partitioning columns
        df = (
            df
            .withColumn("year", year(col(DATE_COLUMN)))
            .withColumn("month", month(col(DATE_COLUMN)))
        )
        
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
    """Main function to process prediction data and write to Iceberg"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Prediction Data Processing Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Prediction_Data_Processor")
        
        # Define file paths
        srvar_file_path = f"{CSV_BASE_DIR}/{SrVAR_DIR}/{NAME_TABLE}_predict_{today}.csv"
        varnn_file_path = f"{CSV_BASE_DIR}/{VARNN_DIR}/{NAME_TABLE}_predict_{today}.csv"
        
        logger.info(f"Processing files: {srvar_file_path} and {varnn_file_path}")
        logger.info(f"Using NAME_TABLE: {NAME_TABLE}")
        logger.info(f"Using P={P} days starting from {START_DATE}")
        
        # Step 2: Read CSV files and rename columns
        srvar_df = read_csv_with_model_suffix(spark, srvar_file_path, "SrVAR")
        varnn_df = read_csv_with_model_suffix(spark, varnn_file_path, "VARNN")
        
        if srvar_df is None or varnn_df is None:
            logger.error("Failed to read required CSV files")
            return
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # Step 3: Merge all dataframes
        final_df = merge_prediction_data(srvar_df, varnn_df)
        
        # Step 4: Write to Iceberg with proper partitioning
        target_table = f"{TARGET_NAMESPACE}.{NAME_TABLE}_predict"
        logger.info(f"Target table: {target_table}")
        write_to_iceberg(final_df, target_table, partition_by=["year", "month"])
        
        # Verify data can be read back
        try:
            verify_df = spark.table(target_table)
            count = verify_df.count()
            logger.info(f"Verification check: Successfully read {count} rows from {target_table}")
            
            # Show ALL columns in verification output
            logger.info("Sample data from target table (showing all columns):")
            verify_df.orderBy(ID_COLUMN).show(10, truncate=False)
            
            # Also show specific column counts to verify structure
            column_count = len(verify_df.columns)
            logger.info(f"Total number of columns in target table: {column_count}")
            
            # List all column names for verification
            logger.info(f"All columns in target table: {verify_df.columns}")
            
        except Exception as e:
            logger.error(f"Verification failed: Cannot read from {target_table}: {str(e)}")
        
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