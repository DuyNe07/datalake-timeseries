#!/usr/bin/env python
# LoadIndicesGold.py - Silver to Gold - Stock Indices
import logging
from datetime import datetime
from typing import List, Dict, Optional
from functools import reduce

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col, year, month, row_number, to_timestamp, date_format  # type: ignore
from pyspark.sql.window import Window  # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SilverToGold_Indices")

# Constants
SOURCE_CATALOG = "datalake"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.silver"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
TARGET_TABLE = f"{TARGET_NAMESPACE}.indices"
DATE_COLUMN = "date"
PRICE_COLUMN = "price" 
ID_COLUMN = "id"

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
    
    logger.info(f"Source Namespace: {SOURCE_NAMESPACE}")
    logger.info(f"Target Namespace: {TARGET_NAMESPACE}")
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

def list_tables(spark: SparkSession, namespace: str) -> List[str]:
    """List all tables in a namespace"""
    try:
        logger.info(f"Listing tables in namespace: {namespace}")
        tables_df = spark.sql(f"SHOW TABLES IN {namespace}")
        
        if tables_df.count() == 0:
            logger.warning(f"No tables found in namespace {namespace}")
            return []
            
        table_names = [row.tableName for row in tables_df.select("tableName").collect()]
        logger.info(f"Found {len(table_names)} tables: {', '.join(table_names)}")
        return table_names
        
    except Exception as e:
        logger.error(f"Failed to list tables in {namespace}: {str(e)}")
        return []

def read_silver_table(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """Read a table from silver layer, selecting only Date and Price columns"""
    full_table_name = f"{SOURCE_NAMESPACE}.{table_name}"
    logger.info(f"Reading from silver table: {full_table_name}")
    
    try:
        df = spark.table(full_table_name)
        if df.rdd.isEmpty():
            logger.warning(f"No data found in silver table {full_table_name}.")
            return None
        
        # Check if required columns exist
        required_columns = [DATE_COLUMN, PRICE_COLUMN]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Table {table_name}: Missing required columns: {missing_columns}")
            logger.info(f"Available columns: {df.columns}")
            return None
        
        # Select only Date and Price columns
        result_df = df.select(col(DATE_COLUMN), col(PRICE_COLUMN))
        
        # Rename Price column to table name
        result_df = result_df.withColumnRenamed(PRICE_COLUMN, table_name)
        
        logger.info(f"Successfully read {result_df.count()} rows from {full_table_name}")
        return result_df
    
    except Exception as e:
        logger.error(f"Error reading table {full_table_name}: {str(e)}")
        return None

def join_dataframes(dfs: List[DataFrame]) -> Optional[DataFrame]:
    """Join all dataframes on Date column using reduce and add ID column"""
    if not dfs:
        logger.error("No dataframes to join")
        return None
    
    logger.info(f"Joining {len(dfs)} dataframes on Date column")
    try:
        # Use reduce to join all dataframes
        final_df = reduce(lambda df1, df2: df1.join(df2, on=DATE_COLUMN, how='outer'), dfs)
        
        # Order by Date first
        final_df = final_df.orderBy(DATE_COLUMN)
        
        # Add ID column that counts from 1, ordered by date
        logger.info("Adding sequential ID column ordered by date")
        window_spec = Window.orderBy(DATE_COLUMN)
        final_df = final_df.withColumn("id", row_number().over(window_spec))
        
        # Add partition columns
        final_df = (
            final_df
            .withColumn("year", year(col(DATE_COLUMN)))
            .withColumn("month", month(col(DATE_COLUMN)))
        )
        
        logger.info(f"Successfully joined dataframes, resulting in {final_df.count()} rows")
        logger.info("Final schema:")
        final_df.printSchema()
        
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
        # Convert date to timestamp for Iceberg compatibility
        df = df.withColumn(DATE_COLUMN, 
                          to_timestamp(
                              date_format(col(DATE_COLUMN), "yyyy-MM-dd HH:mm:ss")
                          )
                         )
        
        # Ensure ID column is of proper type
        df = df.withColumn(ID_COLUMN, col(ID_COLUMN).cast("long"))
        
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
    """Main function to orchestrate the ETL process for stock indices"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Silver to Gold ETL Job for Stock Indices")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Silver_to_Gold_Indices_ETL")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # Define tables to process - All index tables
        tables_to_join = ['russell2000', 'dow_jones', 'msci_world', 'nasdaq100', 's_p500', 'gold']
        
        # Read tables and prepare for joining
        dataframes = []
        for table_name in tables_to_join:
            df = read_silver_table(spark, table_name)
            if df is not None:
                dataframes.append(df)
                logger.info(f"Added {table_name} table to join list")
            else:
                logger.warning(f"Skipping {table_name} due to errors or missing data")
        
        if len(dataframes) < 1:
            logger.error("Not enough tables to join. Need at least one table.")
            return
        
        # Join dataframes
        joined_df = join_dataframes(dataframes)
        if joined_df is None:
            logger.error("Failed to join dataframes. Exiting.")
            return
        
        # Write to gold layer
        write_to_iceberg(joined_df, TARGET_TABLE, partition_by=["year", "month"])
        
        # Verify data can be read back
        try:
            verify_df = spark.table(TARGET_TABLE)
            count = verify_df.count()
            logger.info(f"Verification check: Successfully read {count} rows from {TARGET_TABLE}")
            
            # Show sample data with ID column for verification
            logger.info("Sample data from target table (showing first 5 rows):")
            verify_df.select(ID_COLUMN, DATE_COLUMN, *[col for col in verify_df.columns 
                                                     if col not in [ID_COLUMN, DATE_COLUMN, "year", "month"]])\
                    .orderBy(ID_COLUMN).show(5, truncate=False)
                    
            # Also show specific column counts to verify structure
            column_count = len(verify_df.columns)
            logger.info(f"Total number of columns in target table: {column_count}")
            
            # List all column names for verification
            logger.info(f"All columns in target table: {verify_df.columns}")
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