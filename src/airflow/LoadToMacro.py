#!/usr/bin/env python
# LoadMacroGold.py - Silver to Gold - Macro Economic Indicators
import logging
from datetime import datetime
from typing import List, Dict, Optional
from functools import reduce

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col, year, month, row_number, to_timestamp, date_format  # type: ignore
from pyspark.sql.window import Window  # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SilverToGold_Macro")

# Constants
SOURCE_CATALOG = "datalake"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.silver"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
TARGET_TABLE = f"{TARGET_NAMESPACE}.macro"
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
        .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "software.amazon.awssdk:s3:2.20.130,"
            "software.amazon.awssdk:auth:2.20.130,"
            "software.amazon.awssdk:regions:2.20.130,"
            "software.amazon.awssdk:sts:2.20.130,"
            "software.amazon.awssdk:kms:2.25.30,"
            "software.amazon.awssdk:glue:2.25.30,"
            "software.amazon.awssdk:dynamodb:2.20.130" 
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access-key-id", "admin")
        .config("spark.hadoop.fs.s3a.secret-access-key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.catalog.datalake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.datalake.type", "rest")
        .config("spark.sql.catalog.datalake.uri", "http://nessie:19120/iceberg")
        .config("spark.sql.catalog.datalake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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

def read_price_table(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """Read a price table from silver layer, selecting only Date and Price columns, renaming Price to table name"""
    full_table_name = f"{SOURCE_NAMESPACE}.{table_name}"
    logger.info(f"Reading price data from silver table: {full_table_name}")
    
    try:
        df = spark.table(full_table_name)
        if df.rdd.isEmpty():
            logger.warning(f"No data found in silver table {full_table_name}.")
            return None
        
        # Choose the latest data rows
        window_spec = Window.partitionBy(DATE_COLUMN).orderBy(col("inserted").desc())
        df_ranked = df.withColumn("rnk", rank().over(window_spec)) 
        df = df_ranked.filter(col("rnk") == 1).drop("rnk")

        # Check if required columns exist
        required_columns = [DATE_COLUMN, PRICE_COLUMN]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Table {table_name}: Missing required columns: {missing_columns}")
            logger.info(f"Available columns: {df.columns}")
            return None
        
        # Select Date and Price columns, rename Price to table_name
        result_df = df.select(col(DATE_COLUMN), col(PRICE_COLUMN)).withColumnRenamed(PRICE_COLUMN, table_name)
        
        logger.info(f"Successfully read {result_df.count()} rows from {full_table_name}")
        return result_df
    
    except Exception as e:
        logger.error(f"Error reading table {full_table_name}: {str(e)}")
        return None

def read_inflation_table(spark: SparkSession) -> Optional[DataFrame]:
    """Read inflation table from silver layer, selecting Date, cpi, and inflation_rate columns"""
    table_name = "inflation"
    full_table_name = f"{SOURCE_NAMESPACE}.{table_name}"
    logger.info(f"Reading inflation data from silver table: {full_table_name}")
    
    try:
        df = spark.table(full_table_name)
        if df.rdd.isEmpty():
            logger.warning(f"No data found in inflation table {full_table_name}.")
            return None

        # Choose the latest data rows
        window_spec = Window.partitionBy(DATE_COLUMN).orderBy(col("inserted").desc())
        df_ranked = df.withColumn("rnk", rank().over(window_spec)) 
        df = df_ranked.filter(col("rnk") == 1).drop("rnk")
        
        # Check if required columns exist
        required_columns = [DATE_COLUMN, "cpi", "inflation_rate"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Inflation table: Missing required columns: {missing_columns}")
            logger.info(f"Available columns: {df.columns}")
            return None
        
        # Select required columns without renaming
        result_df = df.select(col(DATE_COLUMN), col("cpi"), col("inflation_rate"))
        
        logger.info(f"Successfully read {result_df.count()} rows from inflation table")
        return result_df
    
    except Exception as e:
        logger.error(f"Error reading inflation table: {str(e)}")
        return None

def read_interest_table(spark: SparkSession) -> Optional[DataFrame]:
    """Read interest table from silver layer, selecting Date and interest_rate columns"""
    table_name = "interest"
    full_table_name = f"{SOURCE_NAMESPACE}.{table_name}"
    logger.info(f"Reading interest data from silver table: {full_table_name}")
    
    try:
        df = spark.table(full_table_name)
        if df.rdd.isEmpty():
            logger.warning(f"No data found in interest table {full_table_name}.")
            return None

        # Choose the latest data rows
        window_spec = Window.partitionBy(DATE_COLUMN).orderBy(col("inserted").desc())
        df_ranked = df.withColumn("rnk", rank().over(window_spec)) 
        df = df_ranked.filter(col("rnk") == 1).drop("rnk")
        
        # Check if required columns exist
        required_columns = [DATE_COLUMN, "interest_rate"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Interest table: Missing required columns: {missing_columns}")
            logger.info(f"Available columns: {df.columns}")
            return None
        
        # Select required columns without renaming
        result_df = df.select(col(DATE_COLUMN), col("interest_rate"))
        
        logger.info(f"Successfully read {result_df.count()} rows from interest table")
        return result_df
    
    except Exception as e:
        logger.error(f"Error reading interest table: {str(e)}")
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
        final_df = final_df.withColumn(ID_COLUMN, row_number().over(window_spec))
        
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
        
        df.createOrReplaceTempView("incoming_data")

        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING incoming_data AS source
        ON target.{DATE_COLUMN} = source.{DATE_COLUMN}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_sql)
        logger.info(f"Successfully wrote data to Iceberg table: {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Iceberg table {table_name}: {str(e)}")
        raise

def main():
    """Main function to orchestrate the ETL process for Macro economic indicators"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Silver to Gold ETL Job for Macro Economic Indicators")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Silver_to_Gold_Macro_ETL")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # Define price tables to process
        price_tables = ['gold', 'oil', 'us_dollar', 'usd_vnd']
        
        # Read price tables
        dataframes = []
        for table_name in price_tables:
            df = read_price_table(spark, table_name)
            if df is not None:
                dataframes.append(df)
                logger.info(f"Added {table_name} price data to join list")
            else:
                logger.warning(f"Skipping {table_name} due to errors or missing data")
        
        # Read inflation table
        inflation_df = read_inflation_table(spark)
        if inflation_df is not None:
            dataframes.append(inflation_df)
            logger.info("Added inflation data to join list")
        else:
            logger.warning("Skipping inflation data due to errors or missing data")
        
        # Read interest table
        interest_df = read_interest_table(spark)
        if interest_df is not None:
            dataframes.append(interest_df)
            logger.info("Added interest rate data to join list")
        else:
            logger.warning("Skipping interest rate data due to errors or missing data")
        
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