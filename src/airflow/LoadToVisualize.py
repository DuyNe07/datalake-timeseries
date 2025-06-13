import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, year, month, row_number, monotonically_increasing_id, to_timestamp, date_format, lit
)
from pyspark.sql.types import LongType, TimestampType  # type: ignore
from pyspark.sql.window import Window  # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("FutureionDataProcessor")

# Constants
CSV_BASE_DIR = "/src/data"
SrVAR_DIR = "SrVAR"
VARNN_DIR = "VARNN"
DATE_COLUMN = "date"
ID_COLUMN = "id"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"
today = datetime.today().date()
today = today.strftime('%d_%m_%Y')
# Test


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
        
        logger.info(f"Successfully read {file_path} and renamed columns with {model_type} suffix")
        logger.info(f"Schema after processing {model_type} data:")
        df.printSchema()
        logger.info(f"Sample data from {model_type}:")
        df.show(5)
        
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
        merged_df = srvar_df.join(
            varnn_df,
            srvar_df["date_SrVAR"] == varnn_df["date_VARNN"],
            "inner"
        )

        # Optionally add a row ID if needed
        merged_df = merged_df.withColumn(ID_COLUMN, monotonically_increasing_id())

        # Set a unified date column if needed
        merged_df = merged_df.withColumn(DATE_COLUMN, col("date_VARNN"))

        # Drop the original date columns to avoid duplicates
        merged_df = merged_df.drop("date_SrVAR", "date_VARNN")
        
        logger.info(f"Successfully merged all dataframes, resulting in {merged_df.count()} rows")
        logger.info("Final schema after merging:")
        merged_df.printSchema()
        logger.info("Sample data after merging (showing all columns):")
        merged_df.show(10)
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Error merging dataframes: {str(e)}")
        raise


def write_to_iceberg(spark: SparkSession, df: DataFrame, table_name: str):
    """Write DataFrame to Iceberg table using overwritePartitions with deduplication logic"""
    logger.info(f"Starting write operation to Iceberg table: {table_name}")
    
    try:
        # Convert date to timestamp and ID to LongType
        df = df.withColumn(DATE_COLUMN, col(DATE_COLUMN).cast("timestamp"))
        
        # Ensure ID column is of proper type
        df = (df.withColumn(ID_COLUMN, col(ID_COLUMN).cast(LongType()))
                .withColumn("year", year(col(DATE_COLUMN)))
                .withColumn("month", month(col(DATE_COLUMN)))
        )
        df.show()

        # Alias for clarity
        min_date = df.agg({"date": "min"}).collect()[0][0]
        max_date = df.agg({"date": "max"}).collect()[0][0]

        # Filter existing table to only affected range
        existing_df = spark.table(table_name) \
                        .filter((col(DATE_COLUMN) >= lit(min_date)) & (col(DATE_COLUMN) <= lit(max_date)))

        # Merge logic (existing in range + new in range, where new overrides existing)
        new_df = df.alias("new")
        existing_df = existing_df.alias("existing")

        # Get updates (dates to replace)
        updates_df = existing_df.join(new_df, on=DATE_COLUMN, how="inner") \
                                .select([col(f"new.{c}") for c in existing_df.columns])

        # Get inserts (dates only in new)
        inserts_df = new_df.join(existing_df.select(DATE_COLUMN), on=DATE_COLUMN, how="left_anti") \
                        .select([col(f"new.{c}") for c in new_df.columns])

        # Get untouched old data outside of update range
        unchanged_df = spark.table(table_name) \
                            .filter((col(DATE_COLUMN) < lit(min_date)) | (col(DATE_COLUMN) > lit(max_date)))

        # Final merge: unchanged + updates + inserts
        merged_df = unchanged_df.unionByName(updates_df).unionByName(inserts_df)

        # Overwrite affected partitions
        merged_df.writeTo(table_name).overwritePartitions()

        logger.info(f"Successfully wrote data to Iceberg table: {table_name}")
    
    except Exception as e:
        logger.error(f"Failed to write to Iceberg table {table_name}: {str(e)}")
        raise

def verify_data(spark: SparkSession, target_table: str):
    # Verify data can be read back
    try:
        verify_df = spark.table(target_table)
        count = verify_df.count()
        logger.info(f"Verification check: Successfully read {count} rows from {target_table}")
        
        # Show ALL columns in verification output
        logger.info("Sample data from target table (showing all columns):")
        verify_df.orderBy(ID_COLUMN).show(10)
        
        # Also show specific column counts to verify structure
        column_count = len(verify_df.columns)
        logger.info(f"Total number of columns in target table: {column_count}")
        
        # List all column names for verification
        logger.info(f"All columns in target table: {verify_df.columns}")
        
    except Exception as e:
        logger.error(f"Verification failed: Cannot read from {target_table}: {str(e)}")
        raise

def process_data(spark: SparkSession, table_name: str, post_fix: str):
    # Define file paths
    srvar_file_path = f"{CSV_BASE_DIR}/{post_fix}/{SrVAR_DIR}/{table_name}_{post_fix}_{today}.csv"
    varnn_file_path = f"{CSV_BASE_DIR}/{post_fix}/{VARNN_DIR}/{table_name}_{post_fix}_{today}.csv"
    
    logger.info(f"Processing files: {srvar_file_path} and {varnn_file_path}")
    logger.info(f"Using NAME_TABLE: {table_name}")
    
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
    target_table = f"{TARGET_NAMESPACE}.{table_name}_{post_fix}"
    logger.info(f"Target table: {target_table}")
    write_to_iceberg(spark, final_df, target_table)
    verify_data(spark, target_table)
    return None


def main(table_name: str, post_fix: str):
    """Main function to process prediction data and write to Iceberg"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Prediction Data Processing Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Prediction_Data_Processor")
        
        process_data(spark, table_name, post_fix)
        
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