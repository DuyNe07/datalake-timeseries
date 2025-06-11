import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col, monotonically_increasing_id # type: ignore
from pyspark.sql.types import FloatType  # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("LoadEvaluation")

# Constants
CSV_BASE_DIR = "/src/data/evaluation"
SrVAR_DIR = "SrVAR"
VARNN_DIR = "VARNN"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.gold"


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
        
        logger.info(f"Successfully read {file_path} and renamed columns with {model_type} suffix")
        logger.info(f"Schema after processing {model_type} data:")
        df.printSchema()
        logger.info(f"Sample data from {model_type}:")
        df.show(5)
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading or processing {file_path}: {str(e)}")
        return None


def process_data(spark: SparkSession, table_name: str):
    # Define file paths
    srvar_file_path = f"{CSV_BASE_DIR}/{SrVAR_DIR}_{table_name}_metrics.csv"
    varnn_file_path = f"{CSV_BASE_DIR}/{VARNN_DIR}_{table_name}_metrics.csv"
    
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

    srvar_df = srvar_df.withColumnRenamed("MSE", "MSE_SrVAR") \
                   .withColumnRenamed("RMSE", "RMSE_SrVAR") \
                   .withColumnRenamed("MAE", "MAE_SrVAR")

    varnn_df = varnn_df.withColumnRenamed("MSE", "MSE_VAR") \
                .withColumnRenamed("RMSE", "RMSE_VAR") \
                .withColumnRenamed("MAE", "MAE_VAR")
    
    # Step 3: Merge all dataframes
    df_joined = srvar_df.join(varnn_df.select("variable", "MSE_VAR", "RMSE_VAR", "MAE_VAR"), on="variable", how="inner")

    # Drop index if exists
    if '_c0' in df_joined.columns:
        df_joined = df_joined.drop('_c0')

    # Cast metrics to Float (optional but recommended)
    metrics = ['MSE_SrVAR', 'RMSE_SrVAR', 'MAE_SrVAR', 'MSE_VAR', 'RMSE_VAR', 'MAE_VAR']
    for col_name in metrics:
        df_joined = df_joined.withColumn(col_name, col(col_name).cast(FloatType()))

    df_joined = df_joined.withColumn("id", monotonically_increasing_id())

    # Write to Iceberg table - create or replace
    df_joined.writeTo(f"datalake.gold.{table_name}_eval").createOrReplace()
    return None


def main(table_name: str):
    """Main function to process prediction data and write to Iceberg"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Prediction Data Processing Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Load_Evaluation")
        
        process_data(spark, table_name)
        
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