import os
import re
import logging
import sys
import datetime
from functools import reduce
from pyspark.sql import SparkSession, DataFrame     # type: ignore
from pyspark.sql.functions import col, regexp_replace   # type: ignore
from current_commit import create_commit, save_commit_info
from rollback import rollback_to_commit

sys.path.append('/nessie')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("InsertRawToBronze")

def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_namespace(spark: SparkSession, namespace: str):
    logger.info(f"Creating namespace {namespace} if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

def clean_folder_name(folder_name: str) -> str:
    cleaned = re.sub(r'\s+', '_', folder_name)
    return cleaned.lower()  

def clean_column_names(df: DataFrame) -> DataFrame:
    new_columns = [re.sub(r'[^A-Za-z0-9]+', '', col_name) for col_name in df.columns]
    df_cleaned = df.toDF(*new_columns)
    return df_cleaned

def read_csv_files(spark: SparkSession, folder_path: str) -> list:
    logger.info(f"Reading CSV files from {folder_path}...")

    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        logger.warning(f"No CSV files found in {folder_path}")
        return None

    df_list = []
    for csv_file in csv_files:
        logger.info(f"Reading file: {csv_file}")
        df = (spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(csv_file))
        df_list.append(df)
    
    return df_list

def process_dataframes(df_list: list) -> DataFrame:
    logger.info("Processing and combining DataFrames...")
    if not df_list:
        return None

    df_concat = reduce(DataFrame.unionByName, df_list)
    df_cleaned = clean_column_names(df_concat)
    logger.info("Data processing completed.")

    return df_cleaned

def write_to_iceberg(df: DataFrame, table_name: str):
    logger.info(f"Writing data to table: {table_name}")
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    logger.info("Data write completed.")

def process_folder(spark: SparkSession, folder_path: str, folder_name: str):
    logger.info(f"Processing folder: '{folder_name}'")
    
    try:
        df_list = read_csv_files(spark, folder_path)
        if not df_list:
            logger.warning(f"Skipping folder {folder_name} - no CSV files found.")
            return

        df_processed = process_dataframes(df_list)
        if df_processed is None:
            logger.warning(f"Failed to process data for {folder_name}.")
            return

        cleaned_folder = clean_folder_name(folder_name)
        table_name = f"datalake.bronze.{cleaned_folder}"

        write_to_iceberg(df_processed, table_name)
        logger.info(f"Successfully processed folder: {folder_name}")
    
    except Exception as e:
        logger.error(f"Error processing folder {folder_name}: {e}", exc_info=True)
        raise

def main():
    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    start_commit_name = f"LoadRawToBronze_{current_time}"
    initial_commit_hash = None

    try:
        logger.info(f"Creating initial commit: {start_commit_name}")
        create_commit(start_commit_name)
        initial_commit_hash = save_commit_info(start_commit_name)

        spark = create_spark_session("CSV_to_Iceberg")
        create_namespace(spark, "datalake.bronze")

        base_dir = "/src/data/raw/"
        for folder in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder)
            if os.path.isdir(folder_path):
                process_folder(spark, folder_path, folder)

        end_commit_name = f"LoadRawToBronze_{current_time}_COMPLETED"
        create_commit(end_commit_name)
        logger.info("All folders processed successfully.")
    
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)

        if initial_commit_hash:
            logger.info(f"Rolling back to initial commit: {initial_commit_hash}")
            rollback_to_commit(initial_commit_hash)
        else:
            logger.error("No initial commit hash saved, cannot rollback")
    
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()