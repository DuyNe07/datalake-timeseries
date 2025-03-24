import os
import re
import logging
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("InsertRawToBronze")

def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_namespace(spark: SparkSession, namespace: str):
    logger.info(f"Creating namespace {namespace} if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

def clean_folder_name(folder_name: str) -> str:
    # Làm sạch tên thư mục và chuyển tất cả ký tự thành chữ thường
    cleaned = re.sub(r'\s+', '_', folder_name)  # Thay thế khoảng trắng bằng dấu _
    return cleaned.lower()  # Chuyển thành chữ thường

def clean_column_names(df: DataFrame) -> DataFrame:
    new_columns = [re.sub(r'[^A-Za-z0-9]+', '', col_name) for col_name in df.columns]
    df_cleaned = df.toDF(*new_columns)
    return df_cleaned

def read_csv_files(spark: SparkSession, folder_path: str) -> DataFrame:
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
    
    # Ghép nối các DataFrame lại với nhau
    df_concat = reduce(DataFrame.unionByName, df_list)
    
    # Làm sạch tên cột
    df_cleaned = clean_column_names(df_concat)
    logger.info("Data processing completed.")
    
    return df_cleaned

def write_to_iceberg(df: DataFrame, table_name: str):
    logger.info(f"Writing data to table: {table_name}")
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    logger.info("Data write completed.")

def process_folder(spark: SparkSession, folder_path: str, folder_name: str):
    logger.info(f"Processing folder: '{folder_name}'")
    
    # Đọc tất cả CSV files trong thư mục
    df_list = read_csv_files(spark, folder_path)
    if not df_list:
        logger.warning(f"Skipping folder {folder_name} - no CSV files found.")
        return
    
    # Xử lý và làm sạch dữ liệu
    df_processed = process_dataframes(df_list)
    if df_processed is None:
        logger.warning(f"Failed to process data for {folder_name}.")
        return
    
    # Xác định tên bảng Iceberg đích, chuyển tên thư mục thành chữ thường
    cleaned_folder = clean_folder_name(folder_name)
    table_name = f"datalake.bronze.{cleaned_folder}"
    
    # Ghi dữ liệu vào bảng
    write_to_iceberg(df_processed, table_name)
    logger.info(f"Successfully processed folder: {folder_name}")

def main():
    try:
        # Khởi tạo Spark session
        spark = create_spark_session("CSV_to_Iceberg")
        
        # Tạo namespace nếu chưa tồn tại
        create_namespace(spark, "datalake.bronze")
        
        # Xử lý từng thư mục trong thư mục gốc
        base_dir = "/src/data/"
        for folder in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder)
            if os.path.isdir(folder_path):
                process_folder(spark, folder_path, folder)
        
        logger.info("All folders processed successfully.")
    
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
