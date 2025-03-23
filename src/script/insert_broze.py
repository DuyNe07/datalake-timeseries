import os
import re
import logging
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SparkJob")

# Khởi tạo Spark Session với tên ứng dụng được cung cấp.
def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

# Tạo namespace trong Iceberg
def create_namespace(spark: SparkSession, full_namespace: str):
    logger.info(f"Creating namespace {full_namespace} if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}")

# Làm sạch tên thư mục
def clean_folder_name(folder_name: str) -> str:
    cleaned = re.sub(r'\s+', '_', folder_name)
    return cleaned

# Làm sạch tên cột
def clean_column_names(df: DataFrame) -> DataFrame:
    new_columns = [re.sub(r'[^A-Za-z0-9]+', '', col_name) for col_name in df.columns]
    df_cleaned = df.toDF(*new_columns)
    return df_cleaned

# Đọc và xử lý tất cả các file CSV trong thư mục
def process_csv_files(spark: SparkSession, folder_path: str) -> DataFrame:
    logger.info(f"Processing folder: {folder_path}")

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
    
    # Ghép nối các DataFrame lại với nhau
    df_concat = reduce(DataFrame.unionByName, df_list)

    df_cleaned = clean_column_names(df_concat)
    logger.info("Data processing completed.")

    return df_cleaned

# Ghi DataFrame vào bảng Iceberg
def write_to_iceberg(df: DataFrame, table_name: str):
    logger.info(f"Writing data to table: {table_name}")

    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    logger.info("Data write completed.")

def main():
    try:
        spark = create_spark_session("CSV_to_Iceberg")
        create_namespace(spark, "datalake.bronze")
        
        base_dir = "/src/data/"
        for folder in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder)
            if os.path.isdir(folder_path):
                
                cleaned_folder = clean_folder_name(folder)
                logger.info(f"Processing folder: '{folder}' as '{cleaned_folder}'")
                
                df = process_csv_files(spark, folder_path)
                if df is None:
                    logger.warning(f"Skipping folder {folder} do not contain CSV files.")
                    continue
                
                table_name = f"datalake.bronze.{cleaned_folder}"
                write_to_iceberg(df, table_name)
        
        logger.info("All folders processed successfully.")
    
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
