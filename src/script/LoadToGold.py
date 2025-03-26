import os
import re
import logging
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, year, avg

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("InsertRawToBronze")

def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_namespace(spark: SparkSession, namespace: str):
    logger.info(f"Creating namespace {namespace} if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

def read_from_iceberg(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Đọc dữ liệu từ bảng Iceberg.
    """
    logger.info(f"Reading data from table: {table_name}")
    return spark.read.format("iceberg").table(table_name)

def process_and_group(df: DataFrame) -> DataFrame:
    """
    Xử lý gom nhóm theo năm và tính trung bình cho các cột còn lại.
    """
    logger.info("Processing data by grouping by year and calculating average for remaining columns.")
    
    # Gom nhóm theo năm và tính trung bình cho các cột còn lại
    grouped_df = df.groupBy(year("date").alias("year")).agg(*[
        avg(col).alias(f"{col}_avg") for col in df.columns if col != "date"
    ])
    
    return grouped_df

def write_to_iceberg(df: DataFrame, table_name: str):
    """
    Ghi dữ liệu vào bảng Iceberg.
    """
    logger.info(f"Writing processed data to table: {table_name}")
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    logger.info("Data write completed.")

def process_silver_to_gold(spark: SparkSession):
    logger.info("Starting to process Silver data and write to Gold.")
    
    # Đọc dữ liệu từ 2 bảng Silver: datalake.silver.gold và datalake.silver.vnd_usd
    df_gold = read_from_iceberg(spark, "datalake.silver.gold")
    df_vnd_use = read_from_iceberg(spark, "datalake.silver.vnd_usd")
    
    # Xử lý dữ liệu và gom nhóm theo năm, tính trung bình
    df_gold_processed = process_and_group(df_gold)
    df_vnd_usd_processed = process_and_group(df_vnd_use)
    
    # Ghi dữ liệu đã xử lý vào bảng Gold
    write_to_iceberg(df_gold_processed, "datalake.gold.gold")
    write_to_iceberg(df_vnd_usd_processed, "datalake.gold.vnd_usd")
    
    logger.info("Silver to Gold processing completed successfully.")

def main():
    try:
        # Khởi tạo Spark session
        spark = create_spark_session("Silver_to_Gold")
        
        # Tạo namespace nếu chưa tồn tại
        create_namespace(spark, "datalake.gold")
        
        # Xử lý dữ liệu từ Silver sang Gold
        process_silver_to_gold(spark)
        
        logger.info("Silver to Gold processing completed successfully.")
    
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
