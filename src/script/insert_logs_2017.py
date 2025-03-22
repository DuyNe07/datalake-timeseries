import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SparkJob")

def create_spark_session(app_name):
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_bronze_table(spark):
    logger.info("Creating Bronze namespace and raw_clickstream table if not exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_clickstream (
            event_id STRING,
            ts TIMESTAMP_NTZ,
            ip STRING,
            url STRING
        )
        USING iceberg
        LOCATION 's3://lakehouse/bronze/raw_clickstream'
    """)

def load_and_write_data(spark, input_path, table_name):
    logger.info(f"Reading data from {input_path}...")
    df = (spark.read.option("header", "true")
                 .option("inferSchema", "true")
                 .csv(input_path))

    logger.info(f"Writing data to {table_name}...")
    df.write.format("iceberg").mode("overwrite").save(table_name)
    logger.info("Data write completed.")

def main():
    try:
        spark = create_spark_session("InsertLogs2017")
        create_bronze_table(spark)
        load_and_write_data(spark, "/src/data/logs_2017.csv", "lakehouse.bronze.raw_clickstream")
        logger.info("Job completed successfully.")
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()