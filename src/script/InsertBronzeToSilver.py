import logging
from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col, last, row_number, to_timestamp   # type: ignore
from pyspark.sql.window import Window   # type: ignore

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BronzeToSilver")

def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_namespace(spark: SparkSession, namespace: str):
    logger.info(f"Creating namespace {namespace} if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

def get_bronze_tables(spark: SparkSession) -> list:
    logger.info("Listing bronze tables...")
    try:
        tables = spark.sql("SHOW TABLES IN datalake.bronze").collect()
        return tables
    except Exception as e:
        logger.error(f"Error listing tables in datalake.bronze: {e}", exc_info=True)
        return []

def process_bronze_table(spark: SparkSession, bronze_table: str) -> DataFrame:
    logger.info(f"Processing bronze table: {bronze_table}")

    try:
        df = spark.table(bronze_table)
    except Exception as e:
        logger.error(f"Error reading bronze table {bronze_table}: {e}", exc_info=True)
        return None

    df = df.select('Date', 'Price', 'Open', 'High', 'Low')

    df = df.withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy").cast("date"))
    df = df.dropDuplicates()

    windowSpec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
    for column in ['Price', 'Open', 'High', 'Low']:
        df = df.withColumn(column, last(col(column), ignorenulls=True).over(windowSpec))

    windowSpecId = Window.orderBy("Date")
    df = df.withColumn("ID", row_number().over(windowSpecId))
    df = df.select("ID", "Date", "Price", "Open", "High", "Low")
    df = df.orderBy("ID")

    for column in ['Price', 'Open', 'High', 'Low']:
        df = df.withColumn(column, col(column).cast("double"))
    
    logger.info(f"Finished processing {bronze_table}")
    return df

def write_to_iceberg(df: DataFrame, table_name: str):
    logger.info(f"Writing data to table: {table_name}")
    try:
        df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        logger.info(f"Data write completed for {table_name}")
    except Exception as e:
        logger.error(f"Error writing data to {table_name}: {e}", exc_info=True)

def process_tables(spark: SparkSession, tables: list):
    if not tables:
        logger.error("No bronze tables found in namespace 'datalake.bronze'.")
        return
    
    for row in tables:
        table_name = row.tableName
        bronze_table_full = f"datalake.bronze.{table_name}"
        silver_table_full = f"datalake.silver.{table_name}"
        
        try:
            # Process the table data
            df_processed = process_bronze_table(spark, bronze_table_full)
            if df_processed is not None:
                # Write to silver table
                write_to_iceberg(df_processed, silver_table_full)
                logger.info(f"Successfully processed {table_name}")
            else:
                logger.warning(f"Skipping {table_name} due to processing failure.")
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}", exc_info=True)

def main():
    try:
        # Create Spark session
        spark = create_spark_session("Bronze_to_Silver")
        
        # Create silver namespace if not exists
        create_namespace(spark, "datalake.silver")
        
        # Get all bronze tables
        bronze_tables = get_bronze_tables(spark)
        
        if bronze_tables:
            # Process each bronze table and write to silver
            process_tables(spark, bronze_tables)
            logger.info("All tables processed and written to silver successfully.")
        else:
            logger.error("No tables found to process.")
        
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
