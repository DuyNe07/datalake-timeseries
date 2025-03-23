import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, last, row_number
from pyspark.sql.window import Window

# Setup logging in English
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BronzeToSilver")

# Initialize Spark Session
def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name).getOrCreate()

# Create namespace in Iceberg if not exists
def create_namespace(spark: SparkSession, namespace: str):
    logger.info(f"Creating namespace {namespace} if not exists...") 
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

# Process one bronze table:
# 1. Select only ['Date', 'Price', 'Open', 'High', 'Low']
# 2. Convert Date to datetime (timestamp)
# 3. Remove duplicate rows
# 4. Replace null values using forward fill
# 5. Add an ID column at the beginning
def process_bronze_table(spark: SparkSession, bronze_table: str) -> DataFrame:
    logger.info(f"Processing table {bronze_table}")
    
    # Read data from the bronze table
    df = spark.table(bronze_table)
    
    # Step 1: Select required columns
    df = df.select('Date', 'Price', 'Open', 'High', 'Low')
    
    # Step 2: Convert Date column to datetime (timestamp)
    # Assuming the Date column is in a format convertible by cast("timestamp")
    df = df.withColumn("Date", col("Date").cast("timestamp"))
    
    # Step 3: Remove duplicate rows
    df = df.dropDuplicates()
    
    # Step 4: Replace null values using forward fill (last non-null value up to current row)
    # Create a window ordered by Date
    windowSpec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
    for column in ['Price', 'Open', 'High', 'Low']:
        df = df.withColumn(column, last(col(column), ignorenulls=True).over(windowSpec))
    
    # Step 5: Add an auto-increment ID column and move it to the first position
    windowSpecId = Window.orderBy("Date")
    df = df.withColumn("ID", row_number().over(windowSpecId))
    df = df.select("ID", "Date", "Price", "Open", "High", "Low")
    
    return df

# Write the processed DataFrame to an Iceberg table in silver namespace
def write_to_iceberg(df: DataFrame, table_name: str):
    logger.info(f"Writing data to table: {table_name}")
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
    logger.info("Data write completed.")

def main():
    try:
        spark = create_spark_session("Bronze_to_Silver")
        
        # Create silver namespace if not exists
        create_namespace(spark, "datalake.silver")
        
        # List all bronze tables in namespace datalake.bronze
        bronze_tables = spark.sql("SHOW TABLES IN datalake.bronze").collect()
        if not bronze_tables:
            logger.error("No bronze tables found in namespace 'datalake.bronze'.")
            return
        
        # Process each bronze table and write to silver
        for row in bronze_tables:
            table_name = row.tableName
            bronze_table_full = f"datalake.bronze.{table_name}"
            silver_table_full = f"datalake.silver.{table_name}"
            
            logger.info(f"Reading bronze table: {bronze_table_full}")
            df_processed = process_bronze_table(spark, bronze_table_full)
            
            logger.info(f"Writing processed data to silver table: {silver_table_full}")
            write_to_iceberg(df_processed, silver_table_full)
        
        logger.info("All tables processed and written to silver successfully.")
        
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
