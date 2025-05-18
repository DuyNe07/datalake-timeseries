import logging
from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col, to_timestamp, regexp_replace, lit, when, isnan  # type: ignore
from pyspark.sql.types import DateType, DoubleType  # type: ignore

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BronzeToGold")

def create_spark_session(app_name: str) -> SparkSession:
    logger.info("Initializing Spark Session...")
    return SparkSession.builder.appName(app_name)\
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "8g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()

def create_namespace(spark: SparkSession, namespace: str):
    logger.info(f"Creating namespace {namespace} if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

def get_bronze_tables(spark: SparkSession) -> list:
    logger.info("Listing bronze tables...")
    try:
        tables = spark.sql("SHOW TABLES IN datalake.bronze").collect()
        list_tables_name = [table.tableName for table in tables]
        print("Bronze tables found:", list_tables_name)
        return list_tables_name
    except Exception as e:
        logger.error(f"Error listing tables in datalake.bronze: {e}", exc_info=True)
        return []

def process_table(spark: SparkSession, bronze_table: str) -> DataFrame:
    logger.info(f"Processing bronze table: {bronze_table}")

    try:
        df = spark.table(bronze_table)
        logger.info(f"Original schema for {bronze_table}:")
        df.printSchema()
    except Exception as e:
        logger.error(f"Error reading bronze table {bronze_table}: {e}", exc_info=True)
        return None

    # Ensure we have required columns
    required_columns = ['Date', 'Price']
    for col_name in required_columns:
        if col_name not in df.columns:
            logger.warning(f"Column {col_name} not found in {bronze_table}. Adding empty column.")
            df = df.withColumn(col_name, lit(None))

    # Select only required columns: Date and Price
    df = df.select('Date', 'Price')
    
    # Try multiple date formats for parsing
    date_formats = ["MM/dd/yyyy", "yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy"]
    
    # Try each date format
    for date_format in date_formats:
        try:
            temp_df = df.withColumn("parsedDate", to_timestamp(col("Date"), date_format).cast("date"))
            if temp_df.filter(col("parsedDate").isNotNull()).count() > 0:
                logger.info(f"Successfully parsed dates using format: {date_format}")
                df = temp_df.withColumn("Date", col("parsedDate")).drop("parsedDate")
                break
        except Exception as e:
            logger.warning(f"Failed to parse dates with format {date_format}: {e}")
    
    # Validate date column
    null_dates = df.filter(col("Date").isNull()).count()
    if null_dates > 0:
        logger.warning(f"{null_dates} null dates found after conversion in {bronze_table}")
    
    # Filter for non-null dates, remove duplicates, and filter for dates from 1995 onwards
    df = df.filter(col("Date").isNotNull())
    df = df.dropDuplicates(["Date"])
    df = df.filter(col("Date") >= "1995-01-01")
    
    # Clean numeric Price values:
    # 1. Replace commas with empty string (,)
    # 2. Keep decimal points (.)
    # 3. Cast to double
    df = df.withColumn("Price", 
                      regexp_replace(col("Price"), ",", ""))
    df = df.withColumn("Price", 
                      when(col("Price").isin("", "-", "."), lit(None))
                      .otherwise(col("Price").cast(DoubleType())))
    
    # Check for any remaining null values after conversion
    null_count = df.filter(col("Price").isNull() | isnan(col("Price"))).count()
    if null_count > 0:
        logger.warning(f"{null_count} null/NaN values found in Price column after conversion")
    
    # Count final records
    final_count = df.count()
    logger.info(f"Finished processing {bronze_table}. Final record count: {final_count}")
    
    return df

def merge_all_tables(spark: SparkSession, table_dfs: dict) -> DataFrame:
    """
    Merge all tables by date and rename price columns to table names
    """
    logger.info("Merging all tables by date...")
    
    if not table_dfs:
        logger.error("No tables to merge")
        return None
    
    # Get first table to start with
    first_table_name = list(table_dfs.keys())[0]
    result_df = table_dfs[first_table_name].withColumnRenamed("Price", first_table_name)
    
    # Join with the rest of the tables
    for table_name, df in list(table_dfs.items())[1:]:
        # Rename Price column to table name
        df = df.withColumnRenamed("Price", table_name)
        
        # Join on Date column
        result_df = result_df.join(df, "Date", "outer")
    
    # Make sure Date column is properly formatted
    result_df = result_df.withColumn("Date", col("Date").cast(DateType()))
    
    # Sort by date
    result_df = result_df.orderBy("Date")
    
    logger.info(f"Merged table schema:")
    result_df.printSchema()
    
    return result_df

def write_to_iceberg(df: DataFrame, table_name: str):
    logger.info(f"Writing data to table: {table_name}")
    try:
        # Show schema before writing
        logger.info(f"Schema for table {table_name} before writing:")
        df.printSchema()
        
        # Write with table properties for better compatibility
        df.write \
            .format("iceberg") \
            .option("write.format.default", "parquet") \
            .option("write.metadata.compression-codec", "gzip") \
            .option("write.distribution-mode", "hash") \
            .option("write.object-storage.enabled", "true") \
            .mode("overwrite") \
            .saveAsTable(table_name)
            
        # Verify table was created successfully
        logger.info(f"Data write completed for {table_name}")
    except Exception as e:
        logger.error(f"Error writing data to {table_name}: {e}", exc_info=True)

def process_tables(spark: SparkSession, tables: list):
    if not tables:
        logger.error("No tables found to process.")
        return
    
    processed_tables = {}
    
    # Process each table individually
    for table_name in tables:
        bronze_table_full = f"datalake.bronze.{table_name}"
        
        try:
            # Process the table data
            df_processed = process_table(spark, bronze_table_full)
            if df_processed is not None and df_processed.count() > 0:
                processed_tables[table_name] = df_processed
                logger.info(f"Successfully processed {table_name}")
            else:
                logger.warning(f"Skipping {table_name} due to processing failure or empty dataset.")
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}", exc_info=True)
    
    # Merge all processed tables
    if processed_tables:
        logger.info(f"Merging {len(processed_tables)} tables together")
        merged_df = merge_all_tables(spark, processed_tables)
        
        if merged_df is not None and merged_df.count() > 0:
            # Write merged table to gold
            gold_table_full = "datalake.gold.all_gold_data"
            write_to_iceberg(merged_df, gold_table_full)
            
            # Verify data can be read back
            try:
                verify_df = spark.table(gold_table_full)
                count = verify_df.count()
                logger.info(f"Verification check: Successfully read {count} rows from {gold_table_full}")
            except Exception as e:
                logger.error(f"Verification failed: Cannot read from {gold_table_full}: {e}")
        else:
            logger.error("Merged table is empty or failed to be created")
    else:
        logger.error("No tables were successfully processed for merging")

def main():
    try:
        # Create Spark session
        spark = create_spark_session("Load_To_Gold")
        
        # Create gold namespace if not exists
        create_namespace(spark, "datalake.gold")
        
        # Define tables to process
        tables = get_bronze_tables(spark)
        
        if tables:
            # Process tables and create merged table
            process_tables(spark, tables)
            logger.info("All tables processed and merged to gold successfully.")
        else:
            logger.error("No tables specified to process.")
        
    except Exception as e:
        logger.error(f"Job failed due to: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()