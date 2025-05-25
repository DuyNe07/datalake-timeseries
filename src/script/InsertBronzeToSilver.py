#!/usr/bin/env python
# InsertBronzeToSilver.py - Bronze to Silver Iceberg ETL
import logging
from datetime import datetime
from typing import List, Optional, Tuple, Dict

from pyspark.sql import SparkSession, DataFrame # type: ignore
from pyspark.sql.functions import (col, to_timestamp, year, month, regexp_replace, when, # type: ignore
                                  date_format, to_date, sequence, explode, lit, min as min_,
                                  max as max_, count, last, row_number, concat, current_timestamp) # type: ignore
import pyspark.sql.functions as F # type: ignore
from pyspark.sql.window import Window # type: ignore
from pyspark.sql.types import StringType, DoubleType, TimestampType, DateType # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("InsertBronzeToSilver")

# Constants
SOURCE_CATALOG = "datalake"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.bronze"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.silver"
START_DATE = "1995-01-05"  # Start date for continuous date generation
END_DATE = "2025-03-07"    # End date for continuous date generation

# Financial columns to be processed
FINANCIAL_COLUMNS = ["price", "open", "high", "low", "volume", "cpi", "inflation_rate", "interest_rate"]

# Date column
DATE_COLUMN = "date"

def create_spark_session(app_name: str) -> SparkSession:
    """Initialize Spark session with required configurations"""
    logger.info("Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    
    logger.info(f"Spark Session initialized successfully. Spark version: {spark.version}")
    
    # Log important configs
    conf = spark.sparkContext.getConf()
    s3_endpoint = conf.get("spark.hadoop.fs.s3a.endpoint", "N/A")
    catalog_type = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.type", "N/A")
    catalog_uri = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.uri", "N/A")
    warehouse = conf.get(f"spark.sql.catalog.{TARGET_CATALOG}.warehouse", "N/A")
    
    logger.info(f"Source Namespace: {SOURCE_NAMESPACE}")
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

def list_tables(spark: SparkSession, namespace: str) -> List[str]:
    """List all tables in a namespace"""
    try:
        logger.info(f"Listing tables in namespace: {namespace}")
        tables_df = spark.sql(f"SHOW TABLES IN {namespace}")
        
        if tables_df.count() == 0:
            logger.warning(f"No tables found in namespace {namespace}")
            return []
            
        table_names = [row.tableName for row in tables_df.select("tableName").collect()]
        logger.info(f"Found {len(table_names)} tables: {', '.join(table_names)}")
        return table_names
        
    except Exception as e:
        logger.error(f"Failed to list tables in {namespace}: {str(e)}")
        return []

def standardize_columns(df: DataFrame) -> DataFrame:
    """Standardize column names and consolidate price columns"""
    logger.info("Standardizing column names and types...")
    
    # Check for price columns that need consolidation
    price_columns = [col_name for col_name in df.columns 
                    if 'price' in col_name.lower() or 'close' in col_name.lower()]
    
    if len(price_columns) > 1:
        logger.info(f"Found multiple price-related columns: {price_columns}")
        
        # If there's no main price column, create one
        if 'price' not in [col.lower() for col in df.columns]:
            logger.info("Creating consolidated 'price' column")
            df = df.withColumn("price", F.coalesce(*[col(c) for c in price_columns]))
    
    # Ensure date column has correct case
    if DATE_COLUMN not in df.columns:
        date_cols = [c for c in df.columns if c.lower() == DATE_COLUMN.lower()]
        if date_cols:
            logger.info(f"Renaming '{date_cols[0]}' to '{DATE_COLUMN}'")
            df = df.withColumnRenamed(date_cols[0], DATE_COLUMN)
    
    return df

def clean_and_cast_columns(df: DataFrame) -> DataFrame:
    """Clean and cast columns to appropriate types"""
    logger.info("Cleaning and casting columns to appropriate types...")
    
    # Ensure date column is date type
    if DATE_COLUMN in df.columns:
        df = df.withColumn(
                DATE_COLUMN,
                F.coalesce(
                    to_timestamp(col(DATE_COLUMN), "MM-dd-yyyy")
                ).cast("date")
            )
    
    # Process financial columns
    for column in [c for c in df.columns if c in FINANCIAL_COLUMNS]:
        if column in df.columns:
            # First check if we need to clean string values
            sample = df.select(column).limit(5).collect()
            needs_cleaning = any(
                isinstance(row[0], str) and (',' in row[0] or '%' in row[0])
                for row in sample if row[0] is not None
            )
            
            if needs_cleaning:
                # Clean commas and percent signs
                df = df.withColumn(
                    column,
                    when(col(column).cast("string").isNotNull(),
                        regexp_replace(
                            regexp_replace(col(column).cast("string"), ",", ""),
                            "%", ""
                        )
                    ).otherwise(col(column))
                )
                logger.info(f"Cleaned string values in column '{column}'")
            
            # Cast to float
            df = df.withColumn(column, col(column).cast("float"))
            logger.info(f"Converted column '{column}' to float type")
    
    return df

def create_date_range_df(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with continuous date range"""
    logger.info(f"Creating date range from {START_DATE} to {END_DATE}...")
    
    date_range_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{START_DATE}'), 
            to_date('{END_DATE}'), 
            interval 1 day
        )) as {DATE_COLUMN}
    """)
    
    logger.info(f"Created continuous date range with {date_range_df.count()} dates")
    return date_range_df

def analyze_data_distribution(df: DataFrame, table_name: str):
    """Analyze data distribution to detect potential issues"""
    logger.info(f"Analyzing data distribution for {table_name}...")
    
    # Check date distribution by year
    year_counts = df.groupBy(year(DATE_COLUMN).alias("year")).count().orderBy("year")
    year_counts.show(25)
    
    # Get min and max dates
    min_max_dates = df.agg(
        min_(DATE_COLUMN).alias("min_date"),
        max_(DATE_COLUMN).alias("max_date")
    ).collect()[0]
    
    logger.info(f"Date range: {min_max_dates.min_date} to {min_max_dates.max_date}")
    
    # Check for gaps in the data
    if min_max_dates.min_date and min_max_dates.max_date:
        total_days = (min_max_dates.max_date - min_max_dates.min_date).days + 1
        actual_days = df.select(DATE_COLUMN).distinct().count()
        
        logger.info(f"Date coverage: {actual_days} dates out of {total_days} days " +
                  f"({actual_days/total_days:.2%})")

def segment_based_forward_fill(df: DataFrame, table_name: str) -> DataFrame:
    """Apply forward fill using date segments to prevent long repetitive sequences"""
    logger.info(f"Applying segment-based forward fill for {table_name}")
    
    # Identify columns that need filling
    columns_to_fill = [c for c in df.columns 
                      if c != DATE_COLUMN and c not in ['year', 'month', 'source_file', 'inserted']]
    
    # Add segment columns for more granular forward fill
    df = df.withColumn("year_segment", year(col(DATE_COLUMN)))
    df = df.withColumn("quarter_segment", 
                      concat(year(col(DATE_COLUMN)), 
                            F.floor((month(col(DATE_COLUMN))-1)/3)))
    df = df.withColumn("month_segment",
                      concat(year(col(DATE_COLUMN)), month(col(DATE_COLUMN))))
    
    # Apply forward fill separately for each column
    for column in columns_to_fill:
        # Check nulls before fill
        #null_count = df.filter(col(column).isNull()).count()
        #logger.info(f"Column '{column}' has {null_count} NULL values before fill")
        
        # Create window specs for each segment granularity
        window_spec_all = Window.orderBy(DATE_COLUMN).rowsBetween(Window.unboundedPreceding, 0)
        window_spec_year = Window.partitionBy("year_segment").orderBy(DATE_COLUMN).rowsBetween(Window.unboundedPreceding, 0)
        window_spec_quarter = Window.partitionBy("quarter_segment").orderBy(DATE_COLUMN).rowsBetween(Window.unboundedPreceding, 0)
        window_spec_month = Window.partitionBy("month_segment").orderBy(DATE_COLUMN).rowsBetween(Window.unboundedPreceding, 0)
        
        # First try filling with the most granular segmentation (month)
        df = df.withColumn(f"{column}_filled_month", last(col(column), True).over(window_spec_month))
        
        # If still null, try quarter segmentation
        df = df.withColumn(f"{column}_filled_quarter", 
                         when(col(f"{column}_filled_month").isNull(), 
                             last(col(column), True).over(window_spec_quarter))
                         .otherwise(col(f"{column}_filled_month")))
        
        # If still null, try year segmentation
        df = df.withColumn(f"{column}_filled_year",
                         when(col(f"{column}_filled_quarter").isNull(),
                             last(col(column), True).over(window_spec_year))
                         .otherwise(col(f"{column}_filled_quarter")))
        
        # Finally, if still null, use the global window
        df = df.withColumn(column,
                         when(col(f"{column}_filled_year").isNull(),
                             last(col(column), True).over(window_spec_all))
                         .otherwise(col(f"{column}_filled_year")))
        
        # Drop the temporary columns
        df = df.drop(f"{column}_filled_month", f"{column}_filled_quarter", f"{column}_filled_year")
        
        # Check nulls after fill
        #null_count = df.filter(col(column).isNull()).count()
        #logger.info(f"Column '{column}' has {null_count} NULL values after fill")
    
    # Drop temporary segment columns
    return df.drop("year_segment", "quarter_segment", "month_segment")

def process_with_forward_fill(spark: SparkSession, df: DataFrame, table_name: str) -> DataFrame:
    """Process DataFrame with continuous dates and forward fill"""
    logger.info(f"Processing {table_name} with forward fill...")
    
    # Analyze source data first
    logger.info("Analyzing source data distribution...")
    analyze_data_distribution(df, table_name)
    
    # 1. Get the full date range DataFrame
    date_range_df = create_date_range_df(spark)
    
    # 2. Join with existing data
    logger.info("Joining with full date range...")
    full_df = date_range_df.join(df, on=DATE_COLUMN, how="left")
    
    # 3. Order by date for forward fill
    full_df = full_df.orderBy(DATE_COLUMN)
    
    # 4. Apply segment-based forward fill for all tables
    result_df = segment_based_forward_fill(full_df, table_name)
    
    # 5. Add/ensure year and month columns for partitioning
    if "year" not in result_df.columns or "month" not in result_df.columns:
        result_df = result_df.withColumn("year", year(col(DATE_COLUMN)))
        result_df = result_df.withColumn("month", month(col(DATE_COLUMN)))
    
    # 6. Validate results
    columns_to_check = [c for c in result_df.columns 
                       if c != DATE_COLUMN and c not in ['year', 'month', 'source_file', 'inserted']]
    
    for column in columns_to_check:
        null_count = result_df.filter(col(column).isNull()).count()
        if null_count > 0:
            logger.warning(f"Column '{column}' still has {null_count} NULL values after processing")
    
    # 7. Show sample data
    logger.info("Sample data after processing:")
    result_df.orderBy(DATE_COLUMN).limit(3).show(truncate=False)
    
    return result_df

def transform_data(spark: SparkSession, df: DataFrame, table_name: str) -> DataFrame:
    """Main transformation function"""
    logger.info(f"Transforming data for table {table_name}")
    
    # 1. Standardize column names
    df = standardize_columns(df)
    
    # 2. Clean and cast columns
    df = clean_and_cast_columns(df)
    
    # 3. Filter to include only data from START_DATE onward
    logger.info(f"Filtering data from {START_DATE} onward")
    df = df.filter(col(DATE_COLUMN) >= START_DATE)
    
    # 4. Process with continuous date range and forward fill
    df = process_with_forward_fill(spark, df, table_name)
    
    return df

def check_duplicate_columns(df: DataFrame) -> DataFrame:
    """Ensure there are no duplicate column names"""
    columns = df.columns
    column_counts = {}
    
    for col_name in columns:
        column_counts[col_name] = column_counts.get(col_name, 0) + 1
        
    duplicates = [col for col, count in column_counts.items() if count > 1]
    if duplicates:
        logger.warning(f"Found duplicate columns: {duplicates}")
        
        # Rename duplicates with suffixes
        unique_columns = []
        seen_columns = {}
        
        for col_name in columns:
            if col_name in seen_columns:
                seen_columns[col_name] += 1
                unique_columns.append(f"{col_name}_{seen_columns[col_name]}")
            else:
                seen_columns[col_name] = 0
                unique_columns.append(col_name)
                
        return df.toDF(*unique_columns)
    
    return df

def write_to_iceberg(df: DataFrame, table_name: str, partition_by: List[str]):
    """Write DataFrame to Iceberg table"""
    logger.info(f"Writing to Iceberg table: {table_name}")
    logger.info(f"Partitioning by: {partition_by}")
    
    # Check for duplicate columns
    df = df.drop(col('inserted'))
    df = check_duplicate_columns(df)
    df = df.withColumn("inserted", current_timestamp())

    try:
        # Prepare writer
        writer = (
            df.write
            .format("iceberg")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        
        # Add partitioning if columns exist
        if all(p in df.columns for p in partition_by):
            # Check if partition columns have non-null values
            null_count = df.filter(" OR ".join([f"{p} IS NULL" for p in partition_by])).count()
            
            if null_count < df.count():  # Only partition if some rows have valid values
                writer = writer.partitionBy(*partition_by)
                logger.info(f"Partitioning by {partition_by}")
            else:
                logger.warning("Skipping partitioning - all partition columns contain NULL values")
        else:
            logger.warning(f"Partition columns {partition_by} not found in DataFrame")
        
        # Write to table
        writer.saveAsTable(table_name)
        logger.info(f"Successfully wrote {df.count()} rows to {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to table {table_name}: {str(e)}")
        raise

def process_table(spark: SparkSession, table_name: str):
    """Process a single table from bronze to silver"""
    logger.info(f"=== Processing table: {table_name} ===")
    start_time = datetime.now()
    
    try:
        # 1. Read from bronze
        source_table = f"{SOURCE_NAMESPACE}.{table_name}"
        logger.info(f"Reading from {source_table}")
        
        df = spark.table(source_table)
        if df.rdd.isEmpty():
            logger.warning(f"No data found in {source_table}. Skipping.")
            return
            
        logger.info(f"Read {df.count()} rows from {source_table}")
        
        # 2. Display schema
        logger.info("Table schema:")
        df.printSchema()
        
        # 3. Transform data
        df_transformed = transform_data(spark, df, table_name)
        
        # 4. Write to silver
        target_table = f"{TARGET_NAMESPACE}.{table_name}"
        write_to_iceberg(df_transformed, target_table, ["year", "month"])
        
        # 5. Log success
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Successfully processed {table_name} in {duration} ===")
        
    except Exception as e:
        logger.error(f"Error processing {table_name}: {str(e)}", exc_info=True)
        raise

def main():
    """Main function to orchestrate the ETL process"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting Bronze to Silver ETL Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session("Bronze_to_Silver_ETL")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # List bronze tables to process
        bronze_tables = list_tables(spark, SOURCE_NAMESPACE)
        
        if not bronze_tables:
            logger.warning(f"No tables found to process in {SOURCE_NAMESPACE}")
            return
            
        # Process each table
        processed_count = 0
        failed_count = 0
        
        for table_name in bronze_tables:
            try:
                process_table(spark, table_name)
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to process table {table_name}")
                failed_count += 1

        # Summarize results
        logger.info("=== Job Summary ===")
        logger.info(f"Total tables found: {len(bronze_tables)}")
        logger.info(f"Successfully processed: {processed_count}")
        logger.info(f"Failed: {failed_count}")
        
    except Exception as e:
        logger.critical(f"Job failed with error: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            end_time = datetime.now()
            logger.info(f"Spark session stopped. Total job duration: {end_time - start_time}")

if __name__ == "__main__":
    main()