import os
import re
import logging
from datetime import datetime
from typing import List, Optional, Tuple, Dict

from pyspark.sql import SparkSession, DataFrame # type: ignore
from pyspark.sql.functions import col, input_file_name, year, month, to_timestamp, current_timestamp # type: ignore

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("InsertRawToBronze")

# Constants
BASE_RAW_DATA_DIR = "/src/data/raw/"
TARGET_CATALOG = "datalake"
TARGET_NAMESPACE = f"{TARGET_CATALOG}.bronze"
DATE_FORMATS = [
    'yyyy-MM-dd',
    'dd-MM-yyyy'
]

# Standard column mapping
STANDARD_COLUMNS = {
    'date': ['date', 'datetime', 'time'],
    'price': ['price', 'close'],
    'open': ['open', 'Open'],
    'high': ['high', 'High'],
    'low': ['low'],
    'volume': ['vol', 'vol.', 'volume', 'Volume', 'Vol.'],
    'change': ['change', 'change %'],
    'id': ['id'],
    'adj': ['adj', 'Adj'],
    'price_tip': ['close_tip'],  
    'adj_price': ['adj_close', 'adj close']
}

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
    
    logger.info(f"Target Catalog: {TARGET_CATALOG}")
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

def clean_name(name: str) -> str:
    """Clean folder or column names for SQL use"""
    if not name:
        return "unnamed"
    
    # Replace non-alphanumeric characters with underscore
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Clean up consecutive underscores
    cleaned = re.sub(r"_+", "_", cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip("_")
    # Convert to lowercase
    cleaned = cleaned.lower()
    # Add leading underscore if starts with digit
    if cleaned and cleaned[0].isdigit():
        cleaned = "_" + cleaned
        
    return cleaned

def clean_column_names(df: DataFrame) -> DataFrame:
    """Clean all column names in DataFrame"""
    original_columns = df.columns
    new_columns = [clean_name(col) for col in original_columns]
    
    # Handle duplicate column names after cleaning
    final_columns = []
    seen = {}
    for col in new_columns:
        if col in seen:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            final_columns.append(col)
    
    if original_columns != final_columns:
        logger.warning(f"Duplicate or invalid column names detected after cleaning. Renaming columns: {list(zip(original_columns, final_columns))}")
        return df.toDF(*final_columns)
    
    return df

def standardize_schema(df: DataFrame) -> DataFrame:
    """Map columns to standard names and ensure consistent schema"""
    # Log original columns for debugging
    logger.info(f"Original columns: {df.columns}")
    
    # Create a mapping from actual column names to standard column names
    column_mapping = {}
    
    # First, find all potential matches
    potential_matches = {}
    for std_col, possible_names in STANDARD_COLUMNS.items():
        for actual_col in df.columns:
            if actual_col.lower() in [name.lower() for name in possible_names]:
                if std_col not in potential_matches:
                    potential_matches[std_col] = []
                potential_matches[std_col].append(actual_col)
    
    # Resolve the mappings, ensuring no duplicate target columns
    used_columns = set()
    for std_col, matched_columns in potential_matches.items():
        if len(matched_columns) == 1:
            # Only one match for this standard column
            column_mapping[matched_columns[0]] = std_col
            used_columns.add(matched_columns[0])
        else:
            # Multiple matches - need to create unique names
            for idx, matched_col in enumerate(matched_columns):
                if idx == 0:
                    # First match gets the standard name
                    column_mapping[matched_col] = std_col
                else:
                    # Subsequent matches get suffixed names
                    column_mapping[matched_col] = f"{std_col}_{idx}"
                used_columns.add(matched_col)
    
    # Include any remaining columns with their original names
    for col in df.columns:
        if col not in used_columns and col != "_source_file":
            column_mapping[col] = col
            
    logger.info(f"Column mapping: {column_mapping}")
    
    # Create a standardized DataFrame
    std_df = df
    
    # Rename columns according to mapping
    for orig_col, std_col in column_mapping.items():
        std_df = std_df.withColumnRenamed(orig_col, std_col)
    
    # Ensure source_file column exists
    if "_source_file" in std_df.columns:
        std_df = std_df.withColumnRenamed("_source_file", "source_file")
    
    logger.info(f"Standardized columns: {std_df.columns}")
    return std_df

def read_all_csv_in_folder(spark: SparkSession, folder_path: str) -> Optional[DataFrame]:
    today = datetime.today().strftime("%d_%m_%Y")
    """Read all CSV files in a folder ending with today's date into a single DataFrame"""
    csv_path_pattern = f"*_{today}.csv"  # note the underscore before date to match your file naming
    
    logger.info(f"Attempting to read all CSV files matching pattern: {csv_path_pattern} in folder: {folder_path}")
    
    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option("multiLine", "true")
            .option("escape", "\"")
            .option("pathGlobFilter", csv_path_pattern)  # Filter files by pattern here
            .csv(folder_path)
        )
        
        if df.rdd.isEmpty():
            logger.warning(f"No data found in CSV files matching pattern {csv_path_pattern} at {folder_path}")
            return None
        
        # Add source file name column
        df = df.withColumn("_source_file", input_file_name())
        
        logger.info(f"Successfully read data from CSV files in {folder_path}. Initial partitions: {df.rdd.getNumPartitions()}")
        
        # Log schema for debugging
        logger.info("DataFrame schema:")
        for field in df.schema.fields:
            logger.info(f"  {field.name}: {field.dataType}")
        
        return df

    except Exception as e:
        logger.error(f"Error reading CSV files from {folder_path} with pattern {csv_path_pattern}: {str(e)}")
        return None

def parse_date_column(df: DataFrame, date_col_name: str) -> DataFrame:
    """Try to parse the date column using multiple formats"""
    logger.info(f"Attempting to parse date column '{date_col_name}' using formats: {DATE_FORMATS}")
    
    # Create a new DataFrame with a parsed date column
    parsed_df = df
    
    # Try each date format in succession
    for idx, date_format in enumerate(DATE_FORMATS):
        try:
            logger.info(f"Trying date format #{idx+1}: {date_format}")
            # Create temporary column with this format
            temp_df = parsed_df.withColumn(
                "parsed_date", 
                to_timestamp(col(date_col_name), date_format)
            )
            
            # Count non-null values with this format
            valid_count = temp_df.filter(col("parsed_date").isNotNull()).count()
            logger.info(f"Format {date_format} produced {valid_count} valid dates")
            
            if valid_count > 0:
                # Use this format since it worked for some values
                parsed_df = temp_df
                logger.info(f"Using format '{date_format}' for date parsing")
                break
        except Exception as e:
            logger.warning(f"Format '{date_format}' failed with error: {str(e)}")
    
    return parsed_df

def add_partition_columns(df: DataFrame) -> Tuple[DataFrame, List[str]]:
    """Add year and month partition columns from date column"""
    logger.info("Attempting to add partition columns...")
    
    # Find date column (case insensitive)
    date_col_candidates = [c for c in df.columns if c.lower() == "date"]
    
    if not date_col_candidates:
        logger.warning("No 'date' column found. Skipping partitioning.")
        return df, []
    
    date_col_name = date_col_candidates[0]
    logger.info(f"Found date column: '{date_col_name}'")
    
    try:
        # Parse the date column
        df_with_parsed_date = parse_date_column(df, date_col_name)
        
        # Check if we successfully parsed any dates
        if "parsed_date" not in df_with_parsed_date.columns:
            logger.warning("Failed to parse date column with any format. Skipping partitioning.")
            return df, []
            
        # Add year and month columns from the parsed date
        df_partitioned = (
            df_with_parsed_date
            .withColumn("year", year(col("parsed_date")))
            .withColumn("month", month(col("parsed_date")))
        )
        
        # Check if partition columns were successfully added
        null_years = df_partitioned.filter(col("year").isNull()).count()
        if null_years > 0:
            logger.warning(f"{null_years} rows have NULL year values after date parsing")
        
        logger.info(f"Successfully added partition columns 'year', 'month' from '{date_col_name}'")
        
        # Return the DataFrame with the partition columns and without the temporary parsed_date column
        return df_partitioned.drop("parsed_date"), ["year", "month"]
            
    except Exception as e:
        logger.error(f"Error adding partition columns: {str(e)}")
        logger.error("Continuing without partition columns")
        return df, []

def check_duplicate_columns(df: DataFrame) -> DataFrame:
    """Check and fix any duplicate column names in the DataFrame"""
    # Get column names and their counts
    column_counts = {}
    for col_name in df.columns:
        column_counts[col_name] = column_counts.get(col_name, 0) + 1
    
    # Check for duplicates
    duplicate_columns = [col for col, count in column_counts.items() if count > 1]
    if duplicate_columns:
        logger.warning(f"Found duplicate column names: {duplicate_columns}")
        
        # Rename duplicates
        new_df = df
        seen_columns = {}
        for col_index, col_name in enumerate(df.columns):
            if col_name in seen_columns:
                # Rename this duplicate
                seen_columns[col_name] += 1
                new_name = f"{col_name}_{seen_columns[col_name]}"
                logger.info(f"Renaming duplicate column '{col_name}' to '{new_name}'")
                new_df = new_df.withColumnRenamed(df.columns[col_index], new_name)
            else:
                seen_columns[col_name] = 0
        
        return new_df
    
    return df

def write_to_iceberg(df: DataFrame, table_name: str, partition_by: List[str]):
    """Write DataFrame to Iceberg table"""
    logger.info(f"Starting write operation to Iceberg table: {table_name}")
    logger.info(f"Partitioning by: {partition_by}")
    logger.info(f"Write mode: append")
    logger.info(f"DataFrame contains {df.count()} rows to write")

    print(df.show(5))
    
    # Check for duplicate column names before writing
    df = check_duplicate_columns(df)
    df = df.withColumn("inserted", current_timestamp()).withColumn("date", date_format("date", "MM-dd-yyyy"))

    if any(sub in table_name for sub in ['inflation', 'interest']):
        df = df.drop(col('_source_file'))
    elif not any(sub in table_name for sub in ['us_dollar', 'russell2000', 'oil', 'dow_jones', 'usd_vnd', 'nasdaq100']):
        logger.info("Dropping column 'volume'")
        df = df.drop(col("volume"))

    try:
        writer = (
            df.write
            .format("iceberg")
            .mode("append")
            .option("iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .option("mergeSchema", "true")
        )

        if partition_by and all(p in df.columns for p in partition_by):
            # Ensure partition columns have valid data
            null_partition_count = df.filter(
                ' OR '.join([f"{p} IS NULL" for p in partition_by])
            ).count()
            
            if null_partition_count > 0:
                logger.warning(f"{null_partition_count} rows have NULL partition values")
            
            if null_partition_count < df.count():  # Only partition if some rows have valid values
                writer = writer.partitionBy(*partition_by)
                logger.info(f"Partitioning by {partition_by}")
            else:
                logger.warning("Skipping partitioning as all partition columns contain NULL values")
        else:
            logger.warning(f"Partition columns missing from DataFrame. Skipping partitioning.")
        
        writer.saveAsTable(table_name)
        logger.info(f"Successfully wrote data to Iceberg table: {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Iceberg table {table_name}: {str(e)}")
        raise

def process_folder(spark: SparkSession, folder_path: str, folder_name: str):
    """Process a single folder: read CSV, clean columns, add partitions, write to Iceberg"""
    logger.info(f"--- Processing folder: '{folder_name}' ---")
    start_time = datetime.now()
    
    try:
        # 1. Read all CSV files in folder
        df = read_all_csv_in_folder(spark, folder_path)
        if df is None:
            logger.warning(f"Skipping folder '{folder_name}' - No valid data found.")
            return
        
        # 2. Clean column names
        df_cleaned = clean_column_names(df)
        
        # 3. Standardize schema
        df_standardized = standardize_schema(df_cleaned)
        
        # 4. Add partition columns
        try:
            df_partitioned, partition_cols = add_partition_columns(df_standardized)
        except Exception as e:
            logger.error(f"Error adding partition columns: {str(e)}", exc_info=True)
            df_partitioned = df_standardized
            partition_cols = []
            
        # 5. Write to Iceberg
        table_name = clean_name(folder_name)
        if not table_name:
            logger.error(f"Invalid folder name '{folder_name}' after cleaning.")
            return
            
        full_table_name = f"{TARGET_NAMESPACE}.{table_name}"
        write_to_iceberg(df_partitioned, full_table_name, partition_cols)
        
        end_time = datetime.now()
        logger.info(f"--- Successfully processed folder '{folder_name}' in {end_time - start_time} ---")
        
    except Exception as e:
        logger.error(f"Error processing folder '{folder_name}': {str(e)}", exc_info=True)
        raise

def main():
    """Main function to orchestrate the ETL process"""
    spark = None
    start_time = datetime.now()
    logger.info("Starting CSV to Iceberg Bronze Load Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session("CSV_to_Iceberg_Bronze")
        
        # Ensure target namespace exists
        create_namespace_if_not_exists(spark, TARGET_NAMESPACE)
        
        # Check raw data directory
        if not os.path.exists(BASE_RAW_DATA_DIR) or not os.path.isdir(BASE_RAW_DATA_DIR):
            logger.error(f"Base raw data directory not found: {BASE_RAW_DATA_DIR}")
            return
        
        # List folders to process
        try:
            folders_to_process = [d for d in os.listdir(BASE_RAW_DATA_DIR) 
                                if os.path.isdir(os.path.join(BASE_RAW_DATA_DIR, d))]
        except Exception as e:
            logger.error(f"Could not list directories in {BASE_RAW_DATA_DIR}: {str(e)}")
            return
        
        if not folders_to_process:
            logger.warning(f"No folders found to process in {BASE_RAW_DATA_DIR}")
            return
            
        logger.info(f"Found {len(folders_to_process)} folders to process: {', '.join(folders_to_process)}")
        
        # Process each folder
        processed_count = 0
        failed_count = 0
        
        for folder_name in folders_to_process:
            folder_path = os.path.join(BASE_RAW_DATA_DIR, folder_name)
            try:
                process_folder(spark, folder_path, folder_name)
                processed_count += 1
            except Exception as e:
                logger.error(f"An unexpected error occurred processing folder '{folder_name}'. See previous logs.")
                failed_count += 1
        
        # Summarize results
        logger.info("--- Job Summary ---")
        logger.info(f"Total folders found: {len(folders_to_process)}")
        logger.info(f"Successfully processed folders: {processed_count}")
        logger.info(f"Failed folders: {failed_count}")
        
    except Exception as e:
        logger.critical(f"Job failed with critical error: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            end_time = datetime.now()
            logger.info(f"Spark Session stopped. Total job duration: {end_time - start_time}")