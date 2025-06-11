from pyspark.sql import SparkSession
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Silver to Gold Transformation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "nessie") \
    .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://datalake/warehouse") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .getOrCreate()

# Read from Silver layer - specifically Gold table
silver_df = spark.sql("SELECT * FROM iceberg.datalake.silver.Gold")

# No transformations - direct copy to Gold layer
silver_df.writeTo("iceberg.datalake.gold.Gold") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.metadata.compression-codec", "gzip") \
    .create()

print("Silver to Gold transformation completed for Gold table")
