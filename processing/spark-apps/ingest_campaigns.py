import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, to_timestamp, col

# Configure logging with more detail
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Campaign Data Ingestion')
    parser.add_argument('--date', required=True, help='Processing date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    logger.debug(f"Starting campaign ingestion with date: {args.date}")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Campaign Data Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    logger.debug("Spark session initialized")
    
    try:
        # Create bronze database if it doesn't exist
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        logger.debug("Ensured bronze database exists")
        
        # Check if the table already exists and show its structure
        tables = spark.sql("SHOW TABLES IN bronze").collect()
        logger.debug(f"Tables in bronze database: {[row.tableName for row in tables]}")
        
        if any(row.tableName == 'raw_marketing_campaigns' for row in tables):
            logger.debug("raw_marketing_campaigns table already exists, showing schema:")
            spark.sql("DESCRIBE TABLE bronze.raw_marketing_campaigns").show(truncate=False)
            
            # Check current record count
            count = spark.sql("SELECT COUNT(*) as count FROM bronze.raw_marketing_campaigns").collect()[0]['count']
            logger.debug(f"Current record count in bronze.raw_marketing_campaigns: {count}")
        
        # Create marketing campaigns table if it doesn't exist
        spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.raw_marketing_campaigns (
                campaign_id STRING,
                campaign_name STRING,
                campaign_type STRING,
                start_date DATE,
                end_date DATE,
                budget DECIMAL(15,2),
                target_audience STRING,
                ingestion_time TIMESTAMP
            ) USING ICEBERG
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        logger.debug("Ensured raw_marketing_campaigns table exists")
        
        # Load from JSON file
        sample_data_path = "/opt/processing/sample_data/campaigns.json"
        logger.debug(f"Reading campaigns from {sample_data_path}")
        
        # Read the JSON file with multiLine option
        campaigns_df = spark.read.option("multiLine", "true").json(sample_data_path)
        
        # Debug: Print schema and sample data
        logger.debug("JSON file schema:")
        campaigns_df.printSchema()
        
        logger.debug("Sample data from JSON file:")
        campaigns_df.show(3, truncate=False)
        
        # Update ingestion time to current time
        campaigns_df = campaigns_df.withColumn("ingestion_time", lit(datetime.now().isoformat()))
        logger.debug("Added ingestion_time column")
        
        # Show campaign data info
        record_count = campaigns_df.count()
        logger.info(f"Loaded {record_count} campaigns from JSON file")
        
        # Convert types
        campaigns_df = campaigns_df.select(
            col("campaign_id"),
            col("campaign_name"),
            col("campaign_type"),
            to_date(col("start_date")).alias("start_date"),
            to_date(col("end_date")).alias("end_date"),
            col("budget").cast("decimal(15,2)").alias("budget"),
            col("target_audience"),
            to_timestamp(col("ingestion_time")).alias("ingestion_time")
        )
        logger.debug("Converted column types")
        
        # Show the processed data before writing
        logger.debug("Processed data (first 5 rows):")
        campaigns_df.show(5, truncate=False)
        
        # Write to Bronze table
        logger.debug("Writing to bronze.raw_marketing_campaigns...")
        campaigns_df.write \
            .mode("append") \
            .insertInto("bronze.raw_marketing_campaigns")
        
        logger.info(f"Ingested {record_count} campaign records")
        
        # Verify the data was written
        after_count = spark.sql("SELECT COUNT(*) as count FROM bronze.raw_marketing_campaigns").collect()[0]['count']
        logger.debug(f"Record count in bronze.raw_marketing_campaigns after insertion: {after_count}")
        
        # Show some sample data from the table
        logger.debug("Sample data from bronze.raw_marketing_campaigns after insertion:")
        spark.sql("SELECT * FROM bronze.raw_marketing_campaigns LIMIT 5").show(truncate=False)
        
    except Exception as e:
        logger.error(f"Error ingesting marketing campaigns: {str(e)}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
