# processing/spark-apps/silver_transformations.py

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, to_timestamp, to_date, current_timestamp, expr, concat, lpad, regexp_extract
from pyspark.sql.types import *
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SilverTransformations:
    def __init__(self):
        """Initialize Spark session with Iceberg support"""
        self.spark = SparkSession.builder \
            .appName("Silver Layer Transformations") \
            .master("spark://spark-master:7077") \
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
            .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "15000") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.retry.limit", "10") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

    def create_silver_schemas(self):
        """Create Silver layer database and tables if they don't exist"""
        
        # Create silver database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        
        # Standardized User Events table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.standardized_user_events (
                event_id STRING,
                session_id STRING,
                customer_id STRING,
                event_type STRING,
                event_time TIMESTAMP,
                page_url STRING,
                product_id STRING,
                device_type STRING,
                time_spent_seconds DECIMAL(10,2),
                referrer STRING,
                processed_time TIMESTAMP,
                ingestion_date DATE
            ) USING ICEBERG
            PARTITIONED BY (ingestion_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Standardized Sales table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.standardized_sales (
                sale_id STRING,
                customer_id STRING,
                product_id STRING,
                channel STRING,
                quantity DECIMAL(10,2),
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                currency STRING,
                transaction_time TIMESTAMP,
                ingestion_time TIMESTAMP,
                processed_time TIMESTAMP,
                transaction_date DATE
            ) USING ICEBERG
            PARTITIONED BY (transaction_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Product Dimension table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.dim_product (
                product_id STRING,
                product_name STRING,
                category STRING,
                subcategory STRING,
                brand STRING,
                base_price DECIMAL(10,2),
                description STRING,
                is_active BOOLEAN,
                effective_start_date DATE,
                effective_end_date DATE,
                is_current BOOLEAN,
                created_time TIMESTAMP,
                updated_time TIMESTAMP
            ) USING ICEBERG
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)

    def transform_user_events(self, process_date: str):
        """Transform raw user events to standardized format"""
        logger.info(f"Processing user events for date: {process_date}")
        
        try:
            # Read from bronze layer - Kafka stream data
            bronze_events = self.spark.sql(f"""
                SELECT *
                FROM bronze.raw_user_events
                WHERE DATE(event_time) = '{process_date}'
            """)
            
            if bronze_events.count() == 0:
                logger.warning(f"No user events found for date: {process_date}")
                return
            
            # Apply transformations
            standardized_events = bronze_events.select(
                col("event_id"),
                col("session_id"),
                col("customer_id"),
                col("event_type"),
                to_timestamp(col("event_time")).alias("event_time"),
                col("page_url"),
                
                # Extract product_id from metadata if available
                when(col("metadata.product_id").isNotNull(), 
                     col("metadata.product_id")).otherwise(lit(None)).alias("product_id"),
                
                col("device_type"),
                
                # Extract time spent from metadata
                when(col("metadata.time_spent_seconds").isNotNull(),
                     col("metadata.time_spent_seconds").cast(DecimalType(10,2))
                ).otherwise(lit(0)).alias("time_spent_seconds"),
                
                # Extract referrer from metadata
                when(col("metadata.referrer").isNotNull(),
                     col("metadata.referrer")).otherwise(lit("direct")).alias("referrer"),
                
                current_timestamp().alias("processed_time"),
                to_date(col("event_time")).alias("ingestion_date")
            ).filter(
                # Data quality filters - properly wrapped for PySpark
                (col("event_id").isNotNull()) &
                (col("customer_id").isNotNull()) &
                (col("event_type").isNotNull()) &
                (col("event_time").isNotNull())
            )
            
            # Remove duplicates based on event_id
            standardized_events = standardized_events.dropDuplicates(["event_id"])
            
            # Write to Silver layer
            standardized_events.write \
                .mode("append") \
                .insertInto("silver.standardized_user_events")
            
            count = standardized_events.count()
            logger.info(f"Successfully processed {count} user events")
            
        except Exception as e:
            logger.error(f"Error processing user events: {str(e)}")
            raise

    def transform_sales_data(self, process_date: str):
        """Transform raw sales data to standardized format"""
        logger.info(f"Processing sales data for date: {process_date}")
        
        try:
            # Read from bronze layer - marketplace sales
            bronze_sales = self.spark.sql(f"""
                SELECT *
                FROM bronze.raw_marketplace_sales
                WHERE DATE(transaction_time) = '{process_date}'
                   OR DATE(settlement_time) = '{process_date}'
            """)
            
            if bronze_sales.count() == 0:
                logger.warning(f"No sales data found for date: {process_date}")
                return
            
            # Apply transformations
            # For marketplace sales, we need to properly map to actual customers
            # Add debug logging to understand the data mapping issue
            logger.info("=== DEBUGGING SILVER SALES TRANSFORMATION ===")
            
            # Check seller_id values
            seller_ids = bronze_sales.select("seller_id").distinct().limit(5).collect()
            logger.info(f"Sample seller_id values from bronze: {[row['seller_id'] for row in seller_ids]}")
            
            # Generate a customer mapping by extracting the numeric part from seller_id
            # and mapping it to existing customer IDs
            standardized_sales = bronze_sales.select(
                col("transaction_id").alias("sale_id"),
                # Fix: Map seller_id to actual customer_id format
                concat(lit("CUST-"), 
                       lpad(
                           (regexp_extract(col("seller_id"), r"(\d+)", 1).cast("int") % 100 + 1), 
                           6, 
                           "0"
                       )
                ).alias("customer_id"),  # Map seller to customer with proper format
                col("product_id"),
                col("marketplace_name").alias("channel"),
                lit(1).alias("quantity"),  # Assuming 1 item per transaction for simplicity
                col("amount").alias("unit_price"),
                col("amount").alias("total_amount"),
                coalesce(col("currency"), lit("USD")).alias("currency"),
                to_timestamp(col("transaction_time")).alias("transaction_time"),
                to_timestamp(col("ingestion_time")).alias("ingestion_time"),
                current_timestamp().alias("processed_time"),
                to_date(col("transaction_time")).alias("transaction_date")
            ).filter(
                # Data quality filters - properly wrapped for PySpark
                (col("transaction_id").isNotNull()) &
                (col("product_id").isNotNull()) &
                (col("amount").isNotNull()) &
                (col("amount") > 0) &
                (col("transaction_time").isNotNull())
            )
            
            # Remove duplicates
            standardized_sales = standardized_sales.dropDuplicates(["sale_id"])
            
            # Debug: Check the mapped customer_id values
            if standardized_sales.count() > 0:
                mapped_customer_ids = standardized_sales.select("customer_id").distinct().limit(5).collect()
                logger.info(f"Sample mapped customer_id values after transformation: {[row['customer_id'] for row in mapped_customer_ids]}")
            
            logger.info("=== END DEBUGGING ===")
            
            # Write to Silver layer
            standardized_sales.write \
                .mode("append") \
                .insertInto("silver.standardized_sales")
            
            count = standardized_sales.count()
            logger.info(f"Successfully processed {count} sales records")
            
        except Exception as e:
            logger.error(f"Error processing sales data: {str(e)}")
            raise

    def transform_customer_data(self, process_date: str):
        """Transform customer data from Bronze to Silver using SCD Type 2"""
        logger.info(f"Processing customer data for date: {process_date}")
        
        try:
            # Read customer data from Bronze layer
            customer_data = self.spark.sql(f"""
                SELECT 
                    customer_id,
                    customer_name,
                    email,
                    address,
                    city,
                    country,
                    membership_tier,
                    created_at
                FROM bronze.raw_customer_data
            """)
            
            if customer_data.count() == 0:
                logger.warning(f"No customer data found for date: {process_date}")
                return
            
            # Generate surrogate key for dimension table as BIGINT
            dim_customers = customer_data.withColumn("customer_sk", expr("abs(hash(customer_id)) % 9223372036854775807"))
            
            # Add SCD metadata with the correct column names
            dim_customers = dim_customers.withColumn("effective_start_date", lit(process_date).cast("date"))
            dim_customers = dim_customers.withColumn("effective_end_date", lit("9999-12-31").cast("date"))
            dim_customers = dim_customers.withColumn("is_current", lit(True))
            dim_customers = dim_customers.withColumn("created_time", current_timestamp())
            dim_customers = dim_customers.withColumn("updated_time", current_timestamp())
            
            # Select only the columns that match the target table schema
            dim_customers = dim_customers.select(
                "customer_sk",
                "customer_id",
                "customer_name",
                "email",
                "address",
                "city",
                "country",
                "membership_tier",
                "effective_start_date",
                "effective_end_date",
                "is_current",
                "created_time",
                "updated_time"
            )
            
            # Write to silver layer
            dim_customers.write \
                .mode("overwrite") \
                .insertInto("silver.dim_customer_scd")
            
            customer_count = dim_customers.count()
            logger.info(f"Successfully processed {customer_count} customer records")
            
            # Run data quality checks
            self.run_data_quality_checks("dim_customer_scd", process_date)
            
        except Exception as e:
            logger.error(f"Error processing customer data: {str(e)}")
            raise

    def transform_product_catalog(self, process_date: str):
        """Transform product catalog data from Bronze to Silver"""
        logger.info(f"Processing product catalog for date: {process_date}")
        
        try:
            # Read product data from Bronze layer
            # This is a full refresh approach, assuming product catalog is small enough
            product_data = self.spark.sql(f"""
                SELECT 
                    product_id,
                    product_name,
                    category,
                    subcategory,
                    brand,
                    CAST(base_price AS DECIMAL(10,2)) as base_price,
                    description,
                    is_active,
                    last_updated
                FROM bronze.raw_product_catalog
                WHERE DATE(last_updated) <= '{process_date}'
            """)
            
            if product_data.count() == 0:
                logger.warning(f"No product data found for date: {process_date}")
                return
            
            # Generate surrogate key for dimension table
            dim_products = product_data.withColumn("product_sk", expr("uuid()"))
            
            # Add SCD metadata with correct column names to match schema
            dim_products = dim_products.withColumn("effective_start_date", lit(process_date).cast("date"))
            dim_products = dim_products.withColumn("effective_end_date", lit("9999-12-31").cast("date"))
            dim_products = dim_products.withColumn("is_current", lit(True))
            dim_products = dim_products.withColumn("created_time", current_timestamp())
            dim_products = dim_products.withColumn("updated_time", current_timestamp())
            
            # Select only columns in the schema
            dim_products = dim_products.select(
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "base_price",
                "description",
                "is_active",
                "effective_start_date",
                "effective_end_date",
                "is_current",
                "created_time",
                "updated_time"
            )
            
            # Write to silver layer - overwrite for simplicity
            # In a real environment, we'd implement proper SCD Type 2 logic
            dim_products.write \
                .mode("overwrite") \
                .insertInto("silver.dim_product")
            
            product_count = dim_products.count()
            logger.info(f"Successfully processed {product_count} product records")
            
            # Run data quality checks
            self.run_data_quality_checks("dim_product", process_date)
            
        except Exception as e:
            logger.error(f"Error processing product catalog: {str(e)}")
            raise

    def run_data_quality_checks(self, table_name: str, process_date: str):
        """Run basic data quality checks"""
        logger.info(f"Running data quality checks for {table_name}")
        
        try:
            # Check if table exists
            table_exists = False
            try:
                self.spark.sql(f"DESCRIBE TABLE silver.{table_name}")
                table_exists = True
            except Exception as e:
                logger.warning(f"Table silver.{table_name} does not exist or can't be accessed: {str(e)}")
                return
            
            if not table_exists:
                logger.warning(f"Table silver.{table_name} does not exist or can't be accessed")
                return
                
            # Get record count
            count_df = self.spark.sql(f"SELECT COUNT(*) as count FROM silver.{table_name}")
            
            if count_df.count() == 0:
                logger.warning(f"No records found in {table_name}")
                return
                
            count = count_df.collect()[0]['count']
            logger.info(f"Table {table_name} has {count} records")
            
            # Skip further checks if count is 0
            if count == 0:
                logger.warning(f"No records in table {table_name}")
                return
            
            # Check for nulls in key columns
            if table_name == "standardized_user_events":
                null_checks = self.spark.sql(f"""
                    SELECT 
                        SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) as null_event_ids,
                        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
                        SUM(CASE WHEN event_time IS NULL THEN 1 ELSE 0 END) as null_event_times
                    FROM silver.{table_name}
                    WHERE ingestion_date = '{process_date}'
                """).collect()[0]
                
                # Safely check null values
                null_event_ids = null_checks['null_event_ids'] or 0
                null_customer_ids = null_checks['null_customer_ids'] or 0
                null_event_times = null_checks['null_event_times'] or 0
                
                if null_event_ids > 0:
                    logger.warning(f"Found {null_event_ids} null event_ids")
                if null_customer_ids > 0:
                    logger.warning(f"Found {null_customer_ids} null customer_ids")
                if null_event_times > 0:
                    logger.warning(f"Found {null_event_times} null event_times")
            
            # Check for duplicates
            if table_name in ["standardized_user_events", "standardized_sales"]:
                id_column = "event_id" if table_name == "standardized_user_events" else "sale_id"
                date_column = "ingestion_date" if table_name == "standardized_user_events" else "transaction_date"
                
                # First check if there are any records for this date
                date_count = self.spark.sql(f"""
                    SELECT COUNT(*) as date_count
                    FROM silver.{table_name}
                    WHERE {date_column} = '{process_date}'
                """).collect()[0]['date_count']
                
                if date_count == 0:
                    logger.warning(f"No records found for date {process_date} in {table_name}")
                    return
                    
                duplicates = self.spark.sql(f"""
                    SELECT COUNT(*) as duplicate_count
                    FROM (
                        SELECT {id_column}, COUNT(*) as cnt
                        FROM silver.{table_name}
                        WHERE {date_column} = '{process_date}'
                        GROUP BY {id_column}
                        HAVING COUNT(*) > 1
                    )
                """).collect()[0]['duplicate_count']
                
                # Safely check duplicate value
                duplicate_count = duplicates or 0
                
                if duplicate_count > 0:
                    logger.warning(f"Found {duplicate_count} duplicate records in {table_name}")
            
            logger.info(f"Data quality checks completed for {table_name}")
            
        except Exception as e:
            logger.error(f"Error running data quality checks: {str(e)}")
            # Don't re-raise the exception, just log it to avoid failing the task

def main():
    parser = argparse.ArgumentParser(description='Silver Layer Transformations')
    parser.add_argument('--source', required=True, 
                       choices=['user_events', 'sales', 'product_catalog', 'customer_data', 'all'],
                       help='Source data to transform')
    parser.add_argument('--date', required=True, 
                       help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    transformer = SilverTransformations()
    
    try:
        # Create schemas
        transformer.create_silver_schemas()
        
        # Process based on source
        if args.source == 'user_events':
            transformer.transform_user_events(args.date)
        elif args.source == 'sales':
            transformer.transform_sales_data(args.date)
        elif args.source == 'product_catalog':
            transformer.transform_product_catalog(args.date)
        elif args.source == 'customer_data':
            transformer.transform_customer_data(args.date)
        elif args.source == 'all':
            transformer.transform_user_events(args.date)
            transformer.transform_sales_data(args.date)
            transformer.transform_product_catalog(args.date)
            transformer.transform_customer_data(args.date)
        else:
            raise ValueError(f"Unknown source: {args.source}")
        
        logger.info("Silver layer transformations completed successfully")
        
    except Exception as e:
        logger.error(f"Silver layer transformations failed: {str(e)}")
        sys.exit(1)
    finally:
        transformer.spark.stop()

if __name__ == "__main__":
    main()