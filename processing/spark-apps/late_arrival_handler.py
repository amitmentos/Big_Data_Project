#!/usr/bin/env python3
"""
Late Arrival Data Handler
Processes late-arriving data and updates existing records in the Silver layer
"""

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LateArrivalHandler:
    def __init__(self):
        """Initialize Spark session with Iceberg support"""
        self.spark = SparkSession.builder \
            .appName("Late Arrival Handler") \
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
            .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.warehouse_path = "s3a://warehouse/"
        
    def setup_iceberg_catalog(self):
        """Setup Iceberg catalog configuration"""
        self.spark.sql("""
            CREATE NAMESPACE IF NOT EXISTS bronze
        """)
        self.spark.sql("""
            CREATE NAMESPACE IF NOT EXISTS silver
        """)
        
    def identify_late_arrivals(self, lookback_hours=48):
        """Identify late-arriving data based on ingestion timestamp"""
        logger.info(f"Identifying late arrivals within {lookback_hours} hours")
        
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
        cutoff_timestamp = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Check for late arrivals in user events
        try:
            late_user_events = self.spark.sql(f"""
                SELECT *
                FROM bronze.user_events
                WHERE ingestion_time > '{cutoff_timestamp}'
                AND event_time < '{cutoff_timestamp}'
            """)
            
            late_count = late_user_events.count()
            logger.info(f"Found {late_count} late-arriving user events")
            
            if late_count > 0:
                self.process_late_user_events(late_user_events)
                
        except Exception as e:
            logger.warning(f"No bronze.user_events table found or error: {e}")
            
        # Check for late arrivals in sales data
        try:
            late_sales = self.spark.sql(f"""
                SELECT *
                FROM bronze.marketplace_sales
                WHERE ingestion_time > '{cutoff_timestamp}'
                AND transaction_time < '{cutoff_timestamp}'
            """)
            
            late_count = late_sales.count()
            logger.info(f"Found {late_count} late-arriving sales records")
            
            if late_count > 0:
                self.process_late_sales_data(late_sales)
                
        except Exception as e:
            logger.warning(f"No bronze.marketplace_sales table found or error: {e}")
    
    def process_late_user_events(self, late_events_df):
        """Process late-arriving user events"""
        logger.info("Processing late-arriving user events")
        
        try:
            # Transform late events using same logic as regular processing
            transformed_events = late_events_df.select(
                col("event_id"),
                col("session_id"),
                col("customer_id"),
                col("event_type"),
                to_timestamp(col("event_time")).alias("event_timestamp"),
                col("page_url"),
                col("device_type"),
                col("metadata.product_id").alias("product_id"),
                col("metadata.category").alias("category"),
                date_format(to_timestamp(col("event_time")), "yyyy-MM-dd").alias("event_date"),
                hour(to_timestamp(col("event_time"))).alias("event_hour"),
                current_timestamp().alias("processed_at")
            ).filter(col("event_timestamp").isNotNull())
            
            # Merge with existing silver data
            transformed_events.createOrReplaceTempView("late_user_events")
            
            self.spark.sql("""
                MERGE INTO silver.user_events AS target
                USING late_user_events AS source
                ON target.event_id = source.event_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            logger.info("Successfully processed late-arriving user events")
            
        except Exception as e:
            logger.error(f"Error processing late user events: {e}")
            # Create table if it doesn't exist
            self.create_silver_user_events_table()
            
    def process_late_sales_data(self, late_sales_df):
        """Process late-arriving sales data"""
        logger.info("Processing late-arriving sales data")
        
        try:
            # Transform late sales using same logic as regular processing
            transformed_sales = late_sales_df.select(
                col("transaction_id"),
                col("marketplace_name"),
                col("seller_id"),
                col("product_id"),
                col("amount").cast("decimal(10,2)"),
                col("currency"),
                col("quantity").cast("integer"),
                to_timestamp(col("transaction_time")).alias("transaction_timestamp"),
                to_timestamp(col("settlement_time")).alias("settlement_timestamp"),
                col("payment_method"),
                col("status"),
                col("marketplace_metadata.commission_rate").cast("decimal(5,3)").alias("commission_rate"),
                col("marketplace_metadata.shipping_cost").cast("decimal(8,2)").alias("shipping_cost"),
                col("marketplace_metadata.tax_amount").cast("decimal(8,2)").alias("tax_amount"),
                date_format(to_timestamp(col("transaction_time")), "yyyy-MM-dd").alias("transaction_date"),
                current_timestamp().alias("processed_at")
            ).filter(col("transaction_timestamp").isNotNull())
            
            # Merge with existing silver data
            transformed_sales.createOrReplaceTempView("late_sales")
            
            self.spark.sql("""
                MERGE INTO silver.marketplace_sales AS target
                USING late_sales AS source
                ON target.transaction_id = source.transaction_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            logger.info("Successfully processed late-arriving sales data")
            
        except Exception as e:
            logger.error(f"Error processing late sales data: {e}")
            # Create table if it doesn't exist
            self.create_silver_sales_table()
    
    def create_silver_user_events_table(self):
        """Create silver user events table if it doesn't exist"""
        logger.info("Creating silver.user_events table")
        
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.user_events (
                event_id STRING,
                session_id STRING,
                customer_id STRING,
                event_type STRING,
                event_timestamp TIMESTAMP,
                page_url STRING,
                device_type STRING,
                product_id STRING,
                category STRING,
                event_date STRING,
                event_hour INTEGER,
                processed_at TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (event_date)
            LOCATION 's3a://warehouse/silver/user_events'
        """)
        
    def create_silver_sales_table(self):
        """Create silver sales table if it doesn't exist"""
        logger.info("Creating silver.marketplace_sales table")
        
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.marketplace_sales (
                transaction_id STRING,
                marketplace_name STRING,
                seller_id STRING,
                product_id STRING,
                amount DECIMAL(10,2),
                currency STRING,
                quantity INTEGER,
                transaction_timestamp TIMESTAMP,
                settlement_timestamp TIMESTAMP,
                payment_method STRING,
                status STRING,
                commission_rate DECIMAL(5,3),
                shipping_cost DECIMAL(8,2),
                tax_amount DECIMAL(8,2),
                transaction_date STRING,
                processed_at TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (transaction_date)
            LOCATION 's3a://warehouse/silver/marketplace_sales'
        """)
    
    def generate_late_arrival_report(self, lookback_hours=48):
        """Generate a report on late-arriving data"""
        logger.info("Generating late arrival report")
        
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
        cutoff_timestamp = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'lookback_hours': lookback_hours,
            'cutoff_timestamp': cutoff_timestamp,
            'late_arrivals': {}
        }
        
        # Count late arrivals by source
        try:
            user_events_count = self.spark.sql(f"""
                SELECT COUNT(*) as count
                FROM bronze.user_events
                WHERE ingestion_time > '{cutoff_timestamp}'
                AND event_time < '{cutoff_timestamp}'
            """).collect()[0]['count']
            
            report['late_arrivals']['user_events'] = user_events_count
            
        except Exception as e:
            logger.warning(f"Could not count late user events: {e}")
            report['late_arrivals']['user_events'] = 0
            
        try:
            sales_count = self.spark.sql(f"""
                SELECT COUNT(*) as count
                FROM bronze.marketplace_sales
                WHERE ingestion_time > '{cutoff_timestamp}'
                AND transaction_time < '{cutoff_timestamp}'
            """).collect()[0]['count']
            
            report['late_arrivals']['marketplace_sales'] = sales_count
            
        except Exception as e:
            logger.warning(f"Could not count late sales: {e}")
            report['late_arrivals']['marketplace_sales'] = 0
        
        logger.info(f"Late arrival report: {report}")
        return report
    
    def run(self, lookback_hours=48, processing_date=None):
        """Main execution method"""
        logger.info(f"Starting late arrival processing with {lookback_hours} hour lookback")
        
        try:
            # Setup Iceberg catalog
            self.setup_iceberg_catalog()
            
            # Identify and process late arrivals
            self.identify_late_arrivals(lookback_hours)
            
            # Generate report
            report = self.generate_late_arrival_report(lookback_hours)
            
            logger.info("Late arrival processing completed successfully")
            return report
            
        except Exception as e:
            logger.error(f"Error in late arrival processing: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Handle late-arriving data')
    parser.add_argument('--lookback-hours', type=int, default=48,
                       help='Hours to look back for late arrivals')
    parser.add_argument('--date', type=str, default=None,
                       help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        handler = LateArrivalHandler()
        report = handler.run(lookback_hours=args.lookback_hours, processing_date=args.date)
        
        logger.info("Late arrival handler completed successfully")
        print(f"Processing report: {report}")
        
    except Exception as e:
        logger.error(f"Late arrival handler failed: {e}")
        raise
    finally:
        handler.spark.stop()

if __name__ == "__main__":
    main() 