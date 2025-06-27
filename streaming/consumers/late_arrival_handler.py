 # processing/spark-apps/late_arrival_handler.py

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
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
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
            .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minio") \
            .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

    def identify_late_arrivals(self, lookback_hours: int, process_date: str):
        """Identify late-arriving data that needs reprocessing"""
        logger.info(f"Identifying late arrivals with {lookback_hours}h lookback from {process_date}")
        
        try:
            # Calculate lookback window
            process_datetime = datetime.strptime(process_date, '%Y-%m-%d')
            lookback_start = process_datetime - timedelta(hours=lookback_hours)
            
            # Find late-arriving marketplace sales
            late_sales = self.spark.sql(f"""
                WITH sales_with_delay AS (
                    SELECT 
                        transaction_id,
                        marketplace_name,
                        seller_id,
                        product_id,
                        amount,
                        currency,
                        quantity,
                        transaction_time,
                        settlement_time,
                        payment_method,
                        status,
                        marketplace_metadata,
                        ingestion_time,
                        
                        -- Calculate arrival delay
                        ROUND(
                            (UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(transaction_time)) / 3600.0, 2
                        ) as arrival_delay_hours,
                        
                        -- Event time vs ingestion time
                        DATE(transaction_time) as event_date,
                        DATE(ingestion_time) as ingestion_date
                        
                    FROM bronze.raw_marketplace_sales
                    WHERE DATE(ingestion_time) = '{process_date}'
                      AND transaction_time >= '{lookback_start.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND transaction_time < '{process_datetime.strftime('%Y-%m-%d %H:%M:%S')}'
                ),
                
                categorized_arrivals AS (
                    SELECT *,
                        CASE 
                            WHEN arrival_delay_hours <= 1 THEN 'immediate'
                            WHEN arrival_delay_hours <= 6 THEN 'slightly_late'
                            WHEN arrival_delay_hours <= 24 THEN 'late'
                            ELSE 'very_late'
                        END as arrival_category,
                        
                        -- Check if this transaction was already processed
                        CASE 
                            WHEN event_date != ingestion_date THEN true
                            ELSE false
                        END as is_late_arrival
                        
                    FROM sales_with_delay
                )
                
                SELECT * FROM categorized_arrivals
                WHERE is_late_arrival = true
                ORDER BY arrival_delay_hours DESC
            """)
            
            late_count = late_sales.count()
            logger.info(f"Found {late_count} late-arriving sales records")
            
            if late_count > 0:
                # Show arrival delay statistics
                delay_stats = late_sales.groupBy("arrival_category") \
                    .agg(
                        count("*").alias("count"),
                        avg("arrival_delay_hours").alias("avg_delay_hours"),
                        max("arrival_delay_hours").alias("max_delay_hours"),
                        min("arrival_delay_hours").alias("min_delay_hours")
                    ) \
                    .orderBy("avg_delay_hours")
                
                logger.info("Late arrival statistics by category:")
                delay_stats.show(truncate=False)
                
                # Show marketplace breakdown
                marketplace_stats = late_sales.groupBy("marketplace_name") \
                    .agg(
                        count("*").alias("late_count"),
                        avg("arrival_delay_hours").alias("avg_delay"),
                        max("arrival_delay_hours").alias("max_delay")
                    ) \
                    .orderBy(desc("late_count"))
                
                logger.info("Late arrivals by marketplace:")
                marketplace_stats.show(truncate=False)
            
            return late_sales
            
        except Exception as e:
            logger.error(f"Error identifying late arrivals: {str(e)}")
            raise

    def reprocess_late_arrivals(self, late_arrivals_df, process_date: str):
        """Reprocess late-arriving data through Silver layer"""
        logger.info("Reprocessing late arrivals through Silver layer...")
        
        try:
            if late_arrivals_df.count() == 0:
                logger.info("No late arrivals to reprocess")
                return
            
            # Get affected event dates for reprocessing
            affected_dates = late_arrivals_df.select("event_date").distinct().collect()
            affected_date_list = [row.event_date for row in affected_dates]
            
            logger.info(f"Affected dates for reprocessing: {affected_date_list}")
            
            # Reprocess each affected date
            for event_date in affected_date_list:
                logger.info(f"Reprocessing Silver layer for date: {event_date}")
                self.reprocess_silver_sales(str(event_date))
                
            # Update processing metadata
            self.update_reprocessing_metadata(late_arrivals_df, process_date)
            
            logger.info("Late arrival reprocessing completed")
            
        except Exception as e:
            logger.error(f"Error reprocessing late arrivals: {str(e)}")
            raise

    def reprocess_silver_sales(self, event_date: str):
        """Reprocess sales data for a specific date in Silver layer"""
        logger.info(f"Reprocessing Silver sales for {event_date}")
        
        try:
            # Get all sales for the event date (including late arrivals)
            all_sales_for_date = self.spark.sql(f"""
                SELECT 
                    transaction_id as sale_id,
                    seller_id as customer_id,
                    product_id,
                    marketplace_name as channel,
                    quantity,
                    amount as unit_price,
                    amount as total_amount,
                    COALESCE(currency, 'USD') as currency,
                    transaction_time,
                    ingestion_time,
                    current_timestamp() as processed_time,
                    DATE(transaction_time) as transaction_date
                FROM bronze.raw_marketplace_sales
                WHERE DATE(transaction_time) = '{event_date}'
                  AND transaction_id IS NOT NULL
                  AND product_id IS NOT NULL
                  AND amount IS NOT NULL
                  AND amount > 0
            """).dropDuplicates(["sale_id"])
            
            # Remove existing records for this date from Silver
            self.spark.sql(f"""
                DELETE FROM silver.standardized_sales
                WHERE transaction_date = '{event_date}'
            """)
            
            # Insert reprocessed records
            if all_sales_for_date.count() > 0:
                all_sales_for_date.write \
                    .mode("append") \
                    .insertInto("silver.standardized_sales")
                
                count = all_sales_for_date.count()
                logger.info(f"Reprocessed {count} sales records for {event_date}")
            else:
                logger.warning(f"No sales data found for reprocessing on {event_date}")
                
        except Exception as e:
            logger.error(f"Error reprocessing Silver sales for {event_date}: {str(e)}")
            raise

    def handle_duplicate_detection(self, late_arrivals_df):
        """Detect and handle duplicate records from late arrivals"""
        logger.info("Detecting duplicates in late arrivals...")
        
        try:
            # Find potential duplicates based on business keys
            duplicates = late_arrivals_df.withColumn(
                "business_key", 
                concat_ws("_", col("marketplace_name"), col("seller_id"), 
                         col("product_id"), col("transaction_time"))
            ).groupBy("business_key") \
             .agg(
                 count("*").alias("duplicate_count"),
                 collect_list("transaction_id").alias("transaction_ids"),
                 max("ingestion_time").alias("latest_ingestion"),
                 min("ingestion_time").alias("earliest_ingestion")
             ).filter(col("duplicate_count") > 1)
            
            duplicate_count = duplicates.count()
            
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} potential duplicate business keys")
                duplicates.show(5, truncate=False)
                
                # Strategy: Keep the record with latest ingestion time
                duplicate_resolution = duplicates.select(
                    explode("transaction_ids").alias("transaction_id"),
                    col("latest_ingestion")
                ).join(
                    late_arrivals_df,
                    ["transaction_id"]
                ).filter(
                    col("ingestion_time") == col("latest_ingestion")
                ).drop("latest_ingestion")
                
                logger.info(f"Resolved duplicates, keeping {duplicate_resolution.count()} records")
                return duplicate_resolution
            else:
                logger.info("No duplicates detected")
                return late_arrivals_df
                
        except Exception as e:
            logger.error(f"Error in duplicate detection: {str(e)}")
            return late_arrivals_df

    def update_reprocessing_metadata(self, late_arrivals_df, process_date: str):
        """Update metadata about reprocessing activities"""
        logger.info("Updating reprocessing metadata...")
        
        try:
            # Create reprocessing summary
            reprocessing_summary = late_arrivals_df.agg(
                count("*").alias("total_late_arrivals"),
                countDistinct("marketplace_name").alias("affected_marketplaces"),
                countDistinct("event_date").alias("affected_dates"),
                avg("arrival_delay_hours").alias("avg_delay_hours"),
                max("arrival_delay_hours").alias("max_delay_hours")
            ).withColumn("process_date", lit(process_date)) \
             .withColumn("reprocessing_timestamp", current_timestamp())
            
            # Log the summary
            summary_row = reprocessing_summary.collect()[0]
            logger.info(f"Reprocessing Summary:")
            logger.info(f"  - Total late arrivals: {summary_row.total_late_arrivals}")
            logger.info(f"  - Affected marketplaces: {summary_row.affected_marketplaces}")
            logger.info(f"  - Affected dates: {summary_row.affected_dates}")
            logger.info(f"  - Average delay: {summary_row.avg_delay_hours:.2f} hours")
            logger.info(f"  - Maximum delay: {summary_row.max_delay_hours:.2f} hours")
            
        except Exception as e:
            logger.error(f"Error updating reprocessing metadata: {str(e)}")

    def validate_reprocessing_results(self, process_date: str, lookback_hours: int):
        """Validate that reprocessing was successful"""
        logger.info("Validating reprocessing results...")
        
        try:
            # Check data consistency between Bronze and Silver
            lookback_start = datetime.strptime(process_date, '%Y-%m-%d') - timedelta(hours=lookback_hours)
            
            bronze_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT transaction_id) as count
                FROM bronze.raw_marketplace_sales
                WHERE transaction_time >= '{lookback_start.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND transaction_time < '{process_date} 00:00:00'
            """).collect()[0].count
            
            silver_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT sale_id) as count
                FROM silver.standardized_sales
                WHERE transaction_time >= '{lookback_start.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND transaction_time < '{process_date} 00:00:00'
            """).collect()[0].count
            
            logger.info(f"Validation Results:")
            logger.info(f"  - Bronze layer records: {bronze_count}")
            logger.info(f"  - Silver layer records: {silver_count}")
            
            if bronze_count == silver_count:
                logger.info("✓ Data consistency validation passed")
                return True
            else:
                logger.warning(f"⚠ Data consistency issue: {bronze_count - silver_count} missing records")
                return False
                
        except Exception as e:
            logger.error(f"Error validating reprocessing: {str(e)}")
            return False

    def run_late_arrival_processing(self, lookback_hours: int, process_date: str):
        """Main method to handle late arrival processing"""
        logger.info(f"Starting late arrival processing for {process_date}")
        
        try:
            # Step 1: Identify late arrivals
            late_arrivals = self.identify_late_arrivals(lookback_hours, process_date)
            
            if late_arrivals.count() == 0:
                logger.info("No late arrivals detected - processing complete")
                return
            
            # Step 2: Handle duplicates
            deduplicated_arrivals = self.handle_duplicate_detection(late_arrivals)
            
            # Step 3: Reprocess through Silver layer
            self.reprocess_late_arrivals(deduplicated_arrivals, process_date)
            
            # Step 4: Validate results
            validation_success = self.validate_reprocessing_results(process_date, lookback_hours)
            
            if validation_success:
                logger.info("Late arrival processing completed successfully")
            else:
                logger.warning("Late arrival processing completed with validation warnings")
                
        except Exception as e:
            logger.error(f"Late arrival processing failed: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Late Arrival Handler')
    parser.add_argument('--lookback-hours', type=int, default=48,
                       help='Hours to look back for late arrivals (default: 48)')
    parser.add_argument('--date', required=True,
                       help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    handler = LateArrivalHandler()
    
    try:
        handler.run_late_arrival_processing(args.lookback_hours, args.date)
        logger.info("Late arrival handling completed successfully")
        
    except Exception as e:
        logger.error(f"Late arrival handling failed: {str(e)}")
        sys.exit(1)
    finally:
        handler.spark.stop()

if __name__ == "__main__":
    main()