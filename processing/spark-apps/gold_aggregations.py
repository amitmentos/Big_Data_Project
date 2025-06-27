# processing/spark-apps/gold_aggregations.py

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

class GoldLayerAggregations:
    def __init__(self):
        """Initialize Spark session with Iceberg support"""
        self.spark = SparkSession.builder \
            .appName("Gold Layer Aggregations") \
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

    def create_gold_schemas(self):
        """Create Gold layer database and tables if they don't exist"""
        
        # Create gold database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        
        # Fact Sales table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.fact_sales (
                sale_id STRING,
                product_id STRING,
                customer_sk BIGINT,
                campaign_id STRING,
                date_id DATE,
                quantity DECIMAL(10,2),
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                channel STRING,
                transaction_time TIMESTAMP,
                ingestion_time TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (date_id)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Fact User Activity table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.fact_user_activity (
                event_id STRING,
                customer_id STRING,
                product_id STRING,
                date_id DATE,
                event_type STRING,
                page_url STRING,
                device_type STRING,
                event_time TIMESTAMP,
                time_spent_seconds DECIMAL(10,2),
                session_id STRING
            ) USING ICEBERG
            PARTITIONED BY (date_id)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Sales Performance Metrics table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.sales_performance_metrics (
                date_id DATE,
                channel STRING,
                total_revenue DECIMAL(15,2),
                total_transactions BIGINT,
                unique_customers BIGINT,
                avg_transaction_value DECIMAL(10,2),
                conversion_rate DECIMAL(5,4)
            ) USING ICEBERG
            PARTITIONED BY (date_id)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Customer Segmentation table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.customer_segmentation (
                customer_id STRING,
                value_segment STRING,
                activity_status STRING,
                purchase_diversity STRING,
                total_spend DECIMAL(15,2),
                total_transactions BIGINT,
                avg_transaction_value DECIMAL(10,2),
                last_purchase_date DATE,
                days_since_last_purchase INT,
                segment_date DATE
            ) USING ICEBERG
            PARTITIONED BY (segment_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Campaign Effectiveness table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.campaign_effectiveness (
                campaign_id STRING,
                date_id DATE,
                spend DECIMAL(15,2),
                revenue DECIMAL(15,2),
                roi DECIMAL(10,4),
                conversions BIGINT,
                cost_per_acquisition DECIMAL(10,2),
                channel STRING,
                customer_segment STRING
            ) USING ICEBERG
            PARTITIONED BY (date_id)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)

    def create_fact_sales(self, process_date: str):
        """Create fact sales table from silver layer data"""
        logger.info(f"Creating fact sales for date: {process_date}")
        
        try:
            # Debug: Check what data is available
            logger.info("=== DEBUGGING FACT SALES CREATION ===")
            
            # Check standardized_sales data
            sales_count = self.spark.sql(f"SELECT COUNT(*) as count FROM silver.standardized_sales WHERE DATE(transaction_time) = '{process_date}'").collect()[0]['count']
            logger.info(f"standardized_sales records for date {process_date}: {sales_count}")
            
            if sales_count > 0:
                # Check customer_id values in standardized_sales
                sales_customer_ids = self.spark.sql(f"SELECT DISTINCT customer_id FROM silver.standardized_sales WHERE DATE(transaction_time) = '{process_date}' LIMIT 10").collect()
                logger.info(f"Sample customer_id values in standardized_sales: {[row['customer_id'] for row in sales_customer_ids]}")
            
            # Check dim_customer_scd data
            dim_customer_count = self.spark.sql("SELECT COUNT(*) as count FROM silver.dim_customer_scd WHERE is_current = true").collect()[0]['count']
            logger.info(f"Current customers in dim_customer_scd: {dim_customer_count}")
            
            if dim_customer_count > 0:
                # Check customer_id values in dim_customer_scd
                dim_customer_ids = self.spark.sql("SELECT DISTINCT customer_id FROM silver.dim_customer_scd WHERE is_current = true LIMIT 10").collect()
                logger.info(f"Sample customer_id values in dim_customer_scd: {[row['customer_id'] for row in dim_customer_ids]}")
            
            # Test the JOIN
            join_test = self.spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM silver.standardized_sales s
                LEFT JOIN silver.dim_customer_scd c 
                    ON s.customer_id = c.customer_id 
                    AND c.is_current = true
                WHERE DATE(s.transaction_time) = '{process_date}'
                  AND c.customer_sk IS NOT NULL
            """).collect()[0]['count']
            logger.info(f"Records after successful JOIN: {join_test}")
            
            logger.info("=== END DEBUGGING ===")
            
            fact_sales = self.spark.sql(f"""
                WITH enriched_sales AS (
                    SELECT 
                        s.sale_id,
                        s.product_id,
                        c.customer_sk,
                        NULL as campaign_id,  -- Would be enriched from campaign attribution
                        DATE(s.transaction_time) as date_id,
                        s.quantity,
                        s.unit_price,
                        s.total_amount,
                        s.channel,
                        s.transaction_time,
                        s.ingestion_time
                    FROM silver.standardized_sales s
                    LEFT JOIN silver.dim_customer_scd c 
                        ON s.customer_id = c.customer_id 
                        AND c.is_current = true
                        AND DATE(s.transaction_time) >= c.effective_start_date
                        AND DATE(s.transaction_time) <= COALESCE(c.effective_end_date, DATE('9999-12-31'))
                    WHERE DATE(s.transaction_time) = '{process_date}'
                )
                SELECT * FROM enriched_sales
            """)
            
            if fact_sales.count() == 0:
                logger.warning(f"No sales data found for date: {process_date}")
                return
            
            # Write to fact table
            fact_sales.write \
                .mode("overwrite") \
                .option("replaceWhere", f"date_id = '{process_date}'") \
                .insertInto("gold.fact_sales")
            
            count = fact_sales.count()
            logger.info(f"Successfully created {count} fact sales records")
            
        except Exception as e:
            logger.error(f"Error creating fact sales: {str(e)}")
            raise

    def create_fact_user_activity(self, process_date: str):
        """Create fact user activity table from silver layer data"""
        logger.info(f"Creating fact user activity for date: {process_date}")
        
        try:
            fact_activity = self.spark.sql(f"""
                SELECT 
                    event_id,
                    customer_id,
                    product_id,
                    DATE(event_time) as date_id,
                    event_type,
                    page_url,
                    device_type,
                    event_time,
                    time_spent_seconds,
                    session_id
                FROM silver.standardized_user_events
                WHERE ingestion_date = '{process_date}'
            """)
            
            if fact_activity.count() == 0:
                logger.warning(f"No user activity data found for date: {process_date}")
                return
            
            # Write to fact table
            fact_activity.write \
                .mode("overwrite") \
                .option("replaceWhere", f"date_id = '{process_date}'") \
                .insertInto("gold.fact_user_activity")
            
            count = fact_activity.count()
            logger.info(f"Successfully created {count} fact user activity records")
            
        except Exception as e:
            logger.error(f"Error creating fact user activity: {str(e)}")
            raise

    def generate_sales_performance_metrics(self, process_date: str):
        """Generate sales performance metrics"""
        logger.info(f"Generating sales performance metrics for date: {process_date}")
        
        try:
            # Debug: Check what data is available
            logger.info("=== DEBUGGING SALES PERFORMANCE METRICS ===")
            
            # Check fact_sales data for the specific date
            fact_sales_for_date = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.fact_sales WHERE date_id = '{process_date}'").collect()[0]['count']
            logger.info(f"fact_sales records for date_id = '{process_date}': {fact_sales_for_date}")
            
            # Check fact_user_activity data for the specific date  
            fact_activity_for_date = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.fact_user_activity WHERE date_id = '{process_date}'").collect()[0]['count']
            logger.info(f"fact_user_activity records for date_id = '{process_date}': {fact_activity_for_date}")
            
            if fact_sales_for_date > 0:
                # Check channels in fact_sales
                channels = self.spark.sql(f"SELECT DISTINCT channel FROM gold.fact_sales WHERE date_id = '{process_date}'").collect()
                logger.info(f"Available channels in fact_sales for {process_date}: {[row['channel'] for row in channels]}")
            
            if fact_activity_for_date > 0:
                # Check device types in fact_user_activity
                devices = self.spark.sql(f"SELECT DISTINCT device_type FROM gold.fact_user_activity WHERE date_id = '{process_date}'").collect()
                logger.info(f"Available device types in fact_user_activity for {process_date}: {[row['device_type'] for row in devices]}")
            
            logger.info("=== END DEBUGGING ===")
            
            # Calculate daily sales metrics by channel
            sales_metrics = self.spark.sql(f"""
                WITH daily_sales AS (
                    SELECT 
                        date_id,
                        channel,
                        SUM(total_amount) as total_revenue,
                        COUNT(DISTINCT sale_id) as total_transactions,
                        COUNT(DISTINCT customer_sk) as unique_customers
                    FROM gold.fact_sales
                    WHERE date_id = '{process_date}'
                    GROUP BY date_id, channel
                ),
                
                channel_activity AS (
                    SELECT 
                        date_id,
                        'website' as channel,  -- Mapping device types to channels
                        COUNT(DISTINCT customer_id) as total_visitors
                    FROM gold.fact_user_activity
                    WHERE date_id = '{process_date}'
                      AND device_type IN ('desktop', 'mobile', 'tablet')
                    GROUP BY date_id
                    
                    UNION ALL
                    
                    SELECT 
                        date_id,
                        'mobile_app' as channel,
                        COUNT(DISTINCT customer_id) as total_visitors
                    FROM gold.fact_user_activity
                    WHERE date_id = '{process_date}'
                      AND device_type = 'mobile'
                      AND page_url LIKE '%/app/%'
                    GROUP BY date_id
                ),
                
                performance_metrics AS (
                    SELECT 
                        ds.date_id,
                        ds.channel,
                        ds.total_revenue,
                        ds.total_transactions,
                        ds.unique_customers,
                        ROUND(ds.total_revenue / ds.total_transactions, 2) as avg_transaction_value,
                        CASE 
                            WHEN ca.total_visitors > 0 
                            THEN ROUND(ds.unique_customers * 1.0 / ca.total_visitors, 4)
                            ELSE 0.0 
                        END as conversion_rate
                    FROM daily_sales ds
                    LEFT JOIN channel_activity ca 
                        ON ds.date_id = ca.date_id 
                        AND ds.channel = ca.channel
                )
                
                SELECT * FROM performance_metrics
            """)
            
            if sales_metrics.count() == 0:
                logger.warning(f"No sales performance data generated for date: {process_date}")
                return
            
            # Write to metrics table
            sales_metrics.write \
                .mode("overwrite") \
                .option("replaceWhere", f"date_id = '{process_date}'") \
                .insertInto("gold.sales_performance_metrics")
            
            count = sales_metrics.count()
            logger.info(f"Successfully generated {count} sales performance metric records")
            
        except Exception as e:
            logger.error(f"Error generating sales performance metrics: {str(e)}")
            raise

    def generate_campaign_effectiveness(self, process_date: str):
        """Generate campaign effectiveness metrics"""
        logger.info(f"Generating campaign effectiveness for date: {process_date}")
        
        try:
            # Debug: Check what data is available
            logger.info("=== DEBUGGING CAMPAIGN EFFECTIVENESS ===")
            
            # Check fact_sales data for the specific date
            fact_sales_for_date = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.fact_sales WHERE date_id = '{process_date}'").collect()[0]['count']
            logger.info(f"fact_sales records for date_id = '{process_date}': {fact_sales_for_date}")
            
            if fact_sales_for_date > 0:
                # Check channels in fact_sales for the date
                website_sales = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.fact_sales WHERE date_id = '{process_date}' AND channel = 'website'").collect()[0]['count']
                other_sales = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.fact_sales WHERE date_id = '{process_date}' AND channel != 'website'").collect()[0]['count']
                logger.info(f"Website sales for {process_date}: {website_sales}")
                logger.info(f"Non-website sales for {process_date}: {other_sales}")
            
            logger.info("=== END DEBUGGING ===")
            
            # Calculate campaign effectiveness metrics
            campaign_effectiveness = self.spark.sql(f"""
                WITH campaign_attribution AS (
                    SELECT 
                        'CAMP_001' as campaign_id,
                        '{process_date}' as date_id,
                        1000.00 as spend,
                        SUM(fs.total_amount) as revenue,
                        COUNT(DISTINCT fs.sale_id) as conversions,
                        'website' as channel,
                        'High Value' as customer_segment
                    FROM gold.fact_sales fs
                    WHERE fs.date_id = '{process_date}'
                      AND fs.channel = 'website'
                    
                    UNION ALL
                    
                    SELECT 
                        'CAMP_002' as campaign_id,
                        '{process_date}' as date_id,
                        750.00 as spend,
                        SUM(fs.total_amount) as revenue,
                        COUNT(DISTINCT fs.sale_id) as conversions,
                        'mobile_app' as channel,
                        'Medium Value' as customer_segment
                    FROM gold.fact_sales fs
                    WHERE fs.date_id = '{process_date}'
                      AND fs.channel != 'website'
                ),
                
                campaign_metrics AS (
                    SELECT 
                        campaign_id,
                        CAST(date_id AS DATE) as date_id,
                        spend,
                        COALESCE(revenue, 0) as revenue,
                        CASE 
                            WHEN spend > 0 THEN ROUND((revenue - spend) / spend, 4)
                            ELSE 0.0 
                        END as roi,
                        conversions,
                        CASE 
                            WHEN conversions > 0 THEN ROUND(spend / conversions, 2)
                            ELSE 0.0 
                        END as cost_per_acquisition,
                        channel,
                        customer_segment
                    FROM campaign_attribution
                    WHERE revenue IS NOT NULL
                )
                
                SELECT * FROM campaign_metrics
            """)
            
            if campaign_effectiveness.count() == 0:
                logger.warning(f"No campaign effectiveness data generated for date: {process_date}")
                return
            
            # Write to campaign effectiveness table
            campaign_effectiveness.write \
                .mode("overwrite") \
                .option("replaceWhere", f"date_id = '{process_date}'") \
                .insertInto("gold.campaign_effectiveness")
            
            count = campaign_effectiveness.count()
            logger.info(f"Successfully generated {count} campaign effectiveness records")
            
        except Exception as e:
            logger.error(f"Error generating campaign effectiveness: {str(e)}")
            raise

    def generate_customer_segmentation(self, process_date: str):
        """Generate customer segmentation"""
        logger.info(f"Generating customer segmentation for date: {process_date}")
        
        try:
            # Debug: Check what data is available in each table
            logger.info("=== DEBUGGING CUSTOMER SEGMENTATION ===")
            
            # Check fact_sales data
            fact_sales_count = self.spark.sql("SELECT COUNT(*) as count FROM gold.fact_sales").collect()[0]['count']
            logger.info(f"Total records in gold.fact_sales: {fact_sales_count}")
            
            if fact_sales_count > 0:
                fact_sales_dates = self.spark.sql("SELECT DISTINCT date_id FROM gold.fact_sales ORDER BY date_id").collect()
                logger.info(f"Available dates in fact_sales: {[row['date_id'] for row in fact_sales_dates]}")
                
                fact_sales_for_date = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.fact_sales WHERE date_id <= '{process_date}'").collect()[0]['count']
                logger.info(f"fact_sales records with date_id <= '{process_date}': {fact_sales_for_date}")
            
            # Check dim_customer_scd data
            customer_count = self.spark.sql("SELECT COUNT(*) as count FROM silver.dim_customer_scd").collect()[0]['count']
            logger.info(f"Total records in silver.dim_customer_scd: {customer_count}")
            
            if customer_count > 0:
                current_customers = self.spark.sql("SELECT COUNT(*) as count FROM silver.dim_customer_scd WHERE is_current = true").collect()[0]['count']
                logger.info(f"Current customers (is_current=true): {current_customers}")
                     # Check customer_sk values
            customer_sks = self.spark.sql("SELECT DISTINCT customer_sk FROM silver.dim_customer_scd WHERE is_current = true LIMIT 10").collect()
            logger.info(f"Sample customer_sk values in dim_customer_scd: {[row['customer_sk'] for row in customer_sks]}")
            
            # Check customer_sk values in fact_sales
            fact_sales_sks = self.spark.sql("SELECT DISTINCT customer_sk FROM gold.fact_sales LIMIT 10").collect()
            logger.info(f"Sample customer_sk values in fact_sales: {[row['customer_sk'] for row in fact_sales_sks]}")
            
            # Check if there are any NULL customer_sk values in fact_sales
            null_customer_sks = self.spark.sql("SELECT COUNT(*) as count FROM gold.fact_sales WHERE customer_sk IS NULL").collect()[0]['count']
            logger.info(f"NULL customer_sk values in fact_sales: {null_customer_sks}")
            
            # Check dim_product data
            product_count = self.spark.sql("SELECT COUNT(*) as count FROM silver.dim_product").collect()[0]['count']
            logger.info(f"Total records in silver.dim_product: {product_count}")
            
            if product_count > 0:
                current_products = self.spark.sql("SELECT COUNT(*) as count FROM silver.dim_product WHERE is_current = true").collect()[0]['count']
                logger.info(f"Current products (is_current=true): {current_products}")
            
            # Check JOIN between fact_sales and dim_customer_scd
            join_test = self.spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM gold.fact_sales fs
                JOIN silver.dim_customer_scd c ON fs.customer_sk = c.customer_sk
                WHERE fs.date_id <= '{process_date}'
                  AND c.is_current = true
            """).collect()[0]['count']
            logger.info(f"Records after JOIN fact_sales <-> dim_customer_scd: {join_test}")
            
            logger.info("=== END DEBUGGING ===")
            
            # Calculate customer metrics for segmentation
            customer_segments = self.spark.sql(f"""
                WITH customer_metrics AS (
                    SELECT 
                        c.customer_sk,
                        c.customer_id,
                        COUNT(DISTINCT fs.sale_id) as total_transactions,
                        SUM(fs.total_amount) as total_spend,
                        AVG(fs.total_amount) as avg_transaction_value,
                        MAX(fs.date_id) as last_purchase_date,
                        DATEDIFF('{process_date}', MAX(fs.date_id)) as days_since_last_purchase,
                        COUNT(DISTINCT p.category) as unique_categories_purchased
                    FROM gold.fact_sales fs
                    JOIN silver.dim_customer_scd c ON fs.customer_sk = c.customer_sk
                    LEFT JOIN silver.dim_product p ON fs.product_id = p.product_id AND p.is_current = true
                    WHERE fs.date_id <= '{process_date}'
                      AND c.is_current = true
                    GROUP BY c.customer_sk, c.customer_id
                ),
                
                segmented_customers AS (
                    SELECT 
                        customer_id,
                        
                        -- Value Segmentation
                        CASE 
                            WHEN total_transactions >= 10 AND avg_transaction_value >= 50 THEN 'High Value'
                            WHEN total_transactions >= 5 AND avg_transaction_value >= 25 THEN 'Medium Value'
                            ELSE 'Low Value'
                        END as value_segment,
                        
                        -- Activity Status
                        CASE 
                            WHEN days_since_last_purchase <= 30 THEN 'Active'
                            WHEN days_since_last_purchase <= 90 THEN 'At Risk'
                            ELSE 'Inactive'
                        END as activity_status,
                        
                        -- Purchase Diversity
                        CASE 
                            WHEN unique_categories_purchased >= 3 THEN 'Diverse'
                            WHEN unique_categories_purchased = 2 THEN 'Focused'
                            ELSE 'Single Category'
                        END as purchase_diversity,
                        
                        total_spend,
                        total_transactions,
                        avg_transaction_value,
                        last_purchase_date,
                        days_since_last_purchase,
                        DATE('{process_date}') as segment_date
                        
                    FROM customer_metrics
                )
                
                SELECT * FROM segmented_customers
            """)
            
            if customer_segments.count() == 0:
                logger.warning(f"No customer segmentation data generated for date: {process_date}")
                return
            
            # Write to segmentation table
            customer_segments.write \
                .mode("overwrite") \
                .option("replaceWhere", f"segment_date = '{process_date}'") \
                .insertInto("gold.customer_segmentation")
            
            count = customer_segments.count()
            logger.info(f"Successfully generated {count} customer segmentation records")
            
        except Exception as e:
            logger.error(f"Error generating customer segmentation: {str(e)}")
            raise

    def generate_aggregations(self, target: str, process_date: str):
        """Generate specific aggregation based on target"""
        try:
            # Create schemas
            self.create_gold_schemas()
            
            # Route to specific aggregation method
            if target == 'fact_sales':
                self.create_fact_sales(process_date)
            elif target == 'fact_user_activity':
                self.create_fact_user_activity(process_date)
            elif target == 'sales_performance_metrics':
                self.generate_sales_performance_metrics(process_date)
            elif target == 'customer_segmentation':
                self.generate_customer_segmentation(process_date)
            elif target == 'campaign_effectiveness':
                self.generate_campaign_effectiveness(process_date)
            elif target == 'all':
                self.create_fact_sales(process_date)
                self.create_fact_user_activity(process_date)
                self.generate_sales_performance_metrics(process_date)
                self.generate_customer_segmentation(process_date)
                self.generate_campaign_effectiveness(process_date)
            else:
                raise ValueError(f"Unknown target: {target}")
            
            logger.info(f"Successfully generated {target} aggregations for {process_date}")
            
        except Exception as e:
            logger.error(f"Error generating {target} aggregations: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Gold Layer Aggregations')
    parser.add_argument('--target', required=True, 
                       choices=['fact_sales', 'fact_user_activity', 'sales_performance_metrics', 
                               'customer_segmentation', 'campaign_effectiveness', 'all'],
                       help='Target aggregation to generate')
    parser.add_argument('--date', required=True, 
                       help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    aggregator = GoldLayerAggregations()
    
    try:
        aggregator.generate_aggregations(args.target, args.date)
        logger.info("Gold layer aggregations completed successfully")
        
    except Exception as e:
        logger.error(f"Gold layer aggregations failed: {str(e)}")
        sys.exit(1)
    finally:
        aggregator.spark.stop()

if __name__ == "__main__":
    main() 