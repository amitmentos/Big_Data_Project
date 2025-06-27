# processing/spark-apps/scd_type2_processor.py

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

class SCDType2Processor:
    def __init__(self):
        """Initialize Spark session with Iceberg support"""
        self.spark = SparkSession.builder \
            .appName("SCD Type 2 Processor") \
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

    def create_scd_schemas(self):
        """Create SCD dimension tables if they don't exist"""
        
        # Create silver database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        
        # Customer SCD Type 2 table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.dim_customer_scd (
                customer_sk BIGINT,
                customer_id STRING,
                customer_name STRING,
                email STRING,
                address STRING,
                city STRING,
                country STRING,
                membership_tier STRING,
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
        
        # Marketing Campaign Dimension (static)
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.dim_marketing_campaign (
                campaign_id STRING,
                campaign_name STRING,
                campaign_type STRING,
                start_date DATE,
                end_date DATE,
                budget DECIMAL(15,2),
                target_audience STRING,
                created_time TIMESTAMP,
                updated_time TIMESTAMP
            ) USING ICEBERG
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Date Dimension
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.dim_date (
                date_id DATE,
                year INT,
                month INT,
                day INT,
                day_of_week STRING,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN,
                quarter STRING,
                week_of_year INT,
                day_of_year INT
            ) USING ICEBERG
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)

    def get_next_surrogate_key(self, table_name: str, key_column: str) -> int:
        """Get the next surrogate key value"""
        try:
            max_key = self.spark.sql(f"""
                SELECT COALESCE(MAX({key_column}), 0) as max_key 
                FROM silver.{table_name}
            """).collect()[0]['max_key']
            return max_key + 1
        except:
            return 1

    def process_customer_scd(self, process_date: str):
        """Process Customer SCD Type 2"""
        logger.info(f"Processing Customer SCD for date: {process_date}")
        
        try:
            # Read new/updated customer data from bronze layer
            new_customers = self.spark.sql(f"""
                SELECT DISTINCT
                    customer_id,
                    customer_name,
                    email,
                    address,
                    city,
                    country,
                    membership_tier,
                    last_updated
                FROM bronze.raw_customer_data
                WHERE DATE(last_updated) = '{process_date}'
                  AND customer_id IS NOT NULL
            """)
            
            if new_customers.count() == 0:
                logger.warning(f"No customer updates found for date: {process_date}")
                return
            
            # Get current dimension records
            try:
                current_customers = self.spark.sql("""
                    SELECT *
                    FROM silver.dim_customer_scd
                    WHERE is_current = true
                """)
            except:
                # If table doesn't exist, create empty DataFrame with proper schema
                schema = StructType([
                    StructField("customer_sk", LongType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("membership_tier", StringType(), True),
                    StructField("effective_start_date", DateType(), True),
                    StructField("effective_end_date", DateType(), True),
                    StructField("is_current", BooleanType(), True),
                    StructField("created_time", TimestampType(), True),
                    StructField("updated_time", TimestampType(), True)
                ])
                current_customers = self.spark.createDataFrame([], schema)
            
            # Identify new customers (not in current dimension)
            if current_customers.count() > 0:
                new_customer_ids = new_customers.join(
                    current_customers.select("customer_id"),
                    "customer_id",
                    "left_anti"
                ).select("customer_id").distinct()
            else:
                new_customer_ids = new_customers.select("customer_id").distinct()
            
            # Identify changed customers
            if current_customers.count() > 0:
                # Join new data with current dimension to find changes
                changed_customers = new_customers.alias("new").join(
                    current_customers.alias("curr"),
                    col("new.customer_id") == col("curr.customer_id"),
                    "inner"
                ).where(
                    # Compare attributes to detect changes
                    (col("new.customer_name") != col("curr.customer_name")) |
                    (col("new.email") != col("curr.email")) |
                    (col("new.address") != col("curr.address")) |
                    (col("new.city") != col("curr.city")) |
                    (col("new.country") != col("curr.country")) |
                    (col("new.membership_tier") != col("curr.membership_tier"))
                ).select(
                    col("new.customer_id"),
                    col("new.customer_name"),
                    col("new.email"),
                    col("new.address"),
                    col("new.city"),
                    col("new.country"),
                    col("new.membership_tier"),
                    col("curr.customer_sk").alias("current_sk")
                )
            else:
                changed_customers = self.spark.createDataFrame([], StructType([
                    StructField("customer_id", StringType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("membership_tier", StringType(), True),
                    StructField("current_sk", LongType(), True)
                ]))
            
            records_to_process = []
            
            # Process new customers
            new_customer_list = new_customer_ids.collect()
            if new_customer_list:
                logger.info(f"Processing {len(new_customer_list)} new customers")
                
                for i, row in enumerate(new_customer_list):
                    customer_id = row['customer_id']
                    customer_data = new_customers.filter(col("customer_id") == customer_id).first()
                    
                    sk = self.get_next_surrogate_key("dim_customer_scd", "customer_sk") + len(records_to_process)
                    
                    record = {
                        'customer_sk': sk,
                        'customer_id': customer_id,
                        'customer_name': customer_data['customer_name'],
                        'email': customer_data['email'],
                        'address': customer_data['address'],
                        'city': customer_data['city'],
                        'country': customer_data['country'],
                        'membership_tier': customer_data['membership_tier'],
                        'effective_start_date': datetime.strptime(process_date, '%Y-%m-%d').date(),
                        'effective_end_date': datetime.strptime('9999-12-31', '%Y-%m-%d').date(),
                        'is_current': True,
                        'created_time': datetime.now(),
                        'updated_time': datetime.now()
                    }
                    records_to_process.append(record)
            
            # Process changed customers
            changed_customer_list = changed_customers.collect()
            if changed_customer_list:
                logger.info(f"Processing {len(changed_customer_list)} changed customers")
                
                # Close current records
                customer_ids_to_close = [row['customer_id'] for row in changed_customer_list]
                
                if customer_ids_to_close:
                    # Update current records to close them
                    for customer_id in customer_ids_to_close:
                        # In a real implementation, you'd use MERGE operations
                        # For now, we'll use a simpler approach
                        self.spark.sql(f"""
                            CREATE OR REPLACE TEMPORARY VIEW customers_to_update AS
                            SELECT * FROM silver.dim_customer_scd 
                            WHERE customer_id = '{customer_id}' AND is_current = true
                        """)
                
                # Create new records for changed customers
                for row in changed_customer_list:
                    sk = self.get_next_surrogate_key("dim_customer_scd", "customer_sk") + len(records_to_process)
                    
                    record = {
                        'customer_sk': sk,
                        'customer_id': row['customer_id'],
                        'customer_name': row['customer_name'],
                        'email': row['email'],
                        'address': row['address'],
                        'city': row['city'],
                        'country': row['country'],
                        'membership_tier': row['membership_tier'],
                        'effective_start_date': datetime.strptime(process_date, '%Y-%m-%d').date(),
                        'effective_end_date': datetime.strptime('9999-12-31', '%Y-%m-%d').date(),
                        'is_current': True,
                        'created_time': datetime.now(),
                        'updated_time': datetime.now()
                    }
                    records_to_process.append(record)
            
            # Insert new records
            if records_to_process:
                new_records_df = self.spark.createDataFrame(records_to_process)
                
                # First, close existing records for changed customers
                if changed_customer_list:
                    customer_ids_str = "'" + "','".join([row['customer_id'] for row in changed_customer_list]) + "'"
                    
                    # Create a temporary view with updated records
                    closed_records = current_customers.filter(
                        col("customer_id").isin([row['customer_id'] for row in changed_customer_list])
                    ).withColumn("effective_end_date", lit(datetime.strptime(process_date, '%Y-%m-%d').date())) \
                     .withColumn("is_current", lit(False)) \
                     .withColumn("updated_time", current_timestamp())
                    
                    # Remove old current records and add closed records
                    remaining_current = current_customers.filter(
                        ~col("customer_id").isin([row['customer_id'] for row in changed_customer_list])
                    )
                    
                    # Union all records and write
                    all_records = remaining_current.unionByName(closed_records).unionByName(new_records_df)
                else:
                    # Just add new records to existing
                    if current_customers.count() > 0:
                        all_records = current_customers.unionByName(new_records_df)
                    else:
                        all_records = new_records_df
                
                # Write to dimension table (overwrite for simplicity)
                all_records.write \
                    .mode("overwrite") \
                    .insertInto("silver.dim_customer_scd")
                
                logger.info(f"Successfully processed {len(records_to_process)} customer records")
            else:
                logger.info("No customer records to process")
                
        except Exception as e:
            logger.error(f"Error processing customer SCD: {str(e)}")
            raise

    def process_marketing_campaigns(self, process_date: str):
        """Process Marketing Campaign dimension (static)"""
        logger.info(f"Processing Marketing Campaigns for date: {process_date}")
        
        try:
            # Read campaign data from bronze layer
            new_campaigns = self.spark.sql(f"""
                SELECT DISTINCT
                    campaign_id,
                    campaign_name,
                    campaign_type,
                    start_date,
                    end_date,
                    budget,
                    target_audience
                FROM bronze.raw_marketing_campaigns
                WHERE DATE(ingestion_time) = '{process_date}'
                  AND campaign_id IS NOT NULL
            """)
            
            if new_campaigns.count() == 0:
                logger.warning(f"No campaign updates found for date: {process_date}")
                return
            
            # Get existing campaigns
            try:
                existing_campaigns = self.spark.sql("SELECT campaign_id FROM silver.dim_marketing_campaign")
                existing_ids = [row['campaign_id'] for row in existing_campaigns.collect()]
            except:
                existing_ids = []
            
            # Filter for truly new campaigns
            if existing_ids:
                new_campaigns = new_campaigns.filter(~col("campaign_id").isin(existing_ids))
            
            if new_campaigns.count() > 0:
                # Add metadata columns
                campaigns_to_insert = new_campaigns.withColumn("created_time", current_timestamp()) \
                                                  .withColumn("updated_time", current_timestamp())
                
                # Write to dimension table
                campaigns_to_insert.write \
                    .mode("append") \
                    .insertInto("silver.dim_marketing_campaign")
                
                count = campaigns_to_insert.count()
                logger.info(f"Successfully processed {count} new campaign records")
            else:
                logger.info("No new campaign records to process")
                
        except Exception as e:
            logger.error(f"Error processing marketing campaigns: {str(e)}")
            raise

    def populate_date_dimension(self, start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
        """Populate date dimension table"""
        logger.info(f"Populating date dimension from {start_date} to {end_date}")
        
        try:
            # Check if date dimension is already populated
            existing_count = self.spark.sql("SELECT COUNT(*) as count FROM silver.dim_date").collect()[0]['count']
            
            if existing_count > 0:
                logger.info(f"Date dimension already contains {existing_count} records")
                return
            
            # Generate date range
            dates_df = self.spark.sql(f"""
                WITH date_range AS (
                    SELECT explode(sequence(
                        to_date('{start_date}'),
                        to_date('{end_date}'),
                        interval 1 day
                    )) as date_id
                )
                SELECT 
                    date_id,
                    YEAR(date_id) as year,
                    MONTH(date_id) as month,
                    DAY(date_id) as day,
                    date_format(date_id, 'EEEE') as day_of_week,
                    CASE WHEN dayofweek(date_id) IN (1, 7) THEN true ELSE false END as is_weekend,
                    false as is_holiday,  -- Simplified - would include actual holiday logic
                    CONCAT('Q', quarter(date_id)) as quarter,
                    weekofyear(date_id) as week_of_year,
                    dayofyear(date_id) as day_of_year
                FROM date_range
                ORDER BY date_id
            """)
            
            # Write to date dimension
            dates_df.write \
                .mode("overwrite") \
                .insertInto("silver.dim_date")
            
            count = dates_df.count()
            logger.info(f"Successfully populated {count} date records")
            
        except Exception as e:
            logger.error(f"Error populating date dimension: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='SCD Type 2 Processor')
    parser.add_argument('--source', required=True, 
                       choices=['customer', 'campaign', 'date', 'all'],
                       help='Source dimension to process')
    parser.add_argument('--date', required=True, 
                       help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    processor = SCDType2Processor()
    
    try:
        # Create schemas
        processor.create_scd_schemas()
        
        # Process based on source
        if args.source == 'customer' or args.source == 'all':
            processor.process_customer_scd(args.date)
        
        if args.source == 'campaign' or args.source == 'all':
            processor.process_marketing_campaigns(args.date)
        
        if args.source == 'date' or args.source == 'all':
            processor.populate_date_dimension()
        
        logger.info("SCD Type 2 processing completed successfully")
        
    except Exception as e:
        logger.error(f"SCD Type 2 processing failed: {str(e)}")
        sys.exit(1)
    finally:
        processor.spark.stop()

if __name__ == "__main__":
    main()