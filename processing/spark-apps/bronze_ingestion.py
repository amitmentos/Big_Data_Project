# processing/spark-apps/bronze_ingestion.py

import argparse
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pyspark.sql import SparkSession
# Import specific functions instead of * to avoid namespace conflicts
from pyspark.sql.functions import col, to_timestamp, to_date, from_json, when, lit, monotonically_increasing_id
from pyspark.sql.types import *
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BronzeIngestion:
    def __init__(self):
        """Initialize Spark session with Iceberg and Kafka support"""
        self.spark = SparkSession.builder \
            .appName("Bronze Layer Ingestion") \
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
            .config("spark.sql.streaming.checkpointLocation", "s3a://warehouse/checkpoints/") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

    def create_bronze_schemas(self):
        """Create Bronze layer database and tables if they don't exist"""
        
        # Create bronze database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        
        # Raw User Events table (from Kafka stream)
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.raw_user_events (
                event_id STRING,
                session_id STRING,
                customer_id STRING,
                event_type STRING,
                event_time TIMESTAMP,
                page_url STRING,
                product_id STRING,
                device_type STRING,
                metadata MAP<STRING, STRING>,
                ingestion_time TIMESTAMP,
                kafka_timestamp TIMESTAMP,
                kafka_partition INT,
                kafka_offset BIGINT
            ) USING ICEBERG
            PARTITIONED BY (days(event_time))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Raw Marketplace Sales table (from Kafka stream with late arrivals)
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.raw_marketplace_sales (
                transaction_id STRING,
                marketplace_name STRING,
                seller_id STRING,
                product_id STRING,
                amount DECIMAL(10,2),
                currency STRING,
                quantity INT,
                transaction_time TIMESTAMP,
                settlement_time TIMESTAMP,
                payment_method STRING,
                status STRING,
                marketplace_metadata MAP<STRING, STRING>,
                ingestion_time TIMESTAMP,
                kafka_timestamp TIMESTAMP,
                kafka_partition INT,
                kafka_offset BIGINT
            ) USING ICEBERG
            PARTITIONED BY (days(transaction_time))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Raw Product Catalog table (batch data)
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.raw_product_catalog (
                product_id STRING,
                product_name STRING,
                category STRING,
                subcategory STRING,
                brand STRING,
                base_price DECIMAL(10,2),
                description STRING,
                is_active BOOLEAN,
                last_updated TIMESTAMP,
                ingestion_time TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(last_updated))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Raw Customer Data table (batch data)
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.raw_customer_data (
                customer_id STRING,
                customer_name STRING,
                email STRING,
                address STRING,
                city STRING,
                country STRING,
                membership_tier STRING,
                created_at TIMESTAMP,
                last_updated TIMESTAMP,
                ingestion_time TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (days(last_updated))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
        
        # Raw Marketing Campaigns table (batch data)
        self.spark.sql("""
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

    def ingest_user_events_stream(self):
        """Ingest real-time user events from Kafka"""
        logger.info("Starting user events stream ingestion...")
        
        try:
            # Read from Kafka stream
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "user_activity_events") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON messages
            user_events_schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_time", StringType(), True),
                StructField("page_url", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
                StructField("ingestion_time", StringType(), True)
            ])
            
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), user_events_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset")
            ).select(
                col("data.event_id"),
                col("data.session_id"),
                col("data.customer_id"),
                col("data.event_type"),
                to_timestamp(col("data.event_time")).alias("event_time"),
                col("data.page_url"),
                when(col("data.metadata.product_id").isNotNull(), 
                     col("data.metadata.product_id")).alias("product_id"),
                col("data.device_type"),
                col("data.metadata"),
                to_timestamp(col("data.ingestion_time")).alias("ingestion_time"),
                col("kafka_timestamp"),
                col("kafka_partition"),
                col("kafka_offset")
            ).filter(
                col("event_id").isNotNull() &
                col("customer_id").isNotNull() &
                col("event_type").isNotNull()
            )
            
            # Write stream to Bronze table
            query = parsed_df.writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("table", "bronze.raw_user_events") \
                .option("checkpointLocation", "s3a://warehouse/checkpoints/user_events") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("User events stream ingestion started")
            return query
            
        except Exception as e:
            logger.error(f"Error starting user events stream: {str(e)}")
            raise

    def ingest_marketplace_sales_stream(self):
        """Ingest marketplace sales from Kafka (with late arrivals)"""
        logger.info("Starting marketplace sales stream ingestion...")
        
        try:
            # Read from Kafka stream
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "marketplace_sales") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON messages
            sales_schema = StructType([
                StructField("transaction_id", StringType(), True),
                StructField("marketplace_name", StringType(), True),
                StructField("seller_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("amount", DecimalType(10,2), True),
                StructField("currency", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("transaction_time", StringType(), True),
                StructField("settlement_time", StringType(), True),
                StructField("payment_method", StringType(), True),
                StructField("status", StringType(), True),
                StructField("marketplace_metadata", MapType(StringType(), StringType()), True),
                StructField("ingestion_time", StringType(), True)
            ])
            
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), sales_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset")
            ).select(
                col("data.transaction_id"),
                col("data.marketplace_name"),
                col("data.seller_id"),
                col("data.product_id"),
                col("data.amount"),
                col("data.currency"),
                col("data.quantity"),
                to_timestamp(col("data.transaction_time")).alias("transaction_time"),
                to_timestamp(col("data.settlement_time")).alias("settlement_time"),
                col("data.payment_method"),
                col("data.status"),
                col("data.marketplace_metadata"),
                to_timestamp(col("data.ingestion_time")).alias("ingestion_time"),
                col("kafka_timestamp"),
                col("kafka_partition"),
                col("kafka_offset")
            ).filter(
                col("transaction_id").isNotNull() &
                col("product_id").isNotNull() &
                col("amount").isNotNull()
            )
            
            # Write stream to Bronze table
            query = parsed_df.writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("table", "bronze.raw_marketplace_sales") \
                .option("checkpointLocation", "s3a://warehouse/checkpoints/marketplace_sales") \
                .trigger(processingTime='60 seconds') \
                .start()
            
            logger.info("Marketplace sales stream ingestion started")
            return query
            
        except Exception as e:
            logger.error(f"Error starting marketplace sales stream: {str(e)}")
            raise

    def ingest_batch_data(self, data_source: str, process_date: str):
        """Ingest batch data sources"""
        logger.info(f"Ingesting batch data: {data_source} for {process_date}")
        
        if data_source == "product_catalog":
            self.ingest_product_catalog(process_date)
        elif data_source == "customer_data":
            self.ingest_customer_data(process_date)
        elif data_source == "marketing_campaigns":
            self.ingest_marketing_campaigns(process_date)
        elif data_source == "sales_transactions":
            self.ingest_sales_transactions(process_date)
        elif data_source == "user_activity":
            self.ingest_user_activity(process_date)
        else:
            logger.warning(f"Unknown batch data source: {data_source}")

    def ingest_product_catalog(self, process_date: str):
        """Simulate product catalog ingestion"""
        logger.info("Generating sample product catalog data...")
        
        try:
            # Create dynamic sample data using timestamp-based IDs
            import random
            import hashlib
            
            # Use process_date and current timestamp to create unique data
            random.seed(hashlib.md5(f"{process_date}_{datetime.now().timestamp()}".encode()).hexdigest())
            
            data = []
            categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty', 'Automotive']
            subcategories = ['Smartphones', 'Laptops', 'Headphones', 'Cameras', 'Accessories']
            brands = ['TechBrand', 'StyleCorp', 'HomeMax', 'SportsPro', 'BookWorld', 'ToyLand', 'BeautyPlus', 'AutoTech']
            
            # Generate unique product ID base using timestamp
            timestamp_base = int(datetime.now().timestamp()) % 100000
            
            for i in range(1, 11):  # 10 products for testing
                product_id = f"PROD-{timestamp_base + i:06d}"
                category = categories[i % len(categories)]
                subcategory = subcategories[i % len(subcategories)]
                brand = brands[i % len(brands)]
                price_val = 10.50 + (i * 5.25)  # Simple price calculation
                
                data.append((
                    product_id,
                    f"Product {product_id}",
                    category,
                    subcategory,
                    brand,
                    f"{price_val:.2f}",  # Convert to string format
                    f"Description for {product_id} - {category} product",
                    True,
                    process_date,
                    datetime.now().isoformat()
                ))
            
            # Define schema explicitly
            schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("base_price", StringType(), True),
                StructField("description", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("last_updated", StringType(), True),
                StructField("ingestion_time", StringType(), True)
            ])
            
            # Create DataFrame
            products_df = self.spark.createDataFrame(data, schema)
            record_count = len(data)  # Store count before DataFrame operations
            
            # Convert types
            products_df = products_df.select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("subcategory"),
                col("brand"),
                col("base_price").cast("decimal(10,2)").alias("base_price"),
                col("description"),
                col("is_active"),
                to_timestamp(col("last_updated")).alias("last_updated"),
                to_timestamp(col("ingestion_time")).alias("ingestion_time")
            )
            
            products_df.write \
                .mode("append") \
                .insertInto("bronze.raw_product_catalog")
            
            logger.info(f"Ingested {record_count} product records")
            
        except Exception as e:
            logger.error(f"Error ingesting product catalog: {str(e)}")
            raise

    def ingest_customer_data(self, process_date: str):
        """Simulate customer data ingestion"""
        logger.info("Generating sample customer data...")
        
        try:
            # Create dynamic sample data that changes each run
            import random
            import hashlib
            
            # Use process_date and current timestamp to create unique data
            random.seed(hashlib.md5(f"{process_date}_{datetime.now().timestamp()}".encode()).hexdigest())
            
            data = []
            cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Seattle', 'Miami', 'Boston', 'Denver', 'Austin']
            countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Sweden', 'Australia']
            tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
            
            # Generate unique customer ID base using timestamp
            timestamp_base = int(datetime.now().timestamp()) % 100000
            
            for i in range(1, 101):  # 100 customers for testing
                customer_id = f"CUST-{timestamp_base + i:06d}"
                
                # Generate some variation in the data
                city_index = random.randint(0, len(cities) - 1)
                country_index = random.randint(0, len(countries) - 1)
                tier_index = random.randint(0, len(tiers) - 1)
                
                # Add some customers with updated info (for SCD testing)
                if i % 10 == 0:  # Every 10th customer gets updated info
                    membership_tier = tiers[(tier_index + 1) % len(tiers)]  # Promote tier
                    last_updated = process_date
                else:
                    membership_tier = tiers[tier_index]
                    last_updated = process_date
                
                data.append((
                    customer_id,
                    f"Customer {timestamp_base + i}",
                    f"customer{timestamp_base + i}@email.com",
                    f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Park'])} St",
                    cities[city_index],
                    countries[country_index],
                    membership_tier,
                    "2024-01-01 00:00:00",
                    last_updated,
                    datetime.now().isoformat()
                ))
            
            # Define schema
            schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("customer_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("address", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("membership_tier", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("last_updated", StringType(), True),
                StructField("ingestion_time", StringType(), True)
            ])
            
            # Create DataFrame
            customers_df = self.spark.createDataFrame(data, schema)
            record_count = len(data)  # Store count before DataFrame operations
            
            # Convert types
            customers_df = customers_df.select(
                col("customer_id"),
                col("customer_name"),
                col("email"),
                col("address"),
                col("city"),
                col("country"),
                col("membership_tier"),
                to_timestamp(col("created_at")).alias("created_at"),
                to_timestamp(col("last_updated")).alias("last_updated"),
                to_timestamp(col("ingestion_time")).alias("ingestion_time")
            )
            
            customers_df.write \
                .mode("append") \
                .insertInto("bronze.raw_customer_data")
            
            logger.info(f"Ingested {record_count} customer records")
            
        except Exception as e:
            logger.error(f"Error ingesting customer data: {str(e)}")
            raise

    def ingest_marketing_campaigns(self, process_date: str):
        """Ingest marketing campaigns from sample JSON file with dynamic updates"""
        logger.info("Loading marketing campaigns from sample data file with dynamic processing...")
        
        try:
            # Load from JSON file instead of hardcoded values
            sample_data_path = "/opt/processing/sample_data/campaigns.json"
            
            # Read the JSON file using Spark with multiLine option
            campaigns_df = self.spark.read.option("multiLine", "true").json(sample_data_path)
            
            # Debug: Show schema
            logger.info("JSON file schema:")
            campaigns_df.printSchema()
            
            # Debug: Show sample data
            logger.info("Sample data from JSON file:")
            campaigns_df.show(3, truncate=False)
            
            # Update ingestion time to current time
            current_timestamp = datetime.now()
            campaigns_df = campaigns_df.withColumn("ingestion_time", lit(current_timestamp.isoformat()))
            
            # Show campaign data info
            record_count = campaigns_df.count()
            logger.info(f"Loaded {record_count} campaigns from JSON file")
            
            # Convert types - only selecting columns that match the table schema
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
            
            # Check if the bronze.raw_marketing_campaigns table exists and has data
            table_exists = False
            try:
                existing_count = self.spark.sql("SELECT COUNT(*) as count FROM bronze.raw_marketing_campaigns").collect()[0]['count']
                table_exists = existing_count > 0
                logger.info(f"Existing records in bronze.raw_marketing_campaigns: {existing_count}")
            except Exception as e:
                logger.info(f"Table doesn't exist or is empty: {str(e)}")
                
            # Option 1: For new tables or empty tables, just insert all records
            if not table_exists:
                logger.info("No existing campaigns found. Inserting all records.")
                campaigns_df.write \
                    .mode("append") \
                    .insertInto("bronze.raw_marketing_campaigns")
                
                logger.info(f"Ingested {record_count} new campaign records")
            # Option 2: For existing tables, ensure we have the latest for each campaign
            else:
                # Create a temporary view of the new data
                campaigns_df.createOrReplaceTempView("new_campaigns_temp")
                
                # Insert all records as we want to maintain history in the bronze layer
                # This is important for the SCD Type 2 processing later
                campaigns_df.write \
                    .mode("append") \
                    .insertInto("bronze.raw_marketing_campaigns")
                
                # Count after insertion
                after_count = self.spark.sql("SELECT COUNT(*) as count FROM bronze.raw_marketing_campaigns").collect()[0]['count']
                logger.info(f"Record count after insertion: {after_count} (added {record_count} records)")
                
                # Show distinct campaign count for verification
                distinct_count = self.spark.sql(
                    "SELECT COUNT(DISTINCT campaign_id) as count FROM bronze.raw_marketing_campaigns"
                ).collect()[0]['count']
                logger.info(f"Distinct campaigns in bronze layer: {distinct_count}")
            
            logger.info(f"Successfully ingested campaigns with {record_count} records")
            
        except Exception as e:
            logger.error(f"Error ingesting marketing campaigns: {str(e)}")
            raise

    def ingest_sales_transactions(self, process_date: str):
        """Simulate sales transactions ingestion"""
        logger.info("Generating sample sales transactions...")
        
        try:
            # Create dynamic sample sales data - isolate random generation completely
            import random as py_random
            import hashlib
            from datetime import datetime, timedelta
            
            logger.info("DEBUG: Starting data generation outside Spark context...")
            
            # Use process_date and current timestamp to create unique data
            seed_str = f"{process_date}_{datetime.now().timestamp()}"
            seed_hash = hashlib.md5(seed_str.encode()).hexdigest()
            py_random.seed(seed_hash)
            logger.info(f"DEBUG: Seed set to: {seed_hash[:10]}...")
            
            data = []
            marketplaces = ['Amazon', 'eBay', 'Shopify', 'Etsy', 'WooCommerce']
            payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Digital Wallet', 'Cash']
            currencies = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
            statuses = ['completed', 'pending', 'shipped', 'delivered']
            
            # Generate unique transaction ID base using timestamp
            timestamp_base = int(datetime.now().timestamp()) % 100000
            logger.info(f"DEBUG: Timestamp base: {timestamp_base}")
            
            # Generate sales for existing customers (use same timestamp base for customer IDs)
            for i in range(1, 11):  # Start with just 10 records for debugging
                logger.info(f"DEBUG: Generating record {i}...")
                
                transaction_id = f"TXN-{timestamp_base + i:08d}"
                customer_id = f"CUST-{timestamp_base + py_random.randint(1, 100):06d}"
                product_id = f"PROD-{timestamp_base + py_random.randint(1, 10):06d}"
                
                # Generate transaction details with explicit string conversion
                marketplace = py_random.choice(marketplaces)
                seller_id = f"SELLER-{py_random.randint(1000, 9999)}"
                
                # Generate amount very carefully - use Python's built-in round, not PySpark's round
                raw_amount_float = py_random.uniform(10.00, 500.00)
                logger.info(f"DEBUG: Record {i} - raw_amount_float: {raw_amount_float} (type: {type(raw_amount_float)})")
                
                # Use round from builtins module directly to avoid PySpark round
                import builtins
                rounded_amount = builtins.round(raw_amount_float, 2)
                logger.info(f"DEBUG: Record {i} - rounded_amount: {rounded_amount} (type: {type(rounded_amount)})")
                
                amount_str = str(rounded_amount)
                logger.info(f"DEBUG: Record {i} - amount_str: '{amount_str}' (type: {type(amount_str)})")
                
                # Generate quantity very carefully  
                raw_quantity_int = py_random.randint(1, 5)
                logger.info(f"DEBUG: Record {i} - raw_quantity_int: {raw_quantity_int} (type: {type(raw_quantity_int)})")
                
                quantity_str = str(raw_quantity_int)
                logger.info(f"DEBUG: Record {i} - quantity_str: '{quantity_str}' (type: {type(quantity_str)})")
                
                currency = py_random.choice(currencies)
                payment_method = py_random.choice(payment_methods)
                status = py_random.choice(statuses)
                
                # Create transaction time around the process date
                base_time = datetime.strptime(process_date, '%Y-%m-%d')
                transaction_time = base_time + timedelta(
                    hours=py_random.randint(0, 23),
                    minutes=py_random.randint(0, 59),
                    seconds=py_random.randint(0, 59)
                )
                settlement_time = transaction_time + timedelta(hours=py_random.randint(1, 72))
                
                # Create metadata - fix None values for MAP type
                metadata = {
                    'promotion_code': f"PROMO{py_random.randint(100, 999)}" if py_random.random() > 0.7 else "",
                    'device_type': py_random.choice(['mobile', 'desktop', 'tablet']),
                    'channel': py_random.choice(['web', 'mobile_app', 'api'])
                }
                logger.info(f"DEBUG: Record {i} - metadata: {metadata} (type: {type(metadata)})")
                
                # Create the tuple with careful validation
                record_tuple = (
                    transaction_id,
                    marketplace,
                    seller_id,
                    product_id,
                    amount_str,  # Should be string
                    currency,
                    quantity_str,  # Should be string
                    transaction_time.isoformat(),
                    settlement_time.isoformat(),
                    payment_method,
                    status,
                    metadata,  # Keep as dict for MAP type
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),  # kafka_timestamp placeholder
                    str(0),  # Convert to string
                    str(i)   # Convert to string
                )
                
                # Validate each field in the tuple
                logger.info(f"DEBUG: Record {i} - tuple length: {len(record_tuple)}")
                for idx, field in enumerate(record_tuple):
                    logger.info(f"DEBUG: Record {i} - field[{idx}]: {field} (type: {type(field)})")
                
                data.append(record_tuple)
                logger.info(f"DEBUG: Record {i} - successfully added to data list")
            
            logger.info(f"DEBUG: Data generation completed. Total records: {len(data)}")
            
            # Define schema
            logger.info("DEBUG: Defining schema...")
            schema = StructType([
                StructField("transaction_id", StringType(), True),
                StructField("marketplace_name", StringType(), True),
                StructField("seller_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("amount", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("quantity", StringType(), True),
                StructField("transaction_time", StringType(), True),
                StructField("settlement_time", StringType(), True),
                StructField("payment_method", StringType(), True),
                StructField("status", StringType(), True),
                StructField("marketplace_metadata", MapType(StringType(), StringType()), True),  # Use MAP type
                StructField("ingestion_time", StringType(), True),
                StructField("kafka_timestamp", StringType(), True),
                StructField("kafka_partition", StringType(), True),
                StructField("kafka_offset", StringType(), True)
            ])
            logger.info("DEBUG: Schema defined successfully")
            
            # Create DataFrame
            logger.info("DEBUG: Creating DataFrame...")
            sales_df = self.spark.createDataFrame(data, schema)
            logger.info("DEBUG: DataFrame created successfully")
            record_count = len(data)  # Store count before DataFrame operations
            
            # Convert types
            logger.info("DEBUG: Converting types...")
            sales_df = sales_df.select(
                col("transaction_id"),
                col("marketplace_name"),
                col("seller_id"),
                col("product_id"),
                col("amount").cast("decimal(10,2)").alias("amount"),
                col("currency"),
                col("quantity").cast("int").alias("quantity"),
                to_timestamp(col("transaction_time")).alias("transaction_time"),
                to_timestamp(col("settlement_time")).alias("settlement_time"),
                col("payment_method"),
                col("status"),
                col("marketplace_metadata"),  # Already MAP type
                to_timestamp(col("ingestion_time")).alias("ingestion_time"),
                to_timestamp(col("kafka_timestamp")).alias("kafka_timestamp"),
                col("kafka_partition").cast("int").alias("kafka_partition"),
                col("kafka_offset").cast("bigint").alias("kafka_offset")
            )
            logger.info("DEBUG: Type conversion completed")
            
            sales_df.write \
                .mode("append") \
                .insertInto("bronze.raw_marketplace_sales")
            
            logger.info(f"Ingested {record_count} sales transaction records")
            
        except Exception as e:
            logger.error(f"Error ingesting sales transactions: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    def ingest_user_activity(self, process_date: str):
        """Simulate user activity events ingestion"""
        logger.info("Generating sample user activity events...")
        
        try:
            # Create dynamic sample user activity data
            import random
            import hashlib
            from datetime import datetime, timedelta
            
            # Use process_date and current timestamp to create unique data
            random.seed(hashlib.md5(f"{process_date}_{datetime.now().timestamp()}".encode()).hexdigest())
            
            data = []
            event_types = ['page_view', 'product_view', 'add_to_cart', 'purchase', 'search', 'login', 'logout']
            device_types = ['mobile', 'desktop', 'tablet']
            page_urls = [
                '/home', '/products', '/checkout', '/cart', '/profile', 
                '/search', '/product/detail', '/category/electronics', '/category/clothing'
            ]
            
            # Generate unique event ID base using timestamp
            timestamp_base = int(datetime.now().timestamp()) % 100000
            
            # Generate user activity events
            for i in range(1, 101):  # 100 user activity events
                event_id = f"EVT-{timestamp_base + i:08d}"
                session_id = f"SES-{timestamp_base + (i // 10):08d}"  # Group events by sessions
                customer_id = f"CUST-{timestamp_base + random.randint(1, 100):06d}"  # Link to customers
                
                event_type = random.choice(event_types)
                device_type = random.choice(device_types)
                page_url = random.choice(page_urls)
                
                # Create event time around the process date
                base_time = datetime.strptime(process_date, '%Y-%m-%d')
                event_time = base_time + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                # Create metadata based on event type
                if event_type in ['product_view', 'add_to_cart', 'purchase']:
                    product_id = f"PROD-{timestamp_base + random.randint(1, 10):06d}"
                else:
                    product_id = None
                
                metadata = {
                    'user_agent': f"Browser/{random.randint(100, 999)}",
                    'ip_address': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    'referrer': random.choice(['google.com', 'facebook.com', 'direct', 'email']),
                }
                if product_id:
                    metadata['product_id'] = product_id
                
                data.append((
                    event_id,
                    session_id,
                    customer_id,
                    event_type,
                    event_time.isoformat(),
                    page_url,
                    product_id,
                    device_type,
                    metadata,  # Keep as dict for MAP type
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),  # kafka_timestamp placeholder
                    str(0),  # Convert to string
                    str(i)   # Convert to string
                ))
            
            # Define schema
            schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_time", StringType(), True),
                StructField("page_url", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),  # Use MAP type
                StructField("ingestion_time", StringType(), True),
                StructField("kafka_timestamp", StringType(), True),
                StructField("kafka_partition", StringType(), True),
                StructField("kafka_offset", StringType(), True)
            ])
            
            # Create DataFrame
            events_df = self.spark.createDataFrame(data, schema)
            record_count = len(data)  # Store count before DataFrame operations
            
            # Convert types
            events_df = events_df.select(
                col("event_id"),
                col("session_id"),
                col("customer_id"),
                col("event_type"),
                to_timestamp(col("event_time")).alias("event_time"),
                col("page_url"),
                col("product_id"),
                col("device_type"),
                col("metadata"),  # Already MAP type
                to_timestamp(col("ingestion_time")).alias("ingestion_time"),
                to_timestamp(col("kafka_timestamp")).alias("kafka_timestamp"),
                col("kafka_partition").cast("int").alias("kafka_partition"),
                col("kafka_offset").cast("bigint").alias("kafka_offset")
            )
            
            events_df.write \
                .mode("append") \
                .insertInto("bronze.raw_user_events")
            
            logger.info(f"Ingested {record_count} user activity event records")
            
        except Exception as e:
            logger.error(f"Error ingesting user activity events: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Bronze Layer Ingestion')
    parser.add_argument('--mode', required=True, 
                       choices=['stream', 'batch'],
                       help='Ingestion mode: stream or batch')
    parser.add_argument('--source', 
                       choices=['user_events', 'marketplace_sales', 'product_catalog', 
                               'customer_data', 'marketing_campaigns', 'sales_transactions', 
                               'user_activity', 'all'],
                       help='Data source to ingest')
    parser.add_argument('--date', 
                       help='Processing date for batch ingestion (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    ingestion = BronzeIngestion()
    
    try:
        # Create schemas
        ingestion.create_bronze_schemas()
        
        if args.mode == 'stream':
            logger.info("Starting streaming ingestion...")
            
            queries = []
            
            if args.source in ['user_events', 'all']:
                queries.append(ingestion.ingest_user_events_stream())
            
            if args.source in ['marketplace_sales', 'all']:
                queries.append(ingestion.ingest_marketplace_sales_stream())
            
            if queries:
                # Wait for all streams to complete
                for query in queries:
                    query.awaitTermination()
            else:
                logger.warning("No streams to start")
                
        elif args.mode == 'batch':
            if not args.date:
                logger.error("Date parameter required for batch ingestion")
                sys.exit(1)
            
            if args.source == 'all':
                ingestion.ingest_batch_data('product_catalog', args.date)
                ingestion.ingest_batch_data('customer_data', args.date)
                ingestion.ingest_batch_data('marketing_campaigns', args.date)
                ingestion.ingest_batch_data('sales_transactions', args.date)
                ingestion.ingest_batch_data('user_activity', args.date)
            else:
                ingestion.ingest_batch_data(args.source, args.date)
        
        logger.info("Bronze ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        sys.exit(1)
    finally:
        ingestion.spark.stop()

if __name__ == "__main__":
    main()