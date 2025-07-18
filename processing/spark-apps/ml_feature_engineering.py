# processing/spark-apps/ml_feature_engineering.py

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, current_timestamp, col  # Explicit imports
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MLFeatureEngineering:
    def __init__(self):
        """Initialize Spark session with Iceberg support"""
        self.spark = SparkSession.builder \
            .appName("ML Feature Engineering") \
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

    def create_feature_schemas(self):
        """Create ML feature tables if they don't exist"""
        
        # Create gold database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        
        # Customer-Product Interactions Feature Table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.customer_product_interactions (
                customer_id STRING,
                product_id STRING,
                
                -- Frequency Features
                view_count_7d BIGINT,
                view_count_30d BIGINT,
                view_count_90d BIGINT,
                purchase_count_7d BIGINT,
                purchase_count_30d BIGINT,
                cart_add_count_7d BIGINT,
                cart_add_count_30d BIGINT,
                
                -- Interaction Features
                avg_time_spent_seconds DECIMAL(10,2),
                cart_abandon_flag BOOLEAN,
                last_interaction_days_ago INT,
                
                -- Category Affinity Features
                category_purchase_count BIGINT,
                category_affinity_score DECIMAL(5,4),
                
                -- Customer Features
                customer_tenure_days INT,
                customer_total_purchases BIGINT,
                customer_avg_order_value DECIMAL(10,2),
                
                -- Product Features
                product_popularity_score DECIMAL(5,4),
                product_avg_rating DECIMAL(3,2),
                product_price_rank_in_category INT,
                
                -- Target Variables
                purchased_next_7d BOOLEAN,
                purchased_next_30d BOOLEAN,
                
                -- Metadata
                feature_date DATE,
                created_time TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (feature_date)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)

    def calculate_recency_features(self, as_of_date: str):
        """Calculate recency-based features"""
        logger.info("Calculating recency features...")
        
        # Define time windows
        date_7d_ago = (datetime.strptime(as_of_date, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
        date_30d_ago = (datetime.strptime(as_of_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        date_90d_ago = (datetime.strptime(as_of_date, '%Y-%m-%d') - timedelta(days=90)).strftime('%Y-%m-%d')
        
        logger.info(f"Date ranges: {date_90d_ago} to {as_of_date}")
        logger.info(f"7d ago: {date_7d_ago}, 30d ago: {date_30d_ago}, 90d ago: {date_90d_ago}")
        
        # Check source data availability
        source_count = self.spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM silver.standardized_user_events 
            WHERE DATE(event_time) >= '{date_90d_ago}' 
              AND DATE(event_time) <= '{as_of_date}'
        """).collect()[0]['count']
        logger.info(f"Found {source_count} user events in date range for recency features")
        
        recency_features = self.spark.sql(f"""
            WITH user_interactions AS (
                SELECT 
                    customer_id,
                    product_id,
                    event_type,
                    event_time,
                    time_spent_seconds,
                    DATEDIFF('{as_of_date}', DATE(event_time)) as days_ago
                FROM silver.standardized_user_events
                WHERE DATE(event_time) >= '{date_90d_ago}'
                  AND DATE(event_time) <= '{as_of_date}'
                  AND customer_id IS NOT NULL
                  AND product_id IS NOT NULL
            ),
            
            recency_agg AS (
                SELECT 
                    customer_id,
                    product_id,
                    
                    -- View counts by time window
                    SUM(CASE WHEN event_type = 'product_view' AND days_ago <= 7 THEN 1 ELSE 0 END) as view_count_7d,
                    SUM(CASE WHEN event_type = 'product_view' AND days_ago <= 30 THEN 1 ELSE 0 END) as view_count_30d,
                    SUM(CASE WHEN event_type = 'product_view' AND days_ago <= 90 THEN 1 ELSE 0 END) as view_count_90d,
                    
                    -- Cart interactions
                    SUM(CASE WHEN event_type = 'add_to_cart' AND days_ago <= 7 THEN 1 ELSE 0 END) as cart_add_count_7d,
                    SUM(CASE WHEN event_type = 'add_to_cart' AND days_ago <= 30 THEN 1 ELSE 0 END) as cart_add_count_30d,
                    
                    -- Interaction quality metrics
                    AVG(CASE WHEN event_type = 'product_view' THEN time_spent_seconds END) as avg_time_spent_seconds,
                    
                    -- Image view ratio (assuming some events might be image views)
                    CASE 
                        WHEN SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) > 0 
                        THEN CAST(SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS DECIMAL(5,4))
                        ELSE CAST(0.0 AS DECIMAL(5,4))
                    END as image_view_ratio,
                    
                    -- Last interaction
                    MIN(days_ago) as last_interaction_days_ago,
                    
                    -- Cart abandonment flag
                    CASE 
                        WHEN SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) > 0 
                         AND SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) = 0 
                        THEN CAST(TRUE AS BOOLEAN)
                        ELSE CAST(FALSE AS BOOLEAN)
                    END as cart_abandon_flag
                    
                FROM user_interactions
                GROUP BY customer_id, product_id
            )
            
            SELECT * FROM recency_agg
        """)
        
        # Debug the recency features result
        logger.info("Debugging recency features result...")
        self.debug_dataframe_schema(recency_features, "RECENCY_FEATURES")
        
        return recency_features

    def calculate_frequency_features(self, as_of_date: str):
        """Calculate frequency-based features"""
        logger.info("Calculating frequency features...")
        
        date_30d_ago = (datetime.strptime(as_of_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"Frequency date range: {date_30d_ago} to {as_of_date}")
        
        # Check source data availability
        sales_count = self.spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM silver.standardized_sales 
            WHERE DATE(transaction_time) >= '{date_30d_ago}' 
              AND DATE(transaction_time) <= '{as_of_date}'
        """).collect()[0]['count']
        logger.info(f"Found {sales_count} sales records in date range for frequency features")
        
        frequency_features = self.spark.sql(f"""
            WITH purchase_history AS (
                SELECT 
                    customer_id,
                    product_id,
                    transaction_time,
                    total_amount,
                    DATEDIFF('{as_of_date}', DATE(transaction_time)) as days_ago
                FROM silver.standardized_sales
                WHERE DATE(transaction_time) >= '{date_30d_ago}'
                  AND DATE(transaction_time) <= '{as_of_date}'
            ),
            
            frequency_agg AS (
                SELECT 
                    customer_id,
                    product_id,
                    
                    -- Purchase counts
                    SUM(CASE WHEN days_ago <= 7 THEN 1 ELSE 0 END) as purchase_count_7d,
                    SUM(CASE WHEN days_ago <= 30 THEN 1 ELSE 0 END) as purchase_count_30d
                    
                FROM purchase_history
                GROUP BY customer_id, product_id
            )
            
            SELECT * FROM frequency_agg
        """)
        
        logger.info(f"Frequency features calculated: {frequency_features.count()} customer-product pairs")
        frequency_features = frequency_features.cache()
        return frequency_features

    def calculate_category_affinity(self, as_of_date: str):
        """Calculate category affinity features"""
        logger.info("Calculating category affinity features...")
        
        date_90d_ago = (datetime.strptime(as_of_date, '%Y-%m-%d') - timedelta(days=90)).strftime('%Y-%m-%d')
        
        logger.info(f"Category affinity date range: {date_90d_ago} to {as_of_date}")
        
        # Check source data availability
        sales_with_products = self.spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM silver.standardized_sales s
            JOIN silver.dim_product p ON s.product_id = p.product_id AND p.is_current = true
            WHERE DATE(s.transaction_time) >= '{date_90d_ago}' 
              AND DATE(s.transaction_time) <= '{as_of_date}'
        """).collect()[0]['count']
        logger.info(f"Found {sales_with_products} sales records with product info for category affinity")
        
        category_features = self.spark.sql(f"""
            WITH customer_category_purchases AS (
                SELECT 
                    s.customer_id,
                    p.category,
                    COUNT(*) as category_purchase_count,
                    SUM(s.total_amount) as category_spend
                FROM silver.standardized_sales s
                JOIN silver.dim_product p ON s.product_id = p.product_id AND p.is_current = true
                WHERE DATE(s.transaction_time) >= '{date_90d_ago}'
                  AND DATE(s.transaction_time) <= '{as_of_date}'
                GROUP BY s.customer_id, p.category
            ),
            
            customer_totals AS (
                SELECT 
                    customer_id,
                    SUM(category_purchase_count) as total_purchases,
                    SUM(category_spend) as total_spend
                FROM customer_category_purchases
                GROUP BY customer_id
            ),
            
            category_affinity AS (
                SELECT 
                    ccp.customer_id,
                    ccp.category,
                    ccp.category_purchase_count,
                    ROUND(ccp.category_purchase_count * 1.0 / ct.total_purchases, 4) as category_affinity_score
                FROM customer_category_purchases ccp
                JOIN customer_totals ct ON ccp.customer_id = ct.customer_id
            ),
            
            product_category_mapping AS (
                SELECT 
                    p.product_id,
                    p.category,
                    ca.customer_id,
                    ca.category_purchase_count,
                    ca.category_affinity_score
                FROM silver.dim_product p
                CROSS JOIN category_affinity ca
                WHERE p.is_current = true
                  AND p.category = ca.category
            )
            
            SELECT 
                customer_id,
                product_id,
                category_purchase_count,
                category_affinity_score
            FROM product_category_mapping
        """)
        
        logger.info(f"Category affinity features calculated: {category_features.count()} customer-product pairs")
        category_features = category_features.cache()
        return category_features

    def calculate_customer_features(self, as_of_date: str):
        """Calculate customer-level features"""
        logger.info("Calculating customer features...")
        
        # Check source data availability
        total_customers = self.spark.sql(f"""
            SELECT COUNT(DISTINCT customer_id) as count 
            FROM silver.standardized_sales 
            WHERE DATE(transaction_time) <= '{as_of_date}'
        """).collect()[0]['count']
        logger.info(f"Found {total_customers} unique customers for customer features")
        
        customer_features = self.spark.sql(f"""
            WITH customer_stats AS (
                SELECT 
                    customer_id,
                    
                    -- Customer tenure (days since first purchase)
                    DATEDIFF('{as_of_date}', MIN(DATE(transaction_time))) as customer_tenure_days,
                    
                    -- Purchase behavior
                    COUNT(*) as customer_total_purchases,
                    AVG(total_amount) as customer_avg_order_value
                    
                FROM silver.standardized_sales
                WHERE DATE(transaction_time) <= '{as_of_date}'
                GROUP BY customer_id
            )
            
            SELECT * FROM customer_stats
        """)
        
        logger.info(f"Customer features calculated: {customer_features.count()} customers")
        customer_features = customer_features.cache()
        return customer_features

    def calculate_product_features(self, as_of_date: str):
        """Calculate product-level features"""
        logger.info("Calculating product features...")
        
        date_30d_ago = (datetime.strptime(as_of_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Check source data availability
        total_products = self.spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM silver.dim_product 
            WHERE is_current = true
        """).collect()[0]['count']
        logger.info(f"Found {total_products} current products for product features")
        
        product_features = self.spark.sql(f"""
            WITH product_stats AS (
                SELECT 
                    p.product_id,
                    p.category,
                    p.base_price,
                    
                    -- Product popularity (based on recent purchases)
                    COALESCE(s.purchase_count, 0) as recent_purchases,
                    
                    -- Price rank within category
                    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY p.base_price DESC) as product_price_rank_in_category
                    
                FROM silver.dim_product p
                LEFT JOIN (
                    SELECT 
                        product_id,
                        COUNT(*) as purchase_count
                    FROM silver.standardized_sales
                    WHERE DATE(transaction_time) >= '{date_30d_ago}'
                      AND DATE(transaction_time) <= '{as_of_date}'
                    GROUP BY product_id
                ) s ON p.product_id = s.product_id
                WHERE p.is_current = true
            ),
            
            category_stats AS (
                SELECT 
                    category,
                    MAX(recent_purchases) as max_purchases_in_category
                FROM product_stats
                GROUP BY category
            ),
            
            product_with_popularity AS (
                SELECT 
                    ps.*,
                    CASE 
                        WHEN cs.max_purchases_in_category > 0 
                        THEN ROUND(ps.recent_purchases * 1.0 / cs.max_purchases_in_category, 4)
                        ELSE 0.0 
                    END as product_popularity_score,
                    
                    -- Simulated average rating (in real implementation, this would come from reviews)
                    ROUND(3.5 + (RAND() * 1.5), 2) as product_avg_rating
                    
                FROM product_stats ps
                JOIN category_stats cs ON ps.category = cs.category
            )
            
            SELECT 
                product_id,
                product_popularity_score,
                product_avg_rating,
                product_price_rank_in_category
            FROM product_with_popularity
        """)
        
        logger.info(f"Product features calculated: {product_features.count()} products")
        product_features = product_features.cache()
        return product_features

    def calculate_target_variables(self, as_of_date: str):
        """Calculate target variables for prediction"""
        logger.info("Calculating target variables...")
        
        date_7d_future = (datetime.strptime(as_of_date, '%Y-%m-%d') + timedelta(days=7)).strftime('%Y-%m-%d')
        date_30d_future = (datetime.strptime(as_of_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"Target date range: {as_of_date} to {date_30d_future}")
        
        # Check if we have future data for targets
        future_sales_count = self.spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM silver.standardized_sales 
            WHERE DATE(transaction_time) >= '{as_of_date}' 
              AND DATE(transaction_time) < '{date_30d_future}'
        """).collect()[0]['count']
        logger.info(f"Found {future_sales_count} future sales records for target calculation")
        
        target_features = self.spark.sql(f"""
            WITH future_purchases AS (
                SELECT 
                    customer_id,
                    product_id,
                    DATE(transaction_time) as purchase_date
                FROM silver.standardized_sales
                WHERE DATE(transaction_time) >= '{as_of_date}'
                  AND DATE(transaction_time) < '{date_30d_future}'
            ),
            
            customer_product_targets AS (
                SELECT 
                    customer_id,
                    product_id,
                    
                    -- 7-day purchase target
                    CASE 
                        WHEN MIN(purchase_date) <= '{date_7d_future}' THEN true 
                        ELSE false 
                    END as purchased_next_7d,
                    
                    -- 30-day purchase target
                    CASE 
                        WHEN COUNT(*) > 0 THEN true 
                        ELSE false 
                    END as purchased_next_30d
                    
                FROM future_purchases
                GROUP BY customer_id, product_id
            )
            
            SELECT * FROM customer_product_targets
        """)
        
        logger.info(f"Target variables calculated: {target_features.count()} customer-product pairs")
        target_features = target_features.cache()
        return target_features

    def generate_comprehensive_features(self, as_of_date: str):
        """Generate comprehensive ML feature set"""
        logger.info(f"Generating ML features for date: {as_of_date}")
        
        try:
            # Calculate individual feature sets with error handling
            logger.info("Step 1: Calculating recency features...")
            recency_features = self.calculate_recency_features(as_of_date)
            
            logger.info("Step 2: Calculating frequency features...")
            frequency_features = self.calculate_frequency_features(as_of_date)
            
            logger.info("Step 3: Calculating category features...")
            category_features = self.calculate_category_affinity(as_of_date)
            
            logger.info("Step 4: Calculating customer features...")
            customer_features = self.calculate_customer_features(as_of_date)
            
            logger.info("Step 5: Calculating product features...")
            product_features = self.calculate_product_features(as_of_date)
            
            logger.info("Step 6: Calculating target features...")
            target_features = self.calculate_target_variables(as_of_date)
            
        except Exception as e:
            logger.error(f"Error during feature calculation: {str(e)}")
            raise
        
        # Get all unique customer-product pairs from interactions
        base_pairs = self.spark.sql(f"""
            SELECT DISTINCT 
                customer_id,
                product_id
            FROM silver.standardized_user_events
            WHERE DATE(event_time) >= DATE_SUB('{as_of_date}', 90)
              AND DATE(event_time) <= '{as_of_date}'
              AND customer_id IS NOT NULL
              AND product_id IS NOT NULL
        """)
        
        logger.info(f"Found {base_pairs.count()} unique customer-product pairs")
        
        # Debug base pairs
        self.debug_dataframe_schema(base_pairs, "BASE_PAIRS")
        
        # Join all features together
        logger.info("Starting feature joins...")
        
        # Join recency features
        final_features = base_pairs.join(recency_features, ["customer_id", "product_id"], "left")
        logger.info("After recency features join:")
        self.debug_dataframe_schema(final_features, "AFTER_RECENCY_JOIN")
        
        # Join frequency features
        final_features = final_features.join(frequency_features, ["customer_id", "product_id"], "left")
        logger.info("After frequency features join:")
        self.debug_dataframe_schema(final_features, "AFTER_FREQUENCY_JOIN")
        
        # Join category features
        final_features = final_features.join(category_features, ["customer_id", "product_id"], "left")
        logger.info("After category features join:")
        self.debug_dataframe_schema(final_features, "AFTER_CATEGORY_JOIN")
        
        # Join customer features  
        final_features = final_features.join(customer_features, ["customer_id"], "left")
        logger.info("After customer features join:")
        self.debug_dataframe_schema(final_features, "AFTER_CUSTOMER_JOIN")
        
        # Join product features
        final_features = final_features.join(product_features, ["product_id"], "left")  
        logger.info("After product features join:")
        self.debug_dataframe_schema(final_features, "AFTER_PRODUCT_JOIN")
        
        # Join target features
        final_features = final_features.join(target_features, ["customer_id", "product_id"], "left")
        logger.info("After target features join:")
        self.debug_dataframe_schema(final_features, "AFTER_TARGET_JOIN")
        
        # Fill null values with appropriate defaults
        final_features = final_features.fillna({
            'view_count_7d': 0,
            'view_count_30d': 0,
            'view_count_90d': 0,
            'purchase_count_7d': 0,
            'purchase_count_30d': 0,
            'cart_add_count_7d': 0,
            'cart_add_count_30d': 0,
            'avg_time_spent_seconds': 0.0,
            'image_view_ratio': 0.0,
            'cart_abandon_flag': False,
            'last_interaction_days_ago': 999,
            'category_purchase_count': 0,
            'category_affinity_score': 0.0,
            'customer_tenure_days': 0,
            'customer_total_purchases': 0,
            'customer_avg_order_value': 0.0,
            'product_popularity_score': 0.0,
            'product_avg_rating': 3.0,
            'product_price_rank_in_category': 999,
            'purchased_next_7d': False,
            'purchased_next_30d': False
        })
        
        # Debug after fillna
        logger.info("After fillna operation:")
        self.debug_dataframe_schema(final_features, "AFTER_FILLNA")
        
        # Add metadata
        final_features = final_features.withColumn("feature_date", lit(as_of_date).cast("date")) \
                                      .withColumn("created_time", current_timestamp())
        
        return final_features

    def debug_dataframe_schema(self, df, df_name):
        """Debug function to print DataFrame schema and sample data"""
        logger.info(f"=== DEBUGGING {df_name} ===")
        logger.info(f"Schema for {df_name}:")
        for field in df.schema.fields:
            logger.info(f"  {field.name}: {field.dataType}")
        
        count = df.count()
        logger.info(f"Row count for {df_name}: {count}")
        
        if count > 0:
            logger.info(f"Sample data from {df_name}:")
            sample_data = df.limit(3).collect()
            for i, row in enumerate(sample_data):
                logger.info(f"  Row {i+1}: {row.asDict()}")
        
        # Check for problematic columns specifically
        problematic_cols = ['image_view_ratio', 'cart_abandon_flag']
        for col_name in problematic_cols:
            if col_name in df.columns:
                col_type = dict(df.dtypes)[col_name]
                logger.info(f"  {col_name} data type: {col_type}")
                
                # Show distinct values for this column
                distinct_vals = df.select(col_name).distinct().limit(5).collect()
                logger.info(f"  {col_name} sample values: {[row[col_name] for row in distinct_vals]}")
        
        logger.info(f"=== END DEBUGGING {df_name} ===")

    def debug_sql_query_result(self, query, query_name):
        """Debug function to test SQL query and show results"""
        logger.info(f"=== DEBUGGING SQL QUERY: {query_name} ===")
        try:
            result_df = self.spark.sql(query)
            self.debug_dataframe_schema(result_df, f"{query_name}_RESULT")
            return result_df
        except Exception as e:
            logger.error(f"Error executing query {query_name}: {str(e)}")
            logger.error(f"Query: {query}")
            raise

    def generate_ml_features(self, as_of_date: str):
        """Main method to generate ML features"""
        try:
            # Create schemas - but use the existing gold directory structure
            logger.info("Creating/ensuring gold database and table schema...")
            
            # Verify existing data in the target table
            try:
                existing_count = self.spark.sql("SELECT COUNT(*) as count FROM gold.customer_product_interactions").collect()[0]['count']
                logger.info(f"Found {existing_count} existing records in gold.customer_product_interactions")
                
                # Check table structure and status
                logger.info("Current structure of gold.customer_product_interactions:")
                self.spark.sql("DESCRIBE TABLE gold.customer_product_interactions").show(truncate=False)
                
                # Check if partitions exist
                logger.info("Current partitions in gold.customer_product_interactions:")
                self.spark.sql("SHOW PARTITIONS gold.customer_product_interactions").show(truncate=False)
            except Exception as e:
                logger.warning(f"Could not check existing data: {str(e)} - this is expected if the table doesn't exist yet")
            
            # Generate features
            features_df = self.generate_comprehensive_features(as_of_date)
            
            # Data quality check
            feature_count = features_df.count()
            logger.info(f"Generated {feature_count} feature records")
            
            if feature_count == 0:
                logger.warning("No features generated - check input data")
                return
            
            # Write to Gold layer using Iceberg format to maintain consistency
            logger.info("Writing ML features to gold layer...")
            
            # Create the table schema first if it doesn't exist
            self.create_feature_schemas()
            
            # Debug the schema of the features dataframe
            logger.info("Schema of features dataframe before writing:")
            features_df.printSchema()
            
            # Show partition columns for features_df
            logger.info("Partition columns for features_df:")
            logger.info(f"Columns available for partitioning: {features_df.columns}")
            
            # Write directly to the Iceberg table - FIXED: Removed partitionBy since table is already partitioned
            logger.info(f"Writing {feature_count} features to gold.customer_product_interactions...")
            
            # Show a sample of data before writing
            logger.info("Sample data to be written:")
            features_df.limit(3).show(truncate=False)
            
            # Select only the columns that match the table schema to avoid mismatches
            logger.info("Selecting columns to match table schema...")
            selected_features_df = features_df.select(
                "customer_id", 
                "product_id",
                "view_count_7d", 
                "view_count_30d", 
                "view_count_90d",
                "purchase_count_7d", 
                "purchase_count_30d",
                "cart_add_count_7d", 
                "cart_add_count_30d",
                "avg_time_spent_seconds",
                "cart_abandon_flag",
                "last_interaction_days_ago",
                "category_purchase_count",
                "category_affinity_score",
                "customer_tenure_days",
                "customer_total_purchases",
                "customer_avg_order_value",
                "product_popularity_score",
                "product_avg_rating",
                "product_price_rank_in_category",
                "purchased_next_7d",
                "purchased_next_30d",
                "feature_date",
                "created_time"
            )
            
            # Double-check the schema after selection
            logger.info("Schema after column selection (should match table schema):")
            selected_features_df.printSchema()
            
            # Write directly to the Iceberg table - FIXED: Removed partitionBy since table is already partitioned
            logger.info(f"Writing {feature_count} features to gold.customer_product_interactions...")
            
            selected_features_df.write \
                .mode("overwrite") \
                .insertInto("gold.customer_product_interactions")
            
            logger.info(f"Successfully wrote {feature_count} ML features to gold.customer_product_interactions")
            
            # Verify data was written correctly
            try:
                after_count = self.spark.sql("SELECT COUNT(*) as count FROM gold.customer_product_interactions").collect()[0]['count']
                logger.info(f"After writing: {after_count} records in gold.customer_product_interactions")
                
                # Count records for this specific feature_date
                today_count = self.spark.sql(f"SELECT COUNT(*) as count FROM gold.customer_product_interactions WHERE feature_date = '{as_of_date}'").collect()[0]['count']
                logger.info(f"Records for feature_date={as_of_date}: {today_count}")
                
                # Check if the new partition exists
                logger.info("Updated partitions in gold.customer_product_interactions:")
                self.spark.sql("SHOW PARTITIONS gold.customer_product_interactions").show(truncate=False)
                
                # Sample data from the table
                logger.info("Sample data from gold.customer_product_interactions:")
                self.spark.sql(f"""
                    SELECT * FROM gold.customer_product_interactions 
                    WHERE feature_date = '{as_of_date}'
                    LIMIT 5
                """).show(truncate=False)
            except Exception as e:
                logger.error(f"Error verifying written data: {str(e)}")
                
            # Sample of generated features for verification
            logger.info("Sample of generated features:")
            features_df.select(
                "customer_id", "product_id", "view_count_7d", "purchase_count_7d",
                "category_affinity_score", "product_popularity_score", "purchased_next_7d"
            ).show(10, truncate=False)
            
        except Exception as e:
            logger.error(f"Error generating ML features: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='ML Feature Engineering')
    parser.add_argument('--date', required=True, 
                       help='Feature generation date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    feature_engineer = MLFeatureEngineering()
    
    try:
        feature_engineer.generate_ml_features(args.date)
        logger.info("ML feature engineering completed successfully")
        
    except Exception as e:
        logger.error(f"ML feature engineering failed: {str(e)}")
        sys.exit(1)
    finally:
        feature_engineer.spark.stop()

if __name__ == "__main__":
    main()