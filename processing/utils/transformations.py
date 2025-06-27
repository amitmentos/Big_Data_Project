 # processing/utils/transformations.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class DataTransformations:
    """Common data transformation utilities for the data platform"""
    
    @staticmethod
    def standardize_timestamps(df: DataFrame, timestamp_columns: list) -> DataFrame:
        """Standardize timestamp columns to UTC"""
        for col_name in timestamp_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    to_timestamp(col(col_name)).cast(TimestampType())
                )
        return df
    
    @staticmethod
    def add_audit_columns(df: DataFrame, process_time: datetime = None) -> DataFrame:
        """Add audit columns for tracking data lineage"""
        if process_time is None:
            process_time = datetime.utcnow()
        
        return df.withColumn("processed_time", lit(process_time)) \
                 .withColumn("data_source", lit("bronze_layer")) \
                 .withColumn("pipeline_version", lit("1.0"))
    
    @staticmethod
    def clean_string_columns(df: DataFrame, string_columns: list) -> DataFrame:
        """Clean string columns by trimming whitespace and handling nulls"""
        for col_name in string_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull() | (trim(col(col_name)) == ""), lit(None))
                    .otherwise(trim(col(col_name)))
                )
        return df
    
    @staticmethod
    def standardize_currency_amounts(df: DataFrame, amount_columns: list, 
                                   currency_column: str = "currency") -> DataFrame:
        """Standardize currency amounts to USD"""
        # Simplified exchange rates (in real scenario, use actual rates)
        exchange_rates = {
            "USD": 1.0,
            "EUR": 1.1,
            "GBP": 1.25,
            "CAD": 0.75,
            "AUD": 0.65
        }
        
        for col_name in amount_columns:
            if col_name in df.columns and currency_column in df.columns:
                # Create exchange rate mapping
                rate_expr = when(col(currency_column) == "USD", lit(1.0))
                for currency, rate in exchange_rates.items():
                    if currency != "USD":
                        rate_expr = rate_expr.when(col(currency_column) == currency, lit(rate))
                rate_expr = rate_expr.otherwise(lit(1.0))  # Default to 1.0 for unknown currencies
                
                # Apply conversion
                df = df.withColumn(
                    f"{col_name}_usd",
                    col(col_name) * rate_expr
                ).withColumn(
                    f"{col_name}_original",
                    col(col_name)
                ).drop(col_name).withColumnRenamed(f"{col_name}_usd", col_name)
        
        return df
    
    @staticmethod
    def extract_date_parts(df: DataFrame, date_column: str) -> DataFrame:
        """Extract date parts for dimensional analysis"""
        return df.withColumn(f"{date_column}_year", year(col(date_column))) \
                 .withColumn(f"{date_column}_month", month(col(date_column))) \
                 .withColumn(f"{date_column}_day", dayofmonth(col(date_column))) \
                 .withColumn(f"{date_column}_quarter", quarter(col(date_column))) \
                 .withColumn(f"{date_column}_dayofweek", dayofweek(col(date_column))) \
                 .withColumn(f"{date_column}_weekofyear", weekofyear(col(date_column)))
    
    @staticmethod
    def detect_outliers(df: DataFrame, numeric_columns: list, 
                       method: str = "iqr", threshold: float = 1.5) -> DataFrame:
        """Detect outliers in numeric columns"""
        for col_name in numeric_columns:
            if col_name in df.columns:
                if method == "iqr":
                    # Calculate IQR
                    quantiles = df.select(
                        expr(f"percentile_approx({col_name}, 0.25)").alias("q1"),
                        expr(f"percentile_approx({col_name}, 0.75)").alias("q3")
                    ).collect()[0]
                    
                    q1, q3 = quantiles.q1, quantiles.q3
                    iqr = q3 - q1
                    lower_bound = q1 - threshold * iqr
                    upper_bound = q3 + threshold * iqr
                    
                    df = df.withColumn(
                        f"{col_name}_is_outlier",
                        when((col(col_name) < lower_bound) | (col(col_name) > upper_bound), 
                             lit(True)).otherwise(lit(False))
                    )
        
        return df
    
    @staticmethod
    def create_hash_key(df: DataFrame, columns: list, 
                       hash_column: str = "hash_key") -> DataFrame:
        """Create hash key from multiple columns for deduplication"""
        concat_expr = concat_ws("_", *[coalesce(col(c).cast("string"), lit("NULL")) 
                                     for c in columns])
        return df.withColumn(hash_column, sha2(concat_expr, 256))
    
    @staticmethod
    def apply_business_rules(df: DataFrame, source_type: str) -> DataFrame:
        """Apply source-specific business rules"""
        if source_type == "user_events":
            # Filter out invalid events
            df = df.filter(
                col("event_time").isNotNull() &
                col("customer_id").isNotNull() &
                col("event_type").isNotNull() &
                (col("event_time") >= lit("2024-01-01")) &
                (col("event_time") <= current_timestamp())
            )
            
        elif source_type == "marketplace_sales":
            # Filter out invalid sales
            df = df.filter(
                col("transaction_id").isNotNull() &
                col("amount").isNotNull() &
                (col("amount") > 0) &
                (col("amount") < 10000) &  # Max transaction limit
                col("transaction_time").isNotNull()
            )
            
        elif source_type == "customer_data":
            # Clean customer data
            df = df.filter(
                col("customer_id").isNotNull() &
                col("email").isNotNull() &
                col("email").rlike(r"^[A-Za-z0-9+_.-]+@(.+)$")  # Basic email validation
            )
        
        return df


class DataQualityUtils:
    """Utilities for data quality checks and validation"""
    
    @staticmethod
    def check_null_percentage(df: DataFrame, columns: list, 
                            threshold: float = 0.05) -> dict:
        """Check null percentage for specified columns"""
        total_rows = df.count()
        results = {}
        
        if total_rows == 0:
            return {"error": "DataFrame is empty"}
        
        for col_name in columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_percentage = null_count / total_rows
                
                results[col_name] = {
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "exceeds_threshold": null_percentage > threshold
                }
        
        return results
    
    @staticmethod
    def check_duplicates(df: DataFrame, key_columns: list) -> dict:
        """Check for duplicate records based on key columns"""
        total_rows = df.count()
        distinct_rows = df.select(*key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows
        
        return {
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": duplicate_count / total_rows if total_rows > 0 else 0
        }
    
    @staticmethod
    def check_value_ranges(df: DataFrame, range_checks: dict) -> dict:
        """Check if values fall within expected ranges"""
        results = {}
        
        for col_name, (min_val, max_val) in range_checks.items():
            if col_name in df.columns:
                out_of_range = df.filter(
                    (col(col_name) < min_val) | (col(col_name) > max_val)
                ).count()
                
                results[col_name] = {
                    "out_of_range_count": out_of_range,
                    "min_expected": min_val,
                    "max_expected": max_val
                }
        
        return results
    
    @staticmethod
    def profile_dataframe(df: DataFrame) -> dict:
        """Generate a basic profile of the DataFrame"""
        profile = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "schema": df.schema.json(),
            "column_stats": {}
        }
        
        # Get basic statistics for numeric columns
        numeric_columns = [field.name for field in df.schema.fields 
                          if field.dataType in [IntegerType(), LongType(), 
                                              FloatType(), DoubleType(), DecimalType(10,2)]]
        
        if numeric_columns:
            stats_df = df.select(*numeric_columns).describe()
            stats_data = stats_df.collect()
            
            for col_name in numeric_columns:
                col_stats = {}
                for row in stats_data:
                    col_stats[row['summary']] = row[col_name]
                profile["column_stats"][col_name] = col_stats
        
        return profile


class IcebergUtils:
    """Utilities for working with Apache Iceberg tables"""
    
    @staticmethod
    def create_iceberg_table(spark: SparkSession, table_name: str, 
                           df: DataFrame, partition_cols: list = None,
                           table_properties: dict = None) -> bool:
        """Create an Iceberg table from DataFrame"""
        try:
            # Create table DDL
            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            
            # Add columns
            column_definitions = []
            for field in df.schema.fields:
                col_def = f"{field.name} {field.dataType.simpleString()}"
                column_definitions.append(col_def)
            
            ddl += ", ".join(column_definitions)
            ddl += ") USING ICEBERG"
            
            # Add partitioning
            if partition_cols:
                partition_by = ", ".join([f"days({col})" if "date" in col.lower() or "time" in col.lower() 
                                        else col for col in partition_cols])
                ddl += f" PARTITIONED BY ({partition_by})"
            
            # Add table properties
            if table_properties:
                props = ", ".join([f"'{k}' = '{v}'" for k, v in table_properties.items()])
                ddl += f" TBLPROPERTIES ({props})"
            
            # Execute DDL
            spark.sql(ddl)
            logger.info(f"Created Iceberg table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating Iceberg table {table_name}: {str(e)}")
            return False
    
    @staticmethod
    def optimize_iceberg_table(spark: SparkSession, table_name: str) -> bool:
        """Optimize Iceberg table (compaction, etc.)"""
        try:
            # Rewrite data files
            spark.sql(f"CALL spark_catalog.system.rewrite_data_files('{table_name}')")
            
            # Expire old snapshots (keep last 7 days)
            expire_timestamp = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            spark.sql(f"""
                CALL spark_catalog.system.expire_snapshots(
                    table => '{table_name}',
                    older_than => TIMESTAMP '{expire_timestamp}'
                )
            """)
            
            logger.info(f"Optimized Iceberg table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error optimizing Iceberg table {table_name}: {str(e)}")
            return False
    
    @staticmethod
    def get_table_history(spark: SparkSession, table_name: str) -> DataFrame:
        """Get table history/snapshots"""
        try:
            return spark.sql(f"SELECT * FROM {table_name}.history")
        except Exception as e:
            logger.error(f"Error getting table history for {table_name}: {str(e)}")
            return None


class KafkaUtils:
    """Utilities for working with Kafka in Spark"""
    
    @staticmethod
    def get_kafka_stream_config() -> dict:
        """Get standard Kafka streaming configuration"""
        return {
            "kafka.bootstrap.servers": "kafka:29092",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": "1000",
            "startingOffsets": "latest"
        }
    
    @staticmethod
    def parse_kafka_message(df: DataFrame, schema: StructType, 
                          timestamp_col: str = "timestamp") -> DataFrame:
        """Parse Kafka message with standard metadata"""
        return df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col(timestamp_col).alias("kafka_timestamp")
        ).select(
            col("data.*"),
            col("topic"),
            col("partition"), 
            col("offset"),
            col("kafka_timestamp")
        )


class MetricsUtils:
    """Utilities for collecting and reporting metrics"""
    
    @staticmethod
    def calculate_processing_metrics(start_time: datetime, 
                                   records_processed: int) -> dict:
        """Calculate processing performance metrics"""
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "records_processed": records_processed,
            "records_per_second": records_processed / duration if duration > 0 else 0
        }
    
    @staticmethod
    def log_processing_summary(job_name: str, metrics: dict, 
                             quality_results: dict = None):
        """Log processing summary with metrics"""
        logger.info(f"=== {job_name} Processing Summary ===")
        logger.info(f"Duration: {metrics['duration_seconds']:.2f} seconds")
        logger.info(f"Records processed: {metrics['records_processed']:,}")
        logger.info(f"Processing rate: {metrics['records_per_second']:.2f} records/second")
        
        if quality_results:
            logger.info("Quality Check Results:")
            for check, result in quality_results.items():
                if isinstance(result, dict) and "exceeds_threshold" in result:
                    status = "FAIL" if result["exceeds_threshold"] else "PASS"
                    logger.info(f"  {check}: {status}")
                else:
                    logger.info(f"  {check}: {result}")
        
        logger.info("=" * (len(job_name) + 25))