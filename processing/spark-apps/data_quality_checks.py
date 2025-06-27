 # processing/spark-apps/data_quality_checks.py

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self):
        """Initialize Spark session with Iceberg support"""
        self.spark = SparkSession.builder \
            .appName("Data Quality Checker") \
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
        
        # Define quality check configurations
        self.quality_rules = {
            'bronze': {
                'raw_user_events': {
                    'required_columns': ['event_id', 'customer_id', 'event_type', 'event_time'],
                    'unique_columns': ['event_id'],
                    'non_null_columns': ['event_id', 'event_type', 'event_time'],
                    'valid_values': {
                        'event_type': ['page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 
                                     'add_to_wishlist', 'search', 'filter_apply', 'sort_apply',
                                     'checkout_start', 'purchase_complete', 'user_login', 'user_logout']
                    },
                    'range_checks': {
                        'event_time': {'min_date': '2024-01-01', 'max_date': '2025-12-31'}
                    }
                },
                'raw_marketplace_sales': {
                    'required_columns': ['transaction_id', 'product_id', 'amount', 'transaction_time'],
                    'unique_columns': ['transaction_id'],
                    'non_null_columns': ['transaction_id', 'product_id', 'amount'],
                    'range_checks': {
                        'amount': {'min_value': 0, 'max_value': 10000}
                    }
                }
            },
            'silver': {
                'standardized_user_events': {
                    'required_columns': ['event_id', 'customer_id', 'event_type', 'event_time'],
                    'unique_columns': ['event_id'],
                    'non_null_columns': ['event_id', 'event_type', 'event_time'],
                    'referential_integrity': {
                        'customer_id': 'silver.dim_customer_scd'
                    }
                },
                'standardized_sales': {
                    'required_columns': ['sale_id', 'product_id', 'total_amount', 'transaction_time'],
                    'unique_columns': ['sale_id'],
                    'non_null_columns': ['sale_id', 'product_id', 'total_amount'],
                    'range_checks': {
                        'total_amount': {'min_value': 0, 'max_value': 50000}
                    }
                },
                'dim_customer_scd': {
                    'required_columns': ['customer_sk', 'customer_id', 'effective_start_date', 'is_current'],
                    'unique_columns': ['customer_sk'],
                    'non_null_columns': ['customer_sk', 'customer_id'],
                    'business_rules': {
                        'scd_integrity': True,
                        'current_record_check': True
                    }
                }
            },
            'gold': {
                'fact_sales': {
                    'required_columns': ['sale_id', 'product_id', 'customer_sk', 'total_amount'],
                    'unique_columns': ['sale_id'],
                    'non_null_columns': ['sale_id', 'product_id', 'total_amount'],
                    'referential_integrity': {
                        'customer_sk': 'silver.dim_customer_scd',
                        'product_id': 'silver.dim_product'
                    }
                },
                'customer_product_interactions': {
                    'required_columns': ['customer_id', 'product_id', 'feature_date'],
                    'non_null_columns': ['customer_id', 'product_id', 'feature_date'],
                    'range_checks': {
                        'view_count_7d': {'min_value': 0, 'max_value': 1000},
                        'category_affinity_score': {'min_value': 0, 'max_value': 1}
                    }
                }
            }
        }

    def check_schema_compliance(self, table_name: str, layer: str) -> dict:
        """Check if table schema matches expected requirements"""
        logger.info(f"Checking schema compliance for {layer}.{table_name}")
        
        results = {
            'check_name': 'schema_compliance',
            'table': f"{layer}.{table_name}",
            'status': 'PASS',
            'issues': []
        }
        
        try:
            # Get table schema
            df = self.spark.table(f"{layer}.{table_name}")
            existing_columns = [field.name for field in df.schema.fields]
            
            # Check required columns
            if table_name in self.quality_rules[layer]:
                required_columns = self.quality_rules[layer][table_name].get('required_columns', [])
                missing_columns = [col for col in required_columns if col not in existing_columns]
                
                if missing_columns:
                    results['status'] = 'FAIL'
                    results['issues'].append(f"Missing required columns: {missing_columns}")
                
                logger.info(f"Required columns check: {len(missing_columns)} missing columns")
            
        except Exception as e:
            results['status'] = 'ERROR'
            results['issues'].append(f"Schema check failed: {str(e)}")
            logger.error(f"Schema compliance check failed: {str(e)}")
        
        return results

    def check_data_completeness(self, table_name: str, layer: str, process_date: str) -> dict:
        """Check data completeness and null values"""
        logger.info(f"Checking data completeness for {layer}.{table_name}")
        
        results = {
            'check_name': 'data_completeness',
            'table': f"{layer}.{table_name}",
            'status': 'PASS',
            'issues': [],
            'metrics': {}
        }
        
        try:
            df = self.spark.table(f"{layer}.{table_name}")
            
            # Filter by date if applicable
            date_column = self.get_date_column(table_name, layer)
            if date_column:
                df = df.filter(col(date_column) == process_date)
            
            total_records = df.count()
            results['metrics']['total_records'] = total_records
            
            if total_records == 0:
                results['status'] = 'WARN'
                results['issues'].append("No records found for the specified date")
                return results
            
            # Check non-null columns
            if table_name in self.quality_rules[layer]:
                non_null_columns = self.quality_rules[layer][table_name].get('non_null_columns', [])
                
                for column in non_null_columns:
                    if column in [field.name for field in df.schema.fields]:
                        null_count = df.filter(col(column).isNull()).count()
                        null_percentage = (null_count / total_records) * 100
                        
                        results['metrics'][f'{column}_null_count'] = null_count
                        results['metrics'][f'{column}_null_percentage'] = round(null_percentage, 2)
                        
                        if null_count > 0:
                            if null_percentage > 5:  # Threshold: 5%
                                results['status'] = 'FAIL'
                                results['issues'].append(f"High null percentage in {column}: {null_percentage:.2f}%")
                            else:
                                if results['status'] != 'FAIL':
                                    results['status'] = 'WARN'
                                results['issues'].append(f"Low null percentage in {column}: {null_percentage:.2f}%")
            
            logger.info(f"Completeness check: {total_records} records, {len(results['issues'])} issues")
            
        except Exception as e:
            results['status'] = 'ERROR'
            results['issues'].append(f"Completeness check failed: {str(e)}")
            logger.error(f"Data completeness check failed: {str(e)}")
        
        return results

    def check_data_uniqueness(self, table_name: str, layer: str, process_date: str) -> dict:
        """Check uniqueness constraints"""
        logger.info(f"Checking data uniqueness for {layer}.{table_name}")
        
        results = {
            'check_name': 'data_uniqueness',
            'table': f"{layer}.{table_name}",
            'status': 'PASS',
            'issues': [],
            'metrics': {}
        }
        
        try:
            df = self.spark.table(f"{layer}.{table_name}")
            
            # Filter by date if applicable
            date_column = self.get_date_column(table_name, layer)
            if date_column:
                df = df.filter(col(date_column) == process_date)
            
            # Check unique columns
            if table_name in self.quality_rules[layer]:
                unique_columns = self.quality_rules[layer][table_name].get('unique_columns', [])
                
                for column in unique_columns:
                    if column in [field.name for field in df.schema.fields]:
                        total_count = df.count()
                        distinct_count = df.select(column).distinct().count()
                        duplicate_count = total_count - distinct_count
                        
                        results['metrics'][f'{column}_total_count'] = total_count
                        results['metrics'][f'{column}_distinct_count'] = distinct_count
                        results['metrics'][f'{column}_duplicate_count'] = duplicate_count
                        
                        if duplicate_count > 0:
                            results['status'] = 'FAIL'
                            results['issues'].append(f"Duplicates found in {column}: {duplicate_count} records")
            
            logger.info(f"Uniqueness check: {len(results['issues'])} issues found")
            
        except Exception as e:
            results['status'] = 'ERROR'
            results['issues'].append(f"Uniqueness check failed: {str(e)}")
            logger.error(f"Data uniqueness check failed: {str(e)}")
        
        return results

    def check_data_validity(self, table_name: str, layer: str, process_date: str) -> dict:
        """Check data validity based on rules"""
        logger.info(f"Checking data validity for {layer}.{table_name}")
        
        results = {
            'check_name': 'data_validity',
            'table': f"{layer}.{table_name}",
            'status': 'PASS',
            'issues': [],
            'metrics': {}
        }
        
        try:
            if table_name not in self.quality_rules[layer]:
                return results
            
            rules = self.quality_rules[layer][table_name]
            if 'validity_rules' not in rules:
                return results
            
            # Load data
            date_column = self.get_date_column(table_name)
            if date_column and process_date:
                try:
                    df = self.spark.sql(f"""
                        SELECT * FROM {layer}.{table_name}
                        WHERE DATE({date_column}) = '{process_date}'
                    """)
                except:
                    # If query fails, try without date filter
                    df = self.spark.table(f"{layer}.{table_name}")
            else:
                df = self.spark.table(f"{layer}.{table_name}")
            
            # Skip check if no data found
            row_count = df.count()
            if row_count == 0:
                logger.warning(f"No data found for {layer}.{table_name} on {process_date}")
                results['status'] = 'WARN'
                results['issues'].append(f"No data found for date {process_date}")
                return results
            
            # Check each validity rule
            for column, validation in rules['validity_rules'].items():
                # Make sure column exists in dataframe
                if column not in [field.name for field in df.schema.fields]:
                    results['issues'].append(f"Column {column} not found in table")
                    continue
                
                # Check allowed values
                if 'allowed_values' in validation:
                    allowed = validation['allowed_values']
                    invalid_count = df.filter(~col(column).isin(allowed) & col(column).isNotNull()).count()
                    results['metrics'][f"{column}_invalid_count"] = invalid_count
                    
                    if invalid_count > 0:
                        results['status'] = 'FAIL'
                        results['issues'].append(f"Invalid values in {column}: {invalid_count} records")
                
                # Check regex pattern
                if 'regex_pattern' in validation:
                    pattern = validation['regex_pattern']
                    invalid_count = df.filter(~col(column).rlike(pattern) & col(column).isNotNull()).count()
                    results['metrics'][f"{column}_invalid_pattern_count"] = invalid_count
                    
                    if invalid_count > 0:
                        results['status'] = 'FAIL'
                        results['issues'].append(f"Pattern validation failed in {column}: {invalid_count} records")
                
                # Check value ranges
                if 'ranges' in validation:
                    ranges = validation['ranges']
                    
                    if 'min_value' in ranges:
                        try:
                            # First check if there are any non-null values to compare
                            non_null_count = df.filter(col(column).isNotNull()).count()
                            if non_null_count > 0:
                                below_min = df.filter(
                                    (col(column) < ranges['min_value']) & 
                                    col(column).isNotNull()
                                ).count()
                                
                                if below_min > 0:
                                    results['status'] = 'FAIL'
                                    results['issues'].append(f"Values below minimum in {column}: {below_min} records")
                        except Exception as e:
                            logger.error(f"Error checking min_value for {column}: {str(e)}")
                            results['issues'].append(f"Error checking min_value for {column}: {str(e)}")
                        
                    if 'max_value' in ranges:
                        try:
                            # First check if there are any non-null values to compare
                            non_null_count = df.filter(col(column).isNotNull()).count()
                            if non_null_count > 0:
                                above_max = df.filter(
                                    (col(column) > ranges['max_value']) & 
                                    col(column).isNotNull()
                                ).count()
                                
                                if above_max > 0:
                                    results['status'] = 'FAIL'
                                    results['issues'].append(f"Values above maximum in {column}: {above_max} records")
                        except Exception as e:
                            logger.error(f"Error checking max_value for {column}: {str(e)}")
                            results['issues'].append(f"Error checking max_value for {column}: {str(e)}")
                    
                    if 'min_date' in ranges:
                        try:
                            # First check if there are any non-null values to compare
                            non_null_count = df.filter(col(column).isNotNull()).count()
                            if non_null_count > 0:
                                before_min = df.filter(
                                    (col(column) < ranges['min_date']) & 
                                    col(column).isNotNull()
                                ).count()
                                
                                if before_min > 0:
                                    results['status'] = 'FAIL'
                                    results['issues'].append(f"Dates before minimum in {column}: {before_min} records")
                        except Exception as e:
                            logger.error(f"Error checking min_date for {column}: {str(e)}")
                            results['issues'].append(f"Error checking min_date for {column}: {str(e)}")
                    
                    if 'max_date' in ranges:
                        try:
                            # First check if there are any non-null values to compare
                            non_null_count = df.filter(col(column).isNotNull()).count()
                            if non_null_count > 0:
                                after_max = df.filter(
                                    (col(column) > ranges['max_date']) & 
                                    col(column).isNotNull()
                                ).count()
                                
                                if after_max > 0:
                                    results['status'] = 'FAIL'
                                    results['issues'].append(f"Dates after maximum in {column}: {after_max} records")
                        except Exception as e:
                            logger.error(f"Error checking max_date for {column}: {str(e)}")
                            results['issues'].append(f"Error checking max_date for {column}: {str(e)}")
            
            # Check SCD integrity for dimension tables
            if 'business_rules' in rules and rules['business_rules'].get('scd_integrity'):
                try:
                    scd_issues = self.check_scd_integrity(df)
                    if scd_issues:
                        results['status'] = 'FAIL'
                        results['issues'].extend(scd_issues)
                except Exception as e:
                    logger.error(f"Error checking SCD integrity: {str(e)}")
                    results['issues'].append(f"Error checking SCD integrity: {str(e)}")
            
            logger.info(f"Validity check: {len(results['issues'])} issues found")
            
        except Exception as e:
            results['status'] = 'ERROR'
            results['issues'].append(f"Validity check failed: {str(e)}")
            logger.error(f"Data validity check failed: {str(e)}")
        
        return results

    def check_scd_integrity(self, df) -> list:
        """Check SCD Type 2 integrity rules"""
        issues = []
        
        try:
            # Check for overlapping date ranges for same customer
            overlapping = df.filter(col("is_current") == True) \
                           .groupBy("customer_id") \
                           .count() \
                           .filter(col("count") > 1) \
                           .count()
            
            if overlapping > 0:
                issues.append(f"Multiple current records found for {overlapping} customers")
            
            # Check for gaps in date ranges
            gaps = df.filter(
                (col("effective_start_date") > col("effective_end_date")) |
                (col("effective_end_date") < col("effective_start_date"))
            ).count()
            
            if gaps > 0:
                issues.append(f"Invalid date ranges found in {gaps} records")
                
        except Exception as e:
            issues.append(f"SCD integrity check failed: {str(e)}")
        
        return issues

    def check_referential_integrity(self, table_name: str, layer: str) -> dict:
        """Check referential integrity constraints"""
        logger.info(f"Checking referential integrity for {layer}.{table_name}")
        
        results = {
            'check_name': 'referential_integrity',
            'table': f"{layer}.{table_name}",
            'status': 'PASS',
            'issues': [],
            'metrics': {}
        }
        
        try:
            if table_name not in self.quality_rules[layer]:
                return results
            
            rules = self.quality_rules[layer][table_name]
            if 'referential_integrity' not in rules:
                return results
            
            df = self.spark.table(f"{layer}.{table_name}")
            
            for fk_column, ref_table in rules['referential_integrity'].items():
                if fk_column in [field.name for field in df.schema.fields]:
                    try:
                        ref_df = self.spark.table(ref_table)
                        
                        # For SCD tables, only check against current records
                        if 'is_current' in [field.name for field in ref_df.schema.fields]:
                            ref_df = ref_df.filter(col("is_current") == True)
                        
                        # Get reference column name (assume same as FK column or primary key)
                        ref_columns = [field.name for field in ref_df.schema.fields]
                        ref_column = fk_column if fk_column in ref_columns else ref_columns[0]
                        
                        # Find orphaned records
                        orphaned = df.join(
                            ref_df.select(ref_column),
                            df[fk_column] == ref_df[ref_column],
                            "left_anti"
                        ).filter(col(fk_column).isNotNull()).count()
                        
                        results['metrics'][f'{fk_column}_orphaned_count'] = orphaned
                        
                        if orphaned > 0:
                            results['status'] = 'FAIL'
                            results['issues'].append(f"Orphaned records in {fk_column}: {orphaned} records")
                            
                    except Exception as e:
                        results['issues'].append(f"Could not check reference {ref_table}: {str(e)}")
            
            logger.info(f"Referential integrity check: {len(results['issues'])} issues found")
            
        except Exception as e:
            results['status'] = 'ERROR'
            results['issues'].append(f"Referential integrity check failed: {str(e)}")
            logger.error(f"Referential integrity check failed: {str(e)}")
        
        return results

    def get_date_column(self, table_name: str, layer: str) -> str:
        """Get the date column for filtering"""
        date_columns = {
            'raw_user_events': 'event_time',
            'raw_marketplace_sales': 'transaction_time',
            'standardized_user_events': 'ingestion_date',
            'standardized_sales': 'transaction_date',
            'customer_product_interactions': 'feature_date'
        }
        return date_columns.get(table_name)

    def run_all_checks(self, layer: str, process_date: str) -> dict:
        """Run all quality checks for a layer"""
        logger.info(f"Running all quality checks for {layer} layer on {process_date}")
        
        overall_results = {
            'layer': layer,
            'process_date': process_date,
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'PASS',
            'table_results': {}
        }
        
        # Get tables for the layer
        tables = list(self.quality_rules.get(layer, {}).keys())
        
        for table_name in tables:
            logger.info(f"Checking table: {table_name}")
            
            table_results = {
                'table': table_name,
                'checks': []
            }
            
            try:
                # Run all checks for this table
                checks = [
                    self.check_schema_compliance(table_name, layer),
                    self.check_data_completeness(table_name, layer, process_date),
                    self.check_data_uniqueness(table_name, layer, process_date),
                    self.check_data_validity(table_name, layer, process_date),
                    self.check_referential_integrity(table_name, layer)
                ]
                
                table_results['checks'] = checks
                
                # Determine table status
                table_status = 'PASS'
                for check in checks:
                    if check['status'] == 'FAIL':
                        table_status = 'FAIL'
                        break
                    elif check['status'] == 'WARN' and table_status == 'PASS':
                        table_status = 'WARN'
                
                table_results['status'] = table_status
                
                # Update overall status
                if table_status == 'FAIL':
                    overall_results['overall_status'] = 'FAIL'
                elif table_status == 'WARN' and overall_results['overall_status'] == 'PASS':
                    overall_results['overall_status'] = 'WARN'
                    
            except Exception as e:
                logger.error(f"Error checking table {table_name}: {str(e)}")
                table_results['status'] = 'ERROR'
                table_results['error'] = str(e)
                overall_results['overall_status'] = 'FAIL'
            
            overall_results['table_results'][table_name] = table_results
        
        # Log summary
        logger.info(f"Quality check summary for {layer}: {overall_results['overall_status']}")
        
        return overall_results

    def save_results(self, results: dict):
        """Save quality check results"""
        try:
            # Convert to JSON for logging
            results_json = json.dumps(results, indent=2, default=str)
            logger.info(f"Quality check results:\n{results_json}")
            
            # In a real implementation, you would save to a monitoring system
            # For now, we'll just log the results
            
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Data Quality Checker')
    parser.add_argument('--layer', required=True, 
                       choices=['bronze', 'silver', 'gold'],
                       help='Data layer to check')
    parser.add_argument('--date', required=True, 
                       help='Processing date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    checker = DataQualityChecker()
    
    try:
        # Run quality checks
        results = checker.run_all_checks(args.layer, args.date)
        
        # Save results
        checker.save_results(results)
        
        # Exit with appropriate code
        if results['overall_status'] == 'FAIL':
            logger.error("Data quality checks failed")
            sys.exit(1)
        elif results['overall_status'] == 'WARN':
            logger.warning("Data quality checks completed with warnings")
        else:
            logger.info("All data quality checks passed")
        
    except Exception as e:
        logger.error(f"Data quality checker failed: {str(e)}")
        sys.exit(1)
    finally:
        checker.spark.stop()

if __name__ == "__main__":
    main()