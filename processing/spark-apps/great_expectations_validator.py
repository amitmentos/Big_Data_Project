# processing/spark-apps/great_expectations_validator.py

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

# Great Expectations imports
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import DataContext
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GreatExpectationsValidator:
    """
    Great Expectations integration for the e-commerce data platform.
    Provides comprehensive data validation using industry-standard expectations.
    """
    
    def __init__(self, context_root_dir: str = None):
        """Initialize Great Expectations Data Context and Spark Session"""
        
        # Initialize Spark Session with Iceberg support
        self.spark = SparkSession.builder \
            .appName("Great Expectations Data Validator") \
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
        
        # Initialize Great Expectations Data Context
        try:
            if context_root_dir is None:
                context_root_dir = "/opt/processing/great_expectations"
            
            self.context = DataContext(context_root_dir=context_root_dir)
            logger.info(f"Great Expectations context initialized from: {context_root_dir}")
        except DataContextError as e:
            logger.error(f"Failed to initialize Great Expectations context: {str(e)}")
            raise
        
        # Define layer-specific validation configurations
        self.layer_configs = {
            'bronze': {
                'datasource': 'bronze_datasource',
                'data_connector': 'bronze_connector',
                'expectation_suites': {
                    'raw_user_events': 'bronze_raw_user_events',
                    'raw_customer_data': 'bronze_raw_customer_data',
                    'raw_marketplace_sales': 'bronze_raw_marketplace_sales',
                    'raw_product_data': 'bronze_raw_product_data'
                }
            },
            'silver': {
                'datasource': 'silver_datasource', 
                'data_connector': 'silver_connector',
                'expectation_suites': {
                    'standardized_user_events': 'silver_standardized_user_events',
                    'standardized_sales': 'silver_standardized_sales',
                    'dim_customer_scd': 'silver_dim_customer_scd',
                    'dim_product': 'silver_dim_product'
                }
            },
            'gold': {
                'datasource': 'gold_datasource',
                'data_connector': 'gold_connector', 
                'expectation_suites': {
                    'fact_sales': 'gold_fact_sales',
                    'customer_product_interactions': 'gold_customer_product_interactions'
                }
            }
        }

    def validate_table(self, layer: str, table_name: str, process_date: str) -> Dict[str, Any]:
        """
        Validate a specific table using Great Expectations
        
        Args:
            layer: Data layer (bronze, silver, gold)
            table_name: Name of the table to validate
            process_date: Date partition to validate (YYYY-MM-DD)
            
        Returns:
            Dictionary containing validation results
        """
        logger.info(f"Starting Great Expectations validation for {layer}.{table_name} on {process_date}")
        
        validation_result = {
            'layer': layer,
            'table_name': table_name,
            'process_date': process_date,
            'timestamp': datetime.now().isoformat(),
            'validation_status': 'UNKNOWN',
            'expectations_passed': 0,
            'expectations_failed': 0,
            'expectations_total': 0,
            'success_percent': 0.0,
            'failed_expectations': [],
            'warnings': [],
            'errors': []
        }
        
        try:
            # Get configuration for the layer
            if layer not in self.layer_configs:
                raise ValueError(f"Unsupported layer: {layer}")
            
            layer_config = self.layer_configs[layer]
            
            # Check if table has an expectation suite
            if table_name not in layer_config['expectation_suites']:
                validation_result['validation_status'] = 'SKIPPED'
                validation_result['warnings'].append(f"No expectation suite found for {table_name}")
                logger.warning(f"No expectation suite configured for {layer}.{table_name}")
                return validation_result
            
            # Get expectation suite name
            suite_name = layer_config['expectation_suites'][table_name]
            
            # Load expectation suite
            try:
                expectation_suite = self.context.get_expectation_suite(suite_name)
                validation_result['expectations_total'] = len(expectation_suite.expectations)
                logger.info(f"Loaded expectation suite '{suite_name}' with {validation_result['expectations_total']} expectations")
            except Exception as e:
                validation_result['validation_status'] = 'ERROR'
                validation_result['errors'].append(f"Failed to load expectation suite '{suite_name}': {str(e)}")
                logger.error(f"Failed to load expectation suite: {str(e)}")
                return validation_result
            
            # Get data asset
            data_asset = self._get_data_asset(layer, table_name, process_date)
            if data_asset is None:
                validation_result['validation_status'] = 'ERROR'
                validation_result['errors'].append(f"Failed to get data asset for {layer}.{table_name}")
                return validation_result
            
            # Create validator
            validator = self.context.get_validator(
                batch_request=data_asset,
                expectation_suite=expectation_suite
            )
            
            # Run validation
            validation_results = validator.validate()
            
            # Process results
            self._process_validation_results(validation_results, validation_result)
            
            # Generate data docs if validation passed
            if validation_result['validation_status'] == 'PASSED':
                try:
                    self.context.build_data_docs()
                    logger.info("Data docs updated successfully")
                except Exception as e:
                    validation_result['warnings'].append(f"Failed to build data docs: {str(e)}")
            
            logger.info(f"Validation completed: {validation_result['validation_status']} "
                       f"({validation_result['expectations_passed']}/{validation_result['expectations_total']} passed)")
            
        except Exception as e:
            validation_result['validation_status'] = 'ERROR'
            validation_result['errors'].append(f"Validation failed with error: {str(e)}")
            logger.error(f"Validation failed: {str(e)}")
        
        return validation_result

    def _get_data_asset(self, layer: str, table_name: str, process_date: str) -> Optional[Dict]:
        """Get data asset configuration for validation"""
        
        try:
            layer_config = self.layer_configs[layer]
            datasource_name = layer_config['datasource']
            data_connector_name = layer_config['data_connector']
            
            # Create batch request based on layer and table
            if layer == 'bronze':
                if 'events' in table_name:
                    partition_column = 'last_updated_day'
                elif 'sales' in table_name:
                    partition_column = 'last_updated_day'
                else:
                    partition_column = 'last_updated_day'
            elif layer == 'silver':
                if 'events' in table_name:
                    partition_column = 'ingestion_date'
                elif 'sales' in table_name:
                    partition_column = 'transaction_date'
                else:
                    partition_column = None
            elif layer == 'gold':
                if 'fact_sales' in table_name:
                    partition_column = 'sale_date'
                elif 'customer_product_interactions' in table_name:
                    partition_column = 'feature_date'
                else:
                    partition_column = None
            
            # Build batch request
            batch_request = {
                'datasource_name': datasource_name,
                'data_connector_name': data_connector_name,
                'data_asset_name': table_name
            }
            
            if partition_column:
                batch_request['batch_identifiers'] = {partition_column: process_date}
            
            return batch_request
            
        except Exception as e:
            logger.error(f"Failed to create data asset: {str(e)}")
            return None

    def _process_validation_results(self, ge_results: Any, validation_result: Dict[str, Any]):
        """Process Great Expectations validation results"""
        
        # Extract statistics
        validation_result['expectations_total'] = len(ge_results.results)
        validation_result['expectations_passed'] = sum(1 for r in ge_results.results if r.success)
        validation_result['expectations_failed'] = validation_result['expectations_total'] - validation_result['expectations_passed']
        
        # Calculate success percentage
        if validation_result['expectations_total'] > 0:
            validation_result['success_percent'] = (validation_result['expectations_passed'] / validation_result['expectations_total']) * 100
        
        # Determine overall status
        if validation_result['expectations_failed'] == 0:
            validation_result['validation_status'] = 'PASSED'
        elif validation_result['success_percent'] >= 80:  # 80% threshold for warnings
            validation_result['validation_status'] = 'WARNING'
        else:
            validation_result['validation_status'] = 'FAILED'
        
        # Collect failed expectations
        for result in ge_results.results:
            if not result.success:
                failed_expectation = {
                    'expectation_type': result.expectation_config.expectation_type,
                    'column': result.expectation_config.kwargs.get('column', 'N/A'),
                    'description': result.expectation_config.meta.get('description', 'No description'),
                    'observed_value': str(result.result.get('observed_value', 'N/A')),
                    'details': str(result.result)
                }
                validation_result['failed_expectations'].append(failed_expectation)

    def validate_layer(self, layer: str, process_date: str, tables: List[str] = None) -> Dict[str, Any]:
        """
        Validate all tables in a specific layer
        
        Args:
            layer: Data layer to validate (bronze, silver, gold)
            process_date: Date partition to validate
            tables: Optional list of specific tables to validate
            
        Returns:
            Dictionary containing validation results for all tables
        """
        logger.info(f"Starting layer validation for {layer} on {process_date}")
        
        layer_result = {
            'layer': layer,
            'process_date': process_date,
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'UNKNOWN',
            'tables_validated': 0,
            'tables_passed': 0,
            'tables_failed': 0,
            'tables_warning': 0,
            'table_results': {}
        }
        
        # Get tables to validate
        if tables is None:
            tables = list(self.layer_configs[layer]['expectation_suites'].keys())
        
        # Validate each table
        for table_name in tables:
            try:
                table_result = self.validate_table(layer, table_name, process_date)
                layer_result['table_results'][table_name] = table_result
                layer_result['tables_validated'] += 1
                
                # Update counters based on status
                status = table_result['validation_status']
                if status == 'PASSED':
                    layer_result['tables_passed'] += 1
                elif status == 'WARNING':
                    layer_result['tables_warning'] += 1
                elif status == 'FAILED':
                    layer_result['tables_failed'] += 1
                    
            except Exception as e:
                logger.error(f"Failed to validate {table_name}: {str(e)}")
                layer_result['table_results'][table_name] = {
                    'validation_status': 'ERROR',
                    'error': str(e)
                }
                layer_result['tables_failed'] += 1
        
        # Determine overall layer status
        if layer_result['tables_failed'] == 0 and layer_result['tables_warning'] == 0:
            layer_result['overall_status'] = 'PASSED'
        elif layer_result['tables_failed'] == 0:
            layer_result['overall_status'] = 'WARNING'
        else:
            layer_result['overall_status'] = 'FAILED'
        
        logger.info(f"Layer validation completed: {layer_result['overall_status']} "
                   f"({layer_result['tables_passed']} passed, {layer_result['tables_warning']} warnings, "
                   f"{layer_result['tables_failed']} failed)")
        
        return layer_result

    def generate_validation_report(self, results: Dict[str, Any]) -> str:
        """Generate a human-readable validation report"""
        
        report_lines = [
            "=" * 80,
            "GREAT EXPECTATIONS VALIDATION REPORT",
            "=" * 80,
            f"Timestamp: {results.get('timestamp', 'N/A')}",
            f"Layer: {results.get('layer', 'N/A')}",
            f"Process Date: {results.get('process_date', 'N/A')}",
            ""
        ]
        
        if 'table_results' in results:
            # Layer-level report
            report_lines.extend([
                f"Overall Status: {results['overall_status']}",
                f"Tables Validated: {results['tables_validated']}",
                f"✅ Passed: {results['tables_passed']}",
                f"⚠️  Warnings: {results['tables_warning']}", 
                f"❌ Failed: {results['tables_failed']}",
                "",
                "TABLE DETAILS:",
                "-" * 40
            ])
            
            for table_name, table_result in results['table_results'].items():
                status_icon = {
                    'PASSED': '✅',
                    'WARNING': '⚠️',
                    'FAILED': '❌',
                    'ERROR': '💥',
                    'SKIPPED': '⏭️'
                }.get(table_result['validation_status'], '❓')
                
                report_lines.append(f"{status_icon} {table_name}: {table_result['validation_status']}")
                
                if table_result.get('expectations_total', 0) > 0:
                    success_pct = table_result.get('success_percent', 0)
                    report_lines.append(f"   └─ {table_result['expectations_passed']}/{table_result['expectations_total']} expectations passed ({success_pct:.1f}%)")
                
                if table_result.get('failed_expectations'):
                    report_lines.append("   └─ Failed expectations:")
                    for failed in table_result['failed_expectations'][:3]:  # Show top 3
                        report_lines.append(f"      • {failed['expectation_type']} on {failed['column']}")
                
                if table_result.get('errors'):
                    for error in table_result['errors']:
                        report_lines.append(f"   └─ Error: {error}")
                
                report_lines.append("")
        else:
            # Table-level report
            status_icon = {
                'PASSED': '✅',
                'WARNING': '⚠️',
                'FAILED': '❌',
                'ERROR': '💥',
                'SKIPPED': '⏭️'
            }.get(results['validation_status'], '❓')
            
            report_lines.extend([
                f"Table: {results.get('table_name', 'N/A')}",
                f"Status: {status_icon} {results['validation_status']}",
                f"Expectations: {results['expectations_passed']}/{results['expectations_total']} passed ({results['success_percent']:.1f}%)",
                ""
            ])
            
            if results.get('failed_expectations'):
                report_lines.extend([
                    "FAILED EXPECTATIONS:",
                    "-" * 20
                ])
                for failed in results['failed_expectations']:
                    report_lines.extend([
                        f"• {failed['expectation_type']}",
                        f"  Column: {failed['column']}",
                        f"  Description: {failed['description']}",
                        f"  Observed: {failed['observed_value']}",
                        ""
                    ])
        
        report_lines.append("=" * 80)
        return "\n".join(report_lines)

    def run_checkpoint(self, checkpoint_name: str) -> Dict[str, Any]:
        """Run a predefined checkpoint"""
        
        try:
            checkpoint = self.context.get_checkpoint(checkpoint_name)
            checkpoint_result = checkpoint.run()
            
            return {
                'checkpoint_name': checkpoint_name,
                'success': checkpoint_result.success,
                'results': checkpoint_result
            }
        except Exception as e:
            logger.error(f"Failed to run checkpoint {checkpoint_name}: {str(e)}")
            return {
                'checkpoint_name': checkpoint_name,
                'success': False,
                'error': str(e)
            }

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description="Great Expectations Data Validator")
    parser.add_argument("--layer", required=True, choices=['bronze', 'silver', 'gold'],
                       help="Data layer to validate")
    parser.add_argument("--table", help="Specific table to validate (optional)")
    parser.add_argument("--date", required=True, help="Process date (YYYY-MM-DD)")
    parser.add_argument("--context-dir", help="Great Expectations context directory")
    parser.add_argument("--output-format", choices=['json', 'text'], default='text',
                       help="Output format")
    
    args = parser.parse_args()
    
    try:
        # Initialize validator
        validator = GreatExpectationsValidator(context_root_dir=args.context_dir)
        
        # Run validation
        if args.table:
            results = validator.validate_table(args.layer, args.table, args.date)
        else:
            results = validator.validate_layer(args.layer, args.date)
        
        # Output results
        if args.output_format == 'json':
            print(json.dumps(results, indent=2))
        else:
            print(validator.generate_validation_report(results))
        
        # Exit with appropriate code
        status = results.get('validation_status') or results.get('overall_status')
        if status in ['FAILED', 'ERROR']:
            sys.exit(1)
        elif status == 'WARNING':
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 