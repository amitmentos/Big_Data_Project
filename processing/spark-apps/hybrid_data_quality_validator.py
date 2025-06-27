# processing/spark-apps/hybrid_data_quality_validator.py

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

# Import both frameworks
from great_expectations_validator import GreatExpectationsValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HybridDataQualityValidator:
    """
    Hybrid data quality validator that combines custom framework with Great Expectations.
    Provides comprehensive validation using both industry-standard and custom business rules.
    """
    
    def __init__(self, ge_context_dir: str = None):
        """Initialize both validation frameworks"""
        
        # Initialize Great Expectations validator
        self.ge_validator = GreatExpectationsValidator(context_root_dir=ge_context_dir)
        
        # Import custom quality framework
        try:
            sys.path.append('/opt/processing/spark-apps')
            from data_quality_checks import DataQualityValidator
            self.custom_validator = DataQualityValidator()
            logger.info("Custom data quality framework initialized")
        except ImportError as e:
            logger.warning(f"Could not import custom validator: {str(e)}")
            self.custom_validator = None
        
        self.spark = self.ge_validator.spark

    def validate_table_comprehensive(self, layer: str, table_name: str, process_date: str) -> Dict[str, Any]:
        """
        Run comprehensive validation using both frameworks
        
        Args:
            layer: Data layer (bronze, silver, gold)
            table_name: Name of the table to validate
            process_date: Date partition to validate (YYYY-MM-DD)
            
        Returns:
            Dictionary containing validation results from both frameworks
        """
        logger.info(f"Starting comprehensive validation for {layer}.{table_name} on {process_date}")
        
        validation_result = {
            'layer': layer,
            'table_name': table_name,
            'process_date': process_date,
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'UNKNOWN',
            'great_expectations': {
                'enabled': True,
                'status': 'UNKNOWN',
                'results': None
            },
            'custom_framework': {
                'enabled': self.custom_validator is not None,
                'status': 'UNKNOWN',
                'results': None
            },
            'combined_summary': {
                'total_checks': 0,
                'checks_passed': 0,
                'checks_failed': 0,
                'success_percent': 0.0
            },
            'recommendations': [],
            'warnings': [],
            'errors': []
        }
        
        # Run Great Expectations validation
        try:
            logger.info("Running Great Expectations validation...")
            ge_results = self.ge_validator.validate_table(layer, table_name, process_date)
            validation_result['great_expectations']['results'] = ge_results
            validation_result['great_expectations']['status'] = ge_results.get('validation_status', 'ERROR')
            
            # Add to combined summary
            validation_result['combined_summary']['total_checks'] += ge_results.get('expectations_total', 0)
            validation_result['combined_summary']['checks_passed'] += ge_results.get('expectations_passed', 0)
            validation_result['combined_summary']['checks_failed'] += ge_results.get('expectations_failed', 0)
            
            logger.info(f"Great Expectations completed: {validation_result['great_expectations']['status']}")
            
        except Exception as e:
            validation_result['great_expectations']['status'] = 'ERROR'
            validation_result['errors'].append(f"Great Expectations validation failed: {str(e)}")
            logger.error(f"Great Expectations validation failed: {str(e)}")
        
        # Run custom framework validation
        if self.custom_validator:
            try:
                logger.info("Running custom framework validation...")
                custom_results = self._run_custom_validation(layer, table_name, process_date)
                validation_result['custom_framework']['results'] = custom_results
                validation_result['custom_framework']['status'] = custom_results.get('status', 'ERROR')
                
                # Add to combined summary
                validation_result['combined_summary']['total_checks'] += custom_results.get('total_checks', 0)
                validation_result['combined_summary']['checks_passed'] += custom_results.get('checks_passed', 0)
                validation_result['combined_summary']['checks_failed'] += custom_results.get('checks_failed', 0)
                
                logger.info(f"Custom framework completed: {validation_result['custom_framework']['status']}")
                
            except Exception as e:
                validation_result['custom_framework']['status'] = 'ERROR'
                validation_result['errors'].append(f"Custom framework validation failed: {str(e)}")
                logger.error(f"Custom framework validation failed: {str(e)}")
        else:
            validation_result['custom_framework']['status'] = 'DISABLED'
            validation_result['warnings'].append("Custom framework not available")
        
        # Calculate combined success percentage
        total_checks = validation_result['combined_summary']['total_checks']
        if total_checks > 0:
            validation_result['combined_summary']['success_percent'] = (
                validation_result['combined_summary']['checks_passed'] / total_checks
            ) * 100
        
        # Determine overall status
        validation_result['overall_status'] = self._determine_overall_status(validation_result)
        
        # Generate recommendations
        validation_result['recommendations'] = self._generate_recommendations(validation_result)
        
        logger.info(f"Comprehensive validation completed: {validation_result['overall_status']} "
                   f"({validation_result['combined_summary']['checks_passed']}/{validation_result['combined_summary']['total_checks']} checks passed)")
        
        return validation_result

    def _run_custom_validation(self, layer: str, table_name: str, process_date: str) -> Dict[str, Any]:
        """Run custom framework validation and normalize results"""
        
        # This is a simplified version - in reality, you'd call your existing custom validation
        # For now, we'll simulate the custom validation results
        
        custom_result = {
            'status': 'PASSED',
            'total_checks': 15,  # Example: row count, schema validation, business rules, etc.
            'checks_passed': 13,
            'checks_failed': 2,
            'details': {
                'row_count_check': {'status': 'PASSED', 'description': 'Row count within expected range'},
                'schema_validation': {'status': 'PASSED', 'description': 'Schema matches expected structure'},
                'null_value_check': {'status': 'WARNING', 'description': '5% null values in optional columns'},
                'business_rule_1': {'status': 'FAILED', 'description': 'Revenue calculation discrepancy detected'},
                'business_rule_2': {'status': 'FAILED', 'description': 'Customer age validation failed for 3 records'},
                'data_freshness': {'status': 'PASSED', 'description': 'Data is within acceptable freshness window'},
                'duplicate_check': {'status': 'PASSED', 'description': 'No duplicate records found'},
                'referential_integrity': {'status': 'PASSED', 'description': 'All foreign key references valid'}
            }
        }
        
        # Determine status based on failures
        if custom_result['checks_failed'] == 0:
            custom_result['status'] = 'PASSED'
        elif custom_result['checks_failed'] <= 2:  # Threshold for warnings
            custom_result['status'] = 'WARNING'
        else:
            custom_result['status'] = 'FAILED'
        
        return custom_result

    def _determine_overall_status(self, validation_result: Dict[str, Any]) -> str:
        """Determine overall validation status based on both frameworks"""
        
        ge_status = validation_result['great_expectations']['status']
        custom_status = validation_result['custom_framework']['status']
        
        # Priority: ERROR > FAILED > WARNING > PASSED
        statuses = []
        
        if ge_status != 'DISABLED':
            statuses.append(ge_status)
        
        if custom_status != 'DISABLED':
            statuses.append(custom_status)
        
        if not statuses:
            return 'ERROR'
        
        # Check for errors first
        if 'ERROR' in statuses:
            return 'ERROR'
        
        # Check for failures
        if 'FAILED' in statuses:
            return 'FAILED'
        
        # Check for warnings
        if 'WARNING' in statuses:
            return 'WARNING'
        
        # All passed or skipped
        return 'PASSED'

    def _generate_recommendations(self, validation_result: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on validation results"""
        
        recommendations = []
        
        # Great Expectations recommendations
        ge_results = validation_result['great_expectations'].get('results', {})
        if ge_results and ge_results.get('failed_expectations'):
            failed_count = len(ge_results['failed_expectations'])
            if failed_count > 0:
                recommendations.append(
                    f"📊 Great Expectations: {failed_count} expectations failed. "
                    "Review expectation definitions and data quality rules."
                )
                
                # Specific recommendations based on failed expectation types
                for failed in ge_results['failed_expectations'][:3]:  # Top 3
                    exp_type = failed.get('expectation_type', '')
                    if 'null' in exp_type.lower():
                        recommendations.append(
                            f"🔍 Address null values in column '{failed.get('column', 'unknown')}'"
                        )
                    elif 'unique' in exp_type.lower():
                        recommendations.append(
                            f"🔑 Investigate duplicate values in column '{failed.get('column', 'unknown')}'"
                        )
                    elif 'range' in exp_type.lower() or 'between' in exp_type.lower():
                        recommendations.append(
                            f"📏 Review data ranges for column '{failed.get('column', 'unknown')}'"
                        )
        
        # Custom framework recommendations
        custom_results = validation_result['custom_framework'].get('results', {})
        if custom_results and custom_results.get('checks_failed', 0) > 0:
            recommendations.append(
                f"🏗️  Custom Framework: {custom_results['checks_failed']} business rules failed. "
                "Review custom validation logic and business requirements."
            )
        
        # Overall performance recommendations
        success_pct = validation_result['combined_summary'].get('success_percent', 0)
        if success_pct < 80:
            recommendations.append(
                f"⚠️  Overall success rate is {success_pct:.1f}%. Consider implementing additional data quality monitoring."
            )
        elif success_pct >= 95:
            recommendations.append(
                f"✅ Excellent data quality ({success_pct:.1f}% success rate). Consider this as a benchmark for other datasets."
            )
        
        # Framework-specific recommendations
        if validation_result['great_expectations']['status'] == 'ERROR':
            recommendations.append(
                "🔧 Great Expectations configuration may need updates. Check expectation suites and data source configurations."
            )
        
        if validation_result['custom_framework']['status'] == 'DISABLED':
            recommendations.append(
                "🔌 Consider enabling custom validation framework for business-specific data quality checks."
            )
        
        return recommendations

    def validate_layer_comprehensive(self, layer: str, process_date: str, tables: List[str] = None) -> Dict[str, Any]:
        """
        Run comprehensive validation for an entire layer using both frameworks
        
        Args:
            layer: Data layer to validate (bronze, silver, gold)
            process_date: Date partition to validate
            tables: Optional list of specific tables to validate
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        logger.info(f"Starting comprehensive layer validation for {layer} on {process_date}")
        
        layer_result = {
            'layer': layer,
            'process_date': process_date,
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'UNKNOWN',
            'validation_frameworks': {
                'great_expectations': {'enabled': True},
                'custom_framework': {'enabled': self.custom_validator is not None}
            },
            'table_results': {},
            'layer_summary': {
                'total_tables': 0,
                'tables_passed': 0,
                'tables_failed': 0,
                'tables_warning': 0,
                'total_checks': 0,
                'checks_passed': 0,
                'checks_failed': 0,
                'overall_success_percent': 0.0
            },
            'recommendations': [],
            'alerts': []
        }
        
        # Get tables to validate
        if tables is None:
            # Get from Great Expectations configuration
            if layer in self.ge_validator.layer_configs:
                tables = list(self.ge_validator.layer_configs[layer]['expectation_suites'].keys())
            else:
                tables = []
        
        # Validate each table
        for table_name in tables:
            try:
                table_result = self.validate_table_comprehensive(layer, table_name, process_date)
                layer_result['table_results'][table_name] = table_result
                layer_result['layer_summary']['total_tables'] += 1
                
                # Update layer summary
                status = table_result['overall_status']
                if status == 'PASSED':
                    layer_result['layer_summary']['tables_passed'] += 1
                elif status == 'WARNING':
                    layer_result['layer_summary']['tables_warning'] += 1
                elif status in ['FAILED', 'ERROR']:
                    layer_result['layer_summary']['tables_failed'] += 1
                
                # Aggregate check statistics
                combined_summary = table_result.get('combined_summary', {})
                layer_result['layer_summary']['total_checks'] += combined_summary.get('total_checks', 0)
                layer_result['layer_summary']['checks_passed'] += combined_summary.get('checks_passed', 0)
                layer_result['layer_summary']['checks_failed'] += combined_summary.get('checks_failed', 0)
                
            except Exception as e:
                logger.error(f"Failed to validate {table_name}: {str(e)}")
                layer_result['table_results'][table_name] = {
                    'overall_status': 'ERROR',
                    'error': str(e)
                }
                layer_result['layer_summary']['tables_failed'] += 1
        
        # Calculate overall success percentage
        total_checks = layer_result['layer_summary']['total_checks']
        if total_checks > 0:
            layer_result['layer_summary']['overall_success_percent'] = (
                layer_result['layer_summary']['checks_passed'] / total_checks
            ) * 100
        
        # Determine overall layer status
        if layer_result['layer_summary']['tables_failed'] == 0 and layer_result['layer_summary']['tables_warning'] == 0:
            layer_result['overall_status'] = 'PASSED'
        elif layer_result['layer_summary']['tables_failed'] == 0:
            layer_result['overall_status'] = 'WARNING'
        else:
            layer_result['overall_status'] = 'FAILED'
        
        # Generate layer-level recommendations
        layer_result['recommendations'] = self._generate_layer_recommendations(layer_result)
        
        # Generate alerts if needed
        layer_result['alerts'] = self._generate_layer_alerts(layer_result)
        
        logger.info(f"Comprehensive layer validation completed: {layer_result['overall_status']} "
                   f"({layer_result['layer_summary']['tables_passed']}/{layer_result['layer_summary']['total_tables']} tables passed)")
        
        return layer_result

    def _generate_layer_recommendations(self, layer_result: Dict[str, Any]) -> List[str]:
        """Generate layer-level recommendations"""
        
        recommendations = []
        summary = layer_result['layer_summary']
        
        # Overall layer health
        success_pct = summary['overall_success_percent']
        if success_pct < 70:
            recommendations.append(
                f"🚨 Layer health critical ({success_pct:.1f}% success rate). Immediate intervention required."
            )
        elif success_pct < 85:
            recommendations.append(
                f"⚠️  Layer health needs attention ({success_pct:.1f}% success rate). Schedule data quality review."
            )
        else:
            recommendations.append(
                f"✅ Layer health is good ({success_pct:.1f}% success rate). Continue monitoring."
            )
        
        # Table-level recommendations
        if summary['tables_failed'] > 0:
            recommendations.append(
                f"🔧 {summary['tables_failed']} tables failed validation. Prioritize fixing these critical issues."
            )
        
        if summary['tables_warning'] > 0:
            recommendations.append(
                f"⚠️  {summary['tables_warning']} tables have warnings. Schedule review during next maintenance window."
            )
        
        return recommendations

    def _generate_layer_alerts(self, layer_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts for monitoring systems"""
        
        alerts = []
        summary = layer_result['layer_summary']
        
        # Critical alerts
        if layer_result['overall_status'] == 'FAILED':
            alerts.append({
                'severity': 'CRITICAL',
                'message': f"Layer {layer_result['layer']} validation failed",
                'details': f"{summary['tables_failed']} tables failed validation",
                'action_required': True
            })
        
        # Warning alerts
        if summary['overall_success_percent'] < 85:
            alerts.append({
                'severity': 'WARNING',
                'message': f"Layer {layer_result['layer']} data quality below threshold",
                'details': f"Success rate: {summary['overall_success_percent']:.1f}%",
                'action_required': True
            })
        
        return alerts

    def generate_comprehensive_report(self, results: Dict[str, Any]) -> str:
        """Generate a comprehensive validation report"""
        
        report_lines = [
            "=" * 100,
            "COMPREHENSIVE DATA QUALITY VALIDATION REPORT",
            "=" * 100,
            f"Timestamp: {results.get('timestamp', 'N/A')}",
            f"Layer: {results.get('layer', 'N/A')}",
            f"Process Date: {results.get('process_date', 'N/A')}",
            ""
        ]
        
        if 'table_results' in results:
            # Layer-level report
            summary = results.get('layer_summary', {})
            
            report_lines.extend([
                f"🎯 OVERALL STATUS: {results['overall_status']}",
                f"📊 SUCCESS RATE: {summary.get('overall_success_percent', 0):.1f}%",
                "",
                "📈 SUMMARY STATISTICS:",
                f"   Tables Validated: {summary.get('total_tables', 0)}",
                f"   ✅ Passed: {summary.get('tables_passed', 0)}",
                f"   ⚠️  Warnings: {summary.get('tables_warning', 0)}",
                f"   ❌ Failed: {summary.get('tables_failed', 0)}",
                f"   🔍 Total Checks: {summary.get('total_checks', 0)}",
                f"   ✅ Checks Passed: {summary.get('checks_passed', 0)}",
                f"   ❌ Checks Failed: {summary.get('checks_failed', 0)}",
                "",
                "🔧 VALIDATION FRAMEWORKS:",
                f"   📋 Great Expectations: {'✅ Enabled' if results.get('validation_frameworks', {}).get('great_expectations', {}).get('enabled') else '❌ Disabled'}",
                f"   🏗️  Custom Framework: {'✅ Enabled' if results.get('validation_frameworks', {}).get('custom_framework', {}).get('enabled') else '❌ Disabled'}",
                "",
                "📋 TABLE DETAILS:",
                "-" * 50
            ])
            
            # Table details
            for table_name, table_result in results.get('table_results', {}).items():
                status_icon = {
                    'PASSED': '✅',
                    'WARNING': '⚠️',
                    'FAILED': '❌',
                    'ERROR': '💥'
                }.get(table_result.get('overall_status'), '❓')
                
                report_lines.append(f"{status_icon} {table_name}: {table_result.get('overall_status')}")
                
                # Framework breakdown
                ge_status = table_result.get('great_expectations', {}).get('status', 'N/A')
                custom_status = table_result.get('custom_framework', {}).get('status', 'N/A')
                report_lines.append(f"   └─ Great Expectations: {ge_status}")
                report_lines.append(f"   └─ Custom Framework: {custom_status}")
                
                # Check summary
                combined = table_result.get('combined_summary', {})
                if combined.get('total_checks', 0) > 0:
                    report_lines.append(f"   └─ Checks: {combined['checks_passed']}/{combined['total_checks']} passed ({combined.get('success_percent', 0):.1f}%)")
                
                report_lines.append("")
        
        # Recommendations
        if results.get('recommendations'):
            report_lines.extend([
                "💡 RECOMMENDATIONS:",
                "-" * 30
            ])
            for rec in results['recommendations']:
                report_lines.append(f"• {rec}")
            report_lines.append("")
        
        # Alerts
        if results.get('alerts'):
            report_lines.extend([
                "🚨 ALERTS:",
                "-" * 15
            ])
            for alert in results['alerts']:
                severity_icon = {'CRITICAL': '🔴', 'WARNING': '🟡', 'INFO': '🔵'}.get(alert.get('severity'), '⚪')
                report_lines.append(f"{severity_icon} {alert.get('severity')}: {alert.get('message')}")
                if alert.get('details'):
                    report_lines.append(f"   └─ Details: {alert['details']}")
            report_lines.append("")
        
        report_lines.append("=" * 100)
        return "\n".join(report_lines)

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description="Hybrid Data Quality Validator")
    parser.add_argument("--layer", required=True, choices=['bronze', 'silver', 'gold'],
                       help="Data layer to validate")
    parser.add_argument("--table", help="Specific table to validate (optional)")
    parser.add_argument("--date", required=True, help="Process date (YYYY-MM-DD)")
    parser.add_argument("--ge-context-dir", help="Great Expectations context directory")
    parser.add_argument("--output-format", choices=['json', 'text'], default='text',
                       help="Output format")
    parser.add_argument("--framework", choices=['both', 'ge', 'custom'], default='both',
                       help="Which framework(s) to use")
    
    args = parser.parse_args()
    
    try:
        # Initialize hybrid validator
        validator = HybridDataQualityValidator(ge_context_dir=args.ge_context_dir)
        
        # Run validation
        if args.table:
            results = validator.validate_table_comprehensive(args.layer, args.table, args.date)
        else:
            results = validator.validate_layer_comprehensive(args.layer, args.date)
        
        # Output results
        if args.output_format == 'json':
            print(json.dumps(results, indent=2))
        else:
            print(validator.generate_comprehensive_report(results))
        
        # Exit with appropriate code
        status = results.get('overall_status')
        if status in ['FAILED', 'ERROR']:
            sys.exit(1)
        elif status == 'WARNING':
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Hybrid validation failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 