 # processing/utils/data_quality.py

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Comprehensive data quality validation framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.quality_rules = self._load_quality_rules()
    
    def _load_quality_rules(self) -> Dict:
        """Load data quality rules configuration"""
        return {
            "null_thresholds": {
                "critical_fields": 0.0,  # No nulls allowed
                "important_fields": 0.05,  # Max 5% nulls
                "optional_fields": 0.20   # Max 20% nulls
            },
            "duplicate_thresholds": {
                "fact_tables": 0.01,     # Max 1% duplicates
                "dimension_tables": 0.0  # No duplicates allowed
            },
            "value_ranges": {
                "amounts": {"min": 0, "max": 100000},
                "percentages": {"min": 0, "max": 100},
                "counts": {"min": 0, "max": 1000000}
            },
            "pattern_validations": {
                "email": r"^[A-Za-z0-9+_.-]+@(.+)$",
                "phone": r"^\+?1?\d{9,15}$",
                "customer_id": r"^CUST-\d{6}$",
                "product_id": r"^PROD-\d{6}$"
            }
        }
    
    def run_comprehensive_validation(self, df: DataFrame, 
                                   table_name: str,
                                   validation_config: Dict) -> Dict:
        """Run comprehensive data quality validation"""
        
        logger.info(f"Starting comprehensive validation for {table_name}")
        
        validation_results = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "total_records": df.count(),
            "validations": {},
            "overall_status": "PASS",
            "issues": [],
            "warnings": []
        }
        
        try:
            # Schema validation
            schema_result = self.validate_schema(df, validation_config.get("expected_schema"))
            validation_results["validations"]["schema"] = schema_result
            
            # Null validation
            null_result = self.validate_nulls(df, validation_config.get("null_rules", {}))
            validation_results["validations"]["nulls"] = null_result
            
            # Duplicate validation
            duplicate_result = self.validate_duplicates(df, validation_config.get("key_columns", []))
            validation_results["validations"]["duplicates"] = duplicate_result
            
            # Value range validation
            range_result = self.validate_value_ranges(df, validation_config.get("range_rules", {}))
            validation_results["validations"]["ranges"] = range_result
            
            # Pattern validation
            pattern_result = self.validate_patterns(df, validation_config.get("pattern_rules", {}))
            validation_results["validations"]["patterns"] = pattern_result
            
            # Business rule validation
            business_result = self.validate_business_rules(df, validation_config.get("business_rules", []))
            validation_results["validations"]["business_rules"] = business_result
            
            # Determine overall status
            overall_status = self._determine_overall_status(validation_results["validations"])
            validation_results["overall_status"] = overall_status
            
            # Collect issues and warnings
            self._collect_issues_and_warnings(validation_results)
            
            logger.info(f"Validation completed for {table_name}: {overall_status}")
            
        except Exception as e:
            logger.error(f"Validation failed for {table_name}: {str(e)}")
            validation_results["overall_status"] = "ERROR"
            validation_results["error"] = str(e)
        
        return validation_results
    
    def validate_schema(self, df: DataFrame, expected_schema: Optional[Dict]) -> Dict:
        """Validate DataFrame schema against expected schema"""
        
        result = {
            "status": "PASS",
            "issues": [],
            "actual_schema": df.schema.json(),
            "column_count": len(df.columns)
        }
        
        if not expected_schema:
            result["status"] = "SKIPPED"
            result["message"] = "No expected schema provided"
            return result
        
        try:
            actual_columns = set(df.columns)
            expected_columns = set(expected_schema.get("columns", []))
            
            # Check missing columns
            missing_columns = expected_columns - actual_columns
            if missing_columns:
                result["status"] = "FAIL"
                result["issues"].append(f"Missing columns: {list(missing_columns)}")
            
            # Check extra columns
            extra_columns = actual_columns - expected_columns
            if extra_columns:
                result["issues"].append(f"Extra columns: {list(extra_columns)}")
            
            # Check data types
            type_mismatches = []
            for field in df.schema.fields:
                expected_type = expected_schema.get("types", {}).get(field.name)
                if expected_type and field.dataType.simpleString() != expected_type:
                    type_mismatches.append({
                        "column": field.name,
                        "expected": expected_type,
                        "actual": field.dataType.simpleString()
                    })
            
            if type_mismatches:
                result["status"] = "FAIL"
                result["issues"].append(f"Type mismatches: {type_mismatches}")
            
        except Exception as e:
            result["status"] = "ERROR"
            result["error"] = str(e)
        
        return result
    
    def validate_nulls(self, df: DataFrame, null_rules: Dict) -> Dict:
        """Validate null values according to rules"""
        
        result = {
            "status": "PASS",
            "null_percentages": {},
            "violations": []
        }
        
        try:
            total_rows = df.count()
            if total_rows == 0:
                result["status"] = "WARN"
                result["message"] = "DataFrame is empty"
                return result
            
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = null_count / total_rows
                result["null_percentages"][column] = {
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 4)
                }
                
                # Check against rules
                rule_type = null_rules.get(column, "optional_fields")
                threshold = self.quality_rules["null_thresholds"].get(rule_type, 0.20)
                
                if null_percentage > threshold:
                    violation = {
                        "column": column,
                        "null_percentage": null_percentage,
                        "threshold": threshold,
                        "severity": "CRITICAL" if rule_type == "critical_fields" else "WARNING"
                    }
                    result["violations"].append(violation)
                    
                    if violation["severity"] == "CRITICAL":
                        result["status"] = "FAIL"
                    elif result["status"] == "PASS":
                        result["status"] = "WARN"
        
        except Exception as e:
            result["status"] = "ERROR"
            result["error"] = str(e)
        
        return result
    
    def validate_duplicates(self, df: DataFrame, key_columns: List[str]) -> Dict:
        """Validate duplicate records"""
        
        result = {
            "status": "PASS",
            "duplicate_analysis": {}
        }
        
        try:
            total_rows = df.count()
            
            if not key_columns:
                result["status"] = "SKIPPED"
                result["message"] = "No key columns specified"
                return result
            
            # Check for duplicates based on key columns
            for key_set in [key_columns, key_columns[:1] if len(key_columns) > 1 else []]:
                if not key_set:
                    continue
                    
                key_name = "_".join(key_set)
                distinct_count = df.select(*key_set).distinct().count()
                duplicate_count = total_rows - distinct_count
                duplicate_percentage = duplicate_count / total_rows if total_rows > 0 else 0
                
                result["duplicate_analysis"][key_name] = {
                    "total_rows": total_rows,
                    "distinct_rows": distinct_count,
                    "duplicate_count": duplicate_count,
                    "duplicate_percentage": round(duplicate_percentage, 4)
                }
                
                # Check against threshold
                threshold = self.quality_rules["duplicate_thresholds"]["fact_tables"]
                if duplicate_percentage > threshold:
                    result["status"] = "FAIL"
                    result["violation"] = {
                        "keys": key_set,
                        "duplicate_percentage": duplicate_percentage,
                        "threshold": threshold
                    }
        
        except Exception as e:
            result["status"] = "ERROR"
            result["error"] = str(e)
        
        return result
    
    def validate_value_ranges(self, df: DataFrame, range_rules: Dict) -> Dict:
        """Validate numeric value ranges"""
        
        result = {
            "status": "PASS",
            "range_violations": []
        }
        
        try:
            for column, range_config in range_rules.items():
                if column not in df.columns:
                    continue
                
                min_val = range_config.get("min")
                max_val = range_config.get("max")
                
                violations = []
                
                if min_val is not None:
                    below_min = df.filter(col(column) < min_val).count()
                    if below_min > 0:
                        violations.append({
                            "type": "below_minimum",
                            "count": below_min,
                            "threshold": min_val
                        })
                
                if max_val is not None:
                    above_max = df.filter(col(column) > max_val).count()
                    if above_max > 0:
                        violations.append({
                            "type": "above_maximum",
                            "count": above_max,
                            "threshold": max_val
                        })
                
                if violations:
                    result["range_violations"].append({
                        "column": column,
                        "violations": violations
                    })
                    result["status"] = "FAIL"
        
        except Exception as e:
            result["status"] = "ERROR"
            result["error"] = str(e)
        
        return result
    
    def validate_patterns(self, df: DataFrame, pattern_rules: Dict) -> Dict:
        """Validate string patterns using regex"""
        
        result = {
            "status": "PASS",
            "pattern_violations": []
        }
        
        try:
            for column, pattern_config in pattern_rules.items():
                if column not in df.columns:
                    continue
                
                pattern = pattern_config.get("pattern") or \
                         self.quality_rules["pattern_validations"].get(pattern_config.get("type"))
                
                if not pattern:
                    continue
                
                total_non_null = df.filter(col(column).isNotNull()).count()
                if total_non_null == 0:
                    continue
                
                matching_count = df.filter(col(column).rlike(pattern)).count()
                violation_count = total_non_null - matching_count
                violation_percentage = violation_count / total_non_null
                
                if violation_count > 0:
                    threshold = pattern_config.get("threshold", 0.05)  # 5% default threshold
                    
                    violation_info = {
                        "column": column,
                        "pattern": pattern,
                        "violation_count": violation_count,
                        "total_checked": total_non_null,
                        "violation_percentage": round(violation_percentage, 4)
                    }
                    
                    if violation_percentage > threshold:
                        result["pattern_violations"].append(violation_info)
                        result["status"] = "FAIL"
        
        except Exception as e:
            result["status"] = "ERROR"
            result["error"] = str(e)
        
        return result
    
    def validate_business_rules(self, df: DataFrame, business_rules: List[Dict]) -> Dict:
        """Validate custom business rules"""
        
        result = {
            "status": "PASS",
            "rule_violations": []
        }
        
        try:
            for rule in business_rules:
                rule_name = rule.get("name", "unnamed_rule")
                condition = rule.get("condition")
                severity = rule.get("severity", "WARNING")
                
                if not condition:
                    continue
                
                try:
                    violation_count = df.filter(expr(f"NOT ({condition})")).count()
                    
                    if violation_count > 0:
                        violation_info = {
                            "rule_name": rule_name,
                            "condition": condition,
                            "violation_count": violation_count,
                            "severity": severity
                        }
                        result["rule_violations"].append(violation_info)
                        
                        if severity == "CRITICAL":
                            result["status"] = "FAIL"
                        elif result["status"] == "PASS":
                            result["status"] = "WARN"
                
                except Exception as rule_error:
                    logger.warning(f"Error evaluating business rule '{rule_name}': {str(rule_error)}")
        
        except Exception as e:
            result["status"] = "ERROR"
            result["error"] = str(e)
        
        return result
    
    def _determine_overall_status(self, validations: Dict) -> str:
        """Determine overall validation status"""
        
        statuses = [v.get("status", "PASS") for v in validations.values()]
        
        if "ERROR" in statuses:
            return "ERROR"
        elif "FAIL" in statuses:
            return "FAIL"
        elif "WARN" in statuses:
            return "WARN"
        else:
            return "PASS"
    
    def _collect_issues_and_warnings(self, validation_results: Dict):
        """Collect all issues and warnings from validation results"""
        
        issues = []
        warnings = []
        
        for validation_type, results in validation_results["validations"].items():
            if results.get("status") == "FAIL":
                issues.extend(results.get("issues", []))
                if "violations" in results:
                    issues.extend([f"{validation_type}: {v}" for v in results["violations"]])
            elif results.get("status") == "WARN":
                warnings.extend(results.get("issues", []))
        
        validation_results["issues"] = issues
        validation_results["warnings"] = warnings
    
    def generate_quality_report(self, validation_results: Dict) -> str:
        """Generate human-readable quality report"""
        
        report_lines = [
            f"Data Quality Report: {validation_results['table_name']}",
            f"Timestamp: {validation_results['timestamp']}",
            f"Total Records: {validation_results['total_records']:,}",
            f"Overall Status: {validation_results['overall_status']}",
            ""
        ]
        
        # Summary by validation type
        for validation_type, results in validation_results["validations"].items():
            status = results.get("status", "UNKNOWN")
            report_lines.append(f"{validation_type.title()}: {status}")
        
        report_lines.append("")
        
        # Issues
        if validation_results["issues"]:
            report_lines.append("ISSUES:")
            for issue in validation_results["issues"]:
                report_lines.append(f"  - {issue}")
            report_lines.append("")
        
        # Warnings
        if validation_results["warnings"]:
            report_lines.append("WARNINGS:")
            for warning in validation_results["warnings"]:
                report_lines.append(f"  - {warning}")
        
        return "\n".join(report_lines)


class DataProfilingEngine:
    """Data profiling and statistics generation"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def profile_dataframe(self, df: DataFrame, table_name: str) -> Dict:
        """Generate comprehensive data profile"""
        
        profile = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "basic_stats": self._get_basic_stats(df),
            "column_profiles": self._profile_columns(df),
            "data_types": self._analyze_data_types(df),
            "correlation_analysis": self._analyze_correlations(df)
        }
        
        return profile
    
    def _get_basic_stats(self, df: DataFrame) -> Dict:
        """Get basic DataFrame statistics"""
        
        return {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "estimated_size_mb": self._estimate_size(df),
            "partition_count": df.rdd.getNumPartitions()
        }
    
    def _profile_columns(self, df: DataFrame) -> Dict:
        """Profile individual columns"""
        
        column_profiles = {}
        
        # Get numeric columns
        numeric_columns = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (IntegerType, LongType, 
                                                        FloatType, DoubleType, DecimalType))]
        
        # Get string columns
        string_columns = [field.name for field in df.schema.fields 
                         if isinstance(field.dataType, StringType)]
        
        # Profile numeric columns
        if numeric_columns:
            stats_df = df.select(*numeric_columns).describe()
            stats_data = {row['summary']: {col: row[col] for col in numeric_columns} 
                         for row in stats_df.collect()}
            
            for col_name in numeric_columns:
                column_profiles[col_name] = {
                    "data_type": "numeric",
                    "stats": {stat: stats_data[stat][col_name] for stat in stats_data.keys()},
                    "null_count": df.filter(col(col_name).isNull()).count(),
                    "distinct_count": df.select(col_name).distinct().count()
                }
        
        # Profile string columns
        for col_name in string_columns:
            column_profiles[col_name] = {
                "data_type": "string",
                "null_count": df.filter(col(col_name).isNull()).count(),
                "distinct_count": df.select(col_name).distinct().count(),
                "avg_length": df.select(avg(length(col(col_name)))).collect()[0][0],
                "max_length": df.select(max(length(col(col_name)))).collect()[0][0]
            }
        
        return column_profiles
    
    def _analyze_data_types(self, df: DataFrame) -> Dict:
        """Analyze data type distribution"""
        
        type_distribution = {}
        for field in df.schema.fields:
            type_name = field.dataType.simpleString()
            type_distribution[type_name] = type_distribution.get(type_name, 0) + 1
        
        return type_distribution
    
    def _analyze_correlations(self, df: DataFrame) -> Dict:
        """Analyze correlations between numeric columns"""
        
        numeric_columns = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (IntegerType, LongType, 
                                                        FloatType, DoubleType, DecimalType))]
        
        if len(numeric_columns) < 2:
            return {}
        
        correlations = {}
        try:
            # Calculate correlation matrix
            for i, col1 in enumerate(numeric_columns):
                for col2 in numeric_columns[i+1:]:
                    correlation = df.stat.corr(col1, col2)
                    if correlation is not None and abs(correlation) > 0.5:  # Only significant correlations
                        correlations[f"{col1}_vs_{col2}"] = round(correlation, 4)
        except Exception as e:
            logger.warning(f"Error calculating correlations: {str(e)}")
        
        return correlations
    
    def _estimate_size(self, df: DataFrame) -> float:
        """Estimate DataFrame size in MB"""
        
        try:
            # This is a rough estimation
            row_count = df.count()
            if row_count == 0:
                return 0.0
            
            # Estimate based on column types and row count
            estimated_bytes = 0
            for field in df.schema.fields:
                if isinstance(field.dataType, StringType):
                    estimated_bytes += row_count * 50  # Assume avg 50 chars
                elif isinstance(field.dataType, (IntegerType, FloatType)):
                    estimated_bytes += row_count * 4
                elif isinstance(field.dataType, (LongType, DoubleType)):
                    estimated_bytes += row_count * 8
                else:
                    estimated_bytes += row_count * 10  # Other types
            
            return round(estimated_bytes / (1024 * 1024), 2)  # Convert to MB
        
        except Exception:
            return 0.0


# Convenience functions
def validate_table_quality(spark: SparkSession, table_name: str, 
                          validation_config: Dict) -> Dict:
    """Convenience function to validate table quality"""
    
    validator = DataQualityValidator(spark)
    df = spark.table(table_name)
    return validator.run_comprehensive_validation(df, table_name, validation_config)

def profile_table(spark: SparkSession, table_name: str) -> Dict:
    """Convenience function to profile table"""
    
    profiler = DataProfilingEngine(spark)
    df = spark.table(table_name)
    return profiler.profile_dataframe(df, table_name)