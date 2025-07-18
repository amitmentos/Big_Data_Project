# Great Expectations Integration

This document describes the integration of Great Expectations with the existing custom data quality framework in the e-commerce data platform.

## Overview

The platform now supports **dual data quality validation** using both:

1. **Great Expectations** - Industry-standard data validation framework
2. **Custom Framework** - Business-specific validation logic

This hybrid approach provides comprehensive data quality coverage across all layers of the medallion architecture.

## Architecture

### Integration Points

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Quality Architecture                 │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌─────────────────┐    ┌─────────────┐ │
│  │    Bronze    │    │     Silver      │    │    Gold     │ │
│  │   Raw Data   │──▶ │ Standardized    │──▶ │ Aggregated  │ │
│  │              │    │     Data        │    │    Data     │ │
│  └──────────────┘    └─────────────────┘    └─────────────┘ │
│           │                    │                     │       │
│           ▼                    ▼                     ▼       │
│  ┌──────────────────────────────────────────────────────────┤
│  │                Data Quality Layer                        │
│  ├──────────────────┬───────────────────────────────────────┤
│  │ Great            │ Custom Framework                      │
│  │ Expectations     │                                       │
│  │                  │ • Business rules                      │
│  │ • Schema         │ • Cross-table validation              │
│  │ • Statistical    │ • Real-time metrics                   │
│  │ • Format         │ • Custom alerts                       │
│  │ • Constraints    │ • Complex calculations                │
│  └──────────────────┴───────────────────────────────────────┘
│                               │                              │
│                               ▼                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │            Unified Reporting & Alerting                │ │
│  │                                                         │ │
│  │ • Comprehensive validation reports                      │ │
│  │ • Combined success metrics                              │ │
│  │ • Actionable recommendations                            │ │
│  │ • Multi-channel alerting                                │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Great Expectations Configuration

### Directory Structure

```
processing/great_expectations/
├── great_expectations.yml              # Main configuration
├── expectations/                       # Expectation suites
│   ├── bronze_raw_user_events.json
│   ├── silver_standardized_user_events.json
│   └── gold_fact_sales.json
├── checkpoints/                        # Validation checkpoints
│   └── bronze_layer_checkpoint.yml
├── uncommitted/                        # Runtime data
│   ├── validations/
│   └── data_docs/
└── plugins/                           # Custom plugins
```

### Data Sources

The platform is configured with three main data sources corresponding to the medallion architecture:

#### Bronze Data Source
- **Purpose**: Raw data validation
- **Location**: `s3a://warehouse/bronze/`
- **Validation Focus**: Schema compliance, data format, basic quality checks
- **Tables**: `raw_user_events`, `raw_customer_data`, `raw_marketplace_sales`, `raw_product_data`

#### Silver Data Source
- **Purpose**: Standardized data validation
- **Location**: `s3a://warehouse/silver/`
- **Validation Focus**: Data transformations, enrichments, standardization
- **Tables**: `standardized_user_events`, `standardized_sales`, `dim_customer_scd`, `dim_product`

#### Gold Data Source
- **Purpose**: Business metrics validation
- **Location**: `s3a://warehouse/gold/`
- **Validation Focus**: Aggregations, business calculations, analytical metrics
- **Tables**: `fact_sales`, `customer_product_interactions`

## Expectation Suites

### Bronze Layer Expectations

**Raw User Events** (`bronze_raw_user_events.json`):
- Column presence and order validation
- Data type enforcement
- Null value constraints
- Valid event types and platforms
- Timestamp format validation
- Basic statistical checks

### Silver Layer Expectations

**Standardized User Events** (`silver_standardized_user_events.json`):
- Enhanced schema validation
- Derived column validation
- Data enrichment quality checks
- Standardization rule compliance
- Cross-field dependencies

### Gold Layer Expectations

**Fact Sales** (`gold_fact_sales.json`):
- Business metric validation
- Aggregation accuracy
- Key relationship integrity
- Financial calculation validation
- Performance threshold monitoring

## Usage

### Command Line Interface

#### Validate Single Table
```bash
# Great Expectations only
python3 processing/spark-apps/great_expectations_validator.py \
  --layer bronze \
  --table raw_user_events \
  --date 2024-12-15 \
  --output-format text

# Hybrid validation (GE + Custom)
python3 processing/spark-apps/hybrid_data_quality_validator.py \
  --layer silver \
  --table standardized_user_events \
  --date 2024-12-15 \
  --framework both
```

#### Validate Entire Layer
```bash
# Validate all tables in gold layer
python3 processing/spark-apps/great_expectations_validator.py \
  --layer gold \
  --date 2024-12-15 \
  --output-format json
```

### Airflow Integration

The platform includes a dedicated DAG for Great Expectations validation:

```python
# orchestration/dags/great_expectations_dag.py
# Runs every 6 hours and validates all layers
```

**DAG Features**:
- Layer-by-layer validation
- External task sensors for pipeline dependencies
- Comprehensive reporting
- Automated alerting
- XCom data sharing between tasks

### Programmatic Usage

```python
from great_expectations_validator import GreatExpectationsValidator
from hybrid_data_quality_validator import HybridDataQualityValidator

# Great Expectations only
ge_validator = GreatExpectationsValidator()
results = ge_validator.validate_layer('bronze', '2024-12-15')

# Hybrid validation
hybrid_validator = HybridDataQualityValidator()
results = hybrid_validator.validate_layer_comprehensive('silver', '2024-12-15')
```

## Validation Results

### Result Structure

```json
{
  "layer": "gold",
  "process_date": "2024-12-15",
  "timestamp": "2024-12-15T10:30:00",
  "overall_status": "PASSED",
  "great_expectations": {
    "enabled": true,
    "status": "PASSED",
    "results": {
      "expectations_total": 20,
      "expectations_passed": 18,
      "expectations_failed": 2,
      "success_percent": 90.0
    }
  },
  "custom_framework": {
    "enabled": true,
    "status": "WARNING",
    "results": {
      "total_checks": 15,
      "checks_passed": 13,
      "checks_failed": 2
    }
  },
  "combined_summary": {
    "total_checks": 35,
    "checks_passed": 31,
    "checks_failed": 4,
    "success_percent": 88.6
  },
  "recommendations": [
    "Great Expectations: 2 expectations failed. Review expectation definitions.",
    "Address null values in column 'product_rating'",
    "Review data ranges for column 'unit_price'"
  ]
}
```

### Status Levels

- **PASSED**: All validations successful
- **WARNING**: Minor issues detected (>80% success rate)
- **FAILED**: Significant issues detected (<80% success rate)
- **ERROR**: Validation process failed
- **SKIPPED**: No expectations defined for table

## Reports and Monitoring

### Text Reports

Human-readable console output with emojis and clear formatting:

```
================================================================================
COMPREHENSIVE DATA QUALITY VALIDATION REPORT
================================================================================
Timestamp: 2024-12-15T10:30:00
Layer: gold
Process Date: 2024-12-15

OVERALL STATUS: PASSED
SUCCESS RATE: 94.2%

SUMMARY STATISTICS:
   Tables Validated: 3
   Passed: 2
   Warnings: 1
   Failed: 0

RECOMMENDATIONS:
• Excellent data quality (94.2% success rate)
• Address null values in column 'product_rating'
================================================================================
```

### JSON Reports

Machine-readable format for integration with monitoring systems and APIs.

### HTML Reports

Great Expectations automatically generates rich HTML documentation with:
- Interactive data profiling
- Expectation suite documentation
- Validation result history
- Data lineage visualization

## Alerting and Notifications

### Alert Levels

- **CRITICAL**: Layer validation failed
- **WARNING**: Success rate below threshold (85%)
- **INFO**: Validation completed successfully

### Integration Points

- **Email**: Critical failures and daily summaries
- **Slack**: Real-time warnings and failures
- **PagerDuty**: Critical production issues
- **Webhooks**: Custom integration endpoints

## Customization

### Adding New Expectations

1. **Create Expectation Suite**:
```json
{
  "expectation_suite_name": "my_custom_table",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "primary_key"},
      "meta": {"description": "Primary key cannot be null"}
    }
  ]
}
```

2. **Update Configuration**:
```python
# Add to layer_configs in great_expectations_validator.py
'expectation_suites': {
    'my_custom_table': 'my_custom_expectation_suite'
}
```

3. **Test Validation**:
```bash
python3 great_expectations_validator.py \
  --layer bronze \
  --table my_custom_table \
  --date 2024-12-15
```

### Custom Business Rules

Extend the custom framework for business-specific validations:

```python
def validate_business_rule(self, df, rule_name):
    """Custom business rule validation"""
    if rule_name == "revenue_consistency":
        # Implement complex business logic
        return validation_result
```

## Performance Considerations

### Optimization Strategies

1. **Partition-aware Validation**: Only validate relevant date partitions
2. **Sampling**: Use data sampling for large datasets
3. **Parallel Execution**: Run validations in parallel across tables
4. **Caching**: Cache expectation suite objects
5. **Resource Management**: Configure Spark resources appropriately

### Monitoring Performance

```python
# Built-in performance metrics
validation_result = {
    'execution_time': '45.2s',
    'rows_validated': 1000000,
    'memory_usage': '2.1GB',
    'spark_tasks': 24
}
```

## Troubleshooting

### Common Issues

#### 1. Data Source Connection Errors
```
Error: Failed to connect to MinIO/S3 data source
Solution: Check network connectivity and credentials
```

#### 2. Schema Mismatch
```
Error: Column 'expected_column' not found in data
Solution: Update expectation suite or verify data schema
```

#### 3. Memory Issues
```
Error: Spark job failed with OutOfMemoryError
Solution: Increase Spark executor memory or use data sampling
```

#### 4. Great Expectations Configuration
```
Error: DataContext initialization failed
Solution: Verify great_expectations.yml configuration
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
export LOG_LEVEL=DEBUG
python3 great_expectations_validator.py --layer bronze --date 2024-12-15
```

## Migration Guide

### From Custom-Only to Hybrid

1. **Assess Current Coverage**: Review existing custom validations
2. **Map to Expectations**: Identify rules that can be converted to GE expectations
3. **Gradual Migration**: Start with one layer, gradually expand
4. **Parallel Running**: Run both frameworks in parallel during transition
5. **Validation**: Compare results between frameworks
6. **Optimization**: Fine-tune thresholds and expectations

### Best Practices

- **Start Small**: Begin with bronze layer basic expectations
- **Iterate**: Gradually add more sophisticated expectations
- **Document**: Maintain clear documentation for custom business rules
- **Monitor**: Track validation performance and adjust as needed
- **Collaborate**: Work with business stakeholders to define meaningful expectations

## Demo

Run the interactive demo to explore Great Expectations integration:

```bash
python3 run_great_expectations_demo.py
```

The demo covers:
- Configuration overview
- Expectation suites walkthrough
- Live validation examples
- Hybrid framework demonstration
- Report generation examples

## Conclusion

The Great Expectations integration provides:

**Industry-standard validation** with proven patterns and practices  
**Comprehensive coverage** across all medallion architecture layers  
**Flexible deployment** supporting both standalone and hybrid modes  
**Rich reporting** with actionable insights and recommendations  
**Enterprise integration** with monitoring and alerting systems  
**Scalable architecture** designed for big data workloads  

This implementation complements rather than replaces the existing custom framework, providing the best of both worlds for comprehensive data quality management.