#!/usr/bin/env python3
# run_great_expectations_demo.py

"""
Great Expectations Integration Demo for E-commerce Data Platform

This script demonstrates the new Great Expectations integration alongside
the existing custom data quality framework.

Usage:
    python3 run_great_expectations_demo.py
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run Great Expectations integration demo"""
    
    print("🚀 Great Expectations Integration Demo")
    print("=" * 60)
    print()
    
    # Check if platform is running
    print("🔍 Checking platform status...")
    platform_status = os.system("docker-compose ps --services --filter 'status=running' | wc -l")
    
    if platform_status != 0:
        print("❌ Platform services not detected. Please start the platform first:")
        print("   ./start_platform_demo.sh")
        return 1
    
    print("✅ Platform services are running")
    print()
    
    # Demo sections
    demo_sections = [
        {
            'title': '📋 Great Expectations Configuration',
            'description': 'Showing the Great Expectations setup for medallion architecture',
            'action': show_ge_configuration
        },
        {
            'title': '🔍 Expectation Suites Overview',
            'description': 'Reviewing expectation suites for bronze, silver, and gold layers',
            'action': show_expectation_suites
        },
        {
            'title': '⚡ Running Validation Demo',
            'description': 'Demonstrating validation with sample data',
            'action': run_validation_demo
        },
        {
            'title': '🔄 Hybrid Framework Demo',
            'description': 'Showing combined Great Expectations + Custom validation',
            'action': run_hybrid_demo
        },
        {
            'title': '📊 Validation Report',
            'description': 'Generating comprehensive validation reports',
            'action': show_validation_reports
        }
    ]
    
    # Run demo sections
    for i, section in enumerate(demo_sections, 1):
        print(f"{section['title']}")
        print(f"   {section['description']}")
        print()
        
        try:
            section['action']()
        except Exception as e:
            print(f"❌ Error in demo section: {str(e)}")
            logger.error(f"Demo section failed: {str(e)}")
        
        if i < len(demo_sections):
            print("\n" + "─" * 60 + "\n")
    
    print("🎉 Great Expectations Integration Demo Complete!")
    print()
    print("Next Steps:")
    print("• Review the generated data quality reports")
    print("• Customize expectation suites for your specific needs")
    print("• Integrate with your CI/CD pipeline")
    print("• Set up automated alerting based on validation results")
    
    return 0

def show_ge_configuration():
    """Show Great Expectations configuration"""
    
    print("📋 Great Expectations Configuration Structure:")
    print()
    
    # Show directory structure
    if os.path.exists("processing/great_expectations"):
        os.system("tree processing/great_expectations/ 2>/dev/null || find processing/great_expectations/ -type f | head -20")
    else:
        print("❌ Great Expectations configuration not found")
        return
    
    print()
    print("📊 Data Sources Configured:")
    print("   • Bronze Layer: Raw data validation (schema, format, basic quality)")
    print("   • Silver Layer: Standardized data validation (transformations, enrichments)")  
    print("   • Gold Layer: Business metrics validation (aggregations, calculations)")
    
    print()
    print("🏗️  Architecture Integration:")
    print("   • S3/MinIO data source connections")
    print("   • Spark execution engine for big data")
    print("   • Iceberg table format support")
    print("   • Medallion architecture patterns")

def show_expectation_suites():
    """Show expectation suites overview"""
    
    print("🔍 Expectation Suites Overview:")
    print()
    
    expectation_files = [
        ("Bronze Raw User Events", "processing/great_expectations/expectations/bronze_raw_user_events.json"),
        ("Silver Standardized Events", "processing/great_expectations/expectations/silver_standardized_user_events.json"),
        ("Gold Fact Sales", "processing/great_expectations/expectations/gold_fact_sales.json")
    ]
    
    for suite_name, file_path in expectation_files:
        print(f"📋 {suite_name}:")
        
        if os.path.exists(file_path):
            # Count expectations
            try:
                import json
                with open(file_path, 'r') as f:
                    suite_data = json.load(f)
                    expectations = suite_data.get('expectations', [])
                    print(f"   └─ {len(expectations)} expectations defined")
                    
                    # Show expectation types
                    exp_types = set()
                    for exp in expectations[:5]:  # Show first 5
                        exp_type = exp.get('expectation_type', '').replace('expect_', '').replace('_', ' ')
                        exp_types.add(exp_type)
                        
                    for exp_type in sorted(exp_types):
                        print(f"      • {exp_type}")
                        
                    if len(expectations) > 5:
                        print(f"      • ... and {len(expectations) - 5} more")
                        
            except Exception as e:
                print(f"   └─ Error reading expectations: {str(e)}")
        else:
            print("   └─ ❌ Suite file not found")
        
        print()

def run_validation_demo():
    """Run validation demo"""
    
    print("⚡ Running Great Expectations Validation Demo:")
    print()
    
    # Use today's date for demo
    demo_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"📅 Demo Date: {demo_date}")
    print()
    
    # Check if we're running in Docker environment
    in_docker = os.path.exists("/.dockerenv")
    
    # Validate bronze layer
    print("🥉 Validating Bronze Layer...")
    
    if in_docker:
        # Running inside container - use direct python call
        bronze_cmd = f"""
        cd /opt/processing/spark-apps && \
        python3 great_expectations_validator.py \
        --layer bronze \
        --date {demo_date} \
        --context-dir /opt/processing/great_expectations \
        --output-format text
        """
    else:
        # Running on host - use docker exec to run in Spark container
        docker_compose_cmd = "docker compose" if os.system("docker compose version > /dev/null 2>&1") == 0 else "docker-compose"
        bronze_cmd = f"""
        {docker_compose_cmd} exec -T spark-master /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --py-files /opt/processing/utils \
        /opt/processing/spark-apps/great_expectations_validator.py \
        --layer bronze \
        --date {demo_date} \
        --context-dir /opt/processing/great_expectations \
        --output-format text
        """
    
    print("   Running bronze validation...")
    result = os.system(bronze_cmd)
    
    if result == 0:
        print("   ✅ Bronze validation completed successfully")
    else:
        print("   ⚠️  Bronze validation completed with issues")
    
    print()
    
    # Show that we can also validate individual tables
    print("🔍 Individual Table Validation Example:")
    if in_docker:
        print("   Command: python3 great_expectations_validator.py --layer silver --table standardized_user_events --date 2024-12-15")
    else:
        print("   Command: docker compose exec spark-master spark-submit /opt/processing/spark-apps/great_expectations_validator.py --layer silver --table standardized_user_events --date 2024-12-15")
    print()

def run_hybrid_demo():
    """Run hybrid framework demo"""
    
    print("🔄 Hybrid Framework Demo (Great Expectations + Custom):")
    print()
    
    demo_date = datetime.now().strftime('%Y-%m-%d')
    
    print("This demonstrates running both validation frameworks together:")
    print()
    print("📊 Great Expectations provides:")
    print("   • Industry-standard data quality expectations")
    print("   • Schema validation and statistical profiling")
    print("   • Data documentation and lineage")
    print("   • Automated data quality reporting")
    print()
    print("🏗️  Custom Framework provides:")
    print("   • Business-specific validation rules")
    print("   • Complex cross-table validations")  
    print("   • Real-time data quality metrics")
    print("   • Custom alerting and monitoring")
    print()
    
    # Show hybrid command example
    print("🔧 Hybrid Validation Command Example:")
    print(f"   python3 hybrid_data_quality_validator.py --layer gold --date {demo_date}")
    print()
    
    print("📈 Benefits of Hybrid Approach:")
    print("   • Comprehensive coverage of all data quality aspects")
    print("   • Best-of-breed tooling for different scenarios")
    print("   • Flexibility to use either framework independently")
    print("   • Unified reporting and monitoring")

def show_validation_reports():
    """Show validation report examples"""
    
    print("📊 Validation Report Examples:")
    print()
    
    # Show sample report structure
    print("🎯 Comprehensive Validation Report Structure:")
    print("""
================================================================================
COMPREHENSIVE DATA QUALITY VALIDATION REPORT
================================================================================
Timestamp: 2024-12-15T10:30:00
Layer: gold
Process Date: 2024-12-15

🎯 OVERALL STATUS: PASSED
📊 SUCCESS RATE: 94.2%

📈 SUMMARY STATISTICS:
   Tables Validated: 3
   ✅ Passed: 2
   ⚠️  Warnings: 1
   ❌ Failed: 0
   🔍 Total Checks: 45
   ✅ Checks Passed: 42
   ❌ Checks Failed: 3

🔧 VALIDATION FRAMEWORKS:
   📋 Great Expectations: ✅ Enabled
   🏗️  Custom Framework: ✅ Enabled

📋 TABLE DETAILS:
──────────────────────────────────────────────────────
✅ fact_sales: PASSED
   └─ Great Expectations: PASSED
   └─ Custom Framework: PASSED
   └─ Checks: 18/20 passed (90.0%)

⚠️  customer_product_interactions: WARNING
   └─ Great Expectations: WARNING
   └─ Custom Framework: PASSED
   └─ Checks: 14/15 passed (93.3%)

💡 RECOMMENDATIONS:
──────────────────────────────────────
• ✅ Excellent data quality (94.2% success rate). Consider this as benchmark.
• 🔍 Address null values in column 'product_rating'
• ⚠️  1 tables have warnings. Schedule review during next maintenance window.

================================================================================
    """)
    
    print("📋 Available Report Formats:")
    print("   • Text format: Human-readable console output")
    print("   • JSON format: Machine-readable for integration")
    print("   • HTML format: Rich web-based reports (via Great Expectations)")
    print()
    
    print("🔔 Alerting Integration:")
    print("   • Email notifications for critical failures")
    print("   • Slack alerts for warnings")
    print("   • PagerDuty integration for urgent issues")
    print("   • Custom webhook support for other systems")

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⏹️  Demo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Demo failed: {str(e)}")
        logger.error(f"Demo failed: {str(e)}")
        sys.exit(1) 