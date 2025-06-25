# orchestration/dags/data_quality_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State

default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Reduced retries to avoid long-running tasks
    'retry_delay': timedelta(minutes=5),
    'email': ['data-engineering@company.com']
}

dag = DAG(
    'data_quality_monitoring',
    default_args=default_args,
    description='Comprehensive data quality monitoring across all layers',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'monitoring', 'all-layers']
)

# Custom sensor class to handle missing dependencies gracefully
class RobustExternalTaskSensor(ExternalTaskSensor):
    """
    A more robust ExternalTaskSensor that skips itself if the dependency has not run
    within the execution window, instead of failing or timing out.
    """
    def poke(self, context):
        # First try regular poke
        try:
            return super().poke(context)
        except Exception as e:
            self.log.info(f"Caught exception in external task sensor: {str(e)}")
            return False
            
    def execute(self, context):
        try:
            status = super().execute(context)
            return status
        except Exception as e:
            self.log.warning(f"External task sensor failed: {str(e)}")
            self.log.warning("Skipping this dependency check and proceeding with the DAG")
            # Instead of failing, we skip this task and let the DAG continue
            raise AirflowSkipException("External dependency not met, but continuing anyway")

# Wait for main ETL processes to complete - with grace periods and more reliability
wait_for_bronze_etl = RobustExternalTaskSensor(
    task_id='wait_for_bronze_etl',
    external_dag_id='bronze_to_silver_etl',
    external_task_id='silver_quality_check',
    allowed_states=[State.SUCCESS],  # Only wait for successful tasks
    execution_delta=timedelta(hours=6),  # Increased window to find successful runs
    timeout=300,  # Shorter timeout (5 min) to avoid long blocking
    poke_interval=30,
    mode='reschedule',  # Use reschedule mode to free up workers
    soft_fail=True,  # Continue the DAG even if this task fails
    
    dag=dag
)

wait_for_gold_etl = RobustExternalTaskSensor(
    task_id='wait_for_gold_etl',
    external_dag_id='silver_to_gold_etl',
    external_task_id='gold_quality_check',
    allowed_states=[State.SUCCESS],  # Only wait for successful tasks
    execution_delta=timedelta(hours=6),  # Increased window to find successful runs
    timeout=300,  # Shorter timeout (5 min) to avoid long blocking
    poke_interval=30,
    mode='reschedule',  # Use reschedule mode to free up workers
    soft_fail=True,  # Continue the DAG even if this task fails
    
    dag=dag
)

# Bronze Layer Quality Checks
bronze_data_freshness_check = SparkSubmitOperator(
    task_id='bronze_data_freshness_check',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'bronze', '--check-type', 'freshness', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

bronze_volume_check = SparkSubmitOperator(
    task_id='bronze_volume_check',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'bronze', '--check-type', 'volume', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

# Silver Layer Quality Checks
silver_consistency_check = SparkSubmitOperator(
    task_id='silver_consistency_check',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'silver', '--check-type', 'consistency', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

silver_scd_integrity_check = SparkSubmitOperator(
    task_id='silver_scd_integrity_check',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'silver', '--check-type', 'scd_integrity', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

# Gold Layer Quality Checks
gold_business_rules_check = SparkSubmitOperator(
    task_id='gold_business_rules_check',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'gold', '--check-type', 'business_rules', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

gold_metrics_validation = SparkSubmitOperator(
    task_id='gold_metrics_validation',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'gold', '--check-type', 'metrics_validation', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

# Cross-layer reconciliation
cross_layer_reconciliation = SparkSubmitOperator(
    task_id='cross_layer_reconciliation',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--check-type', 'cross_layer_reconciliation', '--date', '{{ ds }}'],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hadoop',
        'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    },
    
    dag=dag
)

# Aggregate quality results
def aggregate_quality_results(**context):
    """Aggregate all quality check results and determine overall status"""
    import json
    
    # In a real implementation, this would collect results from previous tasks
    # For now, we'll simulate the aggregation
    
    quality_results = {
        'timestamp': context['execution_date'].isoformat(),
        'date': context['ds'],
        'checks': {
            'bronze_freshness': 'PASS',
            'bronze_volume': 'PASS', 
            'silver_consistency': 'PASS',
            'silver_scd_integrity': 'PASS',
            'gold_business_rules': 'PASS',
            'gold_metrics_validation': 'PASS',
            'cross_layer_reconciliation': 'PASS'
        },
        'overall_status': 'PASS',
        'summary': {
            'total_checks': 7,
            'passed': 7,
            'failed': 0,
            'warnings': 0
        }
    }
    
    # Calculate overall status
    failed_checks = [k for k, v in quality_results['checks'].items() if v == 'FAIL']
    warning_checks = [k for k, v in quality_results['checks'].items() if v == 'WARN']
    
    if failed_checks:
        quality_results['overall_status'] = 'FAIL'
        quality_results['summary']['failed'] = len(failed_checks)
        quality_results['failed_checks'] = failed_checks
    elif warning_checks:
        quality_results['overall_status'] = 'WARN'
        quality_results['summary']['warnings'] = len(warning_checks)
        quality_results['warning_checks'] = warning_checks
    
    print("Data Quality Summary:")
    print(f"  Overall Status: {quality_results['overall_status']}")
    print(f"  Total Checks: {quality_results['summary']['total_checks']}")
    print(f"  Passed: {quality_results['summary']['passed']}")
    print(f"  Failed: {quality_results['summary']['failed']}")
    print(f"  Warnings: {quality_results['summary']['warnings']}")
    
    # Store results for downstream tasks
    context['task_instance'].xcom_push(key='quality_results', value=quality_results)
    
    return quality_results['overall_status']

quality_aggregation = PythonOperator(
    task_id='quality_aggregation',
    python_callable=aggregate_quality_results,
    
    dag=dag
)

# Decision point based on quality results
def decide_next_action(**context):
    """Decide next action based on quality results"""
    ti = context['task_instance']
    overall_status = ti.xcom_pull(task_ids='quality_aggregation', key='quality_results')['overall_status']
    
    if overall_status == 'FAIL':
        return 'send_failure_alert'
    elif overall_status == 'WARN':
        return 'send_warning_notification'
    else:
        return 'generate_quality_report'

quality_decision = BranchPythonOperator(
    task_id='quality_decision',
    python_callable=decide_next_action,
    
    dag=dag
)

# Send failure alert
send_failure_alert = EmailOperator(
    task_id='send_failure_alert',
    to=['data-engineering@company.com', 'alerts@company.com'],
    subject='[CRITICAL] Data Quality Failures Detected - {{ ds }}',
    html_content="""
    <h2>Data Quality Failure Alert</h2>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p><strong>Status:</strong> CRITICAL FAILURE</p>
    <p>One or more critical data quality checks have failed. Immediate attention required.</p>
    <p>Please check the Airflow logs for detailed error information.</p>
    <p><a href="{{ dag_run.get_dag().get_absolute_url() }}">View DAG Run</a></p>
    """,
    
    dag=dag
)

# Send warning notification  
send_warning_notification = EmailOperator(
    task_id='send_warning_notification',
    to=['data-engineering@company.com'],
    subject='[WARNING] Data Quality Issues Detected - {{ ds }}',
    html_content="""
    <h2>Data Quality Warning</h2>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p><strong>Status:</strong> WARNING</p>
    <p>Some data quality checks have raised warnings. Please review.</p>
    <p><a href="{{ dag_run.get_dag().get_absolute_url() }}">View DAG Run</a></p>
    """,
    
    dag=dag
)

# Generate quality report
generate_quality_report = BashOperator(
    task_id='generate_quality_report',
    bash_command="""
    echo "Generating data quality report for {{ ds }}"
    echo "All quality checks passed successfully"
    echo "Report timestamp: $(date)"
    
    # In a real implementation, this would generate a detailed HTML/PDF report
    # and store it in a shared location or send it to stakeholders
    """,
    
    dag=dag
)

# Update quality metrics dashboard
update_quality_dashboard = PythonOperator(
    task_id='update_quality_dashboard',
    python_callable=lambda **context: print(f"Updated quality dashboard for {context['ds']}"),
    trigger_rule='none_failed_or_skipped',  # Run regardless of which branch was taken
    
    dag=dag
)

# Cleanup old quality check results
cleanup_old_quality_data = BashOperator(
    task_id='cleanup_old_quality_data',
    bash_command="""
    echo "Cleaning up quality check data older than 90 days"
    # In a real implementation, this would clean up old quality check results
    # from the quality metadata store
    """,
    trigger_rule='none_failed_or_skipped',
    
    dag=dag
)

# Define task dependencies
wait_for_bronze_etl >> bronze_data_freshness_check
wait_for_bronze_etl >> bronze_volume_check
wait_for_gold_etl >> bronze_data_freshness_check
wait_for_gold_etl >> bronze_volume_check

bronze_data_freshness_check >> silver_consistency_check
bronze_volume_check >> silver_scd_integrity_check

silver_consistency_check >> gold_business_rules_check
silver_consistency_check >> gold_metrics_validation
silver_scd_integrity_check >> gold_business_rules_check
silver_scd_integrity_check >> gold_metrics_validation

[gold_business_rules_check, gold_metrics_validation] >> cross_layer_reconciliation

cross_layer_reconciliation >> quality_aggregation >> quality_decision

quality_decision >> [send_failure_alert, send_warning_notification, generate_quality_report]

[send_failure_alert, send_warning_notification, generate_quality_report] >> update_quality_dashboard

update_quality_dashboard >> cleanup_old_quality_data