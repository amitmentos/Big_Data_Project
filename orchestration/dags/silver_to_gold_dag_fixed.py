# orchestration/dags/silver_to_gold_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException, AirflowSensorTimeout
from airflow.utils.state import State

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'silver_to_gold_etl',
    default_args=default_args,
    description='ETL pipeline from Silver to Gold layer for business analytics and ML features',
    schedule_interval=timedelta(hours=1),  # Run every hour after bronze_to_silver
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'silver', 'gold', 'analytics', 'ml']
)

# Common Spark configuration 
spark_config = {
    'spark.master': 'local[2]',  # Use local mode
    'spark.submit.deployMode': 'client',
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    # Removed Kryo serializer due to compatibility issues with Iceberg
    # 'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
    'spark.sql.catalog.spark_catalog.type': 'hadoop',
    'spark.sql.catalog.spark_catalog.warehouse': 's3a://warehouse/',
    'spark.sql.catalog.spark_catalog.s3.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'minio',
    'spark.hadoop.fs.s3a.secret.key': 'minio123',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.connection.establish.timeout': '15000',
    'spark.hadoop.fs.s3a.connection.timeout': '30000',
    'spark.executor.memory': '2g',
    'spark.driver.memory': '2g'
}

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
        except AirflowSensorTimeout:
            self.log.warning("External task sensor timed out - skipping this task")
            # Instead of failing, we skip this task and let the DAG continue
            raise AirflowSkipException("External dependency not met, but continuing anyway")
        except Exception as e:
            self.log.warning(f"External task sensor failed: {str(e)}")
            self.log.warning("Skipping this dependency check and proceeding with the DAG")
            # Instead of failing, we skip this task and let the DAG continue
            raise AirflowSkipException("External dependency not met, but continuing anyway")

# Note: External sensor removed to prevent deadlock issues
# The Gold layer will run independently based on schedule

# Create Fact Sales table
create_fact_sales = SparkSubmitOperator(
    task_id='create_fact_sales',
    application='/opt/processing/spark-apps/gold_aggregations.py',
    conn_id='spark_default',
    application_args=['--target', 'fact_sales', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Create Fact User Activity table
create_fact_user_activity = SparkSubmitOperator(
    task_id='create_fact_user_activity',
    application='/opt/processing/spark-apps/gold_aggregations.py',
    conn_id='spark_default',
    application_args=['--target', 'fact_user_activity', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Generate Sales Performance Metrics
generate_sales_metrics = SparkSubmitOperator(
    task_id='generate_sales_metrics',
    application='/opt/processing/spark-apps/gold_aggregations.py',
    conn_id='spark_default',
    application_args=['--target', 'sales_performance_metrics', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Generate Customer Segmentation
generate_customer_segments = SparkSubmitOperator(
    task_id='generate_customer_segments',
    application='/opt/processing/spark-apps/gold_aggregations.py',
    conn_id='spark_default',
    application_args=['--target', 'customer_segmentation', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Generate Campaign Performance Metrics
generate_campaign_metrics = SparkSubmitOperator(
    task_id='generate_campaign_metrics',
    application='/opt/processing/spark-apps/gold_aggregations.py',
    conn_id='spark_default',
    application_args=['--target', 'campaign_effectiveness', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Generate ML Features
generate_ml_features = SparkSubmitOperator(
    task_id='generate_ml_features',
    application='/opt/processing/spark-apps/gold_aggregations.py',
    conn_id='spark_default',
    application_args=['--target', 'ml_features', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Gold Layer Quality Check
gold_quality_check = SparkSubmitOperator(
    task_id='gold_quality_check',
    application='/opt/processing/spark-apps/data_quality_checks.py',
    conn_id='spark_default',
    application_args=['--layer', 'gold', '--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/processing/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/processing/jars/hadoop-aws-3.3.4.jar,/opt/processing/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Define task dependencies
# Start directly with the core fact tables (bypassing external sensor for now)
# The fact tables can run in parallel as starting points

# Core fact tables should be created first, then feed into downstream aggregations
create_fact_sales >> [generate_sales_metrics, generate_customer_segments, generate_campaign_metrics]
create_fact_user_activity >> [generate_sales_metrics, generate_customer_segments, generate_campaign_metrics]

# ML features depend on all other aggregations
[generate_sales_metrics, generate_customer_segments, generate_campaign_metrics] >> generate_ml_features

# Quality check is the final step
generate_ml_features >> gold_quality_check
