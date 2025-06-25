# orchestration/dags/ml_feature_generation_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ml-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml_feature_generation',
    default_args=default_args,
    description='Generate ML features from Gold layer data for recommendation engine',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'features', 'recommendation', 'gold']
)

# Common Spark configuration
spark_config = {
    'spark.master': 'local[2]',  # Use local mode
    'spark.submit.deployMode': 'client',
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
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

# Wait for Gold layer to complete
wait_for_gold_completion = ExternalTaskSensor(
    task_id='wait_for_gold_completion',
    external_dag_id='silver_to_gold_etl',
    external_task_id='gold_quality_check',
    execution_delta=timedelta(hours=0),  # Look for tasks in the same execution time
    timeout=600,  # Reduced timeout to 10 minutes
    poke_interval=60,  # Check every minute
    soft_fail=True,  # Continue DAG even if this fails
    dag=dag
)

# Check data availability before feature generation
def check_data_availability(**context):
    """Check if required data is available for feature generation"""
    from pyspark.sql import SparkSession
    
    process_date = context['ds']
    print(f"Checking data availability for date: {process_date}")
    
    spark = SparkSession.builder \
        .appName("ML Feature Data Check") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .getOrCreate()
    
    try:
        # Check if required tables exist and have data
        required_tables = [
            'gold.fact_sales',
            'gold.fact_user_activity',
            'gold.sales_performance_metrics',
            'gold.customer_segmentation'
        ]
        
        for table in required_tables:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0]['count']
                print(f"Table {table}: {count:,} records")
                
                if count == 0:
                    print(f"Warning: No data found in {table}, but continuing...")
                    
            except Exception as e:
                print(f"Warning: Error checking table {table}: {str(e)}, but continuing...")
        
        print(f"Data availability check completed for {process_date}")
        return True
        
    finally:
        spark.stop()

data_availability_check = PythonOperator(
    task_id='data_availability_check',
    python_callable=check_data_availability,
    dag=dag
)

# Generate customer behavior features
generate_behavior_features = SparkSubmitOperator(
    task_id='generate_behavior_features',
    application='/opt/processing/spark-apps/ml_feature_engineering.py',
    conn_id='spark_default',
    application_args=['--date', '{{ ds }}', '--feature-type', 'behavior'],
    conf=spark_config,
    jars='/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Generate product affinity features
generate_affinity_features = SparkSubmitOperator(
    task_id='generate_affinity_features',
    application='/opt/processing/spark-apps/ml_feature_engineering.py',
    conn_id='spark_default',
    application_args=['--date', '{{ ds }}', '--feature-type', 'affinity'],
    conf=spark_config,
    jars='/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Combine all ML features
combine_ml_features = SparkSubmitOperator(
    task_id='combine_ml_features',
    application='/opt/processing/spark-apps/ml_feature_engineering.py',
    conn_id='spark_default',
    application_args=['--date', '{{ ds }}', '--feature-type', 'combine'],
    conf=spark_config,
    jars='/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Validate ML features
validate_ml_features = SparkSubmitOperator(
    task_id='validate_ml_features',
    application='/opt/processing/spark-apps/ml_feature_validation.py',
    conn_id='spark_default',
    application_args=['--date', '{{ ds }}'],
    conf=spark_config,
    jars='/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Generate model training dataset
generate_training_dataset = SparkSubmitOperator(
    task_id='generate_training_dataset',
    application='/opt/processing/spark-apps/ml_feature_engineering.py',
    conn_id='spark_default',
    application_args=['--date', '{{ ds }}', '--feature-type', 'training'],
    conf=spark_config,
    jars='/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Cleanup old features (weekly)
cleanup_old_features = SparkSubmitOperator(
    task_id='cleanup_old_features',
    application='/opt/processing/spark-apps/ml_feature_cleanup.py',
    conn_id='spark_default',
    application_args=['--date', '{{ ds }}', '--retention-days', '30'],
    conf=spark_config,
    jars='/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar',
    verbose=True,
    dag=dag
)

# Define task dependencies
wait_for_gold_completion >> data_availability_check
data_availability_check >> [generate_behavior_features, generate_affinity_features]
[generate_behavior_features, generate_affinity_features] >> combine_ml_features
combine_ml_features >> validate_ml_features
validate_ml_features >> generate_training_dataset
generate_training_dataset >> cleanup_old_features
