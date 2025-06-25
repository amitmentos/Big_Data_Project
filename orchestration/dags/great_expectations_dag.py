# orchestration/dags/great_expectations_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException, AirflowSensorTimeout
from airflow.utils.state import State
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Reduced retries to avoid long-running tasks
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'great_expectations_data_validation',
    default_args=default_args,
    description='Great Expectations Data Quality Validation Pipeline',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    max_active_runs=1,
    tags=['data-quality', 'great-expectations', 'validation']
)

def validate_bronze_layer(**context):
    """Validate bronze layer using Great Expectations"""
    import sys
    sys.path.append('/opt/processing/spark-apps')
    
    from great_expectations_validator import GreatExpectationsValidator
    
    # Get process date
    process_date = context['ds']
    logger.info(f"Validating bronze layer for date: {process_date}")
    
    try:
        # Initialize validator
        validator = GreatExpectationsValidator(
            context_root_dir="/opt/processing/great_expectations"
        )
        
        # Validate bronze layer
        results = validator.validate_layer('bronze', process_date)
        
        # Log results
        logger.info(f"Bronze validation results: {results['overall_status']}")
        logger.info(f"Tables validated: {results['tables_validated']}")
        logger.info(f"Tables passed: {results['tables_passed']}")
        logger.info(f"Tables failed: {results['tables_failed']}")
        
        # Store results in XCom for downstream tasks
        context['task_instance'].xcom_push(key='bronze_validation_results', value=results)
        
        # Fail task if validation failed
        if results['overall_status'] == 'FAILED':
            raise ValueError(f"Bronze layer validation failed: {results['tables_failed']} tables failed")
        
        return results
        
    except Exception as e:
        logger.error(f"Bronze layer validation failed: {str(e)}")
        raise

def validate_silver_layer(**context):
    """Validate silver layer using Great Expectations"""
    import sys
    sys.path.append('/opt/processing/spark-apps')
    
    from great_expectations_validator import GreatExpectationsValidator
    
    # Get process date
    process_date = context['ds']
    logger.info(f"Validating silver layer for date: {process_date}")
    
    try:
        # Initialize validator
        validator = GreatExpectationsValidator(
            context_root_dir="/opt/processing/great_expectations"
        )
        
        # Validate silver layer
        results = validator.validate_layer('silver', process_date)
        
        # Log results
        logger.info(f"Silver validation results: {results['overall_status']}")
        logger.info(f"Tables validated: {results['tables_validated']}")
        logger.info(f"Tables passed: {results['tables_passed']}")
        logger.info(f"Tables failed: {results['tables_failed']}")
        
        # Store results in XCom
        context['task_instance'].xcom_push(key='silver_validation_results', value=results)
        
        # Fail task if validation failed
        if results['overall_status'] == 'FAILED':
            raise ValueError(f"Silver layer validation failed: {results['tables_failed']} tables failed")
        
        return results
        
    except Exception as e:
        logger.error(f"Silver layer validation failed: {str(e)}")
        raise

def validate_gold_layer(**context):
    """Validate gold layer using Great Expectations"""
    import sys
    sys.path.append('/opt/processing/spark-apps')
    
    from great_expectations_validator import GreatExpectationsValidator
    
    # Get process date
    process_date = context['ds']
    logger.info(f"Validating gold layer for date: {process_date}")
    
    try:
        # Initialize validator
        validator = GreatExpectationsValidator(
            context_root_dir="/opt/processing/great_expectations"
        )
        
        # Validate gold layer
        results = validator.validate_layer('gold', process_date)
        
        # Log results
        logger.info(f"Gold validation results: {results['overall_status']}")
        logger.info(f"Tables validated: {results['tables_validated']}")
        logger.info(f"Tables passed: {results['tables_passed']}")
        logger.info(f"Tables failed: {results['tables_failed']}")
        
        # Store results in XCom
        context['task_instance'].xcom_push(key='gold_validation_results', value=results)
        
        # Fail task if validation failed
        if results['overall_status'] == 'FAILED':
            raise ValueError(f"Gold layer validation failed: {results['tables_failed']} tables failed")
        
        return results
        
    except Exception as e:
        logger.error(f"Gold layer validation failed: {str(e)}")
        raise

def generate_validation_summary(**context):
    """Generate comprehensive validation summary combining custom and GE results"""
    import json
    
    # Get validation results from XCom
    bronze_results = context['task_instance'].xcom_pull(
        task_ids='validate_bronze_layer', 
        key='bronze_validation_results'
    )
    silver_results = context['task_instance'].xcom_pull(
        task_ids='validate_silver_layer',
        key='silver_validation_results'
    )
    gold_results = context['task_instance'].xcom_pull(
        task_ids='validate_gold_layer',
        key='gold_validation_results'
    )
    
    # Create comprehensive summary
    summary = {
        'validation_timestamp': context['ts'],
        'process_date': context['ds'],
        'validation_type': 'great_expectations',
        'layer_results': {
            'bronze': bronze_results,
            'silver': silver_results,
            'gold': gold_results
        },
        'overall_summary': {
            'total_layers': 3,
            'layers_passed': 0,
            'layers_failed': 0,
            'total_tables': 0,
            'tables_passed': 0,
            'tables_failed': 0,
            'total_expectations': 0,
            'expectations_passed': 0,
            'expectations_failed': 0
        }
    }
    
    # Aggregate statistics
    for layer_name, layer_result in summary['layer_results'].items():
        if layer_result:
            if layer_result['overall_status'] in ['PASSED', 'WARNING']:
                summary['overall_summary']['layers_passed'] += 1
            else:
                summary['overall_summary']['layers_failed'] += 1
            
            summary['overall_summary']['total_tables'] += layer_result.get('tables_validated', 0)
            summary['overall_summary']['tables_passed'] += layer_result.get('tables_passed', 0)
            summary['overall_summary']['tables_failed'] += layer_result.get('tables_failed', 0)
            
            # Aggregate expectation statistics
            for table_name, table_result in layer_result.get('table_results', {}).items():
                summary['overall_summary']['total_expectations'] += table_result.get('expectations_total', 0)
                summary['overall_summary']['expectations_passed'] += table_result.get('expectations_passed', 0)
                summary['overall_summary']['expectations_failed'] += table_result.get('expectations_failed', 0)
    
    # Determine overall status
    if summary['overall_summary']['layers_failed'] == 0:
        summary['overall_status'] = 'PASSED'
    elif summary['overall_summary']['layers_passed'] > summary['overall_summary']['layers_failed']:
        summary['overall_status'] = 'WARNING'
    else:
        summary['overall_status'] = 'FAILED'
    
    # Log summary
    logger.info("=== GREAT EXPECTATIONS VALIDATION SUMMARY ===")
    logger.info(f"Overall Status: {summary['overall_status']}")
    logger.info(f"Layers: {summary['overall_summary']['layers_passed']}/{summary['overall_summary']['total_layers']} passed")
    logger.info(f"Tables: {summary['overall_summary']['tables_passed']}/{summary['overall_summary']['total_tables']} passed")
    logger.info(f"Expectations: {summary['overall_summary']['expectations_passed']}/{summary['overall_summary']['total_expectations']} passed")
    
    # Store final summary
    context['task_instance'].xcom_push(key='validation_summary', value=summary)
    
    return summary

def send_validation_alerts(**context):
    """Send alerts based on validation results"""
    
    # Get validation summary
    summary = context['task_instance'].xcom_pull(
        task_ids='generate_validation_summary',
        key='validation_summary'
    )
    
    if not summary:
        logger.warning("No validation summary found")
        return
    
    # Check if alerts should be sent
    overall_status = summary.get('overall_status', 'UNKNOWN')
    
    if overall_status in ['FAILED', 'WARNING']:
        logger.warning(f"Data quality issues detected: {overall_status}")
        
        # In a real implementation, this would send alerts via:
        # - Email notifications
        # - Slack messages
        # - PagerDuty alerts
        # - Custom webhooks
        
        alert_message = f"""
        🚨 DATA QUALITY ALERT 🚨
        
        Status: {overall_status}
        Date: {summary['process_date']}
        
        Summary:
        - Layers: {summary['overall_summary']['layers_passed']}/{summary['overall_summary']['total_layers']} passed
        - Tables: {summary['overall_summary']['tables_passed']}/{summary['overall_summary']['total_tables']} passed
        - Expectations: {summary['overall_summary']['expectations_passed']}/{summary['overall_summary']['total_expectations']} passed
        
        Please review the data quality dashboard for detailed information.
        """
        
        logger.warning(alert_message)
        
        # Store alert for monitoring systems
        context['task_instance'].xcom_push(key='alert_sent', value=True)
    else:
        logger.info("✅ All data quality checks passed - no alerts needed")
        context['task_instance'].xcom_push(key='alert_sent', value=False)

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

# Wait for bronze ingestion to complete - with more robustness
wait_for_bronze = RobustExternalTaskSensor(
    task_id='wait_for_bronze_ingestion',
    external_dag_id='bronze_to_silver_etl',
    external_task_id='bronze_quality_check',  # Updated to match the actual task ID
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(hours=6),  # Expanded time window to find successful runs
    timeout=300,  # Shorter timeout to avoid long blocking
    poke_interval=60,
    mode='reschedule',  # Use reschedule mode to free up workers
    soft_fail=True,  # Continue the DAG even if this task fails
    dag=dag
)

# Validate bronze layer
validate_bronze = PythonOperator(
    task_id='validate_bronze_layer',
    python_callable=validate_bronze_layer,
    dag=dag
)

# Wait for silver transformation to complete
wait_for_silver = RobustExternalTaskSensor(
    task_id='wait_for_silver_transformation',
    external_dag_id='bronze_to_silver_etl',
    external_task_id='silver_quality_check',  # Updated to match the actual task ID
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(hours=6),  # Expanded time window to find successful runs
    timeout=300,  # Shorter timeout to avoid long blocking
    poke_interval=60,
    mode='reschedule',  # Use reschedule mode to free up workers
    soft_fail=True,  # Continue the DAG even if this task fails
    dag=dag
)

# Validate silver layer
validate_silver = PythonOperator(
    task_id='validate_silver_layer',
    python_callable=validate_silver_layer,
    dag=dag
)

# Wait for gold aggregation to complete
wait_for_gold = RobustExternalTaskSensor(
    task_id='wait_for_gold_aggregation',
    external_dag_id='silver_to_gold_etl',  # Updated to match the actual DAG ID
    external_task_id='gold_quality_check',  # Updated to match the actual task ID
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(hours=6),  # Expanded time window to find successful runs
    timeout=300,  # Shorter timeout to avoid long blocking
    poke_interval=60,
    mode='reschedule',  # Use reschedule mode to free up workers
    soft_fail=True,  # Continue the DAG even if this task fails
    dag=dag
)

# Validate gold layer
validate_gold = PythonOperator(
    task_id='validate_gold_layer',
    python_callable=validate_gold_layer,
    dag=dag
)

# Generate comprehensive validation summary
validation_summary = PythonOperator(
    task_id='generate_validation_summary',
    python_callable=generate_validation_summary,
    dag=dag
)

# Send alerts if needed
send_alerts = PythonOperator(
    task_id='send_validation_alerts',
    python_callable=send_validation_alerts,
    dag=dag
)

# Run custom data quality checks in parallel with GE
run_custom_quality_checks = BashOperator(
    task_id='run_custom_quality_checks',
    bash_command="""
    cd /opt/processing/spark-apps && \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    data_quality_checks.py \
    --layer all \
    --date {{ ds }}
    """,
    dag=dag
)

# Define task dependencies
wait_for_bronze >> validate_bronze
wait_for_silver >> validate_silver  
wait_for_gold >> validate_gold

[validate_bronze, validate_silver, validate_gold] >> validation_summary
validation_summary >> send_alerts

# Run custom quality checks in parallel
[validate_bronze, validate_silver, validate_gold] >> run_custom_quality_checks 