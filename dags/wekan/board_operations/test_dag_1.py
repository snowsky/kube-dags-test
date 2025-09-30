from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a sample Python function for the PythonOperator
def extract_data():
    print("Extracting data from source...")
    return "Sample data extracted"

# Initialize the DAG
with DAG(
    'sample_etl_dag',
    default_args=default_args,
    description='A sample ETL DAG for Airflow 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 9, 29),
    catchup=False,
) as dag:
    
    # Define tasks
    start_task = DummyOperator(
        task_id='start_workflow'
    )
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    
    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='echo "Transforming data..."'
    )
    
    load_task = DummyOperator(
        task_id='load_data'
    )
    
    end_task = DummyOperator(
        task_id='end_workflow'
    )
    
    # Set task dependencies
    start_task >> extract_task >> transform_task >> load_task >> end_task