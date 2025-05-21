import trino
import time
import mysql.connector
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from lib.operators.konza_trino_operator import KonzaTrinoOperator
from kubernetes import client, config

def scale_trino_workers(namespace="trino", deployment_name="trino-worker", replicas=3, downscaling_okay=True, delay=120):
    """
    Scale the Kubernetes deployment named 'trino-worker' to a specified number of replicas.
    
    Args:
        namespace (str): Kubernetes namespace where the deployment exists
        deployment_name (str): Name of the deployment to scale
        replicas (int): Desired number of replicas (workers)
        downscaling_okay (bool): If this is set to True, the function will reduce the number of workers 
          if the current number of replicas in the cluster is greater than the desired number of workers.
    """
    try:
        # Load Kubernetes configuration
        config.load_incluster_config()  # For running inside a pod in Kubernetes cluster
        
        # Create API client
        apps_v1_api = client.AppsV1Api()
        
        # Get current deployment
        deployment = apps_v1_api.read_namespaced_deployment(
            name=deployment_name,
            namespace=namespace
        )

        if downscaling_okay or deployment.spec.replicas < replicas:
            deployment.spec.replicas = replicas
            
            # Apply the update
            apps_v1_api.patch_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=deployment
            )
            
            print(f"Successfully scaled {deployment_name} to {replicas} replicas in namespace {namespace}")
            time.sleep(delay)
            return True
        else:
            print(f"Skipping downscaling from {deployment.spec.replicas} to {replicas} since downscaling_okay set to False.")
    except Exception as e:
        print(f"Error scaling deployment: {e}")
        raise



def execute_trino_queries(**kwargs):
    ds = kwargs['ds']
    # Retrieve the connection details
    conn = BaseHook.get_connection('trinokonza')
    host = conn.host
    port = conn.port
    user = conn.login
    schema = conn.schema

    # Connect to Trino
    trino_conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog='hive',
        schema=schema,
    )
    cursor = trino_conn.cursor()

    # Define the SQL queries
    queries = [
        """CREATE TABLE IF NOT EXISTS hive.parquet_master_data.patient_account_parquet_pm_by_accid ( admitted varchar, source varchar, unit_id varchar, related_provider_id varchar, accid varchar, index_update varchar ) WITH (partitioned_by = ARRAY['index_update'], bucketed_by = ARRAY['accid'], bucket_count = 64 )""",
        f"""
        INSERT INTO hive.parquet_master_data.patient_account_parquet_pm_by_accid
        SELECT
        admitted ,
        source , 
        unit_id , 
        related_provider_id,
        accid,
        index_update
        FROM patient_account_parquet_pm
        WHERE concat(index_update,'-01') = '{ds}'
        """,
        f"""
        CREATE TABLE test_dag_based_patient_by_acc_id_sorted_3 AS
        SELECT
    accid,
    MAX_BY(
        index_update,
        CAST(ROW(admitted, source, unit_id, related_provider_id) AS ROW(admitted VARCHAR, source VARCHAR, unit_id VARCHAR, related_provider_id VARCHAR))
    ) AS latest_known
        FROM hive.parquet_master_data.patient_account_parquet_pm_by_accid
        GROUP BY accid
        """
    ]
    
    # Execute each query
    for query in queries:
        cursor.execute(query)
        print(f"Executed query: {query}")

    cursor.close()
    trino_conn.close()

with DAG(
    dag_id='bucket_master_patient_index',
    schedule_interval='@monthly',
    max_active_runs=1,
    tags=['C-35'],
    start_date=datetime(2023, 3, 1),
    catchup=True,
) as dag:

    execute_queries_task = PythonOperator(
        task_id='execute_trino_queries',
        python_callable=execute_trino_queries,
        provide_context=True,
    )
    upscale = scale_trino_workers(replicas=20, downscaling_okay=False)
    downscale = scale_trino_workers(replicas=1, downscaling_okay=True)
    upscale >> execute_queries_task >> downscale
    
