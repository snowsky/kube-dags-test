from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from kubernetes import client, config
import time

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

class ScaleTrinoWorkersOperator(PythonOperator):
    """
    Custom operator that scales trino workers.
    
    Args:
        task_id (str): the task id.
        namespace (str): Kubernetes namespace where the deployment exists
        deployment_name (str): Name of the deployment to scale
        replicas (int): Desired number of replicas (workers)
        downscaling_okay (bool): If this is set to True, the function will reduce the number of workers 
          if the current number of replicas in the cluster is greater than the desired number of workers.
        **kwargs: Additional arguments passed to PythonOperator
    """
    
    def __init__(
        self,
        task_id: str,
        namespace="trino",
        deployment_name="trino-worker",
        replicas=3,
        downscaling_okay=True,
        delay=120,
        **kwargs
    ):
        # Define the python_callable that will be executed
        def _execute_function():
            return scale_trino_workers(
                namespace=namespace, 
                deployment_name=deployment_name,
                replicas=replicas,
                downscaling_okay=downscaling_okay,
                delay=delay
            )
        
        # Initialize the parent PythonOperator with our wrapper function
        super().__init__(
            task_id=task_id,
            python_callable=_execute_function,
            **kwargs
        )
        self.namespace = namespace
        self.deployment_name = deployment_name
        self.replicas = replicas
        self.downscaling_okay = downscaling_okay
        self.delay = delay
