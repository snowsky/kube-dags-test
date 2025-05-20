import time
import trino
import logging
import mysql.connector
from datetime import datetime, timedelta, timezone
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from lib.operators.konza_trino_operator import KonzaTrinoOperator
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from datetime import datetime
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
