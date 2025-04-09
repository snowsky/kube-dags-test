def scale_trino_workers(namespace="trino", deployment_name="trino-worker", replicas=3):
    """
    Scale a Kubernetes deployment named 'trino-worker' to a specified number of replicas.
    
    Args:
        namespace (str): Kubernetes namespace where the deployment exists
        deployment_name (str): Name of the deployment to scale
        replicas (int): Desired number of replicas (workers)
    """
    try:
        # Load Kubernetes configuration
        config.load_incluster_config()  # For running inside a pod in Kubernetes cluster
        # Alternatively, use: config.load_kube_config() for local development
        
        # Create API client
        apps_v1_api = client.AppsV1Api()
        
        # Get current deployment
        deployment = apps_v1_api.read_namespaced_deployment(
            name=deployment_name,
            namespace=namespace
        )
        
        # Update replicas
        deployment.spec.replicas = replicas
        
        # Apply the update
        apps_v1_api.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body=deployment
        )
        
        print(f"Successfully scaled {deployment_name} to {replicas} replicas in namespace {namespace}")
        return True
    except Exception as e:
        print(f"Error scaling deployment: {e}")
        raise
