from airflow.hooks.base import BaseHook

def get_azure_connection_string(conn_id: str) -> str:
    """
    Retrieve the Azure Blob Storage connection string from Airflow connections.
    Args:
        conn_id: The Airflow connection ID
    Returns:
        The Azure connection string
    """
    connection = BaseHook.get_connection(conn_id)
    conn_string = connection.extra_dejson.get("connection_string")
    if not conn_string:
        raise ValueError(f"No connection string found in connection {conn_id}")
    return conn_string
