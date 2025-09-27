from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import socket
import importlib

def test_python_execution():
    return "Python execution works!"

def test_connection():
    try:
        socket.create_connection(("10.0.204.6", 8080), timeout=5)
        return "Airflow API connectivity OK!"
    except Exception as e:
        raise RuntimeError(f"Airflow API Connectivity FAILED!: {e}")

def test_logging(**context):
    msg = f"Logging check at {datetime.utcnow()}"
    print(msg)
    return msg

def check_drivers():
    drivers = [
        "psycopg2",   # PostgreSQL
        "mysql.connector",  # MySQL
        "apache-airflow-providers-microsoft-mssql",     # Mssql
        "pymssql",    # SQL Server
        "hl7",  # HL7
        "hlyapy", # HLyapy
    ]

    results = {}
    for driver in drivers:
        try:
            importlib.import_module(driver)
            results[driver] = "Installed!"
        except ImportError:
            results[driver] = "Not installed!"
        except Exception as e:
            results[driver] = f"Error: {e}"

    return results

with DAG(
    dag_id="baseline_tests_with_drivers",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["baseline", "test"],
) as dag:

    python_test = PythonOperator(
        task_id="test_python_execution",
        python_callable=test_python_execution,
    )

    connectivity_test = PythonOperator(
        task_id="test_connection",
        python_callable=test_connection,
    )

    logging_test = PythonOperator(
        task_id="test_logging",
        python_callable=test_logging,
        provide_context=True,
    )

    driver_check = PythonOperator(
        task_id="check_drivers",
        python_callable=check_drivers,
    )

    python_test >> connectivity_test >> logging_test >> driver_check
