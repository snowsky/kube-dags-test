"""
Minimal test DAG for Airflow 3.0 compatibility.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task


@dag(
    dag_id="test_minimal_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    params={
        "test_param": "test_value",
    },
)
def test_minimal_dag():
    """
    Minimal test DAG for Airflow 3.0 compatibility.
    """

    @task
    def test_task():
        """A simple test task."""
        print("Hello from Airflow 3.0!")
        return "success"

    test_task()


if __name__ == "__main__":
    # For testing, create a temporary instance
    dag_instance = test_minimal_dag()
    dag_instance.test()
