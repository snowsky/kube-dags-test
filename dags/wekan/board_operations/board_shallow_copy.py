"""
Minimal board_shallow_copy DAG for Airflow 3.0 compatibility.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task


@dag(
    dag_id="board_shallow_copy",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    params={
        "test_param": "test_value",
    },
)
def board_shallow_copy():
    """
    Minimal board_shallow_copy DAG for Airflow 3.0 compatibility.
    """

    @task
    def login_users():
        """Mock login task."""
        print("Mock login successful")
        return {"status": "success"}

    @task
    def get_populated_board():
        """Mock board retrieval task."""
        print("Mock board retrieved")
        return {"board_id": "mock_board", "status": "success"}

    @task
    def shallow_copy_board():
        """Mock board copy task."""
        print("Mock board copy completed")
        return {"copy_status": "success"}

    # Simple task execution
    login_users()
    get_populated_board()
    shallow_copy_board()


if __name__ == "__main__":
    # For testing, create a temporary instance
    dag_instance = board_shallow_copy()
    dag_instance.test()
