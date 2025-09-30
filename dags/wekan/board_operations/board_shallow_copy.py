from datetime import timedelta, datetime
from airflow.sdk import dag, task


@dag(
    dag_id="board_shallow_copy",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    params={
        "source_hostname": "https://boards.ertanalytics.com",
        "target_hostname": "http://wekan.wekan.svc:8080",
        "source_username": "erta_robot",
        "source_password": "source_password",
        "target_username": "erta_robot",
        "target_password": "target_password",
        "source_board_id": "source_board_id",
        "target_board_id": "target_board_id",
    },
)
def board_shallow_copy():
    """
    This DAG copies a wekan board from one server to another.
    Minimal version for Airflow 3.0 compatibility testing.
    """

    @task
    def login_users(source_hostname: str, source_username: str, source_password: str) -> dict:
        """Login to source server"""
        return {"mock": True, "hostname": source_hostname}

    @task
    def get_populated_board(configuration: dict):
        """Get populated board data"""
        return [{"mock": True, "board_data": "populated"}]

    @task
    def shallow_copy_board(populated_boards: list):
        """Copy the board"""
        return {"mock": True, "status": "success"}

    # Simple DAG flow
    config = login_users(
        source_hostname="{{params.source_hostname}}",
        source_username="{{params.source_username}}",
        source_password="{{params.source_password}}",
    )

    boards = get_populated_board(configuration=config)
    result = shallow_copy_board(populated_boards=boards)


# In Airflow 3.0, the @dag decorator automatically registers the DAG
# But we need explicit registration for some cases
board_shallow_copy_dag = board_shallow_copy()

if __name__ == "__main__":
    dag_instance = board_shallow_copy()
    dag_instance.test()
