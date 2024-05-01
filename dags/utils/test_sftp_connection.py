
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
import logging


@dag(
    schedule="0 * * * *",
    start_date=datetime(2024, 2, 26),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    params={
        "ssh_conn_id": "prd-az1-sftp1airflow",
    },
)
def test_connection_dag():
    @task
    def test_connection_task(ssh_conn_id: str):
        logging.info('Starting Connection Test')
        hook = SFTPHook(
            ssh_conn_id=ssh_conn_id,
        )
        try:
            conn_success = hook.test_connection()
        except Exception as e:
            logging.info(f'Connection Unsuccessful: {e}')
        else:
            logging.info('Connection Successful')
            return conn_success

    test_connection_task(
        ssh_conn_id="{{params.ssh_conn_id}}",
    )

test_sftp_connection_dag = test_connection_dag()

if __name__ == "__main__":
    test_sftp_connection_dag.test()

