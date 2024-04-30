
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook


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
        import logging
        logging.info(print('Starting Connection Test'))
        hook = SFTPHook(
            ssh_conn_id=ssh_conn_id,
        )
        conn_success = hook.test_connection()
        if not conn_success:
            logging.info(print('Connection Unsuccessful'))
            raise RuntimeError('Connection Unsuccessful')
        else:
            logging.info(print('Connection Successful'))
            return conn_success

    test_connection_dag(
        ssh_conn_id="{{params.ssh_conn_id}}",
    )
test_sftp_connection_dag = test_connection_dag()

if __name__ == "__main__":
    test_sftp_connection_dag.test()

