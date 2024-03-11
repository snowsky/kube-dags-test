"""
This DAG copys a log file from ssh server to a storage account.
#These settings need to be set in Airflow application under Admin > Connections
Connection ID will need to match: prd-az1-log2-airflowconnection
Conection Type: SSH
Host: PRD-AZ1-LOG2 private IP address
Username and password: provided by admin of the machine
Port: 22
"""

import calendar
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook


@dag(
    schedule="0 * * * *",
    start_date=datetime(2024, 2, 26),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    params={
        "ssh_conn_id": "prd-az1-log2-airflowconnection",
        "origin_paths": "/var/log/syslog|/var/log/falcon-sensor.log",
        "destination_paths": "/source/|/source/",
    },
)
def ssh_logs_to_storage_account():
    """
    DAG to copy log files from an SSH server to a storage account.
    """

    @task
    def copy_logs(origin_paths: str, destination_paths: str, ssh_conn_id: str):
        """
        This function copies the logs from the SSH server to the storage account.
        """

        origin_paths_list = origin_paths.split("|")
        destination_paths_list = destination_paths.split("|")

        for origin_path, destination_path in zip(
            origin_paths_list, destination_paths_list
        ):
            origin_file_name = origin_path.split("/")[-1]

            utc_unix_timestamp = calendar.timegm(datetime.utcnow().utctimetuple())
            destination_file_path = (
                f"{destination_path}/{utc_unix_timestamp}-{origin_file_name}"
            )

            hook = SFTPHook(
                ssh_conn_id=ssh_conn_id,
            )

            hook.retrieve_file(
                remote_full_path=origin_path,
                local_full_path=destination_file_path,
            )

    copy_logs(
        origin_paths="{{params.origin_paths}}",
        destination_paths="{{params.destination_paths}}",
        ssh_conn_id="{{params.ssh_conn_id}}",
    )


ssh_logs_to_storage_account_dag = ssh_logs_to_storage_account()

if __name__ == "__main__":
    ssh_logs_to_storage_account_dag.test()
