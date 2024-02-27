"""
This is a DAG to copy the SFTP log database from the SFTP server to the SQL server.
This DAG assumes that the Airflow server allows for file system access on the
instance where it is running.
"""

from datetime import timedelta, datetime
import typing

from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    params={
        "destination_db_path": "/path/to/destination/db",
        "origin_db_table_name": "origin_db_table_name",
        "destination_db_table_name": "destination_db_table_name",
    },
)
def sftp_log_retrieval():
    """
    DAG to copy the SFTP log database from the SFTP server to the SQL server.
    """

    import pandas as pd

    @task
    def copy_db(origin_db_path: str, destination_db_path: str):
        """
        This function copies the database from the origin SFTP server to the destination
        """

        import shutil as sh

        sh.copy(origin_db_path, destination_db_path)

    @task
    def get_table_records(db_table_name: str):
        """
        This function selects all the data from the copied database and pushes a Xcom
        with the dataframe resulting from querying the table configured in the variable
        origin_db_table_name.
        """

        hook = SqliteHook(sqlite_conn_id="sftp_log_retrieval_sql")
        table_records = hook.get_pandas_df(f"SELECT * FROM {db_table_name};")

        return table_records

    @task
    def insert_into_db(table_records: pd.DataFrame, db_table_name: str):
        """
        This function inserts all the data from the origin database into the MS SQL
        database configured with the variables server_address db_name in into the
        table configured through the variable db_table_name.
        """

        hook = MsSqlHook(mssql_conn_id="sftp_log_retrieval_mssql")
        engine = hook.get_sqlalchemy_engine()

        table_records.to_sql(name=db_table_name, con=engine, if_exists="replace")

    origin_connection = BaseHook.get_connection("sftp_log_retrieval_sql")
    origin_db_path = typing.cast(str, origin_connection.host)

    copy_db(
        origin_db_path=origin_db_path,
        destination_db_path="{{params.destination_db_path}}",
    )

    table_records_df = typing.cast(
        pd.DataFrame, get_table_records(db_table_name="{{params.origin_db_table_name}}")
    )

    insert_into_db(
        table_records=table_records_df,
        db_table_name="{{params.destination_db_table_name}}",
    )


sftp_log_retrieval_dag = sftp_log_retrieval()

if __name__ == "__main__":
    sftp_log_retrieval_dag.test()
