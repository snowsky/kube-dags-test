""" 
This is a DAG to copy the SFTP log database from the SFTP server to the SQL server.
This DAG assumes that the Airflow server allows for file system access on the
instance where it is running.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

import pendulum


def copy_db(origin_db_path, destination_db_path):
    """
    This function copies the database from the origin SFTP server to the destination.
    """

    import shutil as sh

    sh.copy(origin_db_path, destination_db_path)


def select_from_db(db_path, db_table_name):
    """
    This function selects all the data from the copied database and pushes a Xcom with the dataframe resulting from querying the table configured in the variable origin_db_table_name.
    """

    import pandas as pd
    import sqlite3

    sqlite_connection = sqlite3.connect(db_path)

    data_frame = pd.read_sql_query(f"SELECT * from {db_table_name}", sqlite_connection)
    json_data_frame = data_frame.to_json(orient="split")

    sqlite_connection.close()

    return json_data_frame


def insert_into_db(
    json_data_frame, user, password, server_address, db_name, db_table_name
):
    """
    This function inserts all the data from the origin database into the MS SQL database configured with the variables server_address db_name in into the table configured through the variable db_table_name.
    """

    import pandas as pd
    from sqlalchemy import create_engine
    from io import StringIO

    data_frame = pd.read_json(StringIO(json_data_frame), orient="split")

    ms_sql_engine_string = (
        f"mssql+pymssql://{user}:{password}@{server_address}:1433/{db_name}"
    )

    ms_sql_engine = create_engine(ms_sql_engine_string, echo=False)

    with ms_sql_engine.connect() as con:
        data_frame = data_frame.to_sql(  # pylint: disable=no-member
            db_table_name, con, if_exists="replace", index=False
        )

    ms_sql_engine.dispose()


with DAG(
    "S-6__SFTP_Log_Retrieval",
    schedule_interval="0 0 * * *",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    dagrun_timeout=timedelta(minutes=60),
):
    origin_db_path = Variable.get("origin_db_path")
    destination_db_path = Variable.get("destination_db_path")

    copy_db_task = PythonVirtualenvOperator(
        task_id="copy_db",
        python_callable=copy_db,
        requirements=[],
        system_site_packages=False,
        python_version="3.11",
        op_kwargs={
            "origin_db_path": origin_db_path,
            "destination_db_path": destination_db_path,
        },
    )
    # copy_db_task = PythonOperator(task_id="copy_db", python_callable=copy_db)

    db_path = Variable.get("destination_db_path")
    db_table_name = Variable.get("origin_db_table_name")

    select_from_db_task = PythonVirtualenvOperator(
        task_id="select_from_db",
        python_callable=select_from_db,
        requirements=["pandas==2.2.0", "sqlalchemy==2.0.25"],
        system_site_packages=False,
        python_version="3.11",
        op_kwargs={"db_path": db_path, "db_table_name": db_table_name},
    )
    """ select_from_db_task = PythonOperator(
        task_id="select_from_db", python_callable=select_from_db
    ) """

    user = Variable.get("user")
    password = Variable.get("password")
    server_address = Variable.get("server_address")
    db_name = Variable.get("db_name")
    db_table_name = Variable.get("destination_db_table_name")

    insert_into_db_task = PythonVirtualenvOperator(
        task_id="insert_into_db",
        python_callable=insert_into_db,
        requirements=[
            "pandas==2.2.0",
            "sqlalchemy==2.0.25",
            "pymssql==2.2.11 --no-binary=pymssql",
        ],
        system_site_packages=False,
        python_version="3.11",
        op_kwargs={
            "json_data_frame": '{{ ti.xcom_pull(task_ids="select_from_db") }}',
            "user": user,
            "password": password,
            "server_address": server_address,
            "db_name": db_name,
            "db_table_name": db_table_name,
        },
    )
    """
    insert_into_db_task = PythonOperator(
        task_id="insert_into_db", python_callable=insert_into_db
    )
    """

    copy_db_task >> select_from_db_task >> insert_into_db_task
