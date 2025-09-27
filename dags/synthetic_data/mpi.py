import sys
sys.path.insert(0, '/opt/airflow/dags/repo/dags')

from airflow import DAG
import pendulum
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task

from populations.target_population_impl import output_df_to_target_tbl
MYSQL_CONN_ID = "MariaDB"

default_args = {
    'owner': 'airflow',
}
with DAG(
    'mpi',
    default_args=default_args,
    start_date=pendulum.now().subtract(days=2),
    tags=['synthetic', 'mpi'],
) as dag:
    
    create_test_database = SQLExecuteQueryOperator(
        task_id="create_test_database",
        sql="""
        CREATE DATABASE IF NOT EXISTS hl7_processing_center;
        """,
        conn_id=MYSQL_CONN_ID,
    )

    create_mpi_table = SQLExecuteQueryOperator(
        task_id="create_mpi_table",
        sql="""
        CREATE TABLE hl7_processing_center._mpi_id_master (
            id bigint(20) NOT NULL AUTO_INCREMENT,
            firstname varchar(100) DEFAULT NULL,
            lastname varchar(100) DEFAULT NULL,
            dob varchar(100) DEFAULT NULL,
            sex varchar(100) DEFAULT NULL,
            ssn varchar(100) DEFAULT NULL,
            longitudinal_anomoly varchar(100) DEFAULT NULL,
            first_name_anomoly varchar(100) DEFAULT NULL,
            mrn_anomoly varchar(100) DEFAULT NULL,
            event_timestamp timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY dob_master (dob),
            KEY sex_master (sex),
            KEY mpi_combo (firstname,lastname,dob,sex)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """,
        conn_id=MYSQL_CONN_ID,
    )
    create_test_database >> create_mpi_table
