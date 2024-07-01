"""
# Opt Out List processing DAG

This DAG is used to determine the Master Patient Index (MPI) IDs to patients listed in a set of opt-out lists.

The opt-out lists are assumed to be stored in a POSIX-compatible local path, parameterized by the 
`OPTOUT_FILES_DIRECTORY` constant. The DAG produces an opt-out table comprising all matches of opt-out records
against the MPI table, using one of two methods:
- "standard", or a full match by firstname, lastname, dob and sex
- "special1", or a match using:
  - the last 4 digits of the SSN,
  - the first 3 characters of the lastname,
  - date of birth
  - gender

The DAG can be tested against synthetic MPI data (only using docker-compose!) by running the `populate_test_data` DAG first.
"""
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

from populations.common import CONNECTION_NAME, EXAMPLE_DATA_PATH
from populations.target_population_impl import _fix_engine_if_invalid_params
import logging
import os

from test.constants import (
  SYNTHETIC_MPI_TABLE_SCHEMA,
  SYNTHETIC_MPI_TABLE_NAME,
  SYNTHETIC_MPI_CONNECTION_ID,
  TEST_OPTOUT_FILES_DIRECTORY,
)

# Change these to process real data

OPT_OUT_SCHEMA_NAME = 'test_clientresults'
OPT_OUT_LOAD_TABLE_NAME = 'test_opt_out_list_airflow_load'
OPT_OUT_SANITIZED_TABLE_NAME = 'test_opt_out_list_airflow_sanitized'
OPT_OUT_WITH_MPI_STD_TABLE_NAME = 'test_opt_out_all_related_mpi_by_std'
OPT_OUT_WITH_MPI_SPECIAL1_TABLE_NAME = 'test_opt_out_all_related_mpi_by_special1'
OPT_OUT_LIST_RAW_TABLE_NAME = 'test_opt_out_list_raw'
OPT_OUT_LIST_TABLE_NAME = 'test_opt_out_list'

INDEXED_MPI_TABLE_NAME = 'test_opt_out_mpi_indexed'

MPI_SCHEMA_NAME = SYNTHETIC_MPI_TABLE_SCHEMA
MPI_TABLE_NAME = SYNTHETIC_MPI_TABLE_NAME
OPTOUT_FILES_DIRECTORY = TEST_OPTOUT_FILES_DIRECTORY

default_args = {
    'owner': 'airflow',
    'mysql_conn_id': SYNTHETIC_MPI_CONNECTION_ID, 
}
with DAG(
    'process_opt_out_list',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions'],
) as dag:

    dag.doc_md = __doc__

    @task
    def load_csv_files_to_mysql(
        source_dir: str,
        target_schema: str,
        target_table: str,
        mysql_conn_id: str,
    ):

        from pathlib import Path
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd
        import logging
        from populations.common import CSV_HEADER

        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())

        for file_path in Path(source_dir).glob("*"):
            if file_path.is_file() and file_path.name not in {'OptOutList.csv', 'processed'}:
               logging.info(f"Processing {file_path}")
               dt = pd.read_csv(file_path) 
               dt.rename(columns=CSV_HEADER, inplace=True)
               dt.to_sql(
                  name=target_table,
                  con=engine, 
                  schema=target_schema,
                  # the table already has a PRIMARY KEY set, so an index is not necessary
                  index=False,
                  # we will append records if the table already exists.
                  # Note: if multiple records with the same (fname, lname, dob, sex, ssn) combination are introduced
                  # the query will fail. This is expected. See PRIMARY KEY on opt-out table to relax this restriction.
                  if_exists='append', 
               )



    create_opt_out_schema = MySqlOperator(
        task_id="create_opt_out_schema",
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {OPT_OUT_SCHEMA_NAME};
        """
    )
    drop_opt_out_load_table_if_exists = MySqlOperator(
        task_id="drop_opt_out_load_table_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LOAD_TABLE_NAME}
        """
    )
    create_opt_out_load_table = MySqlOperator(
        task_id="create_opt_out_load_table",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LOAD_TABLE_NAME} (
            id VARCHAR(255),
            fname VARCHAR(255),
            lname VARCHAR(255),
            dob VARCHAR(16),
            sex VARCHAR(16),
            ssn VARCHAR(16),
            respective_vault VARCHAR(255),
            respective_mrn VARCHAR(255),
            opt_choice VARCHAR(255),
            status VARCHAR(255),
            username VARCHAR(255),
            user VARCHAR(255),
            submitted VARCHAR(255),
            completed VARCHAR(255),
            MPI VARCHAR(2555),
            last_update_php VARCHAR(255),
            PRIMARY KEY (fname, lname, dob, sex, ssn)
        );
        """,
    )    
    populate_opt_out_load_table = load_csv_files_to_mysql(
        source_dir=OPTOUT_FILES_DIRECTORY,
        target_schema=OPT_OUT_SCHEMA_NAME,
        target_table=OPT_OUT_LOAD_TABLE_NAME,
    )
    drop_sanitized_table_if_exists = MySqlOperator(
        task_id="drop_sanitized_table_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_SANITIZED_TABLE_NAME}
        """
    )
    create_sanitized_opt_out_table = MySqlOperator(
        task_id="create_sanitized_opt_out_table",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_SANITIZED_TABLE_NAME}
        (
            INDEX (last_4_ssn, first_3_lastname),
            INDEX (first_16_lastname, first_16_firstname)
        )
        AS SELECT
            id, MPI, last_update_php,
            fname, lname, sanitized_dob,
            LEFT(lname, 3) AS first_3_lastname,
            RIGHT(sanitized_ssn, 4) AS last_4_ssn,
            LEFT(lname, 16) AS first_16_lastname,
            LEFT(fname, 16) AS first_16_firstname,
            sex, sanitized_ssn,
            respective_vault, respective_mrn, opt_choice,
            `status`, username, user, submitted, completed,
            raw_dob, raw_ssn
        FROM (
            SELECT 
                id,
                MPI,
                last_update_php,
                fname, 
                lname,
                IF (
                    dob <> 'Unknown' AND dob NOT LIKE '%-%',
                    DATE_FORMAT(STR_TO_DATE(dob, '%m/%d/%Y'),'%Y-%m-%d'),
                    dob
                ) AS sanitized_dob,
                sex,
                LPAD(REPLACE(ssn, '-', ''), 9, '0') AS sanitized_ssn,
                respective_vault,
                respective_mrn,
                opt_choice,
                `status`,
                username,
                user,
                submitted,
                completed,
                -- added for debugging purposes
                dob AS raw_dob,
                ssn AS raw_ssn
            FROM
                {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LOAD_TABLE_NAME}
            WHERE opt_choice <> 'in'
            AND dob IS NOT NULL
            AND lname IS NOT NULL
        ) t
        """,
    )
    drop_indexed_mpi_table_if_exists = MySqlOperator(
        task_id="drop_indexed_mpi_table_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{INDEXED_MPI_TABLE_NAME}
        """
    )
    create_indexed_mpi_table = MySqlOperator(
        task_id="create_indexed_mpi_table",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{INDEXED_MPI_TABLE_NAME}
        (
            INDEX (last_4_ssn, first_3_lastname),
            INDEX (first_16_lastname, first_16_firstname)
        )
        AS SELECT
           id, firstname, lastname, dob, sex, 
           LEFT(lastname, 3) AS first_3_lastname,
           LEFT(lastname, 16) AS first_16_lastname,
           LEFT(firstname, 16) AS first_16_firstname,
           RIGHT(ssn, 4) AS last_4_ssn,
           longitudinal_anomoly, first_name_anomoly, mrn_anomoly
           FROM {MPI_SCHEMA_NAME}.{MPI_TABLE_NAME}
        """,
    )
    drop_with_mpi_std_table_if_exists = MySqlOperator(
        task_id="drop_with_mpi_std_table_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_WITH_MPI_STD_TABLE_NAME}
        """
    )
    create_opt_out_table_with_mpi_std = MySqlOperator(
        task_id="create_opt_out_table_with_mpi_std",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_WITH_MPI_STD_TABLE_NAME}
        (
            PRIMARY KEY (mpid_id),
            INDEX (oolu_fname, oolu_lname, oolu_dob, oolu_sex)
        )
        AS SELECT
            oolu.id AS oolu_id,
            oolu.MPI AS oolu_mpi,
            oolu.fname AS oolu_fname,
            oolu.lname AS oolu_lname,
            oolu.sanitized_dob AS oolu_dob,
            oolu.sex AS oolu_sex,
            oolu.sanitized_ssn AS oolu_ssn,
            oolu.respective_vault AS oolu_respective_vault,
            oolu.respective_mrn AS oolu_respective_mrn,
            oolu.opt_choice AS oolu_opt_choice,
            oolu.status AS oolu_status,
            oolu.username AS oolu_username,
            oolu.user AS oolu_user,
            oolu.submitted AS oolu_submitted,
            oolu.completed AS oolu_completed,
            oolu.last_update_php AS oolu_last_update_php,
            mpid.id AS mpid_id,
            mpid.firstname AS mpid_firstname,
            mpid.lastname AS mpid_lastname,
            mpid.dob AS mpid_dob,
            mpid.sex AS mpid_sex,
            mpid.longitudinal_anomoly AS mpid_longitudinal_anomoly,
            mpid.first_name_anomoly AS mpid_first_name_anomoly,
            mpid.mrn_anomoly AS mpid_mrn_anomoly
        FROM {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_SANITIZED_TABLE_NAME} oolu
        LEFT JOIN {OPT_OUT_SCHEMA_NAME}.{INDEXED_MPI_TABLE_NAME} mpid ON (
            oolu.first_16_lastname = mpid.first_16_lastname AND
            oolu.first_16_firstname = mpid.first_16_firstname AND
            oolu.lname = mpid.lastname AND
            oolu.fname = mpid.firstname AND
            oolu.sanitized_dob = mpid.dob AND
            oolu.sex = mpid.sex
        )
        """,
    )
    
    drop_with_mpi_special1_table_if_exists = MySqlOperator(
        task_id="drop_with_mpi_special1_table_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_WITH_MPI_SPECIAL1_TABLE_NAME}
        """
    )
    create_opt_out_table_with_mpi_special1 = MySqlOperator(
        task_id="create_opt_out_table_with_mpi_special1",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_WITH_MPI_SPECIAL1_TABLE_NAME}
        (
            PRIMARY KEY (mpid_id),
            INDEX (oolu_fname, oolu_lname, oolu_dob, oolu_sex)
        )
        AS SELECT
            oolu.id AS oolu_id,
            oolu.MPI AS oolu_mpi,
            oolu.fname AS oolu_fname,
            oolu.lname AS oolu_lname,
            oolu.sanitized_dob AS oolu_dob,
            oolu.sex AS oolu_sex,
            oolu.sanitized_ssn AS oolu_ssn,
            oolu.respective_vault AS oolu_respective_vault,
            oolu.respective_mrn AS oolu_respective_mrn,
            oolu.opt_choice AS oolu_opt_choice,
            oolu.status AS oolu_status,
            oolu.username AS oolu_username,
            oolu.user AS oolu_user,
            oolu.submitted AS oolu_submitted,
            oolu.completed AS oolu_completed,
            oolu.last_update_php AS oolu_last_update_php,
            mpid.id AS mpid_id,
            mpid.firstname AS mpid_firstname,
            mpid.lastname AS mpid_lastname,
            mpid.dob AS mpid_dob,
            mpid.sex AS mpid_sex,
            mpid.longitudinal_anomoly AS mpid_longitudinal_anomoly,
            mpid.first_name_anomoly AS mpid_first_name_anomoly,
            mpid.mrn_anomoly AS mpid_mrn_anomoly
        FROM {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_SANITIZED_TABLE_NAME} oolu
        LEFT JOIN {OPT_OUT_SCHEMA_NAME}.{INDEXED_MPI_TABLE_NAME} mpid ON (
            oolu.last_4_ssn = mpid.last_4_ssn AND
            oolu.first_3_lastname = mpid.first_3_lastname AND
            oolu.sanitized_dob = mpid.dob AND
            oolu.sex = mpid.sex AND
            oolu.fname = mpid.firstname
        )
        """,
    )
    
    drop_opt_out_list_raw_if_exists = MySqlOperator(
        task_id="drop_opt_out_list_raw_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LIST_RAW_TABLE_NAME}
        """
    )
    create_opt_out_list_raw = MySqlOperator(
        task_id="create_opt_out_list_raw",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LIST_RAW_TABLE_NAME}
        (
            INDEX (mpi)
        ) AS
        
        SELECT 
            oolu_id AS id, 
            mpid_id AS mpi,
            oolu_fname AS fname, 
            oolu_lname AS lname, 
            oolu_dob AS dob, 
            oolu_sex AS sex, 
            oolu_ssn AS ssn,
            oolu_respective_vault AS respective_vault, 
            oolu_respective_mrn AS respective_mrn,
            oolu_opt_choice AS opt_choice, 
            oolu_status AS status, 
            oolu_username AS username, 
            oolu_user AS user,
            oolu_submitted AS submitted, 
            oolu_completed AS completed, 
            oolu_last_update_php AS last_update_php,
            'standard' AS match_method
        FROM 
            {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_WITH_MPI_STD_TABLE_NAME}  
        
        UNION ALL
        
        SELECT 
            oolu_id AS id, 
            mpid_id AS mpi,
            oolu_fname AS fname, 
            oolu_lname AS lname, 
            oolu_dob AS dob, 
            oolu_sex AS sex, 
            oolu_ssn AS ssn,
            oolu_respective_vault AS respective_vault, 
            oolu_respective_mrn AS respective_mrn,
            oolu_opt_choice AS opt_choice, 
            oolu_status AS status, 
            oolu_username AS username, 
            oolu_user AS user,
            oolu_submitted AS submitted, 
            oolu_completed AS completed, 
            oolu_last_update_php AS last_update_php,
            'special1' AS match_method
        FROM
            {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_WITH_MPI_SPECIAL1_TABLE_NAME} 
        """,
    )
    
    drop_opt_out_list_if_exists = MySqlOperator(
        task_id="drop_opt_out_list_if_exists",
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LIST_TABLE_NAME}
        """
    )
    create_opt_out_list = MySqlOperator(
        task_id="create_opt_out_list",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LIST_TABLE_NAME}
        (
            INDEX (mpi)
        ) AS
        SELECT 
            mpi, fname, lname, dob, sex, ssn, 
            respective_vault, respective_mrn, opt_choice,
            status, username, user, submitted, completed,
            last_update_php, 
            GROUP_CONCAT(match_method) AS match_methods
        FROM 
            {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LIST_RAW_TABLE_NAME}
        GROUP BY 
            mpi, fname, lname, dob, sex, ssn, 
            respective_vault, respective_mrn, opt_choice,
            status, username, user, submitted, completed,
            last_update_php
        ORDER BY mpi, respective_mrn, last_update_php DESC
        """,
    )
    
    drop_indexed_mpi_table_if_exists >> create_indexed_mpi_table
    (
        create_opt_out_schema >> drop_opt_out_load_table_if_exists >> 
        create_opt_out_load_table >> populate_opt_out_load_table >> 
        create_sanitized_opt_out_table >>
        create_opt_out_table_with_mpi_std
    )
    create_sanitized_opt_out_table >> create_opt_out_table_with_mpi_special1
    drop_opt_out_list_raw_if_exists >> create_opt_out_list_raw
    create_indexed_mpi_table >> create_opt_out_table_with_mpi_std
    create_indexed_mpi_table >> create_opt_out_table_with_mpi_special1
    create_opt_out_table_with_mpi_std >> create_opt_out_list_raw
    create_opt_out_table_with_mpi_special1 >> create_opt_out_list_raw
    drop_sanitized_table_if_exists >> create_sanitized_opt_out_table
    drop_with_mpi_std_table_if_exists >> create_opt_out_table_with_mpi_std
    drop_with_mpi_special1_table_if_exists >> create_opt_out_table_with_mpi_special1
    drop_opt_out_list_if_exists >> create_opt_out_list
    create_opt_out_list_raw >> create_opt_out_list

