import sys
sys.path.insert(0, '/opt/airflow/dags/repo/dags')

from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task

from populations.target_population_impl import output_df_to_target_tbl
from populations.target_population_impl import _fix_engine_if_invalid_params
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator

from datetime import datetime
import os
import hl7
import pandas as pd
from typing import List, Tuple

MYSQL_CONN_ID = "MariaDB"
SOURCE_DIR = "/opt/airflow/samples/hl7v2"

default_args = {
    'owner': 'airflow',
}
with DAG(
    'ingest_hl7v2_new',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    tags=['mpi', 'hl7v2'],
) as dag:

    def write_batch_to_mysql(
        batch_id: int,
        batch: List[str], 
        mysql_conn_id: str,
        schema: str,
        table: str,
    ):
        
        hook = MySqlHook(conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())
        rows = [
            {"batch_id": batch_id, "file_path": file_path} 
            for file_path in batch
        ]
        dt = pd.DataFrame.from_records(rows)
        dt.to_sql(name=table, con=engine, schema=schema, index=False, if_exists='append')

    @task
    def create_batches(
        message_directory, 
        mysql_tmp_batch_table,
        mysql_tmp_schema,
        conn_id=MYSQL_CONN_ID,
        batch_size=100,
     ):
        batch_paths = []
        batch_id = 0
        
        for message_id, file_name in enumerate(os.listdir(message_directory)):
            
            file_path = os.path.join(message_directory, file_name)
            batch_paths.append(file_path)
            
            if len(batch_paths) >= batch_size:
                write_batch_to_mysql(
                    batch=batch_paths, 
                    batch_id=batch_id,
                    conn_id=mysql_conn_id,
                    schema=mysql_tmp_schema,
                    table=mysql_tmp_batch_table,
                )
                batch_paths = []
                batch_id += 1
        
        if len(batch_paths) > 0:
            write_batch_to_mysql(
                batch=batch_paths, 
                batch_id=batch_id,
                conn_id=mysql_conn_id,
                schema=mysql_tmp_schema,
                table=mysql_tmp_batch_table,
            )
            return list(range(batch_id + 1))
        
        return list(range(batch_id))
    
    def get_pid(file_path: str) -> hl7.Segment:
        with open(file_path) as f:
            message = f.read().replace('\n', '\r')
        hl7v2_message = hl7.parse(message)
        return hl7v2_message.segments('PID')[0]

    @task
    def extract_mpi_identifiers_from_batch(
        batch_id: int, 
        batch_table: str,
        batch_schema: str,
        mpi_new_records_table: str,
        mpi_new_records_schema: str,
        conn_id=MYSQL_CONN_ID,
    ):
        hook = MySqlHook(conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())
        dt = pd.read_sql(f"""
            SELECT message_id, file_path 
            FROM {batch_schema}.{batch_table}
            WHERE batch_id = {batch_id}
            """, 
            con=engine
        )
        
        mpi_identifiers = []
        for _, row in dt.iterrows():
            message_id = row['message_id']
            file_path = row['file_path']
            pid = get_pid(file_path)
            print(pid)
            lastname = pid[5][0][0]
            firstname = pid[5][0][1]
            sex = pid[8][0][0][:1]
            dob = datetime.strptime(pid[7][0], "%Y%m%d")
            try:
                ssn = pid[19][0]
            except IndexError:
                ssn = None

            mpi_row = {
                'message_id': message_id,
                'file_path': file_path,
                'batch_id': batch_id,
                'lastname': lastname, 
                'firstname': firstname, 
                'sex': sex, 
                'dob': dob,
                'ssn': ssn,
            }
            mpi_identifiers.append(mpi_row)
        
        mpi_dt = pd.DataFrame.from_records(mpi_identifiers)
        mpi_dt.to_sql(
            name=mpi_new_records_table, 
            schema=mpi_new_records_schema, 
            con=engine, 
            index=False, 
            if_exists='append',
        )

    create_tmp_database = SQLExecuteQueryOperator(
        task_id="create_tmp_database",
        sql="""
        CREATE DATABASE IF NOT EXISTS tmp_hl7_processing_{{ ds_nodash }};
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    drop_batch_table = SQLExecuteQueryOperator(
        task_id="drop_batch_table",
        sql="""
        DROP TABLE IF EXISTS tmp_hl7_processing_{{ ds_nodash }}.batches
        """,
        conn_id=MYSQL_CONN_ID,
    )

    create_batch_table = SQLExecuteQueryOperator(
        task_id="create_batch_table",
        sql="""
        CREATE TABLE IF NOT EXISTS tmp_hl7_processing_{{ ds_nodash }}.batches (
            message_id BIGINT(20) NOT NULL AUTO_INCREMENT,
            batch_id BIGINT(20) NOT NULL,
            file_path VARCHAR(1024) NOT NULL,
            PRIMARY KEY (message_id),
            KEY (batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    create_new_mpi_records_table = SQLExecuteQueryOperator(
        task_id="create_new_mpi_records_table",
        sql="""
        CREATE TABLE IF NOT EXISTS tmp_hl7_processing_{{ ds_nodash }}.new_mpi_records (
            message_id BIGINT(20),
            batch_id BIGINT(20) NOT NULL,
            file_path VARCHAR(1024) NOT NULL,
            firstname varchar(100) DEFAULT NULL,
            lastname varchar(100) DEFAULT NULL,
            dob varchar(100) DEFAULT NULL,
            sex varchar(100) DEFAULT NULL,
            ssn varchar(100) DEFAULT NULL,
            PRIMARY KEY (message_id),
            KEY (batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    drop_new_mpi_records_table = SQLExecuteQueryOperator(
        task_id="drop_new_mpi_records_table",
        sql="""
        DROP TABLE IF EXISTS tmp_hl7_processing_{{ ds_nodash }}.new_mpi_records
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    batches = create_batches(
        message_directory=SOURCE_DIR, 
        mysql_tmp_batch_table="batches",
        mysql_tmp_schema="tmp_hl7_processing_{{ ds_nodash }}",
    )
    create_tmp_database >> drop_batch_table >> create_batch_table >> batches
    create_tmp_database >> drop_new_mpi_records_table >> create_new_mpi_records_table
    mpi_new_records = extract_mpi_identifiers_from_batch.partial(
        batch_table="batches",
        batch_schema="tmp_hl7_processing_{{ ds_nodash }}",
        mpi_new_records_table="new_mpi_records",
        mpi_new_records_schema="tmp_hl7_processing_{{ ds_nodash }}",
    ).expand(batch_id=batches)
    
    drop_matched_mpi_records = SQLExecuteQueryOperator(
        task_id="drop_matched_records_table",
        sql="""
        DROP TABLE IF EXISTS tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records
        """,
        conn_id=MYSQL_CONN_ID,
    )

    try_match_mpi_records = SQLExecuteQueryOperator(
        task_id="try_match_mpi_records",
        sql="""
        CREATE TABLE IF NOT EXISTS 
            tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records 
        AS SELECT
            new_mpi_records.lastname, new_mpi_records.firstname,
            new_mpi_records.dob, new_mpi_records.sex,
            MAX(mpi_master.id) AS max_mpi_id,
            JSON_ARRAYAGG(message_id) AS message_ids,
            JSON_ARRAYAGG(new_mpi_records.ssn) AS ssn_should_be_singleton
        FROM hl7_processing_center._mpi_id_master mpi_master
        RIGHT OUTER JOIN tmp_hl7_processing_{{ ds_nodash }}.new_mpi_records new_mpi_records
        ON new_mpi_records.lastname = mpi_master.lastname
        AND new_mpi_records.firstname = mpi_master.firstname
        AND new_mpi_records.dob = mpi_master.dob
        AND new_mpi_records.sex = mpi_master.sex
        GROUP BY 
            new_mpi_records.lastname, new_mpi_records.firstname,
            new_mpi_records.dob, new_mpi_records.sex
        """,
        conn_id=MYSQL_CONN_ID,
    )

    drop_new_mpi_identifiers = SQLExecuteQueryOperator(
        task_id="drop_new_mpi_identifiers_table",
        sql="""
        DROP TABLE IF EXISTS tmp_hl7_processing_{{ ds_nodash }}.mpi_ids_to_be_created
        """,
        conn_id=MYSQL_CONN_ID,
    )
    find_new_mpi_identifiers = SQLExecuteQueryOperator(
        task_id="find_new_mpi_identifiers",
        sql="""
        CREATE TABLE IF NOT EXISTS 
            tmp_hl7_processing_{{ ds_nodash }}.mpi_ids_to_be_created
        AS SELECT
            lastname,
            firstname,
            dob,
            sex,
            JSON_EXTRACT(ssn_should_be_singleton, '$[0]') AS ssn
        FROM tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records
        WHERE max_mpi_id IS NULL
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    add_new_mpi_identifiers = SQLExecuteQueryOperator(
        task_id="add_new_mpi_identifiers",
        sql="""
        INSERT INTO hl7_processing_center._mpi_id_master (
            lastname, firstname, dob, sex, ssn
        )
        SELECT
            lastname,
            firstname,
            dob,
            sex,
            ssn
        FROM tmp_hl7_processing_{{ ds_nodash }}.mpi_ids_to_be_created
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    drop_matched_mpi_records_before_recreating = SQLExecuteQueryOperator(
        task_id="drop_matched_records_before_recreating",
        sql="""
        DROP TABLE IF EXISTS tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records
        """,
        conn_id=MYSQL_CONN_ID,
    )
    
    match_mpi_records_again = SQLExecuteQueryOperator(
        task_id="match_mpi_records_again",
        sql="""
        CREATE TABLE IF NOT EXISTS 
            tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records 
        AS SELECT
            new_mpi_records.lastname, new_mpi_records.firstname,
            new_mpi_records.dob, new_mpi_records.sex,
            MAX(mpi_master.id) AS max_mpi_id,
            JSON_ARRAYAGG(message_id) AS message_ids,
            JSON_ARRAYAGG(new_mpi_records.ssn) AS ssn_should_be_singleton
        FROM hl7_processing_center._mpi_id_master mpi_master
        RIGHT OUTER JOIN tmp_hl7_processing_{{ ds_nodash }}.new_mpi_records new_mpi_records
        ON new_mpi_records.lastname = mpi_master.lastname
        AND new_mpi_records.firstname = mpi_master.firstname
        AND new_mpi_records.dob = mpi_master.dob
        AND new_mpi_records.sex = mpi_master.sex
        GROUP BY 
            new_mpi_records.lastname, new_mpi_records.firstname,
            new_mpi_records.dob, new_mpi_records.sex
        """,
        conn_id=MYSQL_CONN_ID,
    )
    matched_mpi_records = SQLExecuteQueryOperator(
        task_id="matched_mpi_records",
        sql="""
        SELECT * FROM tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records
        WHERE max_mpi_id IS NULL
        """,
        conn_id=MYSQL_CONN_ID,
    )
    check_no_unmatched_records = SQLTableCheckOperator(
        task_id="check_no_unmatched_records",
        table="tmp_hl7_processing_{{ ds_nodash }}.matched_mpi_records",
        checks={
            "unmatched_row_check": {
                "check_statement": "SUM(max_mpi_id IS NULL) = 0 OR COUNT(*) = 0",
            }
        },
        conn_id=MYSQL_CONN_ID,
    )

    
    (
        mpi_new_records >> drop_matched_mpi_records >> 
        try_match_mpi_records >> drop_new_mpi_identifiers >> 
        find_new_mpi_identifiers >> add_new_mpi_identifiers >>
        drop_matched_mpi_records_before_recreating >> match_mpi_records_again >>
        matched_mpi_records >> check_no_unmatched_records
    )

