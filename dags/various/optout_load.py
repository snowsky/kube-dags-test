"""
This is a DAG to load new opt outs to the opt out list
"""

import os
import sys
import time
from datetime import timedelta, datetime
import typing

from airflow import XComArg
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


@dag(
    schedule="0 0 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=600),
    params={
        "quality_check_delivery": "1",
        "script_name": "OptOut_Load",
        #"working_optout": "/data/biakonzasftp/C-9/optout_load/",
        "error_file_location": "\source\Run_Errors.txt",
    },
)
def optout_load():
    import pandas as pd
    import logging
    import shutil as sh
    connection = BaseHook.get_connection('prd-az1-sqlw2-airflowconnection')
    hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
    #@task(retries=5, retry_delay=timedelta(minutes=1))
    def move_file_to_processed(file_path):
        file_directory = os.path.dirname(file_path)
        processed_directory = os.path.join(file_directory,'processed')
        if not os.path.exists(processed_directory):
            os.makedirs(processed_directory)
        destination_path = os.path.join(processed_directory, os.path.basename(file_path))
        sh.move(file_path, destination_path)
        print(f"Moved file {file_path} to {destination_path}")
    @task
    def get_opt_out_list() -> pd.DataFrame:
        hookOOL = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
        hookOOL.run(sql="""START TRANSACTION; 
            truncate clientresults.opt_out_list_airflow_load;
            COMMIT;
                """)
        logging.info(print('/source-biakonzasftp/'))
        sourceDir = '/source-biakonzasftp/C-9/optout_load/'
        optOutFiles = os.listdir(sourceDir)
        logging.info(print(optOutFiles))
        for f in optOutFiles:
            logging.info(print(f))
            if f == 'OptOutList.csv' or f == 'processed':
                logging.info(print('Skipping - OptOutList.csv'))
                continue
            print(f"On file: {f}")
            optoutDF = pd.read_csv(sourceDir + f)
            #optoutDFdedupe = optoutDF.where(pd.notnull(optoutDF),None)
            optoutDFdedupe = optoutDF.astype(str)
            print(f"Loading new list from: {f}")
            logging.info(print(optoutDFdedupe))
            rows = [tuple(row) for row in optoutDFdedupe.to_numpy()]
            print('Rows:')
            logging.info(print(rows))
            hook.insert_rows(table='opt_out_list_airflow_load', rows=rows, target_fields=['fname'
                ,'lname'
                ,'dob'
                ,'sex'
                ,'SSN'
                ,'respective_vault'
                ,'respective_mrn'
                ,'opt_choice'
                ,'status'
                ,'username'
                ,'user'
                ,'submitted'
                ,'completed'], replace=True)
            move_file_to_processed(sourceDir + f)
            #break #stop after one file
        #logging.info(print('/source-hqintellectstorage/'))
        #logging.info(os.listdir('/source-hqintellectstorage/'))
        #logging.info(print('/source-reportwriterstorage/'))
        #logging.info(os.listdir('/source-reportwriterstorage/'))
        #logging.info(print("ALL"))
        #logging.info(os.listdir('.'))
        try:
            dfOptOutList = hook.get_pandas_df(
                "SELECT * FROM clientresults.opt_out_list;"
            )
            return dfOptOutList
        except:
            raise ValueError("Error in getting opt_out_list ...retrying in 1 minute")
    @task
    def adjustLoad():
        hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
        hook.run(sql="""START TRANSACTION; 
            update clientresults.opt_out_list_airflow_load
            set dob = date_format(str_to_date(dob, '%m/%d/%Y'),'%Y-%m-%d')
            where dob <> 'Unknown' and dob not like '%-%';
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            UPDATE clientresults.opt_out_list_airflow_load
                    SET ssn = replace(ssn, '-', '')
                    ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            UPDATE clientresults.opt_out_list_airflow_load
                SET ssn = LPAD(ssn, 9, '0')
                ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            insert into clientresults.opt_out_list_airflow_load (
                fname
                ,lname
                ,dob
                ,sex
                ,SSN
                ,respective_mrn
                ,respective_vault
                ,opt_choice
                ,`status`
                ,last_update_php
                ) (
                select 
                fname
                ,lname
                ,dob
                ,sex
                ,SSN
                ,respective_mrn
                ,respective_vault
                ,opt_choice
                ,`status`
                ,last_update_php
                from clientresults.opt_out_list
                where opt_choice <> 'in'
                );
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            drop table if exists temp.opt_out_mpi_id_master_lookup
            ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            create table temp.opt_out_mpi_id_master_lookup like person_master._mpi_id_master;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            insert into temp.opt_out_mpi_id_master_lookup
                select * from person_master._mpi_id_master;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            drop table if exists temp.optouttest_all_related_mpi_by_std
            ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            create table temp.optouttest_all_related_mpi_by_std
                SELECT
                Full_OOLU.id
                ,Full_OOLU.MPI
                ,Full_OOLU.fname
                ,Full_OOLU.lname
                ,Full_OOLU.dob
                ,Full_OOLU.sex
                ,Full_OOLU.SSN
                ,Full_OOLU.respective_vault
                ,Full_OOLU.respective_mrn
                ,Full_OOLU.opt_choice
                ,Full_OOLU.status
                ,Full_OOLU.username
                ,Full_OOLU.user
                ,Full_OOLU.submitted
                ,Full_OOLU.completed
                ,Full_OOLU.last_update_php
                ,MPID.id MPID_ID
                ,MPID.firstname
                ,MPID.lastname
                ,MPID.dob MPID_DOB
                ,MPID.sex MPID_SEX
                ,MPID.ssn MPID_SSN
                ,MPID.longitudinal_anomoly
                ,MPID.first_name_anomoly
                ,MPID.mrn_anomoly
                 FROM clientresults.opt_out_list_airflow_load Full_OOLU
                left join person_master._mpi_id_master MPID on (
                Full_OOLU.fname = MPID.firstname
                and Full_OOLU.lname = MPID.lastname
                and Full_OOLU.dob = MPID.dob
                and Full_OOLU.sex = MPID.sex)
                where Full_OOLU.dob is not null and Full_OOLU.lname is not null
                ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            drop table if exists temp.optouttest_all_related_mpi_by_special1
            ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            create table temp.optouttest_all_related_mpi_by_special1
                SELECT
                Full_OOLU.id
                ,Full_OOLU.MPI
                ,Full_OOLU.fname
                ,Full_OOLU.lname
                ,Full_OOLU.dob
                ,Full_OOLU.sex
                ,Full_OOLU.SSN
                ,Full_OOLU.respective_vault
                ,Full_OOLU.respective_mrn
                ,Full_OOLU.opt_choice
                ,Full_OOLU.status
                ,Full_OOLU.username
                ,Full_OOLU.user
                ,Full_OOLU.submitted
                ,Full_OOLU.completed
                ,Full_OOLU.last_update_php
                ,MPID.id MPID_ID
                ,MPID.firstname
                ,MPID.lastname
                ,MPID.dob MPID_DOB
                ,MPID.sex MPID_SEX
                ,MPID.ssn MPID_SSN
                ,MPID.longitudinal_anomoly
                ,MPID.first_name_anomoly
                ,MPID.mrn_anomoly
                FROM clientresults.opt_out_list_airflow_load Full_OOLU
                left join person_master._mpi_id_master MPID on (
                Full_OOLU.fname = MPID.firstname
                and left(Full_OOLU.lname,3) = left(MPID.lastname,3)
                and Full_OOLU.dob = MPID.dob
                and Full_OOLU.sex = MPID.sex
                and right(Full_OOLU.ssn,4) = right(MPID.ssn,4))
                where Full_OOLU.dob is not null and Full_OOLU.lname is not null
                ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;            
                drop table if exists clientresults.opt_out_list
                            ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            create table clientresults.opt_out_list
                select
                Full_OOLU.id
                ,MPID_ID MPI
                ,Full_OOLU.fname
                ,Full_OOLU.lname
                ,Full_OOLU.dob
                ,Full_OOLU.sex
                ,Full_OOLU.SSN
                ,Full_OOLU.respective_vault
                ,Full_OOLU.respective_mrn
                ,Full_OOLU.opt_choice
                ,Full_OOLU.status
                ,Full_OOLU.username
                ,Full_OOLU.user
                ,Full_OOLU.submitted
                ,Full_OOLU.completed
                ,Full_OOLU.last_update_php
                from temp.optouttest_all_related_mpi_by_std Full_OOLU
                #union
                #select
                #Full_OOLU.id
                #,MPID_ID MPI
                #,Full_OOLU.fname
                #,Full_OOLU.lname
                #,Full_OOLU.dob
                #,Full_OOLU.sex
                #,Full_OOLU.SSN
                #,Full_OOLU.respective_vault
                #,Full_OOLU.respective_mrn
                #,Full_OOLU.opt_choice
                #,Full_OOLU.status
                #,Full_OOLU.username
                #,Full_OOLU.user
                #,Full_OOLU.submitted
                #,Full_OOLU.completed
                #,Full_OOLU.last_update_php
                #from temp.optouttest_all_related_mpi_by_ssn Full_OOLU
                union
                select
                Full_OOLU.id
                ,MPID_ID MPI
                ,Full_OOLU.fname
                ,Full_OOLU.lname
                ,Full_OOLU.dob
                ,Full_OOLU.sex
                ,Full_OOLU.SSN
                ,Full_OOLU.respective_vault
                ,Full_OOLU.respective_mrn
                ,Full_OOLU.opt_choice
                ,Full_OOLU.status
                ,Full_OOLU.username
                ,Full_OOLU.user
                ,Full_OOLU.submitted
                ,Full_OOLU.completed
                ,Full_OOLU.last_update_php
                from temp.optouttest_all_related_mpi_by_special1 Full_OOLU
                ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;            
                drop table if exists temp.optoutshrink_airflow
                                ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            create table temp.optoutshrink_airflow
                SELECT * FROM clientresults.opt_out_list
                group by 
                MPI
                ,fname
                ,lname
                ,dob
                ,sex
                ,SSN
                ,respective_vault
                ,respective_mrn
                ,opt_choice
                ,status
                ,username
                ,user
                ,submitted
                ,completed
                ,last_update_php
                order by MPI, respective_mrn, last_update_php desc
                ;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            truncate clientresults.opt_out_list;
            COMMIT;
                """)
        hook.run(sql="""START TRANSACTION;
            insert into clientresults.opt_out_list
                select * from temp.optoutshrink_airflow
                group by MPI
                ,fname
                ,lname
                ,dob
                ,sex
                ,SSN
                ,respective_vault
                ,respective_mrn
                ;
            COMMIT;
                """)
       
    dfOptOuts = get_opt_out_list()
    print(dfOptOuts)
    adjustedLoad = adjustLoad()
    adjustedLoad << dfOptOuts ## establishes sequence of dfOptOuts first then adjustedLoad
    #@task
    #def get_local_dirs() -> pd.DataFrame:
    #    
    #    return dirs
    ##local_dirs = get_local_dirs()
    ##print(f"Directories: {local_dirs}")
    #get_local_dirs()
    #print("Directories: ")
    #print(get_local_dirs)

optout_load_dag = optout_load()

if __name__ == "__main__":
    optout_load_dag.test()
