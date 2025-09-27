import os
import glob
import logging
from airflow.hooks.base import BaseHook
import boto3
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import tempfile

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'hl7v3_l69_checksum',
    default_args=default_args,
    description='A simple DAG to check the file counts of the HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69 folders',
    #schedule='@hourly',  # Set to run hourly
    #start_date=datetime(2024,9,20),
    tags=['hl7v3_checksums'],
)
# Define the S3 bucket name and subfolder
#BUCKET_NAME = 'konzaandssigrouppipelines'
CURRENT_BUCKET_NAME = '/data/biakonzasftp/C-194/SUP-12046/'
NEW_BUCKET_NAME = '/data/biakonzasftp/C-194/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/'
#S3_SUBFOLDER = 'HL7v3In'
LOCAL_DESTINATION = '/data/biakonzasftp/C-194/' #PRD is '/source-biakonzasftp/C-9/optout_load/' #DEV is '/data/biakonzasftp/L-215/'
TEMP_DIRECTORY = '/data/biakonzasftp/airflow_temp/C-194/'

def list_files_in_hl7v3_l69_current(**kwargs):
    
    for d in os.listdir(CURRENT_BUCKET_NAME):
        #print(d)
        list_of_files = glob.glob(CURRENT_BUCKET_NAME + d + '/**/*.xml', recursive=True)
        number_of_files = len(list_of_files)
        #print(number_of_files)
        logging.info(f'CURRENT FOLDER {d} HAS {number_of_files} FILES')

list_current_files_task = PythonOperator(
    task_id='list_current_files',
    python_callable=list_files_in_hl7v3_l69_current,
    provide_context=True,
    dag=dag,
)

def list_files_in_hl7v3_l69_new(**kwargs):

    for d in os.listdir(NEW_BUCKET_NAME):
        #print(d)
        list_of_files = glob.glob(NEW_BUCKET_NAME + d + '/**/*.xml', recursive=True)
        number_of_files = len(list_of_files)
        #print(number_of_files)
        logging.info(f'NEW FOLDER {d} HAS {number_of_files} FILES')


list_new_files_task = PythonOperator(
    task_id='list_new_files',
    python_callable=list_files_in_hl7v3_l69_new,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
list_current_files_task >>  list_new_files_task

if __name__ == "__main__":
    dag.cli()