import os
import glob
import logging
from airflow.hooks.base_hook import BaseHook
import boto3
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import tempfile
import zipfile
import logging

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'nj_file_move_from_zip_folder_20230915',
    default_args=default_args,
    description='A simple DAG to move the NJ files the HL7In Zipped Dated folders to the L-248',
    #schedule_interval='@hourly',  # Set to run hourly
    #start_date=datetime(2024,9,20),
    tags=['L-248'],
)
# Define the S3 bucket name and subfolder
#BUCKET_NAME = 'konzaandssigrouppipelines'
CURRENT_BUCKET_NAME = '/source-biakonzasftp/C-194/archive_HL7v2/'
#NEW_BUCKET_NAME = '/source-biakonzasftp/C-194/HL7InV3/'
#S3_SUBFOLDER = 'HL7v3In'
LOCAL_DESTINATION = '/source-biakonzasftp/L-248/HL7v2_NJ_202309/' #PRD is '/source-biakonzasftp/C-9/optout_load/' #DEV is '/source-biakonzasftp/L-215/'
TEMP_DIRECTORY = '/source-biakonzasftp/airflow_temp/C-194/'

# Define source directory and SFTP destination path
#source_dir = r"I:\hl7v2\hl7_parsed"
#sftp_destination_path = "/L-248/HL7v2_NJ/"

## Define date range
##start_date = datetime.date(2020, 1, 1)
#start_date = datetime.date(2023, 9, 1)
#end_date = datetime.date(2023, 9, 5)
start_date = datetime(2023, 9, 15, 0, 0, 0)
end_date = datetime(2023, 9, 15, 0, 0, 0)
#logging.info(f'start date should be {start_date}')

#start_date = '20230901'
#end_date = '20230905'

# Retry settings
#MAX_RETRIES = 3
#RETRY_DELAY = 5  # seconds

# Iterate through each date folder
current_date = start_date

def scan_zipped_folder_and_move_nj_files(**kwargs):
    zip_files = [f for f in os.listdir(CURRENT_BUCKET_NAME) if f.lower().endswith('.zip')]
    print("ZIP files found:")
    logging.info(f'start date should be {start_date}')
    logging.info(f'end date should be {end_date}')
    for zip_file in zip_files:
        zip_file_name = zip_file.split('.')[0]
        zip_file_name_dt = datetime.strptime(zip_file_name, "%Y%m%d")
        #logging.info(f'zip_file_name_dt should be {zip_file_name_dt}')
        folder_date = zip_file_name_dt.strftime("%Y-%m-%d")
        #logging.info(f'folder_date should be {folder_date}')
        if zip_file_name_dt <= end_date and zip_file_name_dt >= start_date:
            print(folder_date)
            print(zip_file)
            zip_path = os.path.join(CURRENT_BUCKET_NAME, zip_file)
            print(zip_path)
            # Open the zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    if 'NJ' in file_name and not file_name.endswith('/'):
                        # Read the file content
                        with zip_ref.open(file_name) as source_file:
                            # Get just the filename (no directories)
                            base_name = os.path.basename(file_name)
                            dest_path = os.path.join(LOCAL_DESTINATION, base_name)
                            # Write to destination
                            with open(dest_path, 'wb') as out_file:
                                out_file.write(source_file.read())
                        print(f"Found and moved: {base_name} to {LOCAL_DESTINATION}")

            
scan_zip_folder_task = PythonOperator(
    task_id='scan_zip_folder',
    python_callable=scan_zipped_folder_and_move_nj_files,
    provide_context=True,
    dag=dag,
)



# Set task dependencies
scan_zip_folder_task

if __name__ == "__main__":
    dag.cli()
