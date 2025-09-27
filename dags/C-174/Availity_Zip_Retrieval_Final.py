from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import zipfile
import os
import logging
from collections import namedtuple

# Define bucket details
BucketDetails = namedtuple('BucketDetails', ['aws_conn_id', 'aws_key_pattern', 's3_hook_kwargs'])

AWS_BUCKETS = {
    'konzaandssigrouppipelines':
        BucketDetails(
            aws_conn_id='konzaandssigrouppipelines',
            aws_key_pattern='FromAvaility/{input_file}',
            s3_hook_kwargs={}
        ),
    #'com-ssigroup-insight-attribution-data':
    #    BucketDetails(
    #        aws_conn_id='konzaandssigrouppipelines',
    #        aws_key_pattern='subscriberName=KONZA/subscriptionName=Historical/source=Availity/status=pending/{input_file_replaced}',
    #        s3_hook_kwargs={'encrypt': True, 'acl_policy': 'bucket-owner-full-control'}
    #    )
}

def process_zip_file(zip_file, logger):
    try:
        sftp_hook = SFTPHook(ftp_conn_id='Availity_Diameter_Health__Files_Production_SFTP')
        sftp_client = sftp_hook.get_conn()

        # Read ZIP from SFTP
        with sftp_client.open(zip_file, 'rb') as remote_file:
            zip_bytes = remote_file.read()

        # Archive ZIP
        archive_path = f'/source-biakonzasftp/C-194/archive_C-174/{os.path.basename(zip_file)}'
        with sftp_client.open(archive_path, 'wb') as archive_file:
            archive_file.write(zip_bytes)
        logger.info("Archived ZIP file to: %s", archive_path)

        # Extract and upload each file to S3
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            for file_name in zf.namelist():
                if not file_name.endswith('/'):
                    with zf.open(file_name) as f:
                        file_bytes = f.read()
                        for bucket_name, details in AWS_BUCKETS.items():
                            aws_key = details.aws_key_pattern.replace("{input_file}", file_name).replace("{input_file_replaced}", file_name.replace('/', '__'))
                            s3_hook = S3Hook(aws_conn_id=details.aws_conn_id)
                            s3_hook.load_bytes(
                                bytes_data=file_bytes,
                                key=aws_key,
                                bucket_name=bucket_name,
                                replace=True,
                                **details.s3_hook_kwargs
                            )
                            logger.info("Uploaded %s to s3://%s/%s", file_name, bucket_name, aws_key)

        # Delete original ZIP
        sftp_client.remove(zip_file)
        logger.info("Deleted original ZIP file from SFTP: %s", zip_file)

    except zipfile.BadZipFile:
        logger.warning("Corrupt ZIP file skipped: %s", zip_file)
    except Exception as e:
        logger.error("Failed to process %s: %s", zip_file, str(e), exc_info=True)

def unzip_and_upload_sftp_zips_multithreaded():
    logger = logging.getLogger("airflow.task")
    sftp_hook = SFTPHook(ftp_conn_id='Availity_Diameter_Health__Files_Production_SFTP')
    sftp_client = sftp_hook.get_conn()

    files = sftp_client.listdir('.')
    zip_files = [f for f in files if f.lower().endswith('.zip')]
    logger.info("Number of ZIP files from SFTP: %d", len(zip_files))

    with ThreadPoolExecutor(max_workers=4) as executor:
        for zip_file in zip_files:
            executor.submit(process_zip_file, zip_file, logger)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Availity_Zip_Retrieval_Final',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'Canary', 'Staging_in_Prod'],
) as dag:

    unzip_and_upload_task = PythonOperator(
        task_id='unzip_and_upload_sftp_zips_multithreaded',
        python_callable=unzip_and_upload_sftp_zips_multithreaded,
    )
