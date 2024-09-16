from __future__ import annotations
import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
import logging
#boto3.set_stream_logger('boto3.resources', logging.DEBUG)
#boto3.set_stream_logger('botocore', logging.DEBUG)

def upload_file_to_s3():
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    s3_hook.load_file(
        filename='/data/biakonzasftp/C-126/L-69/05F9745D04CD45AD8F99662F80635AA4.xml',
        key='HL7v3Out/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA521347.xml',
        bucket_name='konzaandssigrouppipelines'
    )

def upload_file_to_s3_SSI_Nashville():
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    #s3_hook.load_file(
    #    filename='/data/biakonzasftp/C-126/L-69/05F9745D04CD45AD8F99662F80635AA4.xml',
    #    key='clientName=KONZA/source=L-69/status=pending/05F9745D04CD45AD8F99662F80635AA53.XML',
    #    bucket_name='com-ssigroup-insight-attribution-data'
    #)
    s3_hook.load_file(
        filename='/data/biakonzasftp/C-126/L-69/05F9745D04CD45AD8F99662F80635AA4.xml',
        key='clientName=KONZA/source=L-69/status=pending/domainOid=2.16.840.1.113883.17.9491/05F9745D04CD45AD8F99662F80635AA25237.XML',
        bucket_name='com-ssigroup-insight-attribution-data', encrypt=True, acl_policy='bucket-owner-full-control'
    )

def sync_s3_buckets():
    source_bucket = 'konzaandssigrouppipelines'
    source_prefix = 'HL7v3Out/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA5.xml'
    #destination_bucket = 'konzaandssigrouppipelines'
    #destination_prefix = 'HL7v3Out/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office #Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA5.xml'
    destination_bucket = 'com-ssigroup-insight-attribution-data'
    destination_prefix = 'clientName=KONZA/source=L-69/status=pending/05F9745D04CD45AD8F99662F80635AA4.XML'

    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')

    # Copy the object
    s3_hook.copy_object(
        source_bucket_key=source_prefix,
        dest_bucket_key=destination_prefix,
        source_bucket_name=source_bucket,
        dest_bucket_name=destination_bucket
    )

    # Optionally, delete the source object if needed
    s3_hook.delete_objects(
        bucket=source_bucket,
        keys=[source_prefix]
    )

with DAG(
    dag_id="C_126_L_69",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    upload_task_konza = PythonOperator(
        task_id="upload_file_to_s3",
        python_callable=upload_file_to_s3,
    )
    upload_task_ssi_nashville = PythonOperator(
        task_id="upload_file_to_s3_SSI_Nashville",
        python_callable=upload_file_to_s3_SSI_Nashville ,
    )

    sync_task = PythonOperator(
        task_id="sync_s3_buckets",
        python_callable=sync_s3_buckets,
    )

    upload_task_konza >> upload_task_ssi_nashville
    #sync_task

if __name__ == "__main__":
    dag.test()
