from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_file_to_s3():
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    s3_hook.load_file(
        filename='/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA4.XML',
        key='path/to/destination/test.txt',
        bucket_name='konzaandssigrouppipelines'
    )

def sync_s3_buckets():
    source_bucket = 'konzaandssigrouppipelines'
    source_prefix = 'HL7v3Out/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA4.XML'
    destination_bucket = 'com-ssigroup-insight-attribution-data'
    destination_prefix = 'clientName=KONZA/source=HL7v2/status=pending/2.16.840.1.113883.3.4382.973/'

    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    s3_hook.sync(
        source_bucket_name=source_bucket,
        source_prefix=source_prefix,
        dest_bucket_name=destination_bucket,
        dest_prefix=destination_prefix,
        delete=True  # Optional: delete files in the destination that are not in the source
    )

with DAG(
    dag_id="C_126_L_69_saved",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    #upload_task = PythonOperator(
    #    task_id="upload_file_to_s3",
    #    python_callable=upload_file_to_s3,
    #)

    sync_task = PythonOperator(
        task_id="sync_s3_buckets",
        python_callable=sync_s3_buckets,
    )

    #upload_task >> sync_task
    sync_task

if __name__ == "__main__":
    dag.test()
