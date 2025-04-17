import datetime
import boto3
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def upload_file_to_s3():
    s3_client = boto3.client('s3')
    s3_client.upload_file(
        Filename='/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA4.XML',
        Bucket='konzaandssigrouppipelines',
        Key='path/to/destination/test.txt'
    )

def sync_s3_buckets():
    s3_resource = boto3.resource('s3')
    source_bucket = s3_resource.Bucket('konzaandssigrouppipelines')
    destination_bucket = s3_resource.Bucket('com-ssigroup-insight-attribution-data')
    
    source_prefix = 'HL7v3Out/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/20240826/2.16.840.1.113883.3.4382.973/05F9745D04CD45AD8F99662F80635AA4.XML'
    destination_prefix = 'clientName=KONZA/source=HL7v2/status=pending/2.16.840.1.113883.3.4382.973/'

    for obj in source_bucket.objects.filter(Prefix=source_prefix):
        copy_source = {'Bucket': source_bucket.name, 'Key': obj.key}
        destination_key = destination_prefix + obj.key[len(source_prefix):]
        destination_bucket.copy(copy_source, destination_key)

    # Optional: delete files in the destination that are not in the source
    source_keys = {obj.key for obj in source_bucket.objects.filter(Prefix=source_prefix)}
    destination_keys = {obj.key for obj in destination_bucket.objects.filter(Prefix=destination_prefix)}

    for key in destination_keys - source_keys:
        destination_bucket.Object(key).delete()

with DAG(
    dag_id="C_126_L_69-boto3",
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
