from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from functools import partial
import math
import logging
import os
from concurrent.futures import as_completed, ThreadPoolExecutor as PoolExecutor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from typing import List

#DEFAULT_DEST_FILES_DIRECTORY2 = '/data/biakonzasftp/C-127/archive'
#DEFAULT_DEST_FILES_DIRECTORY = '/data/biakonzasftp/C-179/HL7v2In_to_Corepoint'
DEFAULT_DEST_FILES_DIRECTORY2 = '/source-biakonzasftp//C-127/archive'
DEFAULT_DEST_FILES_DIRECTORY = '/source-biakonzasftp//C-179/HL7v2In_to_Corepoint'

#DEFAULT_AWS_FOLDER = 'HL7v2In/aa_test'
DEFAULT_AWS_FOLDER = 'HL7v2In/'

DEFAULT_MAX_POOL_WORKERS = 5
DEFAULT_MAX_TASKS = 200
DEFAULT_PAGE_SIZE = 1000
PARALLEL_TASK_LIMIT = 5
DEFAULT_AWS_TAG = ['CPProcessed', 'KONZAID']
DEFAULT_DB_CONN_ID = 'qa-az1-sqlw3-airflowconnection'
sql_hook = MySqlHook(mysql_conn_id=DEFAULT_DB_CONN_ID)

class BucketDetails:
    def __init__(self, aws_conn_id, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.s3_hook_kwargs = s3_hook_kwargs

AWS_BUCKETS = {
    'konzaandssigrouppipelines': BucketDetails('konzaandssigrouppipelines', {}),
    'com-ssigroup-insight-attribution-data': BucketDetails('konzaandssigrouppipelines', {
        'encrypt': True, 'acl_policy': 'bucket-owner-full-control'}),
    'mial-test-bucket': BucketDetails('test_aws', {})
}

default_args = {'owner': 'airflow'}

with DAG(
    dag_id='HL7v2_file_retrieval_from_S3',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    tags=['C-127'],
    concurrency=PARALLEL_TASK_LIMIT,
    params={
        "output_files_dir_path": Param(DEFAULT_DEST_FILES_DIRECTORY, type="string"),
        "aws_bucket": Param('konzaandssigrouppipelines', type="string"),
        "aws_folder": Param(DEFAULT_AWS_FOLDER, type="string"),
        "aws_tag": Param(DEFAULT_AWS_TAG, type="string"),
        "max_pool_workers": Param(DEFAULT_MAX_POOL_WORKERS, type="integer", minimum=0),
        "max_mapped_tasks": Param(DEFAULT_MAX_TASKS, type="integer", minimum=0),
        "page_size": Param(DEFAULT_PAGE_SIZE, type="integer", minimum=0)
    },
) as dag:
    dag_params = dag.params


    def _split_list_into_batches(target_list, max_tasks):
        if target_list:
            chunk_size = math.ceil(len(target_list) / max_tasks)
            return [target_list[i:i + chunk_size] for i in range(0, len(target_list), chunk_size)]
        return []

    @task
    def prep_temp_tables_task():
        query = """
            DROP TABLE IF EXISTS temp.file_keys;
            CREATE TABLE temp.file_keys (file_key varchar(255), batch_id int);
            CREATE INDEX batch_id_index ON temp.file_keys (batch_id);
            DROP TABLE IF EXISTS temp.file_keys_filtered;
            CREATE TABLE temp.file_keys_filtered (file_key varchar(255), batch_id int);
            CREATE INDEX batch_id_filtered_index ON temp.file_keys_filtered (batch_id);
        """
        sql_hook.run(query)

    @task
    def get_file_keys_task(params):
        bucket_name = params["aws_bucket"]
        aws_folder = params["aws_folder"]
        bucket = AWS_BUCKETS[bucket_name]
        s3_hook = S3Hook(aws_conn_id=bucket.aws_conn_id)

        logging.info(f"Listing keys in bucket: {bucket_name} with prefix: '{aws_folder}'")
        key_list = s3_hook.list_keys(bucket_name=bucket_name, prefix=aws_folder, page_size=params["page_size"])

        batch_key_list = _split_list_into_batches(key_list, params["max_mapped_tasks"])
        batch_values = ', '.join([f"('{b}', '{ind}')" for ind, a in enumerate(batch_key_list) for b in a])
        query = f"INSERT INTO temp.file_keys (file_key, batch_id) VALUES {batch_values};"
        sql_hook.run(query)
        return list(range(len(batch_key_list)))

    @task
    def filter_file_keys_task(batch_id, params):
        archive_result = sql_hook.get_records("SELECT file_key FROM archive.processed_files;")
        database_key_list = [r[0] for r in archive_result]

        aws_result = sql_hook.get_records(f"SELECT file_key FROM temp.file_keys WHERE batch_id={batch_id};")
        aws_key_list = [r[0] for r in aws_result]

        bucket_name = params["aws_bucket"]
        bucket = AWS_BUCKETS[bucket_name]
        s3_hook = S3Hook(aws_conn_id=bucket.aws_conn_id)
        conn = s3_hook.get_conn()

        diff_list = []
        for k in aws_key_list:
            tag_set = conn.get_object_tagging(Bucket=bucket_name, Key=k).get('TagSet', [])
            tag_keys = [tag.get("Key") for tag in tag_set]
            tag_dict = {tag.get("Key"): tag.get("Value") for tag in tag_set}
            if "CPProcessed" in tag_dict and "KONZAID" in tag_dict:
                diff_list.append(k)
            #if "CPProcessed" in tag_keys:
                #diff_list.append(k)
            
            ## AA: Uncomment when testing done 
            #if k not in database_key_list:
                #tag_set = conn.get_object_tagging(Bucket=bucket_name, Key=k).get('TagSet', [])
                #tag_keys = [tag.get("Key") for tag in tag_set]
                #if "CPProcessed" in tag_keys:
                    #diff_list.append(k)

        if diff_list:
            batch_values = ', '.join([f"('{key}')" for key in diff_list])
            sql_hook.run(f"INSERT INTO temp.file_keys_filtered (file_key) VALUES {batch_values};")
        return len(diff_list)

    @task
    def split_files_into_batches_task(diff_list_lengths, params):
        diff_list_length = sum(diff_list_lengths)
        if diff_list_length:
            chunk_size = math.ceil(diff_list_length / params["max_mapped_tasks"])
            query = f"""
                UPDATE temp.file_keys_filtered t
                JOIN (
                    SELECT file_key,
                           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
                    FROM temp.file_keys_filtered
                ) n ON t.file_key = n.file_key
                SET t.batch_id = ((n.row_num - 1) / {chunk_size});
            """
            sql_hook.run(query)
            return list(range(math.ceil(diff_list_length / chunk_size)))
        return []

    @task
    def retrieve_file_from_s3_task(batch_id, params: dict):
        from datetime import datetime
        import io
    
        query = f"SELECT file_key FROM temp.file_keys_filtered WHERE batch_id={batch_id};"
        result = sql_hook.get_records(query)
        key_list = [r[0] for r in result]
    
        downloaded_and_deleted_keys = []
        bucket_name = params["aws_bucket"]
        bucket = AWS_BUCKETS[bucket_name]
        s3_hook = S3Hook(aws_conn_id=bucket.aws_conn_id)
        conn = s3_hook.get_conn()
    
        for key in key_list:
            try:
                tag_set = conn.get_object_tagging(Bucket=bucket_name, Key=key).get('TagSet', [])
                tag_dict = {tag.get("Key"): tag.get("Value") for tag in tag_set}
                konzaid_value = tag_dict.get("KONZAID", "")
                original_filename = key.split('/')[-1]
                new_filename = f"KONZA__{konzaid_value}_{original_filename}.hl7"
                date_folder = datetime.now().strftime('%Y-%m-%d')
    
                # Destinations
                dest1 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY, date_folder, new_filename)
                dest2 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY2, date_folder, new_filename)
    
                # Ensure folders exist
                os.makedirs(os.path.dirname(dest1), exist_ok=True)
                os.makedirs(os.path.dirname(dest2), exist_ok=True)
    
                # Download to memory buffer
                logging.info(f"Downloading {key} to:\n → {dest1}\n → {dest2}")
                buffer = io.BytesIO()
                conn.download_fileobj(bucket_name, key, buffer)
    
                buffer.seek(0)
                with open(dest1, 'wb') as f1:
                    f1.write(buffer.read())
    
                buffer.seek(0)
                with open(dest2, 'wb') as f2:
                    f2.write(buffer.read())
    
                if os.path.isfile(dest1) and os.path.isfile(dest2):
                    logging.info(f"Download complete: {key}")
                    downloaded_and_deleted_keys.append(key)
                    #delete function
                    try:
                        s3_hook.delete_objects(bucket_name, [key])
                        logging.info(f"Deleted from S3: {key}")
                    except Exception as e:
                        logging.error(f"Error deleting key {key}: {e}")
                else:
                    logging.warning(f"Incomplete file save for {key}, skipping S3 delete")
    
            except Exception as e:
                logging.error(f"Download or processing error for {key}: {e}")
    
        return downloaded_and_deleted_keys

    prep_temp_tables = prep_temp_tables_task()
    get_file_keys = get_file_keys_task()
    filter_file_keys = filter_file_keys_task.expand(batch_id=get_file_keys)
    split_batches = split_files_into_batches_task(filter_file_keys)
    downloaded = retrieve_file_from_s3_task.expand(batch_id=split_batches)
    #delete_files = delete_file_from_s3_task(downloaded, task_params=dag_params)


    #prep_temp_tables >> get_file_keys >> filter_file_keys >> split_batches >> downloaded >> delete_files
    prep_temp_tables >> get_file_keys >> filter_file_keys >> split_batches >> downloaded

