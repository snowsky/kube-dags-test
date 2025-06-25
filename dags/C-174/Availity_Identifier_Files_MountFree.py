from airflow import DAG
from airflow.decorators import task
from azure.storage.blob import BlobServiceClient
from airflow.models.param import Param
from itertools import islice
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException, AirflowSkipException
from lib.operators.azure_connection_string import get_azure_connection_string
from os import path, makedirs, remove
import shutil
from functools import partial
from datetime import datetime
import math
# If we want to utilise ProcessPoolExecutor we need to set
# AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER = true
from concurrent.futures import as_completed, ThreadPoolExecutor as PoolExecutor

DEFAULT_SOURCE_FILES_DIRECTORY = 'C-174/'#Dev'/data/biakonzasftp/C-174/Availity/UnZipped/' 
DEFAULT_DEST_FILES_DIRECTORY = 'C-194/archive_C-174/' #dev'/data/biakonzasftp/C-194/C-174/dest/'
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'
AZURE_CONNECTION_CONTAINER = 'airflow'
AZURE_CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)
DEFAULT_MAX_POOL_WORKERS = 5
DEFAULT_MAX_TASKS = 2000 # Divide MAX_FILES by 500 to limit number of files.  If too many tasks reduce MAX_FILES in parallel
MAX_FILES = 1_000_000
PARALLEL_TASK_LIMIT = 5  # Change this to large number of prod to remove parallel task limit

class BucketDetails:
    def __init__(self, aws_conn_id, aws_key_pattern, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.aws_key_pattern = aws_key_pattern
        self.s3_hook_kwargs = s3_hook_kwargs


AWS_BUCKETS = {
                #'konzaandssigrouppipelines':
                #   BucketDetails(aws_conn_id='konzaandssigrouppipelines',
                #                 aws_key_pattern='FromAvaility/{input_file}',
                #                 s3_hook_kwargs={}),
               'com-ssigroup-insight-attribution-data':
                   BucketDetails(aws_conn_id='konzaandssigrouppipelines',
                                 aws_key_pattern='subscriberName=KONZA/subscriptionName=Historical/source=Availity/status=pending/{input_file_replaced}',
                                 s3_hook_kwargs={'encrypt': True, 'acl_policy':'bucket-owner-full-control'})
              }


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
}
with DAG(
    dag_id='Availity_Identifier_Delivery_MountFree',
    default_args=default_args,
    schedule='@hourly',
    tags=['C-174'],
    catchup=False,
    max_active_runs=1,
    concurrency=PARALLEL_TASK_LIMIT,
    params={
        "source_files_dir_path": Param(DEFAULT_SOURCE_FILES_DIRECTORY, type="string"),
        "output_files_dir_path": Param(DEFAULT_DEST_FILES_DIRECTORY, type="string"),
        "max_pool_workers": Param(DEFAULT_MAX_POOL_WORKERS, type="integer", minimum=0),
        "max_mapped_tasks": Param(DEFAULT_MAX_TASKS, type="integer", minimum=0),
        "container_name": Param(AZURE_CONNECTION_CONTAINER, type="string", minimum=0),
        "transfer_to_konzaandssigrouppipelines_bucket": Param(True, type="boolean")
    },
) as dag:
    def list_blobs_in_directory(container_name, directory_path, max_files=MAX_FILES):
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=directory_path)
        return [blob.name for blob in islice(blob_list, max_files)]

    @task
    def diff_files_task(params: dict):  
        source_files = list_blobs_in_directory(params['container_name'], params['source_files_dir_path'])
        dest_files = list_blobs_in_directory(params['container_name'], params['output_files_dir_path'])
    
        unique_files = [f for f in source_files if f not in dest_files]
    
        if unique_files:
            return _split_list_into_batches(unique_files, params['max_mapped_tasks'])
        else:
            return []  

    def _split_list_into_batches(target_list,  max_tasks):
        if target_list:
            chunk_size = math.ceil(len(target_list) / max_tasks)
            batches = [target_list[i:i + chunk_size] for i in range(0, len(target_list), chunk_size)]
        else:
            batches = []
        return batches

    def _sanitise_input_directories(params):
        params['source_files_dir_path'] = _sanitise_dir(params['source_files_dir_path'])
        params['output_files_dir_path'] = _sanitise_dir(params['output_files_dir_path'])

    def _sanitise_dir(dir):
        prefix = "" if dir[0] == "/" else "/"
        suffix = "" if dir[-1] == "/" else "/"
        return f'{prefix}{dir}{suffix}'

    def _get_files_from_dir(target_dir):
        import glob
        file_paths = [f.replace(target_dir, '') for f in glob.iglob(f'{target_dir}/**/*', recursive=True) if path.isfile(f)]
        return file_paths

    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def copy_file_task(input_file_list, params: dict):
        max_workers = params['max_pool_workers']
        with PoolExecutor(max_workers=max_workers) as executor:
            future_file_dict = {executor.submit(partial(_copy_file, params), f): f for f in input_file_list}
        _, exceptions = _get_results_from_futures(future_file_dict)
        if exceptions:
            raise AirflowFailException(f'exceptions raised: {exceptions}')

    def _copy_file(params, file):
        input_file_path = path.join(params['source_files_dir_path'], file)
        
        # Create a subfolder with the current date in YYYYMMDD format
        date_folder = datetime.now().strftime('%Y%m%d')
        dest_file_path = path.join(params['output_files_dir_path'], date_folder, file)
        
        makedirs(path.dirname(dest_file_path), exist_ok=True)
        shutil.copy2(input_file_path, dest_file_path)
        
        # Delete the source file after successful copy
        remove(input_file_path)
        
        return file


    def _push_results_from_futures(future_file_dict):
        results, exceptions = _get_results_from_futures(future_file_dict)
        context = get_current_context()
        context['ti'].xcom_push(key='result', value=results)
        if exceptions:
            raise AirflowFailException(f'exceptions raised: {exceptions}')

    def _get_results_from_futures(future_file_dict):
        results = []
        exceptions = []
        for future in as_completed(future_file_dict):
            file = future_file_dict[future]
            try:
                results.append(future.result())
            except Exception as e:
                exceptions.append(str(f'{file}: {str(e)})'))
        return results, exceptions

    def create_upload_file_to_s3_task(bucket_name):
        @task(task_id=bucket_name)
        def upload_file_to_s3_task_def(input_file_list, aws_conn_id, aws_key_pattern, aws_bucket_name, s3_hook_kwargs,
                                   params: dict):
            max_workers = params['max_pool_workers']
            with PoolExecutor(max_workers=max_workers) as executor:
                future_file_dict = {executor.submit(partial(_upload_file_to_s3, params, aws_key_pattern, aws_conn_id,
                                                            aws_bucket_name, s3_hook_kwargs), f): f for f in
                                    input_file_list}
                _push_results_from_futures(future_file_dict)
        return upload_file_to_s3_task_def

    def _upload_file_to_s3(params, aws_key_pattern, aws_conn_id, aws_bucket_name, s3_hook_kwargs, file):
        input_file_path = path.join(params['source_files_dir_path'], file)
        aws_key = aws_key_pattern
        # Add/edit replacements depending upon aws key pattern.
        replacements = {"{input_file}": file, "{input_file_replaced}": file.replace('/','__')}
        for r in replacements:
            aws_key = aws_key.replace(r, replacements[r])
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_hook.load_file(
            filename=input_file_path,
            key=aws_key,
            bucket_name=aws_bucket_name,
            **s3_hook_kwargs
        )
        return file

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def identify_successful_transfers_task(transfer_task_ids, params: dict):
        # A successful transfer is a file that was successfully transferred to ALL target s3 accounts.
        context = get_current_context()
        mapped_task_results = context['ti'].xcom_pull(key='result', task_ids=transfer_task_ids)
        flat_list = [b for a in mapped_task_results for b in a]
        successful_transfers = {r for r in flat_list if flat_list.count(r) == len(transfer_task_ids)}
        return _split_list_into_batches(list(successful_transfers), params['max_mapped_tasks'])

    @task.branch
    def select_buckets_task(params: dict):
        buckets = AWS_BUCKETS
        if not params['transfer_to_konzaandssigrouppipelines_bucket']:
            buckets.pop('konzaandssigrouppipelines')
        return list(buckets.keys())

    diff_files = diff_files_task()
    select_buckets = select_buckets_task()
    transfer_file_to_s3_tasks = []
    for bucket in AWS_BUCKETS:
        upload_file_to_s3_task = create_upload_file_to_s3_task(bucket)
        transfer_file_to_s3 = upload_file_to_s3_task.expand(input_file_list=diff_files,
                                                            aws_conn_id=[AWS_BUCKETS[bucket].aws_conn_id],
                                                            aws_key_pattern=[AWS_BUCKETS[bucket].aws_key_pattern],
                                                            aws_bucket_name=[bucket],
                                                            s3_hook_kwargs=[AWS_BUCKETS[bucket].s3_hook_kwargs])
        transfer_file_to_s3_tasks.append(transfer_file_to_s3)
    identify_successful_transfers = identify_successful_transfers_task(transfer_task_ids=select_buckets)
    archive_transferred_file = copy_file_task.expand(input_file_list=identify_successful_transfers)

    diff_files >> select_buckets >> transfer_file_to_s3_tasks >> identify_successful_transfers >> archive_transferred_file
