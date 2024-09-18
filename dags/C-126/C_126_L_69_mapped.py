from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from os import path, makedirs

SOURCE_FILES_DIRECTORY='/data/biakonzasftp/C-126/L-69/source/'
DEST_FILES_DIRECTORY='/data/biakonzasftp/C-126/L-69/dest/'

default_args = {
    'owner': 'airflow',
}
with DAG(
    dag_id='C_126_L_69_mapped',
    default_args=default_args,
    schedule=None,
    tags=['example', 'C-126', 'L-69'],
    params={
        "source_files_dir_path": Param(SOURCE_FILES_DIRECTORY, type="string"),
        "output_files_dir_path": Param(DEST_FILES_DIRECTORY, type="string")
    },
) as dag:
    @task
    def diff_files_task(params:dict):
        source_files = _get_files_from_dir(params['source_files_dir_path'])
        dest_files = _get_files_from_dir(params['output_files_dir_path'])
        diff_files = [f for f in source_files if f not in dest_files]
        return diff_files

    def _get_files_from_dir(target_dir):
        import glob
        file_paths = [f.replace(target_dir, '') for f in glob.iglob(f'{target_dir}/**/*', recursive=True) if path.isfile(f)]
        return file_paths

    @task(map_index_template="{{ input_file }}")
    def copy_file_task(input_file, params:dict):
        import shutil
        context = get_current_context()
        context["input_file"] = input_file
        input_file_path = path.join(params['source_files_dir_path'], input_file)
        dest_file_path = path.join(params['output_files_dir_path'], input_file)
        makedirs(path.dirname(dest_file_path), exist_ok=True)
        shutil.copy2(input_file_path, dest_file_path)

    @task(map_index_template="{{ input_file }}")
    def upload_file_to_s3_task(input_file, aws_conn_id, aws_key_pattern, aws_bucket_name, s3_hook_kwargs, params:dict):
        context = get_current_context()
        context["input_file"] = input_file
        input_file_path = path.join(params['source_files_dir_path'], input_file)
        aws_key = aws_key_pattern
        # Add/edit replacements depending upon aws key pattern.
        replacements = {"{input_file}": input_file, "{input_file_stub}": input_file.split('/', 1)[-1]}
        for r in replacements:
            aws_key = aws_key.replace(r, replacements[r])
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_hook.load_file(
            filename=input_file_path,
            key=aws_key,
            bucket_name=aws_bucket_name,
            **s3_hook_kwargs
        )

    diff_files = diff_files_task()
    copy_file = copy_file_task.expand(input_file=diff_files)
    transfer_file_to_s3 = upload_file_to_s3_task.expand(input_file=diff_files,
                                                        aws_conn_id=['konzaandssigrouppipelines'],
                                                        aws_key_pattern=['HL7v3Out/HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69/Office Practicum/{input_file}'],
                                                        aws_bucket_name=['konzaandssigrouppipelines'],
                                                        s3_hook_kwargs = [{}])
    transfer_file_to_s3_nashville = upload_file_to_s3_task.expand(input_file=diff_files,
                                                                  aws_conn_id=['konzaandssigrouppipelines'],
                                                                  aws_key_pattern=['clientName=KONZA/source=L-69/status=pending/domainOid={input_file_stub}'],
                                                                  aws_bucket_name=['com-ssigroup-insight-attribution-data'],
                                                                  s3_hook_kwargs=[{'encrypt': True, 'acl_policy':'bucket-owner-full-control'}])

    # This establishes order of the stages. Comment to run all in parallel.
    diff_files >> copy_file >> (transfer_file_to_s3, transfer_file_to_s3_nashville)
