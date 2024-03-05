from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="example_storage_account_and_s3_bucket",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 'Hello from the container, run_this_last!' >> /source/test.txt",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this_1 = BashOperator(
        task_id="also_run_this_1",
        bash_command="echo 'Hello from the container, also_run_this!' >> /source-s3-konzaandssigrouppipelines/test.txt",
    )
    # [END howto_operator_bash_template]
    also_run_this_1 >> run_this_last

    also_run_this_2 = BashOperator(
        task_id="also_run_this_2",
        bash_command="echo 'Hello from the container, also_run_this!' >> /source-s3-konzaandssigroupqa/test.txt",
    )
    also_run_this_2 >> run_this_last

    also_run_this_3 = BashOperator(
        task_id="also_run_this_3",
        bash_command="echo 'Hello from the container, also_run_this!' >> /source-s3-konzaandssigroup/test.txt",
    )
    also_run_this_3 >> run_this_last

# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()
