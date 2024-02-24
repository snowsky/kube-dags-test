import typing
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from airflow.operators.python import get_current_context
import pendulum


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["example3", "example2"],
    params={"example_key": "example_value"},
)
def example_bash_operator():
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="cat /source/test_file",
    )

    run_this >> run_this_last

    @task()
    def runme(i):
        context = get_current_context()
        ti = typing.cast(TaskInstance, context.get("task_instance"))
        print(f"{ti.key} && sleep 1")

    for i in range(3):
        task_repetition = runme(i)

        task_repetition >> run_this

    @task(task_id="also_run_this")
    def also_run_this():
        context = get_current_context()
        ti = typing.cast(TaskInstance, context.get("task_instance"))
        print(ti.key)

    also_run_this() >> run_this_last

    @task(
        task_id="this_will_skip",
    )
    def this_will_skip():
        print('echo "hello world"; exit 99;')

    this_will_skip() >> run_this_last


example_bash_operator_dag = example_bash_operator()

if __name__ == "__main__":
    example_bash_operator_dag.test()
