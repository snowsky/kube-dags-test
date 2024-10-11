from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from populations.common import CONNECTION_NAME

class ReturningMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.conn_id)
        return hook.get_records(
            self.sql,
            parameters=self.parameters
        )


default_args = {
    'owner': 'airflow',
}
with DAG(
    'create_population_definitions',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions'],
) as dag:

    @task(map_index_template="{{ client_name }}")
    def process_client(row, **kwargs):
        client_name = row[0]
        context = get_current_context()
        context["client_name"] = client_name
        trigger = TriggerDagRunOperator(
            task_id=f"target_pop_{client_name.replace(' ', '_')}",
            trigger_dag_id='target_population',
            conf={'client_name': client_name, 'frequency': row[1]},
            dag=dag,
        )
        trigger.execute(context=kwargs)


    clients_to_process = ReturningMySqlOperator(
        task_id='clients_to_process',
        mysql_conn_id=CONNECTION_NAME,
        sql=f"""
        SELECT folder_name, frequency
        FROM _dashboard_requests.clients_to_process_pf1447
        WHERE folder_name is not null and active = 1
        AND (
            frequency = 'Daily' OR
            frequency = 'Revised' OR
            frequency = 'Approved' OR
            frequency = 'Extract'
        )
        """,
        dag=dag
    )

    result = process_client.expand(row=clients_to_process.output)
