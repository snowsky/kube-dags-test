from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.settings import Session
from airflow.utils.dates import days_ago
from airflow.decorators import task


from populations.common import CONNECTION_NAME, EXAMPLE_SCHEMA_NAME

class ReturningMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.conn_id,
                         schema=EXAMPLE_SCHEMA_NAME)
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
    start_date=days_ago(2),
    tags=['example', 'population-definitions'],
) as dag:

    # TODO: add named mapping (available w/ Airflow 2.9.0)
    @task
    def process_client(row):
        import logging
        
        logging.info("We will process the following row:")
        logging.info(row)
        


    clients_to_process = ReturningMySqlOperator(
        task_id='clients_to_process',
        mysql_conn_id=CONNECTION_NAME, 
        sql=f"""
        SELECT folder_name, frequency 
        FROM _dashboard_requests__clients_to_process
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
