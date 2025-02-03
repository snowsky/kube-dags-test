import logging
import time
import trino
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

class KonzaTrinoOperator(PythonOperator):

    def __init__(self, query, **kwargs):

        def execute_trino_query(**kwargs):
            ds = kwargs['ds']
            # Retrieve the connection details
            conn = BaseHook.get_connection('trinokonza')
            host = conn.host
            port = conn.port
            user = conn.login
            schema = conn.schema

            # Connect to Trino
            trino_conn = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog='hive',
                schema=schema,
            )
            cursor = trino_conn.cursor()

            try:
                # the .replace is a no-op if ds not present in query
                cursor.execute(query.replace('<DATEID>', ds))
                print(f"Executed query: {query}")
                logging.info(f'Executed query: {query}')
                # Check the status of the query
                query_id = cursor.query_id
                cursor.execute(f"SELECT state FROM system.runtime.queries WHERE query_id = '{query_id}'")
                status = cursor.fetchone()[0]
                
                if status != 'FINISHED':
                    time.sleep(5)
                    cursor.execute(f"SELECT state FROM system.runtime.queries WHERE query_id = '{query_id}'")
                    status = cursor.fetchone()[0]
                    if status != 'FINISHED':
                        # Get the number of active workers
                        cursor.execute("SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false")
                        active_workers = cursor.fetchone()[0]
                        print(f"Number of active workers: {active_workers}")
                        logging.info(f'Number of active workers: {active_workers}')
                        raise AirflowException(f"Query did not finish successfully. Status: {status} - Query: {query}")
                

            except trino.exceptions.TrinoQueryError as e:
                raise AirflowException(f"Query failed: {str(e)}")
            finally:
                cursor.close()
                trino_conn.close()

        super(KonzaTrinoOperator, self).__init__(
            python_callable=execute_trino_query,
            provide_context=True,
            **kwargs
        )
