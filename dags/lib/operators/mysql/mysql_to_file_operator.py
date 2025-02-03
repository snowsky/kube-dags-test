from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging


class MySqlToFileOperator(MySqlOperator):
    """
    Executes sql code in a specific MySQL database and downloads the results to a file.

    Arguments:
        
        task_id (str): The airflow task id.
        mysql_conn_id (str): The name of the MySQL connection ID.
        sql (str): the sql to be executed against the database. Rows returned by this SQL statement
            will be written to a file.
        output_file_path (str): the file path to which the results of running the SQL statement will
            be written. 
        *args: other positional args to be passed to [MySqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/_api/airflow/providers/mysql/operators/mysql/index.html#airflow.providers.mysql.operators.mysql.MySqlOperator)
        **kwargs: other positional args to be passed to MySqlOperator

    See `dags/examples/example_mysql_to_file.py` for an example DAG using this operator. 

    Note that:
        - the directory in which this file will be created must already exist when this operator
          is called. 
        - the file will be overwritten every time the operator runs.
        - the path in which the file will be written needs to be accessible to all airflow workers. In the example
          DAG this path is `/tmp`. This is usually NOT accessible to all airflow workers, except in the situation
          where a single worker is used (typical in a test environment). In other words, you should not be writing
          files to `/tmp` in a production DAG!
    """   

 
    def __init__(
        self, 
        task_id: str,
        mysql_conn_id: str,
        sql: str,
        output_file_path: str, 
        *args: str,
        **kwargs: str,
    ):
        super(MySqlToFileOperator, self).__init__(
            sql=sql, mysql_conn_id=mysql_conn_id, task_id=task_id, 
            *args, **kwargs
        )
        self.output_file_path = output_file_path

    def execute(self, context):
        hook = MySqlHook(mysql_conn_id=self.conn_id)
        dt = hook.get_pandas_df(self.sql)
        dt.to_csv(self.output_file_path)
        logging.info(f"Wrote {len(dt)} rows to {self.output_file_path}.")
