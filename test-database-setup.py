from airflow import settings
from airflow.models import Connection
import logging

connection_ids = ['MariaDB', 'prd-az1-sqlw2-airflowconnection']

for connection_id in connection_ids:
    conn = Connection(
        conn_id=connection_id,
        conn_type='mysql',
        host='mariadb',
        login='root',
        password='example',
        port=3306
    ) #create a connection object
    session = settings.Session() # get the session
    session.add(conn)
    session.commit() # it will insert the connection object programmatically.  
    logging.info(f"Added {connection_id} dummy connection.")  
