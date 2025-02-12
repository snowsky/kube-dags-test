from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.utils.email import send_email_smtp  # Use send_email_smtp
from airflow.models import Connection
from datetime import datetime
import pandas as pd
import os
import logging
from airflow.utils.email import send_email


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 16),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'fetch_production_failure',
    default_args=default_args,
    description='Fetch data from dbo.production_failure and alert if new production failure is logged',
    schedule_interval='0 */4 * * *',  # Runs every 4 hours
    tags=['SLA-33'],
)

CONN_ID = 'formoperations_prd_az1_opssql_database_windows_net'
LAST_PROCESSED_ID_FILE = "/data/biakonzasftp/SLA-33/last_processed_id.txt"
EMAIL_RECIPIENT = "aagarwal@konza.org"

@task(dag=dag)
def fetch_and_check_production_failure():
    # Create a hook for the connection
    mssql_hook = MsSqlHook(mssql_conn_id=CONN_ID)

    # SQL query to fetch the latest data
    query = "SELECT * FROM dbo.production_failure ORDER BY id DESC"

    # Execute the query and fetch the data
    try:
        connection = mssql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        logging.info(f"Fetched data:\n{df}")
        cursor.close()
        connection.close()
    except Exception as e:
        raise Exception(f"Error fetching data: {e}")

    # Check if the last processed ID file exists
    last_processed_id = 0
    if os.path.exists(LAST_PROCESSED_ID_FILE):
        with open(LAST_PROCESSED_ID_FILE, 'r') as f:
            last_processed_id = int(f.read().strip())

    # Get the latest ID from the current fetch
    latest_id = df['id'].max()

    # Compare and send alert if new data exists
    if latest_id > last_processed_id:
        logging.info(f"New production failure detected. Latest ID: {latest_id}, Last Processed ID: {last_processed_id}")

        # Save the new latest ID to the file
        with open(LAST_PROCESSED_ID_FILE, 'w') as f:
            f.write(str(latest_id))
        
        # Send email alert for the new production failure
        #subject = "New Production Failure Logged"
        
        send_email(
        to='aagarwal@konza.org',
        subject=f'New PF detected',
        html_content=f"New PF detected"
        )
            #send_email_smtp(to=[EMAIL_RECIPIENT], subject=subject, html_content=body)
            #logging.info("Email alert sent successfully.")
    else:
        logging.info(f"No new production failure detected. Latest ID: {latest_id}, Last Processed ID: {last_processed_id}")

fetch_and_check_production_failure()
