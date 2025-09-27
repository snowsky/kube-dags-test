from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.email import send_email
from datetime import datetime
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 17),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'reset_crawler_started_running',
    default_args=default_args,
    description='Update started_running=0 for rows running > 2 hours and email if anything changed',
    schedule='@hourly',
    catchup=False,
    tags=['C-181'],
)

@task(dag=dag)
def process_rows():
    sql_hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')

    # Step 1: Fetch all rows
    query = "SELECT * FROM _dashboard_maintenance.crawler_reference_table;"
    df = sql_hook.get_pandas_df(query)

    if df.empty:
        logging.info("Table is empty.")
        return

    updated_rows = []

    for i, row in df.iterrows():
        logging.info(f"Row {i+1}:\n{row}")  # Log the full row as-is

        if row['started_running'] == 1:
            # Check how long it has been running
            check_diff_query = f"""
                SELECT TIMESTAMPDIFF(MINUTE, '{row['event_timestamp']}', NOW()) AS diff;
            """
            minutes = sql_hook.get_first(check_diff_query)[0]

            if minutes > 120:
                logging.info(f"→ Updating row ID {row['id']} (started_running = 1 for {minutes} minutes)")
                update_query = f"""
                    UPDATE _dashboard_maintenance.crawler_reference_table
                    SET started_running = 0
                    WHERE id = {row['id']};
                """
                sql_hook.run(update_query)
                updated_rows.append(row)
            else:
                logging.info(f"→ Row ID {row['id']} has been running for only {minutes} minutes — skipping.")
        else:
            logging.info(f"→ Row ID {row['id']} already has started_running = 0 — skipping.")

    # Step 2: Send summary email if updates were made
    if updated_rows:
        df_updated = pd.DataFrame(updated_rows)

        html_rows = "".join([
            f"<tr>{''.join([f'<td>{str(val)}</td>' for val in row])}</tr>"
            for _, row in df_updated.iterrows()
        ])
        html_header = "".join([f"<th>{col}</th>" for col in df_updated.columns])
        html_content = f"""
        <html><body>
        <p>The following crawler rows were reset (started_running → 0):</p>
        <table border="1" cellpadding="4" cellspacing="0">
            <tr>{html_header}</tr>
            {html_rows}
        </table>
        <p><strong>DAG:</strong> reset_crawler_started_running</p>
        </body></html>
        """

        send_email(
            #to='aagarwal@konza.org',
            to=['ethompson@konza.org','tlamond@konza.org','ddooley@konza.org','cclark@konza.org','jdenson@konza.org']
            subject=f"[KONZA] {len(updated_rows)} Crawler(s) Reset from started_running = 1",
            html_content=html_content
        )
        logging.info("Email notification sent.")
    else:
        logging.info("No updates made — no email sent.")

# Run task
process_rows()
