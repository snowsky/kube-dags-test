from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os

DEFAULT_DEST_FILES_DIRECTORY = '/source-biakonzasftp/L-250/NEISS_Codes/'
LOOKUP_URL = "https://data.smartidf.services/explore/dataset/us-national-electronic-injury-surveillance-system-neiss-product-codes/download/?format=csv"

def download_neiss_codes():
    os.makedirs(DEFAULT_DEST_FILES_DIRECTORY, exist_ok=True)
    response = requests.get(LOOKUP_URL)
    file_path = os.path.join(DEFAULT_DEST_FILES_DIRECTORY, "neiss_product_codes.csv")
    with open(file_path, "wb") as f:
        f.write(response.content)
    print(f"Downloaded NEISS codes to {file_path}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "download_neiss_codes",
    default_args=default_args,
    description="Download NEISS product codes",
    schedule_interval="@yearly",
    catchup=False,
    tags=['L-250'],
) as dag:

    download_task = PythonOperator(
        task_id="download_neiss_codes",
        python_callable=download_neiss_codes,
    )

    download_task
