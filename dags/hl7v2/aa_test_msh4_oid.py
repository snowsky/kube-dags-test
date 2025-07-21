import re
import yaml
from typing import Dict, List, Union
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
#import hl7
#import os
#from airflow.operators.python import get_current_context
#from hl7v2.msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback

# Optional, if used in your utils file
MSH4_OID_CONFIG = 'hl7v2/msh4_oid.yaml'

# DAG definition
default_args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='hl7v2_msh4_oid',
    default_args=default_args,
    description='Parses HL7 files and resolves domain OID (MSH-4)',
    start_date=datetime(2025, 7, 10),
    catchup=False,
    max_active_runs=1,
    concurrency=100,
    tags=['hl7v2'],
    #params={"hl7_file_path": Param("", type="string", description="Full path to HL7 file")},
)

@task(dag=dag)
def extract_oid_from_hl7():
    """Reads the file path from DAG params, resolves the OID, and prints it."""
    import os
    import subprocess
    subprocess.check_output(["pip", "install", "hl7"])
    import hl7
    from airflow.operators.python import get_current_context
    from hl7v2.msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback


    #ctx = get_current_context()
    #file_path = ctx["params"]["hl7_file_path"]
    #file_path = "/data/biakonzasftp/C-179/OB To SSI EUID1/EUIDTOSSI_20250711160157376.txt"

    file_path = "/data/biakonzasftp/C-179/OB To SSI EUID1/fe69cf89-51e6-4469-9485-05fa65b1b0b3.hl7"

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"HL7 file not found: {file_path}")

    oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(file_path)
    print(f"Resolved OID: {oid}")
    return oid

# task
extract_oid_from_hl7()
