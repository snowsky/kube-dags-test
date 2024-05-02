import paramiko
import pandas as pd
import subprocess
import os
import sys
import shutil
import time
import datetime as dt
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import imp
from time import gmtime, strftime

select_only = 'select_only_disabled' #Skips the join query and goes straight to the existing results table, 'select_only_disabled' or 'select_only'
current_file_name = 'HDI4 - Clinical Dashboards Extract Delivery Prep - All Servers.py'
Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Login.py')
_dashboard_requests_tables = imp.load_source('_dashboard_requests_tables',
                                                  '//prd-az1-sqlw2.ad.konza.org/Batch/py ETL/Reference Variables/_dashboard_requests_tables.py')
#inclusionList = ['24']
inclusionList = [ '18', '17', '24']
#inclusionList = ['18', '24']
#inclusionList = ['17']
#inclusionList = ['4', '5', '6']
for index, server_id in _dashboard_requests_tables.dfAllServerIDs.iterrows():
    if str(server_id[0]) not in inclusionList:
        continue
    destServer = str(server_id[0])
    print(destServer)
    #destServer = '8'
    try:
        proc1 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _patient_account_parquet_pm "' + select_only + '" "accid" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc2 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _vitalsigns_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc3 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _patient_diagnosis_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc4 = subprocess.call(
            r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _patient_contact_parquet_pm "' + select_only + '" "patient_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc5 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_LR_manual_categories.py" _lab_result_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc6 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _patient_allergy_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc7 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _immunization_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc8 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _patient_insurance_parquet_pm "' + select_only + '" "accid" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc9 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _vaccination_parquet_pm "' + select_only + '" "immunization_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc10 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _mpi_id_master_parquet_pm "' + select_only + '" "id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc11 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _unit_parquet_pm "' + select_only + '" "None" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc12 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _battery_parquet_pm "' + select_only + '" "None" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc13 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _lab_test_parquet_pm "' + select_only + '" "None" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc14 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _patient_type_parquet_pm "' + select_only + '" "None" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc15 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _provider_parquet_pm "' + select_only + '" "None" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        proc16 = subprocess.call(r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _medication_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "100000" "' + destServer + '" "' + current_file_name + '"')
        proc17 = subprocess.call(
            r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.9_manual_categories.py" _patient_procedure_parquet_pm "' + select_only + '" "patient_account_id" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
        #proc18 = subprocess.call(
        #    r'C:\Python37\python37.exe "\\prd-az1-sqlw2.ad.konza.org\batch\py ETL\HDI Cluster\Hive_ODBC_4.8_manual_categories.py" _mpi_parquet_pm "' + select_only + '" "accid_ref" "CSV" "1" "500000" "' + destServer + '" "' + current_file_name + '"')
    except TypeError:
        pass

### Test killing the processes - closing the console does not seem to have desired impact
#processlisttokill = [
#    proc1,
#    proc2,
#    proc3,
#    proc4,
#    proc5,
#    proc6 ,
#    proc7 ,
#    proc8 ,
#    proc9 ,
#    proc10,
#    proc11,
#    proc12,
#    proc13,
#    proc14,
#    proc15,
#    proc16,
#    proc17
#]
#
#for p in processlisttokill:
#    p.kill()
#    # or proc1.kill() #for each
