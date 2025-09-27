from populations.client_profiles.client_profie import ClientProfile
from populations.client_profiles.internally_delivered_client_profile import InternallyDeliveredClientProfile
from populations.utils import execute_query
from airflow.models import Variable
import pandas as pd 
import logging
from airflow.hooks.base import BaseHook
import pymysql

class CsvInternallyDeliveredClientProfile(InternallyDeliveredClientProfile):

    def _get_dataframe(self):
        return pd.read_csv(self.input_file_path) ### Change to read_excel when this is available ###
    