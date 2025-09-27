from populations.client_profiles.client_profie import ClientProfile
from populations.utils import execute_query, _get_engine_from_conn
from airflow.models import Variable
import pandas as pd 
import logging
from airflow.hooks.base import BaseHook
import pymysql
from abc import abstractmethod
import datetime
from sqlalchemy import create_engine

class InternallyDeliveredClientProfile(ClientProfile):
    def __init__(self, input_file_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file_path = input_file_path

    def _get_mysql_connection(self, connection_name='prd-az1-sqlw2-airflowconnection', char_set='utf8mb4', cursor_type=pymysql.cursors.DictCursor):
        connection = BaseHook.get_connection(connection_name)
        return pymysql.connect(
            host=connection.host, 
            user=connection.login, 
            password=connection.password,
            charset=char_set, 
            cursorclass=cursor_type
        )
    
    def _get_mysql_engine(self, connection_name='prd-az1-sqlw2-airflowconnection'):
        return _get_engine_from_conn(connection_name)
    
    def _get_mpi(self, firstname, lastname, dob, gender, cursor):
        sqlQuery = "select person_master.fn_getmpi('" + lastname + "','" + firstname + "','" + dob + "','" + gender + "');"
        cursor.execute(sqlQuery)
        # Fetch all the rows
        results = cursor.fetchall()
        if len(results) != 1:
            raise ValueError(f"expected exactly 1 MPI row: returned {len(results)} rows")
        mpi_val = str(results[0]).split(":")[1][1:-1]
        return mpi_val if mpi_val != '0' else ''

    def _get_mpis_for_dataframe(self, df, connection):
        cursor = connection.cursor()
        mpis = []
        for i, row in df.iterrows():
            ### cursor portion ###
            firstname = str(row['Patient First Name']).replace("'", r"\'")  # '' returns 0
            lastname = str(row['Patient Last Name']).replace("'", r"\'")
            #dob = datetime.datetime.strptime(str(row['DOB']), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")  # '19960801'
            #dob = datetime.datetime.strptime(str(row['DOB']), "%Y-%m-%d").strftime("%Y%m%d")  # '19960801'
            dob_str = str(row['DOB'])
            try:
                dob = datetime.datetime.strptime(dob_str, "%m/%d/%Y").strftime("%Y%m%d")  # '19960801'
            except ValueError as e:
                raise ValueError(f"problem processing DOB '{dob_str}' for row {i}: {e}")
            gender = str(row['Gender   (M or F)'])  # 'F'
            mpi = self._get_mpi(firstname, lastname, dob, gender, cursor)
            mpis.append(mpi)
        cursor.close()
        return mpis
    
    @abstractmethod
    def _get_dataframe(self):
        pass 
    
    def process_population_data(self, facility_ids: str):
        ## STEP 1 would be to create dataframe from self.csv_path ##
        ## STEP 2 for every row in the dataframe call function to get mpi and store it in dataframe ##
        ## STEP 3 Insert MPI's in to target table ##
        df = self._get_dataframe()
        connection = self._get_mysql_connection()
        df['mpi'] = self._get_mpis_for_dataframe(df, connection)
        connection.close()
        engine = self._get_mysql_engine()
        connection = engine.connect()
        logging.info(f'writing to {self._schema}.{self.target_table} using {connection}')
        df.to_sql(name=self.target_table, con=connection, schema=self._schema, if_exists='replace', chunksize=100)
        connection.close()


