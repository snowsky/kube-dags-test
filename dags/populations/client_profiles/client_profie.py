from abc import ABC, abstractmethod
from populations.utils import execute_query
from airflow.models import Variable
import logging

class ClientProfile(ABC):
    def __init__(
        self, 
        client_name: str, 
        ending_db: int,
        conn_id="prd-az1-sqlw2-airflowconnection", 
        schema="temp", 
        data_source="clientdatashare.target_population",
        target_table_prefix="tp",
        mpi_crosswalk=False,
        csg=True,
        delimiter=",",
    ):
        #  Properties look to be the same across all clients
        self._conn_id = conn_id
        #self._conn_id = "qa-az1-sqlw3-airflowconnection"
        #self._conn_id_sqlw2 = "prd-az1-sqlw2-airflowconnection"
        self._schema = schema
        self._client_name = client_name
        self._data_source = data_source
        self._target_table_prefix = target_table_prefix
        self._ending_db = ending_db
        self._mpi_crosswalk = mpi_crosswalk
        self._csg = csg
        self._delimiter = delimiter

    #@abstractmethod
    #def process_population_data(self):
    #    pass

    def process_population_data(self, facility_ids: str):
        self._query_population_data()
        self._query_and_insert_mpi(facility_ids)
        logging.info(f'facility ids should be {facility_ids}')
    
    
    def _query_population_data(self):
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table};
                CREATE TABLE {self._schema}.{self.target_table} AS
                SELECT MPI, provider_first_name
                FROM {self._data_source} WHERE 1 = 0;
                """
        execute_query(query, self._conn_id)

    
    def _query_and_insert_mpi(self, facility_ids):
        mpi_source_table = 'pa_thirtysix_month_lookback'
        #facility_ids_unpiped = Variable.get(facility_ids.replace('|', ','), deserialize_json=True) 
        #facility_ids_unpiped = facility_ids.replace('|', ',')
        quoted_values = ', '.join([f"'{value}'" for value in facility_ids.split(self._delimiter)])
        #logging.info(f'facility ids unpiped should be {facility_ids_unpiped}')
        logging.info(f'facility ids quote should be {quoted_values}')
        query = f"""
                INSERT INTO {self._schema}.{self.target_table} (MPI)
                (SELECT MPI FROM temp.{mpi_source_table}
                WHERE unit_id IN ({quoted_values})
                AND admitted > DATE_SUB(now(), INTERVAL 18 MONTH)
                AND MPI <> '999999999999999'
                group by MPI);
                """
        execute_query(query, self._conn_id)
    
    @property
    def client_name(self):
        return self._client_name

    #@property
    #@abstractmethod
    #def target_table(self):
    #    pass

    @property
    def target_table(self):
        return f"{self._target_table_prefix}{self.client_name_fmt}"
    
    @property
    def ending_db(self):
        return self._ending_db

    @property
    def mpi_crosswalk(self):
        return self._mpi_crosswalk

    @property
    def csg(self):
        return self._csg

    @property
    def conn_id(self):
        return self._conn_id

    @property
    def schema(self):
        return self._schema

    @property
    def client_name_fmt(self):
        return self.client_name.replace(' ', '').lower()
