from populations.client_profiles.client_profie import ClientProfile
from populations.utils import execute_query
from airflow.models import Variable
import logging


class Womens_Health_Connecticut_LLC(ClientProfile):
    def __init__(self):
        super().__init__()
        self._client_name = "Womens Health Connecticut LLC"
        self._data_source = "clientdatashare.target_population"
        self._target_table_prefix = "tp"
        self._ending_db = 18
        self._mpi_crosswalk = False
        self._csg = True

    def process_population_data(self, facility_ids):
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
        facility_ids_unpiped = facility_ids.replace('|', ',')
        quoted_values = ', '.join([f"'{value}'" for value in facility_ids_unpiped.split(',')])
        logging.info(f'facility ids unpiped should be {facility_ids_unpiped}')
        logging.info(f'facility ids unpiped quote should be {quoted_values}')
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
    def client_name_fmt(self):
        return self.client_name.replace(' ', '').lower()
        
