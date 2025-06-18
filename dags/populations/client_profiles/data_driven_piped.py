from populations.client_profiles.client_profie import ClientProfile
from populations.utils import execute_query
from airflow.models import Variable
import logging

class DataDrivenPipedClientProfile(ClientProfile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #super().__init__()
        #self._client_name = client_name
        #self._data_source = "clientdatashare.target_population"
        #self._target_table_prefix = "tp"
        #self._ending_db = ending_db
        #self._mpi_crosswalk = False
        #self._csg = True

    #logging.info(f'client_name line 17 should be {client_name}')
    
    
    def process_population_data(self, facility_ids):
        self._query_population_data()
        self._query_population_data_delivery_table()
        self._query_and_insert_mpi(facility_ids)
        logging.info(f'facility ids should be {facility_ids}')
        self._query_to_remove_opt_outs()

    def _query_population_data(self):
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_prep;
                CREATE TABLE {self._schema}.{self.target_table}_prep AS
                SELECT MPI, provider_first_name
                FROM {self._data_source} WHERE 1 = 0;
                """
        execute_query(query, self._conn_id)
      
    def _query_population_data_delivery_table(self):
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
                INSERT INTO {self._schema}.{self.target_table}_prep (MPI)
                (SELECT MPI FROM temp.{mpi_source_table}
                WHERE unit_id IN ({quoted_values})
                AND admitted > DATE_SUB(now(), INTERVAL 18 MONTH)
                AND MPI <> '999999999999999'
                AND MPI <> '<NA>'
                group by MPI);
                """
        execute_query(query, self._conn_id)
    
    def _query_to_remove_opt_outs(self):
        logging.info(f'removing any opt outs before adding to the csg and csga tables')
        query = f"""
                INSERT INTO {self._schema}.{self.target_table} (MPI)
                (SELECT TP.MPI FROM {self._schema}.{self.target_table}_prep TP
                LEFT JOIN clientresults.opt_out_list OPT on TP.MPI = OPT.MPI
                WHERE OPT.MPI is null AND TP.MPI <> 'nan'
                AND TP.MPI <> '999999999999999'
                AND TP.MPI <> '<NA>');
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