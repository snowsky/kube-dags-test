from populations.client_profiles.client_profie import ClientProfile
from populations.utils import execute_query


class ClientA(ClientProfile):
    def __init__(self):
        super().__init__()
        self._client_name = "Air Flow Client A"
        self._data_source = "clientdatashare.target_population"
        self._target_table = f"tp{self.client_name_fmt}"
        self._ending_db = 45
        self._unit_vault_facility_ids = "123456"
        self._mpi_crosswalk = False
        self._csg = True

    def process_population_data(self):
        self._query_population_data()
        self._query_and_insert_mpi()


    def _query_population_data(self):
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self._target_table};
                CREATE TABLE {self._schema}.{self._target_table} AS
                SELECT MPI, provider_first_name
                FROM {self._data_source} WHERE 1 = 0;
                """
        execute_query(query, self._conn_id)

    def _query_and_insert_mpi(self):
        mpi_source_table = 'pa_thirtysix_month_lookback'
        query = f"""
                INSERT INTO {self._schema}.{self._target_table} (MPI)
                (SELECT MPI FROM clientresults_airflow.{mpi_source_table}
                WHERE unit_id IN ({self._unit_vault_facility_ids})
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
        return self._target_table

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
