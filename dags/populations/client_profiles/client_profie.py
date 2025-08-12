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
        self._query_population_data_demo_info()
        self._query_population_data_delivery_table()
        self._query_and_insert_mpi(facility_ids)
        logging.info(f'facility ids should be {facility_ids}')
        self._query_to_remove_opt_outs()
        self._query_to_add_demographic_info()
        self._query_to_add_demographic_info_prep_table()
        self._query_to_add_demographic_info_prep_table_a()
        self._query_to_add_demographic_info_prep_table_b()
        self._query_to_add_demographic_info_prep_table_interval()
        self._query_to_add_demographic_info_prep_table_interval_update()
        self._query_to_remove_duplicated_mpis()
    
    
    def _query_population_data(self):
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_prep;
                CREATE TABLE {self._schema}.{self.target_table}_prep AS
                SELECT MPI, first_name, last_name, dob, sex, provider_first_name
                FROM {self._data_source} WHERE 1 = 0;
                """
        execute_query(query, self._conn_id)

    def _query_population_data_demo_info(self):
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_demo_info;
                CREATE TABLE {self._schema}.{self.target_table}_demo_info AS
                SELECT MPI, first_name, last_name, dob, sex, provider_first_name
                FROM {self._data_source} WHERE 1 = 0;
                """
        execute_query(query, self._conn_id)
    
    def _query_population_data_delivery_table(self):
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table};
                CREATE TABLE {self._schema}.{self.target_table} AS
                SELECT MPI, first_name, last_name, dob, sex, provider_first_name
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
                INSERT INTO {self._schema}.{self.target_table}_demo_info (MPI)
                (SELECT TP.MPI FROM {self._schema}.{self.target_table}_prep TP
                LEFT JOIN clientresults.opt_out_list OPT on TP.MPI = OPT.MPI
                WHERE OPT.MPI is null AND TP.MPI <> 'nan'
                AND TP.MPI <> '999999999999999'
                AND TP.MPI <> '<NA>');
                """
        execute_query(query, self._conn_id)
    
    def _query_to_add_demographic_info(self):
        logging.info(f'adding the demographic info to deduplicate')
        query = f"""
                UPDATE {self._schema}.{self.target_table}_demo_info TP
                left join person_master._mpi_id_master MPI on MPI.id = TP.MPI
                set TP.first_name = MPI.firstname,
                    TP.last_name = MPI.lastname,
                    TP.dob = MPI.dob,
                    TP.sex = MPI.sex
                    ;
                """
        execute_query(query, self._conn_id)
    
    def _query_to_add_demographic_info_prep_table(self):
        logging.info(f'adding the demographic info to deduplicate demo prep table')
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_demographics_prep;
                CREATE TABLE {self._schema}.{self.target_table}_demographics_prep AS
                SELECT MPI, first_name, last_name, dob, sex FROM {self._schema}.{self.target_table}_demo_info
                GROUP BY MPI,first_name,last_name,dob,sex
                ORDER BY MPI ASC
                    ;
                """
        execute_query(query, self._conn_id)

    def _query_to_add_demographic_info_prep_table_a(self):
        logging.info(f'adding the demographic info to deduplicate prep a')
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_demographics_prep_a;
                CREATE TABLE {self._schema}.{self.target_table}_demographics_prep_a AS
                SELECT prep_a.*, @val1:=@val1+1 AS rn
                FROM {self._schema}.{self.target_table}_demographics_prep prep_a
                CROSS JOIN (SELECT @val1:=0) AS val1_init
                    ;
                """
        execute_query(query, self._conn_id)

    def _query_to_add_demographic_info_prep_table_b(self):
        logging.info(f'adding the demographic info to deduplicate prep b')
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_demographics_prep_b;
                CREATE TABLE {self._schema}.{self.target_table}_demographics_prep_b AS
                SELECT prep_b.*, @val2:=@val2+1 AS rn
                FROM {self._schema}.{self.target_table}_demographics_prep prep_b
                CROSS JOIN (SELECT @val2:=1) AS val2_init
                    ;
                """
        execute_query(query, self._conn_id)
        
    def _query_to_add_demographic_info_prep_table_interval(self):
        logging.info(f'adding the demographic info to deduplicate interval')
        query = f"""
                DROP TABLE IF EXISTS {self._schema}.{self.target_table}_interval;
                CREATE TABLE {self._schema}.{self.target_table}_interval AS
                SELECT 
                	#@row1:=@row1+1 AS id,
                	a.MPI MPI_A,
                	b.MPI MPI_B,
                	a.first_name A_first_name,
                	b.first_name B_first_name,
                	a.last_name A_last_name,
                	b.last_name B_last_name,
                	a.dob A_dob,
                	b.dob B_dob,
                	a.sex A_sex,
                	b.sex B_sex
    
                FROM
                (
                    SELECT * from {self._schema}.{self.target_table}_demographics_prep_a
                ) a
                INNER JOIN
                (
                	SELECT * from {self._schema}.{self.target_table}_demographics_prep_b
                ) b ON a.rn = b.rn
                #CROSS JOIN (SELECT @row1:=0) AS row1_init
                where a.first_name = b.first_name
                and  a.last_name = b.last_name
                and  a.dob = b.dob
                and  a.sex = b.sex
                    ;
                """
        execute_query(query, self._conn_id)

    def _query_to_add_demographic_info_prep_table_interval_update(self):
        logging.info(f'adding the demographic info to deduplicate interval update')
        query = f"""
                SET SQL_SAFE_UPDATES = 0;
                UPDATE {self._schema}.{self.target_table}_demo_info TT
                left join {self._schema}.{self.target_table}_interval INTV_A on INTV_A.MPI_A = TT.MPI
                set MPI = case when INTV_A.MPI_A > INTV_A.MPI_B THEN INTV_A.MPI_A when INTV_A.MPI_B > INTV_A.MPI_A THEN INTV_A.MPI_B ELSE TT.MPI end
                    ;
                """
        execute_query(query, self._conn_id)

    def _query_to_remove_duplicated_mpis(self):
        logging.info(f'removing the duplicated rows from mpi mapping')
        query = f"""
                INSERT INTO {self._schema}.{self.target_table}
                (SELECT * FROM {self._schema}.{self.target_table}_demo_info TP
                WHERE TP.sex <> '')
                ;
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
        return self.client_name.replace(' ', '').replace('(', '').replace(')', '').replace('-', '_').replace('#', '').lower()
