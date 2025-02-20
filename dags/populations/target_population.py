import sys
sys.path.append("/opt/airflow/dags/repo/dags")
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

from populations.client_factory import get_client_profile

csg_table = 'client_security_groupings'
mpi_table = 'client_extract_mpi_crosswalk'
default_args = {
    'owner': 'airflow',
}
with DAG(
    'target_population',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions'],
) as dag:

    @task(map_index_template="{{ client_name }}")
    def assess_client_frequency_task(**kwargs):
        _map_index(kwargs['dag_run'].conf.get('client_name'))
        client_frequency = kwargs['dag_run'].conf.get('frequency')
        approved_suffix = ['', '_approved'] if client_frequency == 'Approved' else ['']
        return approved_suffix

    @task(map_index_template="{{ client_name }}")
    def reset_client_frequency_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'].conf.get('client_name'))
        client_frequency = kwargs['dag_run'].conf.get('frequency')
        if client_frequency == 'Approved' or 'Revised':

            mysql_op = MySqlOperator(
                task_id='reset_client_frequency',
                mysql_conn_id=client_profile.conn_id,
                sql=f"""
                update _dashboard_requests.clients_to_process
                set frequency = ''
                where `folder_name` = '{client_profile.client_name}';
                """,
                dag=dag
            )
            mysql_op.execute(dict())
        else:
            raise AirflowSkipException

    @task(map_index_template="{{ client_name }}")
    def reset_client_frequency_hierarchy_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'].conf.get('client_name'))
        client_frequency = kwargs['dag_run'].conf.get('frequency')
        if client_frequency == 'Approved' or 'Revised':

            mysql_op = MySqlOperator(
                task_id='reset_client_frequency_hierarchy',
                mysql_conn_id=client_profile.conn_id,
                sql=f"""
                update clientresults.client_hierarchy
                set frequency = ''
                where `folder_name` = '{client_profile.client_name}';
                """,
                dag=dag
            )
            mysql_op.execute(dict())
        else:
            raise AirflowSkipException
            
    @task(map_index_template="{{ client_name }}", trigger_rule=TriggerRule.NONE_FAILED)
    def process_population_data_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'].conf.get('client_name'))
        client_profile.process_population_data(kwargs['dag_run'].conf.get('facility_ids'))
        import logging
        logging.info(kwargs['dag_run'].conf.get('facility_ids'))


    @task(map_index_template="{{ client_name }}", trigger_rule=TriggerRule.NONE_FAILED)
    def extract_client_security_groupings_task(approved_suffix, **kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'].conf.get('client_name'), approved_suffix)
        if not _skip_task(csg_table, client_profile):
            mysql_op = MySqlOperator(
                task_id='extract_client_security_groupings',
                mysql_conn_id=client_profile.conn_id,
                sql=f"""
                insert into clientresults.client_security_groupings{approved_suffix} (`Client`, MPI, Provider_lname, server_id)
                select '{client_profile.client_name}', TP.`MPI`, TP.`provider_first_name`,'{client_profile.ending_db}' from {client_profile.schema}.{client_profile.target_table} TP
                group by MPI, provider_first_name;
                """,
                dag=dag
            )
            mysql_op.execute(dict())

    @task(map_index_template="{{ client_name }}", trigger_rule=TriggerRule.NONE_FAILED)
    def extract_mpi_crosswalk_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'].conf.get('client_name'))
        if not _skip_task(mpi_table, client_profile):
            mysql_op = MySqlOperator(
                task_id='extract_mpi_crosswalk',
                mysql_conn_id=client_profile.conn_id,
                sql=f"""
                insert into clientresults.client_extract_mpi_crosswalk (`Client`, MPI, server_id,36_month_visit_reference_accid,ins_member_id,client_identifier,client_identifier_2)
                select '{client_profile.client_name}', TP.`MPI`,'{client_profile.ending_db}', '', ins_member_id,client_identifier,client_identifier_2 from {client_profile.schema}.{client_profile.target_table} TP
                #left join temp.pa_thirtysix_month_lookback TML on TP.MPI = TML.MPI
                where TP.MPI is not null and length(TP.MPI) > 1;
                """,
                dag=dag
            )
            mysql_op.execute(dict())

    def clear_results_table_by_client_task(table_name, approved_suffix, **kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'].conf.get('client_name'), approved_suffix)
        if not _skip_task(table_name, client_profile):
            mysql_op = MySqlOperator(
                task_id='clear_results_table',
                mysql_conn_id=client_profile.conn_id,
                sql=f"""
                delete from clientresults.{table_name}{approved_suffix}
                where `Client` = '{client_profile.client_name}'
                ;
                """,
                dag=dag
            )
            mysql_op.execute(dict())

    def _get_client_profile_and_map_index(client_name, index_suffix=''):
        _map_index(client_name + index_suffix)
        client_profile = get_client_profile(client_name)
        return client_profile

    def _map_index(index):
        context = get_current_context()
        context["client_name"] = index

    def _skip_task(table_name, client_profile):
        if table_name == csg_table:
            if client_profile.csg:
                return False
        elif table_name == mpi_table:
            if client_profile.mpi_crosswalk:
                return False
        raise AirflowSkipException

    assess_client_frequency = assess_client_frequency_task()
    reset_client_frequency = reset_client_frequency_task()
    reset_client_frequency_hierarchy = reset_client_frequency_hierarchy_task()
    target_population = process_population_data_task()
    clear_results_table_by_client_csg = \
        task(task_id=f'clear_results_table_by_client-{csg_table}',
             map_index_template="{{ client_name }}",
             trigger_rule=TriggerRule.NONE_FAILED)(clear_results_table_by_client_task).expand(table_name=[csg_table], approved_suffix=assess_client_frequency)
    clear_results_table_by_client_mpi_cw = \
        task(task_id=f'clear_results_table_by_client-{mpi_table}',
             map_index_template="{{ client_name }}",
             trigger_rule=TriggerRule.NONE_FAILED)(clear_results_table_by_client_task)(table_name=mpi_table, approved_suffix='')
    extract_client_security_groupings = extract_client_security_groupings_task.expand(approved_suffix=assess_client_frequency)
    extract_mpi_crosswalk = extract_mpi_crosswalk_task()

    assess_client_frequency >> reset_client_frequency >> reset_client_frequency_hierarchy >> target_population >>  (clear_results_table_by_client_csg >> extract_client_security_groupings,
                                                                                   clear_results_table_by_client_mpi_cw >> extract_mpi_crosswalk)

