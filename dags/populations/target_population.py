import sys
sys.path.append("/opt/airflow/dags/repo/dags")
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.decorators import task

from populations.target_population_impl import process_csv, get_latest_csv, get_client_profile, \
     output_df_to_target_tbl

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
    def target_population_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'])
        csv_path = get_latest_csv(client_profile.source_directory)
        output_df = process_csv(csv_path)
        output_df_to_target_tbl(output_df, client_profile.schema, client_profile.target_table,
                                client_profile.conn_id)


    @task(map_index_template="{{ client_name }}")
    def extract_client_security_groupings_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'])
        approved_suffix = '_approved' if client_profile.approved else ''
        mysql_op = MySqlOperator(
            task_id='extract_client_security_groupings',
            mysql_conn_id=client_profile.conn_id,
            sql=f"""
            insert into clientresults.client_security_groupings{approved_suffix} (`Client`, MPI, Provider_lname, server_id)
            select '{client_profile.client_name}', TP.`MPI`, TP.`provider_first_name`,'{client_profile.ending_db}' from {client_profile.schema}.{client_profile.target_table} TP
            group by MPI;
            """,
            dag=dag
        )
        mysql_op.execute(dict())

    @task(map_index_template="{{ client_name }}")
    def extract_mpi_crosswalk_task(**kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'])
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

    def clear_results_table_by_client_task(table_name, **kwargs):
        client_profile = _get_client_profile_and_map_index(kwargs['dag_run'])
        approved_suffix = '_approved' if client_profile.approved and table_name == csg_table else ''
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

    def _get_client_profile_and_map_index(dag_run):
        client_name = dag_run.conf.get('client_name')
        context = get_current_context()
        context["client_name"] = client_name
        client_profile = get_client_profile(client_name)
        return client_profile


    target_population = target_population_task()
    clear_results_table_by_client_csg = \
        task(task_id=f'clear_results_table_by_client-{csg_table}',
             map_index_template="{{ client_name }}")(clear_results_table_by_client_task)(table_name=csg_table)
    clear_results_table_by_client_mpi_cw = \
        task(task_id=f'clear_results_table_by_client-{mpi_table}',
             map_index_template="{{ client_name }}")(clear_results_table_by_client_task)(table_name=mpi_table)
    extract_client_security_groupings = extract_client_security_groupings_task()
    extract_mpi_crosswalk = extract_mpi_crosswalk_task()

    target_population >> (clear_results_table_by_client_csg >> extract_client_security_groupings,
                          clear_results_table_by_client_mpi_cw >> extract_mpi_crosswalk)
