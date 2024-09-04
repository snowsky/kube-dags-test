from airflow import DAG
import os
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
from airflow.decorators import task
from airflow.models import Connection
import logging

default_args = {
    'owner': 'airflow',
}

PG_DATABASE = "postgres"
PG_CONN_ID = "data_export_conn_ad_admin" # Note access token for this IP currently expires every 60 mins. Look for solution to this.
DATA_EXPORT_DATABASE = "data-export"
DATA_EXPORT_CONN_ID = "data_export_conn"

with DAG(
    'create_data_export_user',
    default_args=default_args,
    schedule=None,
    tags=['data_export'],
    params={
    "service_principal_obj_id": Param("", type="string"),
    "schema": Param("", type="string")
     },
) as dag:
    def _create_sql_str_user(schema, serv_p_obj_id):
        role = f"{schema}_user"
        return f"""
        SELECT * from pg_catalog.pgaadauth_create_principal_with_oid(rolename => CAST('{role}' as text), objectid => CAST('{serv_p_obj_id}' as text), objecttype => CAST('service' as text), isadmin => false, ismfa => false);
        """
    
    def _create_sql_str_schema(schema, user):
        return f"""
        CREATE SCHEMA "{schema}" AUTHORIZATION "{user}";
        """

    def _create_sql_assign(schema):
        role = f"{schema}_user"
        return f"""
        GRANT USAGE ON SCHEMA "{schema}" TO "{role}";
        GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}";
        ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO "{role}";
        """

    @task
    def create_schema(params: dict):
        schema = params['schema']
        con = Connection.get_connection_from_secrets(DATA_EXPORT_CONN_ID)
        op = SQLExecuteQueryOperator(
            task_id="create_schema",
            conn_id=DATA_EXPORT_CONN_ID,
            sql=_create_sql_str_schema(schema, con.login),
        )
        op.execute(dict())

    @task
    def create_user(params: dict):
        schema = params['schema']
        serv_p_obj_id = params['service_principal_obj_id']
        op = SQLExecuteQueryOperator(
            task_id="create_user",
            conn_id=PG_CONN_ID,
            sql=_create_sql_str_user(schema, serv_p_obj_id),
        )
        op.execute(dict())

    @task
    def assign_role(params: dict):
        schema = params['schema']
        op = SQLExecuteQueryOperator(
            task_id="assign_role",
            conn_id=DATA_EXPORT_CONN_ID,
            sql=_create_sql_assign(schema),
        )
        op.execute(dict())


    create_schema_task = create_schema()
    create_user_task = create_user()
    assign_role_task = assign_role()
    
    create_user_task >> create_schema_task >> assign_role_task
