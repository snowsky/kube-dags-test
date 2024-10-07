
def execute_query(query, conn_id):
    engine = _fix_engine_if_invalid_params(_get_engine_from_conn(conn_id))
    with engine.connect() as con:
        result = con.execute(query)
    return result

def _get_engine_from_conn(conn_id):
    from airflow.providers.mysql.hooks.mysql import MySqlHook as hook
    db_hook = hook(mysql_conn_id=conn_id)
    engine = db_hook.get_sqlalchemy_engine()
    return engine

def _fix_engine_if_invalid_params(engine):
    invalid_param = '__extra__'
    query_items = engine.url.query.items()
    if invalid_param in [k for (k, v) in query_items]:
        from sqlalchemy.engine.url import URL
        from sqlalchemy.engine import create_engine
        import logging

        modified_query_items = {k: v for k, v in query_items if k != invalid_param}
        modified_url = URL.create(
            drivername=engine.url.drivername,
            username=engine.url.username,
            password=engine.url.password,
            host=engine.url.host,
            port=engine.url.port,
            database=engine.url.database,
            query=modified_query_items
        )
        logging.info(f'Note: {invalid_param} removed from {query_items} in engine url')
        engine = create_engine(modified_url)
    return engine

