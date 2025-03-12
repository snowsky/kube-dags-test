

def execute_query(query, conn_id):
    engine = _get_engine_from_conn(conn_id)
    with engine.connect() as con:
        result = con.execute(query)
    return result

def _get_engine_from_conn(conn_id):
    from airflow.providers.mysql.hooks.mysql import MySqlHook as hook
    db_hook = hook(mysql_conn_id=conn_id)
    engine = db_hook.get_sqlalchemy_engine()
    engine = _fix_engine_if_invalid_params(engine)
    return engine

def output_df_to_target_tbl(output_df, schema, target_table, conn_id):
    engine = _get_engine_from_conn(conn_id)
    output_df.to_sql(name=target_table, con=engine, schema=schema, if_exists='replace', chunksize=5000, index_label='id')

def _fix_engine_if_invalid_params(engine):
    invalid_param = '__extra__'
    query_items = engine.url.query.items()
    extra = {}
    for k, v in query_items:
        if k == invalid_param:
            extra = v
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
        logging.info(f'Extra: {extra}')
        #engine = create_engine(modified_url, connect_args=extra)
        #engine = create_engine(modified_url, connect_args={"ssl": {"ca": "/home/jovyan/ssl/PRD-AZ1-SQLW3.pem"}})
        engine = create_engine(modified_url, connect_args={"ssl": {"ca": "/source-biakonzasftp/C-126/test/prd-az1-sqlw3.crt.pem"}})
    return engine

def get_latest_csv(source_path):
    import logging
    from os import listdir, path
    files = [path.join(source_path, f) for f in listdir(source_path) if ".csv" in f and not "~lock" in f]
    logging.info(f"Found files: {files}")
    latest_file = max(files, key=path.getctime)
    logging.info(f"Selected file: {latest_file}")
    return latest_file
