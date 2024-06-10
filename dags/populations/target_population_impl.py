import pandas as pd
from pydantic import ValidationError
from collections import namedtuple
from copy import copy
from populations.client_factory import ClientFactory

ColumnHeader = namedtuple('ColumnHeader', 'input output')
_attribute_header_map = {
    'patient_first_name': ColumnHeader('Patient First Name', 'first_name'),
    'patient_last_name': ColumnHeader('Patient Last Name', 'last_name'),
    'date_of_birth': ColumnHeader('DOB', 'dob'),
    'gender': ColumnHeader('Gender', 'sex'),
    'residential_address': ColumnHeader('Residential Address', 'street1'),
    'city': ColumnHeader('City', 'City'),
    'state': ColumnHeader('State', 'State'),
    'zip_code': ColumnHeader('Zip Code', 'zip'),
    'insurance_id': ColumnHeader('Insurance ID', 'ins_member_id'),
    'custom_text': ColumnHeader('CustomText1', 'client_identifier'),
    'attributed_provider_first_name': ColumnHeader('Attributed Facility Provider First Name', 'provider_first_name'),
    'attributed_provider_last_name': ColumnHeader('Attributed Facility Provider First Name', 'Attributed Physician Last Name'),
    'attributed_provider_npi': ColumnHeader('Attributed Provider NPI', 'Attributed Physician NPI'),
    'mpi': ColumnHeader(None, 'mpi'),
    'client_identifier_2': ColumnHeader(None, 'client_identifier_2')  # Remove?
}

output_df_attributes = [
    'patient_first_name',
    'patient_last_name',
    'date_of_birth',
    'gender',
    'residential_address',
    'city',
    'state',
    'zip_code',
    'mpi',
    'insurance_id',
    'custom_text',
    'attributed_provider_first_name',  # This has been added as referred to in SQL, but not in original py script.
    'client_identifier_2'   # This has been added as referred to in SQL, but not in original py script.
]

def _validate_data(data):
    from populations.pydantic_models.population_definition_entry import PopulationDefinitionEntry
    entry_list = []
    for index, row in data.dropna(subset=['Gender']).iterrows():
        try:
            popDef = PopulationDefinitionEntry(patient_first_name=row[_attribute_header_map['patient_first_name'].input],
                                               patient_last_name=row[_attribute_header_map['patient_last_name'].input],
                                               date_of_birth=row[_attribute_header_map['date_of_birth'].input],
                                               gender=row[_attribute_header_map['gender'].input],
                                               residential_address=row[_attribute_header_map['residential_address'].input],
                                               city=row[_attribute_header_map['city'].input],
                                               state=row[_attribute_header_map['state'].input],
                                               zip_code=row[_attribute_header_map['zip_code'].input],
                                               insurance_id=row[_attribute_header_map['insurance_id'].input],
                                               custom_text=row[_attribute_header_map['custom_text'].input],
                                               attributed_provider_first_name=row[_attribute_header_map['attributed_provider_first_name'].input],
                                               attributed_provider_last_name=row[_attribute_header_map['attributed_provider_last_name'].input],
                                               attributed_provider_npi=row[_attribute_header_map['attributed_provider_npi'].input]
                                               )
            entry_list.append(popDef)
        except ValidationError as e:
            raise ValueError(f"Error on row {index} - {str(e)}")
    return entry_list


def _populate_mpi(entry):
    sql_query = _generate_mpi_query(entry)
    #Replace 'MariaDB' with Konza conn id.
    *_, last = _execute_query(sql_query, 'MariaDB')
    entry.mpi = last[0]  # This returns last row as in orig script.
    return entry.mpi


def _generate_mpi_query(entry):
    first_name = entry.patient_first_name.replace("'", r"\'")
    last_name = entry.patient_last_name.replace("'", r"\'")
    dob = str(entry.date_of_birth).replace("-", "")
    schema = 'person_master'
    return f"select {schema}.fn_getmpi('{last_name}','{first_name}','{dob}','{entry.gender}');"


def _execute_query(query, conn_id):
    engine = _get_engine_from_conn(conn_id)
    with engine.connect() as con:
        result = con.execute(query)
    return result


def process_csv(input_csv_path):
    import pandas as pd
    df = pd.read_csv(input_csv_path)
    entry_list = _validate_data(df)
    for i in reversed(range(len(entry_list))):
        mpi = _populate_mpi(entry_list[i])
        # this filters results based on mpi, replicates criteria from existing scripts.
        # 1) mpi cannot be 0
        # 2) mpi must be of a character length > 1
        if mpi < 10:
            del entry_list[i]
    output_df = create_output_df(entry_list)
    return output_df


def get_latest_csv(source_path):
    import logging
    from os import listdir, path
    files = [path.join(source_path, f) for f in listdir(source_path) if ".csv" in f and not "~lock" in f]
    logging.info(f"Found files: {files}")
    latest_file = max(files, key=path.getctime)
    logging.info(f"Selected file: {latest_file}")
    return latest_file


def create_output_df(entry_list):
    template_dict = {_attribute_header_map[attribute].output: None for attribute in output_df_attributes}
    rows = []
    for entry in entry_list:
        input_dict = copy(template_dict)
        for attribute in output_df_attributes:
            match attribute:
                case 'date_of_birth':
                    input_dict[_attribute_header_map['date_of_birth'].output] = entry.get_formatted_date()
                case 'client_identifier_2':  # Remove me?
                    input_dict['client_identifier_2'] = 'placeholder_id'
                case _:
                    input_dict[_attribute_header_map[attribute].output] = getattr(entry, attribute)
        rows.append(input_dict)
    return pd.DataFrame(rows)


def get_client_profile(client_name):
    return ClientFactory.get_client_profile(client_name)


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


def output_df_to_target_tbl(output_df, schema, target_table, conn_id):
    engine = _get_engine_from_conn(conn_id)
    output_df.to_sql(name=target_table, con=engine, schema=schema, if_exists='replace', chunksize=5000, index_label='id')


def _get_engine_from_conn(conn_id):
    from airflow.providers.mysql.hooks.mysql import MySqlHook as hook
    db_hook = hook(mysql_conn_id=conn_id)
    engine = _fix_engine_if_invalid_params(db_hook.get_sqlalchemy_engine())
    return engine
