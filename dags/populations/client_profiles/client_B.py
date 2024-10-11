import pandas as pd
from pydantic import ValidationError
from collections import namedtuple
from copy import copy

from populations.client_profiles.client_profie import ClientProfile
from populations.utils import get_latest_csv, execute_query, output_df_to_target_tbl
from populations.pydantic_models.population_definition_entry import PopulationDefinitionEntry

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

class ClientB(ClientProfile):
    def __init__(self):
        super().__init__()
        self._client_name = "Air Flow Client B"
        self._data_source = "/home/jovyan/konza-dags/test/dags/populations/pop_data/client_b"
        self._target_table = f"tp{self.client_name_fmt}"
        self._ending_db = 46
        self._mpi_crosswalk = True
        self._csg = True

    def process_population_data(self):
        csv_path = get_latest_csv(self._data_source)
        output_df = self._process_client(csv_path)
        output_df_to_target_tbl(output_df, self.schema, self.target_table,
                                self.conn_id)

    def _process_client(self, input_csv_path):
        raise Exception('Skipped on Purpose')
        df = pd.read_csv(input_csv_path)
        entry_list = self._validate_data(df)
        for i in reversed(range(len(entry_list))):
            mpi = self._populate_mpi(entry_list[i])
            # this filters results based on mpi, replicates criteria from existing scripts.
            # 1) mpi cannot be 0
            # 2) mpi must be of a character length > 1
            if mpi < 10:
                del entry_list[i]
        output_df = self._create_output_df(entry_list)
        return output_df

    def _validate_data(self, data):
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

    def _populate_mpi(self, entry):
        sql_query = self._generate_mpi_query(entry)
        #Replace 'MariaDB' with Konza conn id.
        *_, last = execute_query(sql_query, 'MariaDB')
        entry.mpi = last[0]  # This returns last row as in orig script.
        return entry.mpi

    def _generate_mpi_query(self, entry):
        first_name = entry.patient_first_name.replace("'", r"\'")
        last_name = entry.patient_last_name.replace("'", r"\'")
        dob = str(entry.date_of_birth).replace("-", "")
        schema = 'person_master'
        return f"select {schema}.fn_getmpi('{last_name}','{first_name}','{dob}','{entry.gender}');"

    def _create_output_df(self, entry_list):
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
