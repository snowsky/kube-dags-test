from populations.client_profiles.data_driven import DataDrivenClientProfile
#from populations.client_profiles.data_driven_piped import DataDrivenPipedClientProfile
from populations.client_profiles.csv_internally_delivered import CsvInternallyDeliveredClientProfile
#from populations.client_profiles.excel_internally_delivered import ExcelInternallyDeliveredClientProfile
from typing import List, Tuple
import os

# Change internal_source_path to /data/biakonzasftp/ instead of /source-biakonzasftp/ when testing locally

def get_client_profile(folder_name, ending_db, frequency, facility_ids, airflow_client_profile, internal_source_path="/source-biakonzasftp/C-25/"):
    if airflow_client_profile == 'data_driven':
        return DataDrivenClientProfile(folder_name, ending_db)
    elif airflow_client_profile == 'csv_internally_delivered':
        client_source_path = os.path.join(internal_source_path, folder_name)
        if not os.path.exists(client_source_path):
            raise ValueError(f"path does not exist: {client_source_path}")
        file_names = list(os.listdir(client_source_path))
        if len(file_names) == 0:
            raise ValueError(f"no files found in: {client_source_path}")
        elif len(file_names) > 1:
            raise ValueError(f"multiple files found in: {client_source_path}")
        input_file_path = os.path.join(client_source_path, file_names[0])
        return CsvInternallyDeliveredClientProfile(input_file_path, folder_name, ending_db)
    #elif airflow_client_profile == 'excel_internally_delivered':
    #    return ExcelInternallyDeliveredClientProfile(input_file_path=input_file_path, folder_name=folder_name, ending_db=ending_db)
    else:
        raise ValueError(f"Unknown client profile: {folder_name}")
