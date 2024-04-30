from dags.ccds.basic_extract_dag import basic_extract_print
from mock import patch, ANY
import pandas as pd
import numpy as np


def _create_test_parquet(*args, **kwargs):
    output_path = args[1]
    print(output_path)
    df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
    df.to_parquet(output_path)


@patch('dags.ccds.basic_extract_dag.extract_demographic_info_from_xmls_to_parquet',
       wraps=_create_test_parquet)
def test_basic_extract_dag(extract_demo_patch):
    ret_df = basic_extract_print()
    extract_demo_patch.assert_called_with('/opt/airflow/ccda/', ANY)
    assert np.all(ret_df['col1'] == [1, 2])
    assert np.all(ret_df['col2'] == [3, 4])
