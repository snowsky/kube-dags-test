from dags.populations.pydantic_models.population_definition_entry import PopulationDefinitionEntry
from dags.populations.target_population_impl import _generate_mpi_query, get_latest_csv_impl, process_csv_impl
from mock import patch
from tempfile import TemporaryDirectory


def _test_entry(attributed_provider_npi=101, patient_first_name='James', patient_last_name='Bond',
                   date_of_birth='1920-11-11', gender='M'):
    entry = PopulationDefinitionEntry(attributed_provider_npi=attributed_provider_npi,
                                      patient_first_name=patient_first_name,
                                      patient_last_name=patient_last_name,
                                      date_of_birth=date_of_birth,
                                      gender=gender)
    return entry


class TestTargetPopulation:
    # def test_generate_mpi_query(self):
    #     entry = _test_entry(attributed_provider_npi=101,
    #                         patient_first_name='Brian',
    #                         patient_last_name="O'Driscoll",
    #                         date_of_birth='1979-01-21')
    #     sql_query = _generate_mpi_query(entry)
    #     expected_query = r"select person_master.fn_getmpi('O\'Driscoll','Brian','19790121','M');"
    #     assert sql_query == expected_query

    def test_generate_mpi_query(self):
        entry = _test_entry()
        sql_query = _generate_mpi_query(entry)
        expected_query = r"select examples.fn_getmpi('Bond','James','19201111','M');"
        assert sql_query == expected_query

    def test_process_csv_impl(self):
        latest_file = get_latest_csv_impl('pop_data')
        assert latest_file == 'pop_data/pop_data.csv'

    def _set_mpi(self, entry, value):
        entry.mpi = value
        return value

    @patch('dags.populations.target_population_impl._populate_mpi')
    def test_process_csv_impl(self, populate_mpi_mock):
        populate_mpi_mock.side_effect = lambda x: self._set_mpi(x, 1)

        csv_path = 'pop_data/client_a/pop_data.csv'

        with TemporaryDirectory() as td:
            entry_list = process_csv_impl(csv_path, td + '/output.csv')
        assert len(entry_list) == 3
        for index, entry in enumerate(entry_list):
            assert entry.mpi == 1

    @patch('dags.populations.target_population_impl._populate_mpi')
    def test_process_csv_impl_zero(self, populate_mpi_mock):
        populate_mpi_mock.side_effect = lambda x: self._set_mpi(x, 0)

        csv_path = 'pop_data/client_b/pop_data.csv'

        with TemporaryDirectory() as td:
            entry_list = process_csv_impl(csv_path, td + '/output.csv')
        assert len(entry_list) == 0
