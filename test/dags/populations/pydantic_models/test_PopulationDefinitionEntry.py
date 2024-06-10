import pytest
from dags.populations.pydantic_models.population_definition_entry import PopulationDefinitionEntry
from pydantic import ValidationError


def _test_entry(attributed_provider_npi=101, patient_first_name='James', patient_last_name='Bond',
                date_of_birth='1920-11-11', gender='M', mpi=0):
    entry = PopulationDefinitionEntry(attributed_provider_npi=attributed_provider_npi,
                                      patient_first_name=patient_first_name,
                                      patient_last_name=patient_last_name,
                                      date_of_birth=date_of_birth,
                                      gender=gender,
                                      mpi=mpi,)
    return entry


class TestPopulationDefinitionEntry:
    def test_validation_valid(self):
        _test_entry()

    def test_validation_invalid_npi(self):
        with pytest.raises(ValidationError):
            _test_entry(attributed_provider_npi="test")

    def test_validation_float_npi(self):
        entry = _test_entry(attributed_provider_npi=101.1)
        assert entry.attributed_provider_npi == 101

    def test_validation_invalid_date(self):
        with pytest.raises(ValidationError):
            _test_entry(date_of_birth="1920/11/11")

    def test_validation_invalid_first_name(self):
        with pytest.raises(ValidationError):
            _test_entry(patient_first_name="Test123")

    def test_validation_invalid_last_name(self):
        with pytest.raises(ValidationError):
            _test_entry(patient_last_name="Test123")

    def test_validation_invalid_gender(self):
        with pytest.raises(ValidationError):
            _test_entry(gender="test")

    def test_invalid_mpi_text(self):
        with pytest.raises(ValidationError):
            _test_entry(mpi="test")

    def test_valid_mpi_numeric(self):
        _test_entry(mpi=1337)

    def test_valid_mpi_numeric_str(self):
        _test_entry(mpi='1337')

    def test_invalid_mpi_float(self):
        with pytest.raises(ValidationError):
            _test_entry(mpi=50.5)
