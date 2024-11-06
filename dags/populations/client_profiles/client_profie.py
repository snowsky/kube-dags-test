from abc import ABC, abstractmethod

class ClientProfile(ABC):
    def __init__(self):
        #  Properties look to be the same across all clients
        self._conn_id = "prd-az1-sqlw2-airflowconnection"
        self._schema = "temp"

    @abstractmethod
    def process_population_data(self):
        pass

    @property
    @abstractmethod
    def client_name(self):
        pass

    @property
    @abstractmethod
    def target_table(self):
        pass

    @property
    @abstractmethod
    def ending_db(self):
        pass

    @property
    @abstractmethod
    def mpi_crosswalk(self):
        pass

    @property
    @abstractmethod
    def csg(self):
        pass

    @property
    def conn_id(self):
        return self._conn_id

    @property
    def schema(self):
        return self._schema
