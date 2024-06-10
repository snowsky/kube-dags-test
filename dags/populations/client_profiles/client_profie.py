from abc import ABC, abstractmethod


class ClientProfile(ABC):
    def __init__(self):
        #  Properties look to be the same across all clients
        self._conn_id = "MariaDB"
        self._schema = "temp"

    @property
    @abstractmethod
    def client_name(self):
        pass

    @property
    @abstractmethod
    def source_directory(self):
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
    def approved(self):
        pass

    @property
    def conn_id(self):
        return self._conn_id

    @property
    def schema(self):
        return self._schema
