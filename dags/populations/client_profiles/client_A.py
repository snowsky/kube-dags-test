from populations.client_profiles.client_profie import ClientProfile


class ClientA(ClientProfile):
    def __init__(self):
        super().__init__()
        self._client_name = "Air Flow Client A"
        self._source_directory = "/opt/airflow/dags/sftp_mock/pop_data/client_a"
        self._target_table = "tpairflowclienta"
        self._ending_db = 45
        self._approved = True  # Pull in from DB or another source?

    @property
    def client_name(self):
        return self._client_name

    @property
    def source_directory(self):
        return self._source_directory

    @property
    def target_table(self):
        return self._target_table

    @property
    def ending_db(self):
        return self._ending_db

    @property
    def approved(self):
        return self._approved
