from populations.client_profiles.client_profie import ClientProfile


class ClientB(ClientProfile):
    def __init__(self):
        super().__init__()
        self._client_name = "Air Flow Client B"
        self._source_directory = "/opt/airflow/dags/sftp_mock/pop_data/client_b"
        self._target_table = "tpairflowclientb"
        self._ending_db = 46
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
