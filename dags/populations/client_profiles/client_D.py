from populations.client_profiles.client_A import ClientA


class ClientD(ClientA):
    def __init__(self):
        super().__init__()
        self._client_name = "Air Flow Client D"
        self._ending_db = 50
        self._unit_vault_facility_ids = "999999"
