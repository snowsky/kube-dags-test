from populations.client_profiles.client_A import ClientA
from populations.client_profiles.client_B import ClientB
from populations.client_profiles.client_C import ClientC
from populations.client_profiles.client_D import ClientD


class ClientFactory:
    client_mapping = {
        'Air Flow Client A': ClientA,
        'Air Flow Client B': ClientB,
        'Air Flow Client C': ClientC,
        'Air Flow Client D': ClientD
    }

    @classmethod
    def get_client_profile(cls, client_name):
        client_class = cls.client_mapping.get(client_name)
        if client_class:
            return client_class()
        else:
            raise ValueError(f"Unknown client name: {client_name}")


def get_client_profile(client_name):
    return ClientFactory.get_client_profile(client_name)
