from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.graph.client_nx import NetworkXGraphClient
# In the future, we would also import a config model here.


class GraphClientBuilder:
    """
    Constructs and returns a configured graph client.

    This builder provides a single entry point for creating graph clients,
    hiding the complexity of which backend is being used. For the MVP, it
    is hardcoded to return a NetworkX client.
    """

    @staticmethod
    def get_client() -> GraphClientInterface:
        """
        Factory method to get the configured graph client.

        # TODO: Read from a config file to determine which client to instantiate.
        """
        # For now, we only have one implementation.
        return NetworkXGraphClient()
