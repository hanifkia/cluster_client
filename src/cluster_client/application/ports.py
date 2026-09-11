from typing import Protocol

from cluster_client.domain import GroupId


class NodeGateway(Protocol):
    """Application port for the REST API exposed by one cluster node."""

    def exists(self, host: str, group_id: GroupId) -> bool:
        ...

    def ensure_present(self, host: str, group_id: GroupId) -> None:
        ...

    def ensure_absent(self, host: str, group_id: GroupId) -> None:
        ...
