from collections import defaultdict

import pytest

from cluster_client.application import ClusterGroupService, ClusterOperationError
from cluster_client.domain import GroupId


class FakeGateway:
    def __init__(self, state: dict[str, bool]) -> None:
        self.state = state.copy()
        self.failures: dict[tuple[str, str], list[Exception]] = defaultdict(list)
        self.calls: list[tuple[str, str]] = []

    def fail_once(self, method: str, host: str, exc: Exception) -> None:
        self.failures[(method, host)].append(exc)

    def _maybe_fail(self, method: str, host: str) -> None:
        planned = self.failures[(method, host)]
        if planned:
            raise planned.pop(0)

    def exists(self, host: str, group_id: GroupId) -> bool:
        self.calls.append(("exists", host))
        self._maybe_fail("exists", host)
        return self.state[host]

    def ensure_present(self, host: str, group_id: GroupId) -> None:
        self.calls.append(("ensure_present", host))
        self._maybe_fail("ensure_present", host)
        self.state[host] = True

    def ensure_absent(self, host: str, group_id: GroupId) -> None:
        self.calls.append(("ensure_absent", host))
        self._maybe_fail("ensure_absent", host)
        self.state[host] = False


def test_create_only_changes_missing_hosts() -> None:
    gateway = FakeGateway({"n1": True, "n2": False, "n3": False})
    service = ClusterGroupService(["n1", "n2", "n3"], gateway)

    report = service.create(GroupId("g1"))

    assert gateway.state == {"n1": True, "n2": True, "n3": True}
    assert report.changed_hosts == ("n2", "n3")
    assert report.skipped_hosts == ("n1",)


def test_create_failure_rolls_back_attempted_hosts() -> None:
    gateway = FakeGateway({"n1": False, "n2": False, "n3": False})
    gateway.fail_once("ensure_present", "n2", RuntimeError("boom"))
    service = ClusterGroupService(["n1", "n2", "n3"], gateway)

    with pytest.raises(ClusterOperationError) as error:
        service.create(GroupId("g1"))

    assert error.value.failed_host == "n2"
    assert gateway.state == {"n1": False, "n2": False, "n3": False}
    assert ("ensure_absent", "n2") in gateway.calls
    assert ("ensure_absent", "n1") in gateway.calls


def test_delete_failure_restores_original_state() -> None:
    gateway = FakeGateway({"n1": True, "n2": True, "n3": False})
    gateway.fail_once("ensure_absent", "n2", RuntimeError("boom"))
    service = ClusterGroupService(["n1", "n2", "n3"], gateway)

    with pytest.raises(ClusterOperationError):
        service.delete(GroupId("g1"))

    assert gateway.state == {"n1": True, "n2": True, "n3": False}


def test_preflight_failure_mutates_nothing() -> None:
    gateway = FakeGateway({"n1": False, "n2": False})
    gateway.fail_once("exists", "n2", RuntimeError("GET failed"))
    service = ClusterGroupService(["n1", "n2"], gateway)

    with pytest.raises(ClusterOperationError):
        service.create(GroupId("g1"))

    assert gateway.state == {"n1": False, "n2": False}
    assert not any(method.startswith("ensure_") for method, _ in gateway.calls)


def test_rollback_failures_are_preserved() -> None:
    gateway = FakeGateway({"n1": False, "n2": False})
    gateway.fail_once("ensure_present", "n2", RuntimeError("create failed"))
    gateway.fail_once("ensure_absent", "n1", RuntimeError("rollback failed"))
    service = ClusterGroupService(["n1", "n2"], gateway)

    with pytest.raises(ClusterOperationError) as error:
        service.create(GroupId("g1"))

    assert set(error.value.rollback_errors) == {"n1"}
