from dataclasses import dataclass
from typing import Callable, Iterable

from cluster_client.application.ports import NodeGateway
from cluster_client.domain import GroupId


@dataclass(frozen=True, slots=True)
class OperationReport:
    operation: str
    group_id: str
    changed_hosts: tuple[str, ...]
    skipped_hosts: tuple[str, ...]


class ClusterOperationError(RuntimeError):
    """Raised when the requested cluster operation could not complete atomically."""

    def __init__(
        self,
        *,
        operation: str,
        group_id: GroupId,
        failed_host: str | None,
        cause: Exception,
        rollback_errors: dict[str, Exception] | None = None,
    ) -> None:
        self.operation = operation
        self.group_id = group_id
        self.failed_host = failed_host
        self.cause = cause
        self.rollback_errors = rollback_errors or {}

        location = f" on {failed_host}" if failed_host else " during preflight"
        message = f"{operation} of group {group_id} failed{location}: {cause}"
        if self.rollback_errors:
            hosts = ", ".join(sorted(self.rollback_errors))
            message += f"; rollback also failed on: {hosts}"
        super().__init__(message)


class ClusterGroupService:
    """Coordinates all-node changes and compensating rollback."""

    def __init__(self, hosts: Iterable[str], gateway: NodeGateway) -> None:
        normalized = tuple(dict.fromkeys(host.strip() for host in hosts if host.strip()))
        if not normalized:
            raise ValueError("at least one cluster host is required")
        self._hosts = normalized
        self._gateway = gateway

    def create(self, group_id: GroupId) -> OperationReport:
        initial = self._snapshot(group_id, operation="create")
        targets = [host for host, exists in initial.items() if not exists]
        skipped = [host for host, exists in initial.items() if exists]

        return self._apply(
            operation="create",
            group_id=group_id,
            targets=targets,
            skipped=skipped,
            mutate=self._gateway.ensure_present,
            compensate=self._gateway.ensure_absent,
        )

    def delete(self, group_id: GroupId) -> OperationReport:
        initial = self._snapshot(group_id, operation="delete")
        targets = [host for host, exists in initial.items() if exists]
        skipped = [host for host, exists in initial.items() if not exists]

        return self._apply(
            operation="delete",
            group_id=group_id,
            targets=targets,
            skipped=skipped,
            mutate=self._gateway.ensure_absent,
            compensate=self._gateway.ensure_present,
        )

    def _snapshot(self, group_id: GroupId, *, operation: str) -> dict[str, bool]:
        state: dict[str, bool] = {}
        for host in self._hosts:
            try:
                state[host] = self._gateway.exists(host, group_id)
            except Exception as exc:
                # No mutation has happened yet, so there is nothing to roll back.
                raise ClusterOperationError(
                    operation=operation,
                    group_id=group_id,
                    failed_host=host,
                    cause=exc,
                ) from exc
        return state

    def _apply(
        self,
        *,
        operation: str,
        group_id: GroupId,
        targets: list[str],
        skipped: list[str],
        mutate: Callable[[str, GroupId], None],
        compensate: Callable[[str, GroupId], None],
    ) -> OperationReport:
        attempted: list[str] = []

        for host in targets:
            attempted.append(host)
            try:
                mutate(host, group_id)
            except Exception as exc:
                rollback_errors = self._rollback(attempted, group_id, compensate)
                raise ClusterOperationError(
                    operation=operation,
                    group_id=group_id,
                    failed_host=host,
                    cause=exc,
                    rollback_errors=rollback_errors,
                ) from exc

        return OperationReport(
            operation=operation,
            group_id=str(group_id),
            changed_hosts=tuple(targets),
            skipped_hosts=tuple(skipped),
        )

    @staticmethod
    def _rollback(
        attempted: list[str],
        group_id: GroupId,
        compensate: Callable[[str, GroupId], None],
    ) -> dict[str, Exception]:
        errors: dict[str, Exception] = {}
        for host in reversed(attempted):
            try:
                compensate(host, group_id)
            except Exception as exc:
                # Rollback is best effort. Preserve all failures for the caller.
                errors[host] = exc
        return errors
