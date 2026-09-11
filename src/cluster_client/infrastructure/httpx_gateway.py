from __future__ import annotations

import time
from collections.abc import Callable
from urllib.parse import quote

import httpx

from cluster_client.domain import GroupId


class NodeRequestError(RuntimeError):
    def __init__(self, host: str, message: str) -> None:
        self.host = host
        super().__init__(f"{host}: {message}")


class HttpxNodeGateway:
    """httpx adapter with retries plus state reconciliation for ambiguous failures."""

    _TRANSIENT_STATUS = {408, 429}

    def __init__(
        self,
        *,
        scheme: str = "http",
        timeout: float = 2.0,
        max_attempts: int = 3,
        backoff_base: float = 0.2,
        client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._scheme = scheme
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._sleeper = sleeper
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> HttpxNodeGateway:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def exists(self, host: str, group_id: GroupId) -> bool:
        url = self._group_url(host, group_id)
        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._client.get(url)
                if response.status_code == 200:
                    payload = response.json()
                    if payload.get("groupId") != str(group_id):
                        raise NodeRequestError(host, "GET returned an unexpected groupId")
                    return True
                if response.status_code == 404:
                    return False
                if self._is_transient(response.status_code):
                    last_error = NodeRequestError(
                        host, f"GET returned transient HTTP {response.status_code}"
                    )
                else:
                    raise NodeRequestError(host, f"GET returned HTTP {response.status_code}")
            except httpx.TransportError as exc:
                last_error = exc
            except ValueError as exc:
                raise NodeRequestError(host, "GET returned invalid JSON") from exc

            if attempt < self._max_attempts:
                self._sleep(attempt)

        raise NodeRequestError(host, f"GET failed after retries: {last_error}")

    def ensure_present(self, host: str, group_id: GroupId) -> None:
        self._mutate_to_state(host, group_id, desired_present=True)

    def ensure_absent(self, host: str, group_id: GroupId) -> None:
        self._mutate_to_state(host, group_id, desired_present=False)

    def _mutate_to_state(
        self, host: str, group_id: GroupId, *, desired_present: bool
    ) -> None:
        url = self._collection_url(host)
        method = "POST" if desired_present else "DELETE"
        expected = 201 if desired_present else 200
        payload = {"groupId": str(group_id)}
        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._client.request(method, url, json=payload)
                if response.status_code == expected:
                    return

                # 400 on create often means "already exists". 404 on delete is also
                # harmless if the desired state is already reached. For any transient
                # server error, reconcile before deciding to retry.
                should_reconcile = (
                    self._is_transient(response.status_code)
                    or (desired_present and response.status_code == 400)
                    or (not desired_present and response.status_code == 404)
                )
                if not should_reconcile:
                    raise NodeRequestError(
                        host, f"{method} returned HTTP {response.status_code}"
                    )
                last_error = NodeRequestError(
                    host, f"{method} returned HTTP {response.status_code}"
                )
            except httpx.TransportError as exc:
                # A timeout does not tell us whether the node changed state.
                last_error = exc

            try:
                if self.exists(host, group_id) is desired_present:
                    return
            except NodeRequestError as reconcile_error:
                last_error = reconcile_error

            if attempt < self._max_attempts:
                self._sleep(attempt)

        state = "present" if desired_present else "absent"
        raise NodeRequestError(
            host,
            f"could not make group {group_id} {state} after retries: {last_error}",
        )

    def _collection_url(self, host: str) -> str:
        return f"{self._base_url(host)}/v1/group/"

    def _group_url(self, host: str, group_id: GroupId) -> str:
        encoded = quote(str(group_id), safe="")
        return f"{self._base_url(host)}/v1/group/{encoded}/"

    def _base_url(self, host: str) -> str:
        clean = host.strip().rstrip("/")
        if clean.startswith("http://") or clean.startswith("https://"):
            return clean
        return f"{self._scheme}://{clean}"

    @classmethod
    def _is_transient(cls, status_code: int) -> bool:
        return status_code >= 500 or status_code in cls._TRANSIENT_STATUS

    def _sleep(self, attempt: int) -> None:
        self._sleeper(self._backoff_base * (2 ** (attempt - 1)))
