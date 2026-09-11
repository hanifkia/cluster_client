import httpx

from cluster_client.domain import GroupId
from cluster_client.infrastructure import HttpxNodeGateway


def gateway_for(handler, *, attempts: int = 3) -> HttpxNodeGateway:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return HttpxNodeGateway(
        client=client,
        max_attempts=attempts,
        backoff_base=0,
        sleeper=lambda _: None,
    )


def test_create_timeout_is_reconciled_with_get() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        if request.method == "POST":
            raise httpx.ReadTimeout("lost response", request=request)
        return httpx.Response(200, json={"groupId": "g1"})

    gateway = gateway_for(handler)
    gateway.ensure_present("node1", GroupId("g1"))

    assert calls == ["POST /v1/group/", "GET /v1/group/g1/"]


def test_delete_500_is_success_if_get_confirms_absent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(500)
        return httpx.Response(404)

    gateway = gateway_for(handler)
    gateway.ensure_absent("node1", GroupId("g1"))


def test_exists_retries_transient_get() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(500)
        return httpx.Response(200, json={"groupId": "g1"})

    gateway = gateway_for(handler)

    assert gateway.exists("node1", GroupId("g1")) is True
    assert attempts == 2


def test_create_400_is_idempotent_if_group_exists() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(400)
        return httpx.Response(200, json={"groupId": "g1"})

    gateway = gateway_for(handler)
    gateway.ensure_present("node1", GroupId("g1"))
