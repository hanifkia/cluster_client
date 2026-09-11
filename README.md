# Cluster Client

A small Python client that creates or deletes a group on every cluster node while trying to preserve the pre-operation state when something fails.

## Problem interpretation

The cluster exposes the same API on every node:

- `POST /v1/group/` -> create, expected `201`
- `DELETE /v1/group/` -> delete, expected `200`
- `GET /v1/group/{groupId}/` -> `200` when present, `404` when absent

The API is unstable, so a timeout or `5xx` can leave the result ambiguous. A request may have reached the server even when the client did not receive a successful response.

## Assumptions

1. All nodes use the same API contract and a group is identified only by `groupId`.
2. Recreating a deleted group with the same `groupId` is a valid compensation for a failed delete operation.
3. The client is allowed to use `GET` to reconcile state after an ambiguous mutation result.
4. A final error means retries and reconciliation were unable to prove that the requested node state was reached.
5. If the cluster starts in a mixed state, `create` converges it to present and `delete` converges it to absent. If the operation fails, only nodes the operation attempted to change are compensated, so the original mixed state is preserved as closely as possible.
6. Rollback can also fail because the same remote API is unstable. The client therefore performs best-effort rollback on every attempted node and reports any rollback failures explicitly.
7. Operations are deliberately sequential. This is slower than parallel fan-out, but it limits the number of in-flight changes and makes rollback behavior easier to reason about for this challenge.
8. No other writer is concurrently changing the same `groupId`. The provided API has no lock, version, or transaction primitive, so concurrent writers cannot be made safe purely from the client side.

## Architecture

The structure is intentionally small and DDD-inspired rather than framework-heavy:

```text
src/cluster_client/
├── domain/
│   └── group.py             # GroupId value object
├── application/
│   ├── ports.py             # NodeGateway interface
│   └── group_service.py     # orchestration + compensation
├── infrastructure/
│   └── httpx_gateway.py     # REST adapter, retry, reconciliation
└── cli.py                   # entry point
```

The domain does not know about HTTP. The application layer depends on a small port, and the infrastructure layer implements that port with `httpx`.

## Reliability strategy

Before mutating anything, the application reads the state of every node. If preflight fails, no mutation is attempted.

For each node mutation:

1. Send `POST` or `DELETE`.
2. If it succeeds, continue.
3. On timeout, network error, relevant `4xx`, or transient `5xx`, use `GET` to check the real state.
4. If `GET` proves the desired state, treat the mutation as successful.
5. Otherwise retry with a small exponential backoff.
6. If a node still fails, compensate every node attempted so far back to its pre-operation state.

The failed node is included in compensation because its mutation may have succeeded even though the response was lost.

This is a client-side compensation strategy, not a distributed transaction. Without server-side transaction/idempotency primitives, strict atomicity cannot be guaranteed during partitions or persistent API failures.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

## Usage

Via environment variable:

```bash
export CLUSTER_HOSTS=node1.example.com,node2.example.com,node3.example.com
export CLUSTER_SCHEME=https

cluster-client create my-group
cluster-client delete my-group
```

Or pass hosts directly:

```bash
cluster-client create my-group --hosts node1.example.com node2.example.com node3.example.com --scheme https
```

On success the command prints a small JSON report and exits with code `0`. On operation failure it writes the error to stderr and exits with code `1`.

## Tests

```bash
pytest
```

The unit tests cover:

- creating only missing copies;
- delete/create rollback;
- no mutation when preflight fails;
- rollback failure reporting;
- timeout reconciliation;
- transient `5xx` retry;
- idempotent handling when the desired state is already reached.

E2E tests are intentionally not included because the challenge says the cluster API itself is out of scope.

## Docker

Build:

```bash
docker build -t cluster-client:local .
```

Run:

```bash
docker run --rm \
  -e CLUSTER_HOSTS=node1.example.com,node2.example.com,node3.example.com \
  -e CLUSTER_SCHEME=https \
  cluster-client:local create my-group
```

## Kubernetes

The client is a one-shot command, so the example uses a Kubernetes `Job` instead of a long-running `Deployment`.

```bash
kubectl apply -f manifests/configmap.yaml
kubectl apply -f manifests/job.yaml
```