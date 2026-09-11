import argparse
import json
import os
import sys

from cluster_client.application import ClusterGroupService, ClusterOperationError
from cluster_client.domain import GroupId
from cluster_client.infrastructure import HttpxNodeGateway


def _hosts_from_env() -> list[str]:
    raw = os.getenv("CLUSTER_HOSTS", "")
    return [host.strip() for host in raw.split(",") if host.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cluster-client",
        description="Create or delete a group consistently across cluster nodes.",
    )
    parser.add_argument("operation", choices=("create", "delete"))
    parser.add_argument("group_id")
    parser.add_argument(
        "--hosts",
        nargs="+",
        default=None,
        help="Cluster hosts. Defaults to comma-separated CLUSTER_HOSTS.",
    )
    parser.add_argument(
        "--scheme",
        choices=("http", "https"),
        default=os.getenv("CLUSTER_SCHEME", "http"),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("CLUSTER_TIMEOUT_SECONDS", "2")),
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=int(os.getenv("CLUSTER_MAX_ATTEMPTS", "3")),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    hosts = args.hosts or _hosts_from_env()
    if not hosts:
        print("No hosts configured. Use --hosts or CLUSTER_HOSTS.", file=sys.stderr)
        return 2

    try:
        group_id = GroupId(args.group_id)
        with HttpxNodeGateway(
            scheme=args.scheme,
            timeout=args.timeout,
            max_attempts=args.attempts,
        ) as gateway:
            service = ClusterGroupService(hosts, gateway)
            report = getattr(service, args.operation)(group_id)
    except (ValueError, ClusterOperationError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "operation": report.operation,
                "groupId": report.group_id,
                "changedHosts": report.changed_hosts,
                "skippedHosts": report.skipped_hosts,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
