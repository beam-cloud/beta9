from __future__ import annotations

import argparse

from .config import resolve_run_config
from .runner import BenchmarkRunner


DEFAULT_SUITES = {
    "cache": "cache-default",
    "fs": "fs-default",
    "clip": "clip-runtime",
    "startup": "startup-default",
    "sandbox": "sandbox-default",
    "full": "local-full",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bin/bench",
        description="Run beta9 benchmark suites with path evidence and JSONL metrics.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command, default_suite in DEFAULT_SUITES.items():
        sub = subparsers.add_parser(command, help=f"run the {command} benchmark suite")
        sub.set_defaults(command=command, suite_default=default_suite)
        add_common_args(sub)
    return parser


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--suite", default=None, help="suite YAML name or path")
    parser.add_argument("--profile", default=None, help="beta9 config profile")
    parser.add_argument("--config", default=None, help="beta9 config.ini path")
    parser.add_argument("--token", default=None, help="gateway token override")
    parser.add_argument("--gateway-url", default=None, help="gateway HTTP URL override")
    parser.add_argument("--grpc-addr", default=None, help="gateway gRPC address override")
    parser.add_argument("--namespace", default=None, help="Kubernetes namespace")
    parser.add_argument("--out-dir", default=None, help="benchmark output directory")
    parser.add_argument("--dry-run", action="store_true", help="expand suite without cluster work")
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override a suite parameter",
    )
    parser.add_argument(
        "--script-arg",
        action="append",
        default=[],
        help="append a raw argument to the underlying suite script",
    )


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args, unknown = parser.parse_known_args(argv)
    args.suite = args.suite or args.suite_default
    args.script_arg = [*(args.script_arg or []), *unknown]
    config = resolve_run_config(args)
    raise SystemExit(BenchmarkRunner(config).run())
