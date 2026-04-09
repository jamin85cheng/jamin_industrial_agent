"""Development-only runtime bootstrap entrypoint."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dev.runtime_bootstrap import (  # noqa: E402
    bootstrap_development_runtime,
    ensure_alert_demo_data,
    ensure_alert_rule_defaults,
    ensure_device_demo_data,
    ensure_report_storage,
    ensure_system_config_storage,
)
from src.utils.config import load_config  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Development-only runtime bootstrap helper.")
    parser.add_argument(
        "command",
        choices=["all", "devices", "rules", "alerts", "reports", "system-config"],
        help="Bootstrap slice to run.",
    )
    parser.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path to the YAML configuration file.",
    )
    parser.add_argument(
        "--tenant-id",
        default="default",
        help="Tenant id to target for demo/runtime seed data.",
    )
    parser.add_argument(
        "--without-demo-data",
        action="store_true",
        help="Skip demo devices and demo alerts when running the `all` command.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
    db_config = config.get("database", {})

    if args.command == "all":
        payload = bootstrap_development_runtime(
            db_config,
            tenant_id=args.tenant_id,
            include_demo_data=not args.without_demo_data,
        )
    elif args.command == "devices":
        payload = ensure_device_demo_data(db_config, tenant_id=args.tenant_id)
    elif args.command == "rules":
        payload = ensure_alert_rule_defaults(db_config, tenant_id=args.tenant_id)
    elif args.command == "alerts":
        payload = ensure_alert_demo_data(db_config, tenant_id=args.tenant_id)
    elif args.command == "reports":
        payload = ensure_report_storage(db_config)
    else:
        payload = ensure_system_config_storage(db_config)

    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
