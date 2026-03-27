#!/usr/bin/env python3
"""
Project startup script.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent))

from src.core import RuleEngine, TagMapper
from src.data import PLCCollector


def setup_logging(log_level: str = "INFO"):
    logger.remove()
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | <cyan>{module}</cyan> | {message}"
        ),
        level=log_level,
        colorize=True,
    )
    logger.add(
        "logs/app.log",
        rotation="10 MB",
        retention="30 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module} | {message}",
    )


def load_yaml_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def build_addresses(mapper: TagMapper) -> dict:
    return {
        tag_id: info.get("plc_address") or info.get("address") or tag_id
        for tag_id, info in mapper.tag_dict.items()
    }


def flatten_collection_payload(data: dict) -> dict:
    values = data.get("values", {})
    flattened = {}
    for key, payload in values.items():
        if isinstance(payload, dict):
            flattened[key] = payload.get("value")
        else:
            flattened[key] = payload
    return flattened


def main():
    parser = argparse.ArgumentParser(description="Jamin Industrial Agent startup")
    parser.add_argument("--collect", action="store_true", help="Run collection only")
    parser.add_argument("--rules", action="store_true", help="Run rules only")
    parser.add_argument("--demo", action="store_true", help="Use simulated PLC data")
    parser.add_argument(
        "--config",
        type=str,
        default="config/settings.yaml",
        help="Path to the YAML config file",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger.info("Starting Jamin Industrial Agent")

    try:
        config = load_yaml_config(args.config)
        logger.info(f"Loaded config: {args.config}")

        tag_mapping_file = config.get("tag_mapping", {}).get(
            "file", "config/tag_mapping.xlsx"
        )
        mapper = TagMapper(tag_mapping_file)
        logger.info(f"Loaded {len(mapper.tag_dict)} tag mappings")

        rules_config = config.get("rules", {})
        rules_file = rules_config.get("config_file", "config/rules.json")
        engine = RuleEngine(
            rules_file,
            evaluation_interval=rules_config.get("evaluation_interval", 10),
            config=rules_config.get("strategy", {}),
        )
        logger.info(f"Loaded {len(engine.rules)} rules")

        plc_config = dict(config.get("plc", {}))
        if args.demo:
            plc_config["type"] = "simulated"
            logger.info("Demo mode enabled; using simulated PLC data")

        collector = PLCCollector(plc_config)
        logger.info(
            "Collector initialized: "
            f"{plc_config.get('type', 's7')}@{plc_config.get('host', 'localhost')}"
        )

        def process_data(data: dict):
            current_values = flatten_collection_payload(data)
            alerts = engine.evaluate(current_values)
            if alerts:
                logger.warning(f"Detected {len(alerts)} alerts")
                for alert in alerts:
                    logger.warning(
                        f"[{alert['severity'].upper()}] {alert['name']} | "
                        f"label={alert.get('label', '')}"
                    )
                    if alert.get("suggested_actions"):
                        logger.warning(f"Suggested action: {alert['suggested_actions'][0]}")
            else:
                logger.debug(f"{data.get('timestamp', 'unknown')} - system normal")

        collector.register_callback(process_data)
        addresses = build_addresses(mapper)
        collector.start_collection(addresses)

        logger.info("System started. Press Ctrl+C to stop.")
        if not args.collect and not args.rules:
            logger.info("Dashboard: http://localhost:8501")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user")
        finally:
            collector.stop_collection()
            collector.disconnect()

        logger.info("System stopped cleanly")
    except Exception as exc:
        logger.exception(f"Startup failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()

