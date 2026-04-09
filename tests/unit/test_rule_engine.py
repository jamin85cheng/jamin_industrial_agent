import json

import pytest

from src.rules.rule_engine import RuleEngine
from src.rules.rule_parser import RuleParser


class TestRuleParser:
    def test_parse_threshold_condition(self):
        parser = RuleParser()

        condition = {
            "type": "threshold",
            "metric": "DO",
            "operator": "<",
            "threshold": 2.0,
        }

        func = parser.compile_condition(condition)

        assert func({"DO": 1.5}) is True
        assert func({"DO": 3.0}) is False

    def test_parse_logic_condition_supports_operator_alias(self):
        parser = RuleParser()

        condition = {
            "type": "logic",
            "operator": "AND",
            "conditions": [
                {"type": "threshold", "metric": "Pump_Status", "operator": "==", "threshold": 1},
                {"type": "threshold", "metric": "Flow", "operator": "==", "threshold": 0},
            ],
        }

        func = parser.compile_condition(condition)

        assert func({"Pump_Status": 1, "Flow": 0}) is True
        assert func({"Pump_Status": 1, "Flow": 10}) is False

    def test_parse_rate_of_change_supports_legacy_min_change(self):
        parser = RuleParser()

        condition = {
            "type": "rate_of_change",
            "metric": "Temperature",
            "window_minutes": 5,
            "min_change": 10,
        }

        func = parser.compile_condition(condition)

        data = {
            "Temperature": 52,
            "_history": {
                "Temperature": [40] * 10,
            },
        }

        assert func(data) is True

    def test_parse_threshold_supports_tag_and_value_aliases(self):
        parser = RuleParser()

        condition = {
            "type": "threshold",
            "tag": "vibration",
            "operator": ">",
            "value": 8,
        }

        func = parser.compile_condition(condition)

        assert func({"vibration": 9.2}) is True
        assert func({"vibration": 7.8}) is False


class TestRuleEngine:
    @pytest.fixture
    def rule_engine(self, tmp_path):
        rules = {
            "rules": [
                {
                    "rule_id": "RULE_001",
                    "name": "Low DO",
                    "description": "Dissolved oxygen is below threshold",
                    "enabled": True,
                    "severity": "critical",
                    "condition": {
                        "type": "threshold",
                        "metric": "DO",
                        "operator": "<",
                        "threshold": 2.0,
                    },
                    "suggested_actions": ["Increase aeration"],
                }
            ]
        }

        rules_file = tmp_path / "test_rules.json"
        rules_file.write_text(json.dumps(rules), encoding="utf-8")

        return RuleEngine(str(rules_file))

    def test_load_rules(self, rule_engine):
        assert len(rule_engine.rules) == 1
        assert rule_engine.rules[0]["rule_id"] == "RULE_001"

    def test_evaluate_single_rule(self, rule_engine):
        alerts = rule_engine.evaluate({"DO": 1.5})

        assert len(alerts) == 1
        assert alerts[0]["rule_id"] == "RULE_001"
        assert alerts[0]["severity"] == "critical"

    def test_evaluate_no_alert(self, rule_engine):
        alerts = rule_engine.evaluate({"DO": 3.0})

        assert len(alerts) == 0

    def test_alert_suppression(self, rule_engine):
        alerts1 = rule_engine.evaluate({"DO": 1.5})
        alerts2 = rule_engine.evaluate({"DO": 1.5})

        assert len(alerts1) == 1
        assert len(alerts2) == 0

    def test_get_statistics(self, rule_engine):
        stats = rule_engine.get_statistics()

        assert stats["total_rules"] == 1
        assert stats["enabled_rules"] == 1
        assert stats["is_running"] is False


class TestRuleEngineIntegration:
    def test_complex_scenario(self):
        parser = RuleParser()

        rule = {
            "type": "logic",
            "operator": "OR",
            "conditions": [
                {
                    "type": "logic",
                    "operator": "AND",
                    "conditions": [
                        {"type": "threshold", "metric": "DO", "operator": "<", "threshold": 2.0},
                        {"type": "threshold", "metric": "PH", "operator": "<", "threshold": 6.0},
                    ],
                },
                {
                    "type": "threshold",
                    "metric": "COD",
                    "operator": ">",
                    "threshold": 100,
                },
            ],
        }

        func = parser.compile_condition(rule)

        assert func({"DO": 1.5, "PH": 5.5, "COD": 50}) is True
        assert func({"DO": 3.0, "PH": 7.0, "COD": 150}) is True
        assert func({"DO": 3.0, "PH": 7.0, "COD": 50}) is False
