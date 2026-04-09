from src.dev.runtime_bootstrap import bootstrap_development_runtime


def test_bootstrap_development_runtime_can_skip_demo_data(tmp_path):
    config = {
        "sqlite": {"path": str(tmp_path / "metadata.db")},
        "postgres": {"enabled": False},
    }

    payload = bootstrap_development_runtime(
        config,
        tenant_id="default",
        include_demo_data=False,
    )

    assert payload["rules"]["rule_total"] >= 1
    assert "devices" not in payload
    assert "alerts" not in payload


def test_bootstrap_development_runtime_can_seed_demo_data(tmp_path):
    config = {
        "sqlite": {"path": str(tmp_path / "metadata.db")},
        "postgres": {"enabled": False},
    }

    payload = bootstrap_development_runtime(
        config,
        tenant_id="default",
        include_demo_data=True,
    )

    assert payload["rules"]["rule_total"] >= 1
    assert payload["devices"]["device_total"] >= 1
    assert payload["alerts"]["alert_total"] >= 1
