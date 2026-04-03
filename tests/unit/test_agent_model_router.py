"""Agent model routing tests."""

from pathlib import Path

from src.models.agent_model_router import AgentModelRouter
from src.utils.config import load_config


def test_agent_model_router_returns_role_specific_profile():
    router = AgentModelRouter(
        config={
            "llm": {
                "generation": {"temperature": 0.2, "max_tokens": 512},
                "endpoints": {
                    "local": {"base_url": "http://127.0.0.1:8588/v1", "api_key": "none"},
                    "cloud": {"base_url": "https://example.com/v1", "api_key": "secret"},
                },
                "agent_routing": {
                    "enabled": True,
                    "agents": {
                        "default": {"endpoint": "local", "model": "Qwen3.5-9B"},
                        "coordinator": {"endpoint": "cloud", "model": "DeepSeek-V3.2"},
                    },
                },
            }
        }
    )

    default_profile = router.get_profile("mechanical")
    coordinator_profile = router.get_profile("coordinator")

    assert default_profile is not None
    assert default_profile.model == "Qwen3.5-9B"
    assert coordinator_profile is not None
    assert coordinator_profile.model == "DeepSeek-V3.2"


def test_agent_model_router_builds_client_from_route():
    router = AgentModelRouter(
        config={
            "llm": {
                "generation": {"temperature": 0.2, "max_tokens": 512},
                "endpoints": {
                    "local": {"base_url": "http://127.0.0.1:8588/v1", "api_key": "none"},
                },
                "agent_routing": {
                    "enabled": True,
                    "agents": {
                        "default": {"endpoint": "local", "model": "Qwen3.5-9B"},
                    },
                },
            }
        }
    )

    client = router.get_client("mechanical")

    assert client is not None
    assert client.model == "Qwen3.5-9B"
    assert client.base_url == "http://127.0.0.1:8588/v1"


def test_load_config_resolves_project_relative_path_independent_of_cwd(monkeypatch):
    original_cwd = Path.cwd()
    try:
        monkeypatch.chdir(Path(original_cwd.anchor))
        config = load_config()
    finally:
        monkeypatch.chdir(original_cwd)

    assert config["llm"]["agent_routing"]["enabled"] is True
