import asyncio

import pytest
from fastapi import HTTPException

import src.api.dependencies as auth_dependencies
import src.api.main as api_main
from src.api.repositories.auth_repository import AuthRepository


@pytest.fixture(autouse=True)
def restore_auth_session_state(tmp_path):
    original_users = auth_dependencies._users.items()
    original_tokens = auth_dependencies._tokens.items()
    original_security_config = dict(auth_dependencies._security_config)
    original_environment = auth_dependencies._runtime_environment
    original_repository = auth_dependencies._auth_repository

    case_dir = tmp_path / "auth_session_flow"
    case_dir.mkdir(parents=True, exist_ok=True)
    auth_dependencies._auth_repository = AuthRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )
    auth_dependencies._runtime_environment = "development"
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "dev-secret",
            "demo_users": {"enabled": True, "allow_in_production": False},
            "api_keys": {"enabled": False, "keys": []},
        }
    )
    auth_dependencies._users.clear()
    auth_dependencies._tokens.clear()

    yield

    auth_dependencies._users.clear()
    auth_dependencies._tokens.clear()
    for key, value in original_users:
        auth_dependencies._users.set(key, value)
    for key, value in original_tokens:
        auth_dependencies._tokens.set(key, value)
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(original_security_config)
    auth_dependencies._runtime_environment = original_environment
    auth_dependencies._auth_repository = original_repository


def test_login_returns_refresh_token_bundle():
    auth_dependencies.init_default_users()

    response = asyncio.run(api_main.login({"username": "admin", "password": "admin123"}))
    refresh_payload = auth_dependencies.verify_token(response["refresh_token"])

    assert response["token_type"] == "bearer"
    assert response["refresh_expires_in"] > response["expires_in"]
    assert refresh_payload is not None
    assert refresh_payload["token_type"] == "refresh"


def test_refresh_session_rotates_refresh_token():
    auth_dependencies.init_default_users()
    login_response = asyncio.run(api_main.login({"username": "admin", "password": "admin123"}))

    old_refresh_payload = auth_dependencies.verify_token(login_response["refresh_token"])
    refreshed = asyncio.run(api_main.refresh_session({"refresh_token": login_response["refresh_token"]}))
    new_refresh_payload = auth_dependencies.verify_token(refreshed["refresh_token"])
    old_record = auth_dependencies.get_refresh_token_record(old_refresh_payload["jti"])
    new_record = auth_dependencies.get_refresh_token_record(new_refresh_payload["jti"])

    assert refreshed["refresh_token"] != login_response["refresh_token"]
    assert old_record is not None
    assert old_record["revoked_at"] is not None
    assert old_record["replaced_by_token_id"] == new_refresh_payload["jti"]
    assert new_record is not None
    assert new_record["revoked_at"] is None


def test_logout_revokes_refresh_token():
    auth_dependencies.init_default_users()
    login_response = asyncio.run(api_main.login({"username": "viewer", "password": "viewer123"}))
    refresh_payload = auth_dependencies.verify_token(login_response["refresh_token"])

    logout_response = asyncio.run(api_main.logout_session({"refresh_token": login_response["refresh_token"]}))
    record = auth_dependencies.get_refresh_token_record(refresh_payload["jti"])

    assert logout_response["success"] is True
    assert record is not None
    assert record["revoked_at"] is not None

    with pytest.raises(HTTPException, match="Refresh token is no longer valid") as exc_info:
        asyncio.run(api_main.refresh_session({"refresh_token": login_response["refresh_token"]}))

    assert exc_info.value.status_code == 401
