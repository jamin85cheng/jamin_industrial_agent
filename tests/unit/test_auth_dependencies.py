import asyncio
from datetime import timedelta

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

import src.api.dependencies as auth_dependencies
import src.api.main as api_main
from src.api.repositories.auth_repository import AuthRepository


@pytest.fixture(autouse=True)
def restore_auth_state(tmp_path):
    original_users = auth_dependencies._users.items()
    original_tokens = auth_dependencies._tokens.items()
    original_security_config = dict(auth_dependencies._security_config)
    original_environment = auth_dependencies._runtime_environment
    original_repository = auth_dependencies._auth_repository
    original_storage_initialized = auth_dependencies._auth_storage_initialized
    original_storage_repository_id = auth_dependencies._auth_storage_repository_id

    case_dir = tmp_path / "auth_dependencies"
    case_dir.mkdir(parents=True, exist_ok=True)
    auth_dependencies._auth_repository = AuthRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )
    auth_dependencies._auth_storage_initialized = False
    auth_dependencies._auth_storage_repository_id = None

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
    auth_dependencies._auth_storage_initialized = original_storage_initialized
    auth_dependencies._auth_storage_repository_id = original_storage_repository_id


def test_hash_password_round_trip():
    hashed = auth_dependencies.hash_password("operator123")

    assert hashed.startswith("pbkdf2_sha256$")
    assert auth_dependencies.verify_password("operator123", hashed) is True
    assert auth_dependencies.verify_password("wrong-password", hashed) is False


def test_init_default_users_hashes_passwords():
    auth_dependencies._runtime_environment = "development"
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "",
            "demo_users": {"enabled": True, "allow_in_production": False},
            "api_keys": {"enabled": False, "keys": []},
        }
    )

    auth_dependencies.init_default_users()

    assert auth_dependencies._users.size() == 3
    admin_user = auth_dependencies._users.get("admin")
    assert admin_user is not None
    assert "password" not in admin_user
    assert auth_dependencies.verify_password("admin123", admin_user["password_hash"]) is True


def test_init_default_users_skips_when_demo_mode_disabled():
    auth_dependencies._runtime_environment = "production"
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "prod-secret-value",
            "demo_users": {"enabled": True, "allow_in_production": False},
            "api_keys": {"enabled": False, "keys": []},
        }
    )

    auth_dependencies.init_default_users()

    assert auth_dependencies._users.size() == 0


def test_ensure_security_configuration_requires_secret_in_production():
    auth_dependencies._runtime_environment = "production"
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "",
            "demo_users": {"enabled": False, "allow_in_production": False},
            "api_keys": {"enabled": False, "keys": []},
        }
    )

    with pytest.raises(RuntimeError, match="JWT secret is not configured"):
        auth_dependencies.ensure_security_configuration()


def test_get_configured_api_keys_reads_from_config():
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "dev-secret",
            "demo_users": {"enabled": True, "allow_in_production": False},
            "api_keys": {
                "enabled": True,
                "keys": [
                    {
                        "key": "configured-key",
                        "user_id": "api_user_1",
                        "username": "api-user",
                        "roles": ["operator"],
                        "tenant_id": "default",
                        "permissions": ["data:read"],
                    }
                ],
            },
        }
    )

    configured = auth_dependencies.get_configured_api_keys()

    assert "configured-key" in configured
    assert configured["configured-key"]["username"] == "api-user"


def test_login_returns_503_when_demo_users_are_disabled():
    auth_dependencies._users.clear()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(api_main.login({"username": "admin", "password": "admin123"}))

    assert exc_info.value.status_code == 503


def test_login_verifies_hashed_passwords():
    auth_dependencies._users.set(
        "admin",
        {
            "user_id": "admin",
            "username": "admin",
            "password_hash": auth_dependencies.hash_password("admin123"),
            "roles": ["admin"],
            "tenant_id": "default",
            "permissions": ["*"],
        },
    )

    response = asyncio.run(api_main.login({"username": "admin", "password": "admin123"}))

    assert response["token_type"] == "bearer"
    assert response["user"]["username"] == "admin"


def test_get_current_user_uses_role_permissions_from_repository():
    auth_dependencies._runtime_environment = "development"
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "dev-secret",
            "demo_users": {"enabled": False, "allow_in_production": False},
            "api_keys": {"enabled": False, "keys": []},
        }
    )
    auth_dependencies.ensure_auth_storage()
    auth_dependencies._auth_repository.upsert_user(
        {
            "user_id": "rbac_operator",
            "username": "rbac_operator",
            "password_hash": auth_dependencies.hash_password("operator123"),
            "roles": ["operator"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
            "is_demo": False,
        }
    )

    token = auth_dependencies.create_access_token(
        user_id="rbac_operator",
        username="rbac_operator",
        roles=["viewer"],
        tenant_id="default",
    )

    current_user = asyncio.run(
        auth_dependencies.get_current_user(
            HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        )
    )

    assert current_user.roles == ["operator"]
    assert "device:write" in current_user.permissions


def test_get_current_user_rejects_refresh_token_credentials():
    auth_dependencies._runtime_environment = "development"
    auth_dependencies._security_config.clear()
    auth_dependencies._security_config.update(
        {
            "jwt_secret": "dev-secret",
            "demo_users": {"enabled": False, "allow_in_production": False},
            "api_keys": {"enabled": False, "keys": []},
        }
    )
    auth_dependencies.ensure_auth_storage()
    auth_dependencies._auth_repository.upsert_user(
        {
            "user_id": "refresh_only_user",
            "username": "refresh_only_user",
            "password_hash": auth_dependencies.hash_password("viewer123"),
            "roles": ["viewer"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
            "is_demo": False,
        }
    )

    refresh_bundle = auth_dependencies.create_refresh_token(
        user_id="refresh_only_user",
        username="refresh_only_user",
        roles=["viewer"],
        tenant_id="default",
    )

    with pytest.raises(HTTPException, match="Access token is required") as exc_info:
        asyncio.run(
            auth_dependencies.get_current_user(
                HTTPAuthorizationCredentials(
                    scheme="Bearer",
                    credentials=refresh_bundle["token"],
                )
            )
        )

    assert exc_info.value.status_code == 401


def test_ensure_auth_storage_only_initializes_current_repository_once(monkeypatch):
    calls = []

    def _fake_init_schema():
        calls.append("init_schema")

    def _fake_ensure_tenant(*, tenant_id, name):
        calls.append((tenant_id, name))

    monkeypatch.setattr(auth_dependencies._auth_repository, "init_schema", _fake_init_schema)
    monkeypatch.setattr(auth_dependencies._auth_repository, "ensure_tenant", _fake_ensure_tenant)

    auth_dependencies.ensure_auth_storage()
    auth_dependencies.ensure_auth_storage()

    assert calls == [
        "init_schema",
        ("default", "Default Tenant"),
    ]
