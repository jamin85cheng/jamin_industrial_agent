from datetime import timedelta

from src.api.repositories.auth_repository import AuthRepository


def _make_repo(tmp_path):
    case_dir = tmp_path / "auth_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    return AuthRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )


def test_auth_repository_initializes_schema_and_default_tenant(tmp_path):
    repo = _make_repo(tmp_path)

    repo.init_schema()
    tenant = repo.ensure_tenant(tenant_id="default", name="Default Tenant")
    roles = repo.list_roles()

    assert tenant["id"] == "default"
    assert repo.get_tenant("default") is not None
    assert {"admin", "operator", "viewer"}.issubset({role["id"] for role in roles})


def test_auth_repository_upserts_and_fetches_user(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")

    stored = repo.upsert_user(
        {
            "user_id": "operator",
            "username": "operator",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["operator"],
            "permissions": ["data:read", "alert:read"],
            "tenant_id": "default",
            "is_active": True,
            "is_demo": True,
        }
    )

    fetched_by_id = repo.get_user("operator")
    fetched_by_username = repo.get_user_by_username("operator")

    assert stored["username"] == "operator"
    assert fetched_by_id is not None
    assert fetched_by_id["roles"] == ["operator"]
    assert fetched_by_username is not None
    assert fetched_by_username["direct_permissions"] == ["data:read", "alert:read"]
    assert "alert:acknowledge" in fetched_by_username["role_permissions"]
    assert "data:read" in fetched_by_username["permissions"]


def test_auth_repository_marks_last_login(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")
    repo.upsert_user(
        {
            "user_id": "viewer",
            "username": "viewer",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["viewer"],
            "permissions": ["report:read"],
            "tenant_id": "default",
            "is_active": True,
            "is_demo": True,
        }
    )

    repo.mark_login_success("viewer")

    fetched = repo.get_user("viewer")
    assert fetched is not None
    assert fetched["last_login_at"] is not None


def test_auth_repository_resolves_role_permissions(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")

    repo.upsert_user(
        {
            "user_id": "rbac_operator",
            "username": "rbac-operator",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["operator"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
        }
    )

    fetched = repo.get_user("rbac_operator")

    assert fetched is not None
    assert fetched["direct_permissions"] == []
    assert "device:write" in fetched["role_permissions"]
    assert "alert:acknowledge" in fetched["permissions"]


def test_auth_repository_stores_and_revokes_refresh_tokens(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")
    repo.upsert_user(
        {
            "user_id": "session_user",
            "username": "session-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["viewer"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
        }
    )

    stored = repo.store_refresh_token(
        token_id="refresh-token-id",
        user_id="session_user",
        tenant_id="default",
        expires_at=repo.get_user("session_user")["created_at"],
    )
    repo.mark_refresh_token_used("refresh-token-id")
    repo.revoke_refresh_token("refresh-token-id", replaced_by_token_id="next-token")

    fetched = repo.get_refresh_token("refresh-token-id")

    assert stored["token_id"] == "refresh-token-id"
    assert fetched is not None
    assert fetched["last_used_at"] is not None
    assert fetched["revoked_at"] is not None
    assert fetched["replaced_by_token_id"] == "next-token"


def test_auth_repository_updates_user_without_overwriting_password(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")
    original = repo.upsert_user(
        {
            "user_id": "editable_user",
            "username": "editable-user",
            "password_hash": "pbkdf2_sha256$original",
            "roles": ["viewer"],
            "permissions": ["report:read"],
            "tenant_id": "default",
            "is_active": True,
            "is_demo": False,
        }
    )

    updated = repo.update_user(
        "editable_user",
        {
            "roles": ["operator"],
            "permissions": ["report:export"],
            "is_active": False,
        },
    )

    assert updated["password_hash"] == original["password_hash"]
    assert updated["roles"] == ["operator"]
    assert updated["direct_permissions"] == ["report:export"]
    assert updated["is_active"] is False


def test_auth_repository_updates_tenant_metadata(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="tenant-a", name="Tenant A")

    updated = repo.update_tenant(
        "tenant-a",
        {
            "name": "Tenant Alpha",
            "status": "suspended",
            "settings": {"region": "cn-east"},
        },
    )

    assert updated["name"] == "Tenant Alpha"
    assert updated["status"] == "suspended"
    assert updated["settings"] == {"region": "cn-east"}


def test_auth_repository_lists_active_refresh_sessions_with_user_context(tmp_path):
    repo = _make_repo(tmp_path)
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")
    repo.upsert_user(
        {
            "user_id": "session_user",
            "username": "session-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["viewer"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
        }
    )

    active = repo.store_refresh_token(
        token_id="active-token",
        user_id="session_user",
        tenant_id="default",
        expires_at=repo.get_user("session_user")["created_at"] + timedelta(days=1),
    )
    repo.store_refresh_token(
        token_id="revoked-token",
        user_id="session_user",
        tenant_id="default",
        expires_at=active["expires_at"],
    )
    repo.revoke_refresh_token("revoked-token")

    active_sessions = repo.list_refresh_tokens()
    all_sessions = repo.list_refresh_tokens(include_revoked=True)

    assert [session["token_id"] for session in active_sessions] == ["active-token"]
    assert {session["token_id"] for session in all_sessions} == {"active-token", "revoked-token"}
    assert active_sessions[0]["username"] == "session-user"
    assert active_sessions[0]["user_is_active"] is True
