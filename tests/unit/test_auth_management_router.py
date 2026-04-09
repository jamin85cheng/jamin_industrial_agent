import asyncio
from datetime import timedelta

import pytest
from fastapi import HTTPException

from src.api.dependencies import UserContext, verify_password
from src.api.repositories.auth_repository import AuthRepository
from src.api.routers.auth import (
    AuthRoleListResponse,
    AuthSessionListResponse,
    AuthTenantCreateRequest,
    AuthTenantUpdateRequest,
    AuthUserCreateRequest,
    AuthUserUpdateRequest,
    create_tenant,
    create_user,
    get_permission_snapshot,
    list_roles,
    list_sessions,
    list_tenants,
    list_users,
    revoke_session,
    update_tenant,
    update_user,
)


def _make_repo(tmp_path):
    case_dir = tmp_path / "auth_management_router"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = AuthRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )
    repo.init_schema()
    repo.ensure_tenant(tenant_id="default", name="Default Tenant")
    return repo


def _admin_user():
    return UserContext(
        user_id="admin",
        username="admin",
        roles=["admin"],
        tenant_id="default",
        permissions=["*"],
    )


def _tenant_operator(tenant_id: str = "tenant-a"):
    return UserContext(
        user_id="operator",
        username="operator",
        roles=["operator"],
        tenant_id=tenant_id,
        permissions=["user:read", "user:write"],
    )


def test_admin_can_create_and_list_tenants(tmp_path):
    repo = _make_repo(tmp_path)

    created = asyncio.run(
        create_tenant(
            request=AuthTenantCreateRequest(id="tenant-a", name="Tenant A"),
            user=_admin_user(),
            auth_repository=repo,
        )
    )
    listing = asyncio.run(list_tenants(user=_admin_user(), auth_repository=repo))

    assert created.id == "tenant-a"
    assert listing.total == 2
    assert {tenant.id for tenant in listing.tenants} == {"default", "tenant-a"}


def test_admin_can_create_user_in_specific_tenant(tmp_path):
    repo = _make_repo(tmp_path)
    repo.ensure_tenant(tenant_id="tenant-a", name="Tenant A")

    created = asyncio.run(
        create_user(
            request=AuthUserCreateRequest(
                username="alice",
                password="StrongPass123",
                roles=["operator"],
                permissions=["user:read"],
                tenant_id="tenant-a",
            ),
            user=_admin_user(),
            auth_repository=repo,
        )
    )

    stored = repo.get_user_by_username("alice")
    assert created.tenant_id == "tenant-a"
    assert stored is not None
    assert stored["password_hash"] != "StrongPass123"
    assert verify_password("StrongPass123", stored["password_hash"]) is True


def test_list_roles_returns_seeded_rbac_catalog(tmp_path):
    repo = _make_repo(tmp_path)

    response = asyncio.run(list_roles(user=_admin_user(), auth_repository=repo))

    assert isinstance(response, AuthRoleListResponse)
    assert {"admin", "operator", "viewer"}.issubset({role.id for role in response.roles})
    admin_role = next(role for role in response.roles if role.id == "admin")
    assert "*" in admin_role.permissions


def test_non_admin_list_users_is_scoped_to_own_tenant(tmp_path):
    repo = _make_repo(tmp_path)
    repo.ensure_tenant(tenant_id="tenant-a", name="Tenant A")
    repo.ensure_tenant(tenant_id="tenant-b", name="Tenant B")
    repo.upsert_user(
        {
            "user_id": "tenant_a_user",
            "username": "tenant-a-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["operator"],
            "permissions": ["user:read"],
            "tenant_id": "tenant-a",
            "is_active": True,
        }
    )
    repo.upsert_user(
        {
            "user_id": "tenant_b_user",
            "username": "tenant-b-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["operator"],
            "permissions": ["user:read"],
            "tenant_id": "tenant-b",
            "is_active": True,
        }
    )

    response = asyncio.run(
        list_users(
            tenant_id=None,
            include_inactive=True,
            user=_tenant_operator("tenant-a"),
            auth_repository=repo,
        )
    )

    assert response.total == 1
    assert response.users[0].tenant_id == "tenant-a"
    assert response.users[0].username == "tenant-a-user"


def test_non_admin_cannot_create_cross_tenant_user(tmp_path):
    repo = _make_repo(tmp_path)
    repo.ensure_tenant(tenant_id="tenant-a", name="Tenant A")
    repo.ensure_tenant(tenant_id="tenant-b", name="Tenant B")

    with pytest.raises(HTTPException, match="Cross-tenant user management is not allowed") as exc_info:
        asyncio.run(
            create_user(
                request=AuthUserCreateRequest(
                    username="bob",
                    password="StrongPass123",
                    roles=["viewer"],
                    permissions=["user:read"],
                    tenant_id="tenant-b",
                ),
                user=_tenant_operator("tenant-a"),
                auth_repository=repo,
            )
        )

    assert exc_info.value.status_code == 403


def test_create_user_rejects_unknown_role(tmp_path):
    repo = _make_repo(tmp_path)

    with pytest.raises(HTTPException, match="Unknown roles: tenant_manager") as exc_info:
        asyncio.run(
            create_user(
                request=AuthUserCreateRequest(
                    username="role-check",
                    password="StrongPass123",
                    roles=["tenant_manager"],
                    permissions=[],
                    tenant_id="default",
                ),
                user=_admin_user(),
                auth_repository=repo,
            )
        )

    assert exc_info.value.status_code == 422


def test_admin_can_update_user_status_and_roles(tmp_path):
    repo = _make_repo(tmp_path)
    repo.upsert_user(
        {
            "user_id": "managed_user",
            "username": "managed-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["viewer"],
            "permissions": ["report:read"],
            "tenant_id": "default",
            "is_active": True,
        }
    )

    updated = asyncio.run(
        update_user(
            user_id="managed_user",
            request=AuthUserUpdateRequest(
                roles=["operator"],
                permissions=["report:export"],
                is_active=False,
            ),
            user=_admin_user(),
            auth_repository=repo,
        )
    )

    assert updated.roles == ["operator"]
    assert updated.is_active is False
    assert "report:export" in updated.permissions


def test_current_user_cannot_deactivate_self(tmp_path):
    repo = _make_repo(tmp_path)
    repo.upsert_user(
        {
            "user_id": "admin",
            "username": "admin",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["admin"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
        }
    )

    with pytest.raises(HTTPException, match="cannot deactivate") as exc_info:
        asyncio.run(
            update_user(
                user_id="admin",
                request=AuthUserUpdateRequest(is_active=False),
                user=_admin_user(),
                auth_repository=repo,
            )
        )

    assert exc_info.value.status_code == 400


def test_admin_can_update_tenant_status(tmp_path):
    repo = _make_repo(tmp_path)
    repo.ensure_tenant(tenant_id="tenant-b", name="Tenant B")

    updated = asyncio.run(
        update_tenant(
            tenant_id="tenant-b",
            request=AuthTenantUpdateRequest(name="Tenant Beta", status="suspended"),
            user=_admin_user(),
            auth_repository=repo,
        )
    )

    assert updated.name == "Tenant Beta"
    assert updated.status == "suspended"


def test_admin_can_list_active_sessions(tmp_path):
    repo = _make_repo(tmp_path)
    repo.upsert_user(
        {
            "user_id": "managed_user",
            "username": "managed-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["viewer"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
        }
    )
    repo.store_refresh_token(
        token_id="active-session",
        user_id="managed_user",
        tenant_id="default",
        expires_at=repo.get_user("managed_user")["created_at"] + timedelta(days=1),
    )
    repo.store_refresh_token(
        token_id="revoked-session",
        user_id="managed_user",
        tenant_id="default",
        expires_at=repo.get_user("managed_user")["created_at"] + timedelta(days=1),
    )
    repo.revoke_refresh_token("revoked-session")

    response = asyncio.run(
        list_sessions(
            tenant_id=None,
            user_id=None,
            include_revoked=False,
            user=_admin_user(),
            auth_repository=repo,
        )
    )

    assert isinstance(response, AuthSessionListResponse)
    assert response.total == 1
    assert response.sessions[0].token_id == "active-session"
    assert response.sessions[0].username == "managed-user"
    assert response.sessions[0].status == "active"


def test_admin_can_revoke_session(tmp_path):
    repo = _make_repo(tmp_path)
    repo.upsert_user(
        {
            "user_id": "managed_user",
            "username": "managed-user",
            "password_hash": "pbkdf2_sha256$demo",
            "roles": ["viewer"],
            "permissions": [],
            "tenant_id": "default",
            "is_active": True,
        }
    )
    repo.store_refresh_token(
        token_id="revoke-target",
        user_id="managed_user",
        tenant_id="default",
        expires_at=repo.get_user("managed_user")["created_at"] + timedelta(days=1),
    )

    revoked = asyncio.run(
        revoke_session(
            token_id="revoke-target",
            user=_admin_user(),
            auth_repository=repo,
        )
    )

    stored = repo.get_refresh_token("revoke-target")
    assert revoked.status == "revoked"
    assert stored is not None
    assert stored["revoked_at"] is not None


def test_default_tenant_cannot_be_suspended(tmp_path):
    repo = _make_repo(tmp_path)

    with pytest.raises(HTTPException, match="default tenant cannot be suspended") as exc_info:
        asyncio.run(
            update_tenant(
                tenant_id="default",
                request=AuthTenantUpdateRequest(status="suspended"),
                user=_admin_user(),
                auth_repository=repo,
            )
        )

    assert exc_info.value.status_code == 400


def test_permission_snapshot_returns_current_claims():
    user = UserContext(
        user_id="viewer",
        username="viewer",
        roles=["viewer"],
        tenant_id="default",
        permissions=["report:read"],
    )

    snapshot = asyncio.run(get_permission_snapshot(user=user))

    assert snapshot["user_id"] == "viewer"
    assert snapshot["permissions"] == ["report:read"]
