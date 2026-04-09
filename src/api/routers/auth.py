"""Authentication and tenant management API."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from src.api.dependencies import (
    UserContext,
    get_auth_repository,
    get_current_user,
    hash_password,
    require_permissions,
    require_roles,
)
from src.api.repositories.auth_repository import AuthRepository

router = APIRouter(prefix="/auth", tags=["认证与租户管理"])

ALLOWED_TENANT_STATUSES = {"active", "suspended", "pending", "expired"}


class AuthManagedUser(BaseModel):
    user_id: str
    username: str
    roles: List[str]
    permissions: List[str]
    tenant_id: str
    is_active: bool
    is_demo: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None


class AuthManagedUserListResponse(BaseModel):
    total: int
    users: List[AuthManagedUser]


class AuthUserCreateRequest(BaseModel):
    user_id: Optional[str] = Field(default=None, min_length=3, max_length=64)
    username: str = Field(..., min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(..., min_length=8, max_length=128)
    roles: List[str] = Field(default_factory=list)
    permissions: List[str] = Field(default_factory=list)
    tenant_id: Optional[str] = Field(default=None, min_length=1, max_length=64)
    is_active: bool = True
    is_demo: bool = False


class AuthUserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    roles: Optional[List[str]] = None
    permissions: Optional[List[str]] = None
    tenant_id: Optional[str] = Field(default=None, min_length=1, max_length=64)
    is_active: Optional[bool] = None
    is_demo: Optional[bool] = None


class AuthTenantRecord(BaseModel):
    id: str
    name: str
    status: str
    created_at: Optional[datetime] = None
    settings: Dict[str, Any] = Field(default_factory=dict)


class AuthTenantListResponse(BaseModel):
    total: int
    tenants: List[AuthTenantRecord]


class AuthRoleRecord(BaseModel):
    id: str
    name: str
    description: str
    permissions: List[str]
    is_system: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AuthRoleListResponse(BaseModel):
    total: int
    roles: List[AuthRoleRecord]


class AuthSessionRecord(BaseModel):
    token_id: str
    user_id: str
    username: str
    tenant_id: str
    created_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    replaced_by_token_id: Optional[str] = None
    user_is_active: bool = False
    status: str


class AuthSessionListResponse(BaseModel):
    total: int
    sessions: List[AuthSessionRecord]


class AuthTenantCreateRequest(BaseModel):
    id: str = Field(..., min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    name: str = Field(..., min_length=1, max_length=128)
    status: str = Field(default="active", pattern=r"^(active|suspended|pending|expired)$")
    settings: Dict[str, Any] = Field(default_factory=dict)


class AuthTenantUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    status: Optional[str] = Field(default=None, pattern=r"^(active|suspended|pending|expired)$")
    settings: Optional[Dict[str, Any]] = None


def _is_admin(user: UserContext) -> bool:
    return user.has_role("admin") or user.has_permission("*")


def _resolve_user_scope(requested_tenant_id: Optional[str], user: UserContext) -> str:
    effective_tenant_id = requested_tenant_id or user.tenant_id or "default"
    if _is_admin(user):
        return effective_tenant_id
    if effective_tenant_id != (user.tenant_id or "default"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cross-tenant user management is not allowed",
        )
    return effective_tenant_id


def _validate_roles(auth_repository: AuthRepository, roles: Optional[List[str]]) -> None:
    if roles is None:
        return
    available_role_ids = {role["id"] for role in auth_repository.list_roles()}
    missing_roles = [role_id for role_id in roles if role_id not in available_role_ids]
    if missing_roles:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown roles: {', '.join(missing_roles)}",
        )


def _ensure_tenant_exists(auth_repository: AuthRepository, tenant_id: str) -> None:
    if not auth_repository.get_tenant(tenant_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )


def _build_session_record(session: Dict[str, Any]) -> AuthSessionRecord:
    expires_at = session.get("expires_at")
    revoked_at = session.get("revoked_at")
    status_label = "active"
    if revoked_at is not None:
        status_label = "revoked"
    elif isinstance(expires_at, datetime) and expires_at <= datetime.now(timezone.utc):
        status_label = "expired"

    return AuthSessionRecord(
        token_id=str(session["token_id"]),
        user_id=str(session["user_id"]),
        username=str(session.get("username") or session["user_id"]),
        tenant_id=str(session.get("tenant_id") or "default"),
        created_at=session.get("created_at"),
        expires_at=expires_at,
        last_used_at=session.get("last_used_at"),
        revoked_at=revoked_at,
        replaced_by_token_id=session.get("replaced_by_token_id"),
        user_is_active=bool(session.get("user_is_active", False)),
        status=status_label,
    )


@router.get("/users", response_model=AuthManagedUserListResponse)
async def list_users(
    tenant_id: Optional[str] = Query(default=None),
    include_inactive: bool = Query(default=True),
    user: UserContext = Depends(require_permissions("user:read")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    effective_tenant_id = _resolve_user_scope(tenant_id, user)
    users = auth_repository.list_users(
        tenant_id=None if _is_admin(user) and tenant_id is None else effective_tenant_id,
        include_inactive=include_inactive,
    )
    return AuthManagedUserListResponse(
        total=len(users),
        users=[AuthManagedUser(**item) for item in users],
    )


@router.get("/roles", response_model=AuthRoleListResponse)
async def list_roles(
    user: UserContext = Depends(require_permissions("user:read")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    _ = user
    roles = auth_repository.list_roles()
    return AuthRoleListResponse(
        total=len(roles),
        roles=[AuthRoleRecord(**item) for item in roles],
    )


@router.get("/sessions", response_model=AuthSessionListResponse)
async def list_sessions(
    tenant_id: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    include_revoked: bool = Query(default=False),
    user: UserContext = Depends(require_roles("admin")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    _ = user
    sessions = auth_repository.list_refresh_tokens(
        tenant_id=tenant_id,
        user_id=user_id,
        include_revoked=include_revoked,
    )
    records = [_build_session_record(item) for item in sessions]
    return AuthSessionListResponse(total=len(records), sessions=records)


@router.post("/sessions/{token_id}/revoke", response_model=AuthSessionRecord)
async def revoke_session(
    token_id: str,
    user: UserContext = Depends(require_roles("admin")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    _ = user
    session = auth_repository.get_refresh_token(token_id)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session {token_id} not found",
        )

    if session.get("revoked_at") is None:
        auth_repository.revoke_refresh_token(token_id)

    updated = auth_repository.get_refresh_token(token_id) or session
    owner = auth_repository.get_user(str(updated["user_id"])) or {}
    return _build_session_record(
        {
            **updated,
            "username": owner.get("username", updated["user_id"]),
            "user_is_active": owner.get("is_active", False),
        }
    )


@router.post("/users", response_model=AuthManagedUser, status_code=status.HTTP_201_CREATED)
async def create_user(
    request: AuthUserCreateRequest,
    user: UserContext = Depends(require_permissions("user:write")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    effective_tenant_id = _resolve_user_scope(request.tenant_id, user)
    _validate_roles(auth_repository, request.roles)
    _ensure_tenant_exists(auth_repository, effective_tenant_id)

    existing_user = auth_repository.get_user_by_username(request.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Username {request.username} already exists",
        )

    user_id = request.user_id or f"USR_{uuid.uuid4().hex[:12].upper()}"
    if auth_repository.get_user(user_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"User {user_id} already exists",
        )

    created = auth_repository.upsert_user(
        {
            "user_id": user_id,
            "username": request.username,
            "password_hash": hash_password(request.password),
            "roles": request.roles,
            "permissions": request.permissions,
            "tenant_id": effective_tenant_id,
            "is_active": request.is_active,
            "is_demo": request.is_demo,
        }
    )
    return AuthManagedUser(**created)


@router.patch("/users/{user_id}", response_model=AuthManagedUser)
async def update_user(
    user_id: str,
    request: AuthUserUpdateRequest,
    user: UserContext = Depends(require_permissions("user:write")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    existing = auth_repository.get_user(user_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found")

    target_tenant_id = request.tenant_id or existing.get("tenant_id") or "default"
    _resolve_user_scope(target_tenant_id, user)
    _validate_roles(auth_repository, request.roles)
    _ensure_tenant_exists(auth_repository, target_tenant_id)

    if request.username and request.username != existing["username"]:
        username_owner = auth_repository.get_user_by_username(request.username)
        if username_owner and username_owner["user_id"] != user_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Username {request.username} already exists",
            )

    if user.user_id == user_id and request.is_active is False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot deactivate the currently authenticated user",
        )

    if user.user_id == user_id and request.roles is not None and "admin" not in request.roles and user.has_role("admin"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot remove your own admin role from the current session",
        )

    updated = auth_repository.update_user(
        user_id,
        {
            key: value
            for key, value in request.model_dump().items()
            if value is not None
        },
    )
    return AuthManagedUser(**updated)


@router.get("/tenants", response_model=AuthTenantListResponse)
async def list_tenants(
    user: UserContext = Depends(require_roles("admin")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    _ = user
    tenants = auth_repository.list_tenants()
    return AuthTenantListResponse(
        total=len(tenants),
        tenants=[AuthTenantRecord(**tenant) for tenant in tenants],
    )


@router.post("/tenants", response_model=AuthTenantRecord, status_code=status.HTTP_201_CREATED)
async def create_tenant(
    request: AuthTenantCreateRequest,
    user: UserContext = Depends(require_roles("admin")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    _ = user
    if request.status not in ALLOWED_TENANT_STATUSES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid tenant status")
    if auth_repository.get_tenant(request.id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Tenant {request.id} already exists",
        )

    created = auth_repository.create_tenant(
        tenant_id=request.id,
        name=request.name,
        status=request.status,
        settings=request.settings,
    )
    return AuthTenantRecord(**created)


@router.patch("/tenants/{tenant_id}", response_model=AuthTenantRecord)
async def update_tenant(
    tenant_id: str,
    request: AuthTenantUpdateRequest,
    user: UserContext = Depends(require_roles("admin")),
    auth_repository: AuthRepository = Depends(get_auth_repository),
):
    _ = user
    existing = auth_repository.get_tenant(tenant_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Tenant {tenant_id} not found")

    if tenant_id == "default" and request.status in {"suspended", "expired"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The default tenant cannot be suspended or expired",
        )

    updated = auth_repository.update_tenant(
        tenant_id,
        {
            key: value
            for key, value in request.model_dump().items()
            if value is not None
        },
    )
    return AuthTenantRecord(**updated)


@router.get("/me/permissions")
async def get_permission_snapshot(
    user: UserContext = Depends(get_current_user),
):
    return {
        "user_id": user.user_id,
        "username": user.username,
        "roles": user.roles,
        "tenant_id": user.tenant_id,
        "permissions": user.permissions,
    }
