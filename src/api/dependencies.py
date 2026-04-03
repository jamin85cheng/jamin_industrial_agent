"""
Dependency helpers for the FastAPI backend.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.thread_safe import ThreadSafeDict

try:
    import jwt  # type: ignore
except ImportError:
    jwt = None

_config = load_config()
_database_adapter = build_runtime_database_adapter(_config.get("database", {}))

SECRET_KEY = _config.get("security", {}).get("jwt_secret", "your-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
security = HTTPBearer(auto_error=False)


class UserContext:
    """Authenticated user context."""

    def __init__(
        self,
        user_id: str,
        username: str,
        roles: Optional[List[str]] = None,
        tenant_id: Optional[str] = None,
        permissions: Optional[List[str]] = None,
    ):
        self.user_id = user_id
        self.username = username
        self.roles = roles or []
        self.tenant_id = tenant_id
        self.permissions = permissions or []

    def has_permission(self, permission: str) -> bool:
        return (
            "*" in self.permissions
            or permission in self.permissions
            or "admin" in self.roles
        )

    def has_role(self, role: str) -> bool:
        return role in self.roles


_users: ThreadSafeDict[Dict[str, Any]] = ThreadSafeDict()
_tokens: ThreadSafeDict[Dict[str, Any]] = ThreadSafeDict()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_access_token(
    user_id: str,
    username: str,
    roles: List[str],
    tenant_id: Optional[str] = None,
) -> str:
    expire = utc_now() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": user_id,
        "username": username,
        "roles": roles,
        "tenant_id": tenant_id,
        "exp": int(expire.timestamp()),
        "iat": int(utc_now().timestamp()),
    }
    token = _encode_token(payload)
    _tokens.set(
        token,
        {
            "user_id": user_id,
            "username": username,
            "roles": roles,
            "tenant_id": tenant_id,
            "expires_at": expire.isoformat(),
        },
    )
    return token


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        return _decode_token(token)
    except Exception:
        return None


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _encode_token(payload: Dict[str, Any]) -> str:
    if jwt is not None:
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    header = {"alg": ALGORITHM, "typ": "JWT"}
    header_segment = _urlsafe_b64encode(
        json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    payload_segment = _urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    signature = hmac.new(
        SECRET_KEY.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    return f"{header_segment}.{payload_segment}.{_urlsafe_b64encode(signature)}"


def _decode_token(token: str) -> Dict[str, Any]:
    if jwt is not None:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    try:
        header_segment, payload_segment, signature_segment = token.split(".")
    except ValueError as exc:
        raise ValueError("Malformed token") from exc

    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    expected_signature = hmac.new(
        SECRET_KEY.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    actual_signature = _urlsafe_b64decode(signature_segment)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ValueError("Invalid token signature")

    payload = json.loads(_urlsafe_b64decode(payload_segment).decode("utf-8"))
    exp = payload.get("exp")
    if exp is not None and int(exp) < int(utc_now().timestamp()):
        raise ValueError("Token expired")
    return payload


def init_default_users():
    if _users.size() > 0:
        return

    default_users = [
        {
            "user_id": "admin",
            "username": "admin",
            "password": "admin123",
            "roles": ["admin"],
            "tenant_id": "default",
            "permissions": ["*"],
        },
        {
            "user_id": "operator",
            "username": "operator",
            "password": "operator123",
            "roles": ["operator"],
            "tenant_id": "default",
            "permissions": [
                "device:read",
                "device:write",
                "data:read",
                "alert:read",
                "alert:acknowledge",
                "report:read",
                "report:export",
            ],
        },
        {
            "user_id": "viewer",
            "username": "viewer",
            "password": "viewer123",
            "roles": ["viewer"],
            "tenant_id": "default",
            "permissions": [
                "device:read",
                "data:read",
                "alert:read",
                "report:read",
            ],
        },
    ]

    for user in default_users:
        _users.set(user["user_id"], user)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> UserContext:
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials were not provided",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_data = _users.get(payload["sub"])
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    return UserContext(
        user_id=payload["sub"],
        username=payload["username"],
        roles=payload.get("roles", []),
        tenant_id=payload.get("tenant_id"),
        permissions=user_data.get("permissions", []),
    )


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> Optional[UserContext]:
    if not credentials:
        return None
    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None


def require_permissions(*permissions: str):
    async def permission_checker(user: UserContext = Depends(get_current_user)):
        missing = [permission for permission in permissions if not user.has_permission(permission)]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing permissions: {', '.join(missing)}",
            )
        return user

    return permission_checker


def require_roles(*roles: str):
    async def role_checker(user: UserContext = Depends(get_current_user)):
        if not any(user.has_role(role) for role in roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required role: {', '.join(roles)}",
            )
        return user

    return role_checker


async def get_db_connection():
    with _database_adapter.connect() as connection:
        yield connection


async def get_tenant_id(
    user: UserContext = Depends(get_current_user),
    x_tenant_id: Optional[str] = Header(None),
) -> str:
    return user.tenant_id or x_tenant_id or "default"


class TenantContext:
    def __init__(self, tenant_id: str, db_connection=None):
        self.tenant_id = tenant_id
        self.db_connection = db_connection

    def apply_tenant_filter(self, query: str) -> str:
        if "WHERE" in query.upper():
            return f"{query} AND tenant_id = '{self.tenant_id}'"
        return f"{query} WHERE tenant_id = '{self.tenant_id}'"


async def get_tenant_context(
    tenant_id: str = Depends(get_tenant_id),
    db=Depends(get_db_connection),
) -> TenantContext:
    return TenantContext(tenant_id, db)


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> Optional[UserContext]:
    if not x_api_key:
        return None

    api_keys = {
        "demo-key-001": {
            "user_id": "api_user_001",
            "username": "api_user",
            "roles": ["operator"],
            "tenant_id": "default",
            "permissions": ["data:read", "device:read"],
        }
    }

    key_data = api_keys.get(x_api_key)
    if not key_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    return UserContext(
        user_id=key_data["user_id"],
        username=key_data["username"],
        roles=key_data["roles"],
        tenant_id=key_data["tenant_id"],
        permissions=key_data.get("permissions", []),
    )


async def get_current_user_or_api_key(
    user: Optional[UserContext] = Depends(get_optional_user),
    api_user: Optional[UserContext] = Depends(verify_api_key),
) -> UserContext:
    if user:
        return user
    if api_user:
        return api_user
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication is required",
    )


def log_request(request: Request, user: Optional[UserContext] = None):
    from src.utils.structured_logging import get_logger

    logger = get_logger("api.access")
    logger.info(
        f"{request.method} {request.url.path}",
        extra={
            "method": request.method,
            "path": request.url.path,
            "client_ip": request.client.host if request.client else None,
            "user_id": user.user_id if user else None,
            "user_agent": request.headers.get("user-agent"),
        },
    )
