"""
Dependency helpers for the FastAPI backend.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.api.repositories.auth_repository import AuthRepository
from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.structured_logging import get_logger
from src.utils.thread_safe import ThreadSafeDict

try:
    import jwt  # type: ignore
except ImportError:
    jwt = None

_config = load_config()
_database_adapter = build_runtime_database_adapter(_config.get("database", {}))
_auth_repository = AuthRepository(_config.get("database", {}))
_security_config = _config.get("security", {})
_runtime_environment = str(
    _config.get("project", {}).get("environment", "development")
).lower()
logger = get_logger("api.auth")

JWT_SECRET_PLACEHOLDERS = {"", "your-secret-key", "change-me", "replace-me"}
DEVELOPMENT_JWT_SECRET_FALLBACK = "development-only-jwt-secret"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7
PASSWORD_HASH_ITERATIONS = 600_000
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
_auth_storage_initialized = False
_auth_storage_repository_id: Optional[int] = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_development_environment() -> bool:
    return _runtime_environment == "development"


def get_jwt_secret() -> str:
    secret = str(_security_config.get("jwt_secret", "") or "").strip()
    if secret and secret not in JWT_SECRET_PLACEHOLDERS:
        return secret

    if is_development_environment():
        logger.warning(
            "JWT secret is not configured; using a development-only fallback secret."
        )
        return DEVELOPMENT_JWT_SECRET_FALLBACK

    raise RuntimeError(
        "JWT secret is not configured. Set security.jwt_secret or JWT_SECRET before starting a non-development environment."
    )


def ensure_security_configuration() -> None:
    get_jwt_secret()
    if not is_development_environment() and demo_users_enabled():
        raise RuntimeError(
            "Demo users are enabled in a non-development environment. Disable security.demo_users or explicitly allow them only for controlled demo deployments."
        )
    ensure_auth_storage()


def ensure_auth_storage() -> None:
    global _auth_storage_initialized, _auth_storage_repository_id
    repository_marker = id(_auth_repository)
    if _auth_storage_initialized and _auth_storage_repository_id == repository_marker:
        return
    _auth_repository.init_schema()
    _auth_repository.ensure_tenant(tenant_id="default", name="Default Tenant")
    _auth_storage_initialized = True
    _auth_storage_repository_id = repository_marker


def get_auth_repository() -> AuthRepository:
    ensure_auth_storage()
    return _auth_repository


def demo_users_enabled() -> bool:
    demo_config = _security_config.get("demo_users", {})
    if not demo_config.get("enabled", True):
        return False
    if is_development_environment():
        return True
    return bool(demo_config.get("allow_in_production", False))


def hash_password(password: str, *, iterations: int = PASSWORD_HASH_ITERATIONS) -> str:
    salt = hashlib.sha256(f"{password}:{utc_now().isoformat()}".encode("utf-8")).digest()[:16]
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    salt_segment = _urlsafe_b64encode(salt)
    digest_segment = _urlsafe_b64encode(derived)
    return f"pbkdf2_sha256${iterations}${salt_segment}${digest_segment}"


def verify_password(password: str, stored_password: str) -> bool:
    if not stored_password:
        return False

    if not stored_password.startswith("pbkdf2_sha256$"):
        return hmac.compare_digest(password, stored_password)

    try:
        _, iteration_segment, salt_segment, digest_segment = stored_password.split("$", 3)
        iterations = int(iteration_segment)
        salt = _urlsafe_b64decode(salt_segment)
        expected_digest = _urlsafe_b64decode(digest_segment)
    except Exception:
        return False

    actual_digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_digest, expected_digest)


def build_default_user_payloads() -> List[Dict[str, Any]]:
    return [
        {
            "user_id": "admin",
            "username": "admin",
            "password_hash": hash_password("admin123"),
            "roles": ["admin"],
            "tenant_id": "default",
            "permissions": [],
            "is_active": True,
            "is_demo": True,
        },
        {
            "user_id": "operator",
            "username": "operator",
            "password_hash": hash_password("operator123"),
            "roles": ["operator"],
            "tenant_id": "default",
            "permissions": [],
            "is_active": True,
            "is_demo": True,
        },
        {
            "user_id": "viewer",
            "username": "viewer",
            "password_hash": hash_password("viewer123"),
            "roles": ["viewer"],
            "tenant_id": "default",
            "permissions": [],
            "is_active": True,
            "is_demo": True,
        },
    ]


def _cache_user_record(user: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(user)
    _users.set(normalized["user_id"], normalized)
    return normalized


def get_user_record(user_id: str) -> Optional[Dict[str, Any]]:
    ensure_auth_storage()
    cached = _users.get(user_id)
    if cached:
        return cached

    user = _auth_repository.get_user(user_id)
    if user:
        return _cache_user_record(user)
    return None


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    ensure_auth_storage()
    user = _auth_repository.get_user_by_username(username)
    if user:
        return _cache_user_record(user)

    for _, cached_user in _users.items():
        if cached_user.get("username") == username:
            return cached_user
    return None


def count_available_users() -> int:
    ensure_auth_storage()
    persistent_users = _auth_repository.list_users()
    if persistent_users:
        for user in persistent_users:
            _cache_user_record(user)
        return len(persistent_users)
    return _users.size()


def mark_user_login_success(user_id: str) -> None:
    ensure_auth_storage()
    _auth_repository.mark_login_success(user_id)
    refreshed = _auth_repository.get_user(user_id)
    if refreshed:
        _cache_user_record(refreshed)


def get_refresh_token_record(token_id: str) -> Optional[Dict[str, Any]]:
    ensure_auth_storage()
    return _auth_repository.get_refresh_token(token_id)


def revoke_refresh_token_record(
    token_id: str,
    *,
    replaced_by_token_id: Optional[str] = None,
) -> None:
    ensure_auth_storage()
    _auth_repository.revoke_refresh_token(
        token_id,
        replaced_by_token_id=replaced_by_token_id,
    )


def mark_refresh_token_used(token_id: str) -> None:
    ensure_auth_storage()
    _auth_repository.mark_refresh_token_used(token_id)


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
        "token_type": "access",
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


def create_refresh_token(
    user_id: str,
    username: str,
    roles: List[str],
    tenant_id: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_auth_storage()
    expires_at = utc_now() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    token_id = uuid.uuid4().hex
    payload = {
        "sub": user_id,
        "username": username,
        "roles": roles,
        "tenant_id": tenant_id,
        "token_type": "refresh",
        "jti": token_id,
        "exp": int(expires_at.timestamp()),
        "iat": int(utc_now().timestamp()),
    }
    token = _encode_token(payload)
    _auth_repository.store_refresh_token(
        token_id=token_id,
        user_id=user_id,
        tenant_id=tenant_id or "default",
        expires_at=expires_at,
    )
    return {
        "token": token,
        "token_id": token_id,
        "expires_at": expires_at,
        "expires_in": int(timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS).total_seconds()),
    }


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
    secret_key = get_jwt_secret()
    if jwt is not None:
        return jwt.encode(payload, secret_key, algorithm=ALGORITHM)

    header = {"alg": ALGORITHM, "typ": "JWT"}
    header_segment = _urlsafe_b64encode(
        json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    payload_segment = _urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    signature = hmac.new(
        secret_key.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    return f"{header_segment}.{payload_segment}.{_urlsafe_b64encode(signature)}"


def _decode_token(token: str) -> Dict[str, Any]:
    secret_key = get_jwt_secret()
    if jwt is not None:
        return jwt.decode(token, secret_key, algorithms=[ALGORITHM])

    try:
        header_segment, payload_segment, signature_segment = token.split(".")
    except ValueError as exc:
        raise ValueError("Malformed token") from exc

    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    expected_signature = hmac.new(
        secret_key.encode("utf-8"),
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
    if not demo_users_enabled():
        return

    ensure_auth_storage()
    existing_users = _auth_repository.list_users(tenant_id="default")
    if existing_users:
        for user in existing_users:
            _cache_user_record(user)
        return

    for user in build_default_user_payloads():
        _cache_user_record(_auth_repository.upsert_user(user))


def get_configured_api_keys() -> Dict[str, Dict[str, Any]]:
    api_key_config = _security_config.get("api_keys", {})
    if not api_key_config.get("enabled", False):
        return {}

    configured_keys: Dict[str, Dict[str, Any]] = {}
    for item in api_key_config.get("keys", []):
        key = str(item.get("key", "") or "").strip()
        if not key:
            continue
        configured_keys[key] = {
            "user_id": item.get("user_id", "api_user"),
            "username": item.get("username", "api_user"),
            "roles": list(item.get("roles", [])),
            "tenant_id": item.get("tenant_id", "default"),
            "permissions": list(item.get("permissions", [])),
        }
    return configured_keys


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

    token_type = payload.get("token_type", "access")
    if token_type != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token is required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_data = get_user_record(payload["sub"])
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    return UserContext(
        user_id=user_data.get("user_id", payload["sub"]),
        username=user_data.get("username", payload["username"]),
        roles=user_data.get("roles", payload.get("roles", [])),
        tenant_id=user_data.get("tenant_id", payload.get("tenant_id")),
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

    api_keys = get_configured_api_keys()
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
