"""Database-backed repository for tenants, roles, and local auth users."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.runtime_migrations import apply_runtime_schema_migrations


DEFAULT_ROLE_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "id": "admin",
        "name": "Administrator",
        "description": "Full platform administrator",
        "permissions": ["*"],
        "is_system": True,
    },
    {
        "id": "operator",
        "name": "Operator",
        "description": "Operational user for diagnosis and alert handling",
        "permissions": [
            "device:read",
            "device:write",
            "data:read",
            "alert:read",
            "alert:acknowledge",
            "report:read",
            "report:export",
        ],
        "is_system": True,
    },
    {
        "id": "viewer",
        "name": "Viewer",
        "description": "Read-only access for dashboards and reports",
        "permissions": [
            "device:read",
            "data:read",
            "alert:read",
            "report:read",
        ],
        "is_system": True,
    },
    {
        "id": "engineer",
        "name": "Engineer",
        "description": "Engineering access for deeper diagnostics and knowledge maintenance",
        "permissions": [
            "device:read",
            "device:write",
            "data:read",
            "alert:read",
            "alert:acknowledge",
            "report:read",
            "report:export",
            "knowledge:read",
            "knowledge:write",
        ],
        "is_system": True,
    },
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _json_loads(payload: Any, default: Any) -> Any:
    if payload is None:
        return default
    if isinstance(payload, (list, dict)):
        return payload
    return json.loads(payload)


def _unique(values: Sequence[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


class AuthRepository:
    """Persists tenants, RBAC metadata, and local auth users to the metadata database."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config or load_config().get("database", {})
        self.adapter = build_runtime_database_adapter(self.db_config)
        self.backend = self.adapter.backend
        self.schema = str(self.db_config.get("postgres", {}).get("schema", "public"))

    @contextmanager
    def _connect(self):
        with self.adapter.connect() as connection:
            yield connection

    def _placeholder(self) -> str:
        return "%s" if self.backend == "postgres" else "?"

    def _table(self, table_name: str) -> str:
        if self.backend == "postgres":
            return f'"{self.schema}".{table_name}'
        return table_name

    def _fetch_all(self, cursor) -> List[Dict[str, Any]]:
        rows = cursor.fetchall()
        if not rows:
            return []
        first = rows[0]
        if isinstance(first, dict):
            return list(rows)
        if hasattr(first, "keys"):
            return [dict(row) for row in rows]
        columns = [column[0] for column in (cursor.description or [])]
        return [dict(zip(columns, row)) for row in rows]

    def _fetch_one(self, cursor) -> Optional[Dict[str, Any]]:
        rows = self._fetch_all(cursor)
        return rows[0] if rows else None

    def _in_clause(self, count: int) -> str:
        if count <= 0:
            raise ValueError("IN clause requires at least one item")
        return ", ".join(self._placeholder() for _ in range(count))

    def init_schema(self) -> None:
        if self.backend == "postgres":
            apply_runtime_schema_migrations(self.db_config)
            self.ensure_default_roles()
            return
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tenants (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    settings_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_roles (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    is_system INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_role_permissions (
                    role_id TEXT NOT NULL,
                    permission TEXT NOT NULL,
                    PRIMARY KEY (role_id, permission),
                    FOREIGN KEY (role_id) REFERENCES auth_roles(id) ON DELETE CASCADE
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_users (
                    user_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    roles_json TEXT NOT NULL DEFAULT '[]',
                    permissions_json TEXT NOT NULL DEFAULT '[]',
                    tenant_id TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    is_demo INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_login_at TEXT NULL,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_user_roles (
                    user_id TEXT NOT NULL,
                    role_id TEXT NOT NULL,
                    PRIMARY KEY (user_id, role_id),
                    FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE,
                    FOREIGN KEY (role_id) REFERENCES auth_roles(id) ON DELETE CASCADE
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_refresh_tokens (
                    token_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    last_used_at TEXT NULL,
                    revoked_at TEXT NULL,
                    replaced_by_token_id TEXT NULL,
                    FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
                )
                """
            )
            connection.commit()

        self.ensure_default_roles()

    def ensure_default_roles(self) -> None:
        for role in DEFAULT_ROLE_DEFINITIONS:
            self.ensure_role(
                role_id=role["id"],
                name=role["name"],
                description=role.get("description", ""),
                permissions=role.get("permissions", []),
                is_system=bool(role.get("is_system", True)),
            )

    def ensure_role(
        self,
        *,
        role_id: str,
        name: str,
        description: str = "",
        permissions: Optional[List[str]] = None,
        is_system: bool = False,
    ) -> Dict[str, Any]:
        existing = self.get_role(role_id)
        normalized_permissions = _unique(permissions or [])
        now = utc_now()

        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".auth_roles (id, name, description, is_system, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        is_system = EXCLUDED.is_system,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        role_id,
                        name,
                        description,
                        is_system,
                        existing.get("created_at", now) if existing else now,
                        now,
                    ),
                )
                cursor.execute(
                    f'DELETE FROM "{self.schema}".auth_role_permissions WHERE role_id = %s',
                    (role_id,),
                )
                for permission in normalized_permissions:
                    cursor.execute(
                        f"""
                        INSERT INTO "{self.schema}".auth_role_permissions (role_id, permission)
                        VALUES (%s, %s)
                        ON CONFLICT (role_id, permission) DO NOTHING
                        """,
                        (role_id, permission),
                    )
            else:
                cursor.execute(
                    """
                    INSERT INTO auth_roles (id, name, description, is_system, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        description = excluded.description,
                        is_system = excluded.is_system,
                        updated_at = excluded.updated_at
                    """,
                    (
                        role_id,
                        name,
                        description,
                        1 if is_system else 0,
                        (existing.get("created_at", now) if existing else now).isoformat()
                        if isinstance(existing.get("created_at", now) if existing else now, datetime)
                        else (existing.get("created_at", now) if existing else now),
                        now.isoformat(),
                    ),
                )
                cursor.execute(
                    "DELETE FROM auth_role_permissions WHERE role_id = ?",
                    (role_id,),
                )
                for permission in normalized_permissions:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO auth_role_permissions (role_id, permission)
                        VALUES (?, ?)
                        """,
                        (role_id, permission),
                    )
            connection.commit()

        return self.get_role(role_id) or {
            "id": role_id,
            "name": name,
            "description": description,
            "permissions": normalized_permissions,
            "is_system": is_system,
            "created_at": existing.get("created_at") if existing else now,
            "updated_at": now,
        }

    def get_role(self, role_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_roles')} WHERE id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (role_id,))
            row = self._fetch_one(cursor)
        if not row:
            return None
        return self._normalize_role(row)

    def list_roles(self) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_roles')} ORDER BY is_system DESC, id ASC"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = self._fetch_all(cursor)
        return [self._normalize_role(row) for row in rows]

    def get_permissions_for_roles(self, role_ids: Sequence[str]) -> List[str]:
        normalized_role_ids = _unique(list(role_ids))
        if not normalized_role_ids:
            return []
        placeholders = self._in_clause(len(normalized_role_ids))
        sql = (
            f"SELECT permission FROM {self._table('auth_role_permissions')} "
            f"WHERE role_id IN ({placeholders}) ORDER BY permission ASC"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(normalized_role_ids))
            rows = self._fetch_all(cursor)
        return _unique([str(row["permission"]) for row in rows])

    def ensure_tenant(
        self,
        *,
        tenant_id: str,
        name: str,
        status: str = "active",
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        existing = self.get_tenant(tenant_id)
        if existing:
            return existing

        created_at = utc_now()
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".tenants (id, name, status, created_at, settings_json)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    """,
                    (tenant_id, name, status, created_at, _json_dumps(settings or {})),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO tenants (id, name, status, created_at, settings_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        tenant_id,
                        name,
                        status,
                        created_at.isoformat(),
                        _json_dumps(settings or {}),
                    ),
                )
            connection.commit()
        return self.get_tenant(tenant_id) or {
            "id": tenant_id,
            "name": name,
            "status": status,
            "created_at": created_at,
            "settings": settings or {},
        }

    def create_tenant(
        self,
        *,
        tenant_id: str,
        name: str,
        status: str = "active",
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self.get_tenant(tenant_id):
            raise ValueError(f"Tenant {tenant_id} already exists")
        return self.ensure_tenant(
            tenant_id=tenant_id,
            name=name,
            status=status,
            settings=settings,
        )

    def update_tenant(
        self,
        tenant_id: str,
        updates: Dict[str, Any],
    ) -> Dict[str, Any]:
        existing = self.get_tenant(tenant_id)
        if not existing:
            raise ValueError(f"Tenant {tenant_id} does not exist")

        merged = {
            "name": updates.get("name", existing["name"]),
            "status": updates.get("status", existing.get("status", "active")),
            "settings": updates.get("settings", existing.get("settings", {})),
        }
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    UPDATE "{self.schema}".tenants
                    SET name = %s, status = %s, settings_json = %s::jsonb
                    WHERE id = %s
                    """,
                    (
                        merged["name"],
                        merged["status"],
                        _json_dumps(merged["settings"]),
                        tenant_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE tenants
                    SET name = ?, status = ?, settings_json = ?
                    WHERE id = ?
                    """,
                    (
                        merged["name"],
                        merged["status"],
                        _json_dumps(merged["settings"]),
                        tenant_id,
                    ),
                )
            connection.commit()
        return self.get_tenant(tenant_id) or {
            "id": tenant_id,
            "name": merged["name"],
            "status": merged["status"],
            "created_at": existing["created_at"],
            "settings": merged["settings"],
        }

    def get_tenant(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('tenants')} WHERE id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (tenant_id,))
            row = self._fetch_one(cursor)
        if not row:
            return None
        return {
            "id": row["id"],
            "name": row["name"],
            "status": row.get("status", "active"),
            "created_at": _parse_datetime(row.get("created_at")),
            "settings": _json_loads(row.get("settings_json"), {}),
        }

    def list_tenants(self) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('tenants')} ORDER BY created_at ASC, id ASC"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = self._fetch_all(cursor)
        return [
            {
                "id": row["id"],
                "name": row["name"],
                "status": row.get("status", "active"),
                "created_at": _parse_datetime(row.get("created_at")),
                "settings": _json_loads(row.get("settings_json"), {}),
            }
            for row in rows
        ]

    def upsert_user(self, user: Dict[str, Any]) -> Dict[str, Any]:
        tenant_id = user.get("tenant_id", "default")
        self.ensure_tenant(
            tenant_id=tenant_id,
            name="Default Tenant" if tenant_id == "default" else tenant_id,
        )

        role_ids = _unique(list(user.get("roles", [])))
        direct_permissions = _unique(list(user.get("permissions", [])))
        for role_id in role_ids:
            if not self.get_role(role_id):
                self.ensure_role(
                    role_id=role_id,
                    name=role_id.replace("_", " ").title(),
                    description="Imported legacy role",
                    permissions=[],
                    is_system=False,
                )

        existing = self.get_user(user["user_id"])
        created_at = existing.get("created_at") if existing else utc_now()
        updated_at = utc_now()
        values = {
            "user_id": user["user_id"],
            "username": user["username"],
            "password_hash": user["password_hash"],
            "roles_json": _json_dumps(role_ids),
            "permissions_json": _json_dumps(direct_permissions),
            "tenant_id": tenant_id,
            "is_active": bool(user.get("is_active", True)),
            "is_demo": bool(user.get("is_demo", False)),
            "created_at": created_at,
            "updated_at": updated_at,
            "last_login_at": user.get("last_login_at"),
        }

        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".auth_users (
                        user_id, username, password_hash, roles_json, permissions_json,
                        tenant_id, is_active, is_demo, created_at, updated_at, last_login_at
                    ) VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        password_hash = EXCLUDED.password_hash,
                        roles_json = EXCLUDED.roles_json,
                        permissions_json = EXCLUDED.permissions_json,
                        tenant_id = EXCLUDED.tenant_id,
                        is_active = EXCLUDED.is_active,
                        is_demo = EXCLUDED.is_demo,
                        updated_at = EXCLUDED.updated_at,
                        last_login_at = EXCLUDED.last_login_at
                    """,
                    (
                        values["user_id"],
                        values["username"],
                        values["password_hash"],
                        values["roles_json"],
                        values["permissions_json"],
                        values["tenant_id"],
                        values["is_active"],
                        values["is_demo"],
                        values["created_at"],
                        values["updated_at"],
                        values["last_login_at"],
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO auth_users (
                        user_id, username, password_hash, roles_json, permissions_json,
                        tenant_id, is_active, is_demo, created_at, updated_at, last_login_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username = excluded.username,
                        password_hash = excluded.password_hash,
                        roles_json = excluded.roles_json,
                        permissions_json = excluded.permissions_json,
                        tenant_id = excluded.tenant_id,
                        is_active = excluded.is_active,
                        is_demo = excluded.is_demo,
                        updated_at = excluded.updated_at,
                        last_login_at = excluded.last_login_at
                    """,
                    (
                        values["user_id"],
                        values["username"],
                        values["password_hash"],
                        values["roles_json"],
                        values["permissions_json"],
                        values["tenant_id"],
                        1 if values["is_active"] else 0,
                        1 if values["is_demo"] else 0,
                        values["created_at"].isoformat() if isinstance(values["created_at"], datetime) else values["created_at"],
                        values["updated_at"].isoformat() if isinstance(values["updated_at"], datetime) else values["updated_at"],
                        values["last_login_at"].isoformat() if isinstance(values["last_login_at"], datetime) else values["last_login_at"],
                    ),
                )
            self._sync_user_roles(cursor, user["user_id"], role_ids)
            connection.commit()

        return self.get_user(user["user_id"]) or dict(user)

    def update_user(self, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        existing = self.get_user(user_id)
        if not existing:
            raise ValueError(f"User {user_id} does not exist")

        merged = {
            "user_id": user_id,
            "username": updates.get("username", existing["username"]),
            "password_hash": updates.get("password_hash", existing["password_hash"]),
            "roles": updates.get("roles", existing["roles"]),
            "permissions": updates.get(
                "permissions",
                existing.get("direct_permissions", existing.get("permissions", [])),
            ),
            "tenant_id": updates.get("tenant_id", existing.get("tenant_id", "default")),
            "is_active": updates.get("is_active", existing.get("is_active", True)),
            "is_demo": updates.get("is_demo", existing.get("is_demo", False)),
            "last_login_at": existing.get("last_login_at"),
        }
        return self.upsert_user(merged)

    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_users')} WHERE user_id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (user_id,))
            row = self._fetch_one(cursor)
        return self._normalize_user(row) if row else None

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_users')} WHERE username = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (username,))
            row = self._fetch_one(cursor)
        return self._normalize_user(row) if row else None

    def list_users(self, *, tenant_id: Optional[str] = None, include_inactive: bool = True) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_users')}"
        params: List[Any] = []
        filters: List[str] = []
        if tenant_id:
            filters.append(f"tenant_id = {self._placeholder()}")
            params.append(tenant_id)
        if not include_inactive:
            filters.append(f"is_active = {self._placeholder()}")
            params.append(True if self.backend == "postgres" else 1)
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        sql += " ORDER BY username ASC"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params))
            return [self._normalize_user(row) for row in self._fetch_all(cursor)]

    def mark_login_success(self, user_id: str) -> None:
        timestamp = utc_now()
        sql = (
            f"UPDATE {self._table('auth_users')} SET last_login_at = {self._placeholder()}, "
            f"updated_at = {self._placeholder()} WHERE user_id = {self._placeholder()}"
        )
        params = (
            timestamp if self.backend == "postgres" else timestamp.isoformat(),
            timestamp if self.backend == "postgres" else timestamp.isoformat(),
            user_id,
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()

    def store_refresh_token(
        self,
        *,
        token_id: str,
        user_id: str,
        tenant_id: str,
        expires_at: datetime,
    ) -> Dict[str, Any]:
        created_at = utc_now()
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".auth_refresh_tokens (
                        token_id, user_id, tenant_id, created_at, expires_at, last_used_at, revoked_at, replaced_by_token_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (token_id) DO UPDATE SET
                        expires_at = EXCLUDED.expires_at,
                        tenant_id = EXCLUDED.tenant_id
                    """,
                    (token_id, user_id, tenant_id, created_at, expires_at, None, None, None),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO auth_refresh_tokens (
                        token_id, user_id, tenant_id, created_at, expires_at, last_used_at, revoked_at, replaced_by_token_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(token_id) DO UPDATE SET
                        expires_at = excluded.expires_at,
                        tenant_id = excluded.tenant_id
                    """,
                    (
                        token_id,
                        user_id,
                        tenant_id,
                        created_at.isoformat(),
                        expires_at.isoformat(),
                        None,
                        None,
                        None,
                    ),
                )
            connection.commit()
        return self.get_refresh_token(token_id) or {
            "token_id": token_id,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "created_at": created_at,
            "expires_at": expires_at,
            "last_used_at": None,
            "revoked_at": None,
            "replaced_by_token_id": None,
        }

    def get_refresh_token(self, token_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_refresh_tokens')} WHERE token_id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (token_id,))
            row = self._fetch_one(cursor)
        if not row:
            return None
        return self._normalize_refresh_token(row)

    def list_refresh_tokens(
        self,
        *,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        include_revoked: bool = False,
    ) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('auth_refresh_tokens')}"
        params: List[Any] = []
        filters: List[str] = []
        if user_id:
            filters.append(f"user_id = {self._placeholder()}")
            params.append(user_id)
        if tenant_id:
            filters.append(f"tenant_id = {self._placeholder()}")
            params.append(tenant_id)
        if not include_revoked:
            filters.append("revoked_at IS NULL")
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        sql += " ORDER BY created_at DESC, token_id DESC"

        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params))
            rows = self._fetch_all(cursor)

        sessions = [self._normalize_refresh_token(row) for row in rows]
        user_ids = _unique([str(session["user_id"]) for session in sessions])
        owners = {session_user_id: self.get_user(session_user_id) for session_user_id in user_ids}

        enriched_sessions: List[Dict[str, Any]] = []
        for session in sessions:
            owner = owners.get(session["user_id"]) or {}
            enriched_sessions.append(
                {
                    **session,
                    "username": owner.get("username", session["user_id"]),
                    "user_is_active": owner.get("is_active", False),
                }
            )
        return enriched_sessions

    def mark_refresh_token_used(self, token_id: str) -> None:
        timestamp = utc_now()
        sql = (
            f"UPDATE {self._table('auth_refresh_tokens')} SET last_used_at = {self._placeholder()} "
            f"WHERE token_id = {self._placeholder()}"
        )
        params = (
            timestamp if self.backend == "postgres" else timestamp.isoformat(),
            token_id,
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()

    def revoke_refresh_token(
        self,
        token_id: str,
        *,
        replaced_by_token_id: Optional[str] = None,
    ) -> None:
        timestamp = utc_now()
        sql = (
            f"UPDATE {self._table('auth_refresh_tokens')} "
            f"SET revoked_at = {self._placeholder()}, replaced_by_token_id = {self._placeholder()} "
            f"WHERE token_id = {self._placeholder()}"
        )
        params = (
            timestamp if self.backend == "postgres" else timestamp.isoformat(),
            replaced_by_token_id,
            token_id,
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()

    def _normalize_role(self, row: Dict[str, Any]) -> Dict[str, Any]:
        role_id = row["id"]
        sql = (
            f"SELECT permission FROM {self._table('auth_role_permissions')} "
            f"WHERE role_id = {self._placeholder()} ORDER BY permission ASC"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (role_id,))
            permission_rows = self._fetch_all(cursor)
        return {
            "id": role_id,
            "name": row["name"],
            "description": row.get("description", ""),
            "permissions": [str(item["permission"]) for item in permission_rows],
            "is_system": bool(row.get("is_system", False)),
            "created_at": _parse_datetime(row.get("created_at")),
            "updated_at": _parse_datetime(row.get("updated_at")),
        }

    def _normalize_user(self, row: Dict[str, Any]) -> Dict[str, Any]:
        direct_permissions = list(_json_loads(row.get("permissions_json"), []))
        fallback_roles = list(_json_loads(row.get("roles_json"), []))
        role_ids = self._get_user_role_ids(row["user_id"], fallback_roles)
        role_permissions = self.get_permissions_for_roles(role_ids)
        effective_permissions = _unique(direct_permissions + role_permissions)
        return {
            "user_id": row["user_id"],
            "username": row["username"],
            "password_hash": row["password_hash"],
            "roles": role_ids,
            "direct_permissions": direct_permissions,
            "role_permissions": role_permissions,
            "permissions": effective_permissions,
            "tenant_id": row.get("tenant_id"),
            "is_active": bool(row.get("is_active", True)),
            "is_demo": bool(row.get("is_demo", False)),
            "created_at": _parse_datetime(row.get("created_at")),
            "updated_at": _parse_datetime(row.get("updated_at")),
            "last_login_at": _parse_datetime(row.get("last_login_at")),
        }

    def _normalize_refresh_token(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "token_id": row["token_id"],
            "user_id": row["user_id"],
            "tenant_id": row["tenant_id"],
            "created_at": _parse_datetime(row.get("created_at")),
            "expires_at": _parse_datetime(row.get("expires_at")),
            "last_used_at": _parse_datetime(row.get("last_used_at")),
            "revoked_at": _parse_datetime(row.get("revoked_at")),
            "replaced_by_token_id": row.get("replaced_by_token_id"),
        }

    def _get_user_role_ids(self, user_id: str, fallback_roles: Optional[List[str]] = None) -> List[str]:
        sql = (
            f"SELECT role_id FROM {self._table('auth_user_roles')} "
            f"WHERE user_id = {self._placeholder()} ORDER BY role_id ASC"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (user_id,))
            rows = self._fetch_all(cursor)
        if rows:
            return [str(row["role_id"]) for row in rows]
        return list(fallback_roles or [])

    def _sync_user_roles(self, cursor, user_id: str, role_ids: List[str]) -> None:
        if self.backend == "postgres":
            cursor.execute(
                f'DELETE FROM "{self.schema}".auth_user_roles WHERE user_id = %s',
                (user_id,),
            )
            for role_id in role_ids:
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".auth_user_roles (user_id, role_id)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id, role_id) DO NOTHING
                    """,
                    (user_id, role_id),
                )
        else:
            cursor.execute(
                "DELETE FROM auth_user_roles WHERE user_id = ?",
                (user_id,),
            )
            for role_id in role_ids:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO auth_user_roles (user_id, role_id)
                    VALUES (?, ?)
                    """,
                    (user_id, role_id),
                )
