"""
Main FastAPI application for the backend API.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from importlib import import_module

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from src.api.dependencies import (
    count_available_users,
    create_access_token,
    create_refresh_token,
    ensure_security_configuration,
    get_refresh_token_record,
    get_user_record,
    get_user_by_username,
    get_current_user,
    mark_refresh_token_used,
    mark_user_login_success,
    revoke_refresh_token_record,
    REFRESH_TOKEN_EXPIRE_DAYS,
    verify_token,
    verify_password,
)
from src.utils.config import load_config
from src.utils.database_bootstrap import bootstrap_runtime_dependencies
from src.utils.structured_logging import get_logger
logger = get_logger("api")
config = load_config()
cors_config = config.get("api", {}).get("cors", {})
allow_origins = cors_config.get("origins", ["*"]) if cors_config.get("enabled", True) else []
api_config = config.get("api", {})
api_host = str(api_config.get("host", "127.0.0.1"))
api_port = int(api_config.get("port", 8600))
reload_enabled = str(config.get("project", {}).get("environment", "development")).lower() == "development"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize lightweight runtime dependencies."""
    logger.info("Starting API service")
    ensure_security_configuration()
    bootstrap_result = bootstrap_runtime_dependencies(config)
    migration_result = bootstrap_result.get("migrate", {})
    logger.info(
        "Runtime bootstrap completed",
        extra={
            "migration_backend": migration_result.get("backend"),
            "applied_versions": migration_result.get("applied_versions", []),
            "migration_cached": migration_result.get("cached", False),
            "bootstrap_cached": bootstrap_result.get("cached", False),
            "seeded_demo_data": bootstrap_result.get("include_demo_data", False),
        },
    )
    diagnosis_runtime = None
    try:
        diagnosis_router = import_module("src.api.routers.diagnosis_v2")
        diagnosis_runtime = await diagnosis_router.bootstrap_diagnosis_runtime()
        logger.info(
            "Diagnosis runtime bootstrapped",
            extra={
                "executor_backend": (diagnosis_runtime or {}).get("executor", {}).get("backend"),
                "auto_resumed_task_count": len((diagnosis_runtime or {}).get("auto_resumed_task_ids", [])),
            },
        )
    except Exception as exc:
        logger.warning(f"Diagnosis runtime bootstrap skipped: {exc}")
    try:
        intelligence_runtime = import_module("src.intelligence.runtime")
        runtime_payload = await intelligence_runtime.bootstrap_intelligence_runtime()
        logger.info(
            "Industrial intelligence runtime bootstrapped",
            extra={
                "scheduler_running": (runtime_payload or {}).get("scheduler", {}).get("running"),
                "patrol_interval_seconds": (runtime_payload or {}).get("scheduler", {}).get("interval_seconds"),
            },
        )
    except Exception as exc:
        logger.warning(f"Industrial intelligence bootstrap skipped: {exc}")
    try:
        collection_runtime = import_module("src.plc.runtime")
        collection_payload = await collection_runtime.bootstrap_collection_runtime()
        logger.info(
            "PLC collection runtime bootstrapped",
            extra={
                "device_count": (collection_payload or {}).get("device_count"),
                "running": (collection_payload or {}).get("is_running"),
            },
        )
    except Exception as exc:
        logger.warning(f"PLC collection runtime bootstrap skipped: {exc}")
    try:
        yield
    finally:
        try:
            diagnosis_router = import_module("src.api.routers.diagnosis_v2")
            await diagnosis_router.shutdown_diagnosis_runtime()
        except Exception as exc:
            logger.warning(f"Diagnosis runtime shutdown skipped: {exc}")
        try:
            intelligence_runtime = import_module("src.intelligence.runtime")
            await intelligence_runtime.shutdown_intelligence_runtime()
        except Exception as exc:
            logger.warning(f"Industrial intelligence shutdown skipped: {exc}")
        try:
            collection_runtime = import_module("src.plc.runtime")
            await collection_runtime.shutdown_collection_runtime()
        except Exception as exc:
            logger.warning(f"PLC collection runtime shutdown skipped: {exc}")
        logger.info("Stopping API service")


app = FastAPI(
    title="Jamin Industrial Agent API",
    description=(
        "Industrial monitoring, collection, alerting, analysis, and diagnosis APIs."
    ),
    version="v1.0.0-beta2",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


def _include_router(module_path: str):
    try:
        module = import_module(module_path)
        app.include_router(module.router)
        logger.info(f"Included router: {module_path}")
    except Exception as exc:
        logger.warning(f"Skipped router {module_path}: {exc}")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Record basic request telemetry."""
    started = time.time()
    logger.info(
        f"{request.method} {request.url.path}",
        extra={
            "method": request.method,
            "path": request.url.path,
            "query_params": str(request.query_params),
            "client_ip": request.client.host if request.client else None,
        },
    )

    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = round((time.time() - started) * 1000, 2)
        logger.exception(
            "Unhandled request error",
            extra={
                "method": request.method,
                "path": request.url.path,
                "process_time_ms": elapsed_ms,
            },
        )
        raise

    elapsed = time.time() - started
    response.headers["X-Process-Time"] = str(round(elapsed, 4))
    logger.info(
        f"{request.method} {request.url.path} -> {response.status_code}",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "process_time_ms": round(elapsed * 1000, 2),
        },
    )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": f"HTTP_{exc.status_code}",
                "message": exc.detail,
                "path": request.url.path,
            }
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(
        "Unexpected application error",
        extra={
            "method": request.method,
            "path": request.url.path,
            "exception_type": type(exc).__name__,
        },
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Internal server error",
                "path": request.url.path,
            }
        },
    )


for router_module in [
    "src.api.routers.auth",
    "src.api.routers.health",
    "src.api.routers.devices",
    "src.api.routers.collection",
    "src.api.routers.alerts",
    "src.api.routers.reports",
    "src.api.routers.system_config",
    "src.api.routers.analysis",
    "src.api.routers.knowledge",
    "src.api.routers.diagnosis_v2",
    "src.api.routers.intelligence",
]:
    _include_router(router_module)


@app.get("/")
async def root():
    return {
        "name": "Jamin Industrial Agent API",
        "version": "v1.0.0-beta2",
        "status": "running",
        "docs": "/docs",
        "health": "/health",
        "features": [
            "collection",
            "alerts",
            "reports",
            "system-config",
            "analysis",
            "knowledge",
            "multi-agent-diagnosis",
            "industrial-intelligence",
        ],
    }


@app.post("/auth/login")
async def login(credentials: dict):
    username = credentials.get("username")
    password = credentials.get("password")

    if count_available_users() == 0:
        raise HTTPException(
            status_code=503,
            detail="Local demo users are disabled. Configure persistent authentication before using password login in this environment.",
        )

    matched_user = get_user_by_username(username or "")

    stored_password = (
        matched_user.get("password_hash")
        if matched_user
        else None
    ) or (
        matched_user.get("password")
        if matched_user
        else None
    )

    if not matched_user or not verify_password(password or "", stored_password or ""):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    mark_user_login_success(matched_user["user_id"])

    access_token = create_access_token(
        user_id=matched_user["user_id"],
        username=matched_user["username"],
        roles=matched_user.get("roles", []),
        tenant_id=matched_user.get("tenant_id"),
    )
    refresh_bundle = create_refresh_token(
        user_id=matched_user["user_id"],
        username=matched_user["username"],
        roles=matched_user.get("roles", []),
        tenant_id=matched_user.get("tenant_id"),
    )
    return {
        "access_token": access_token,
        "refresh_token": refresh_bundle["token"],
        "token_type": "bearer",
        "expires_in": 1800,
        "refresh_expires_in": REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
        "user": {
            "user_id": matched_user["user_id"],
            "username": matched_user["username"],
            "roles": matched_user.get("roles", []),
        },
    }


@app.post("/auth/refresh")
async def refresh_session(payload: dict):
    refresh_token = payload.get("refresh_token")
    refresh_payload = verify_token(refresh_token or "")

    if not refresh_payload or refresh_payload.get("token_type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    token_id = str(refresh_payload.get("jti") or "").strip()
    if not token_id:
        raise HTTPException(status_code=401, detail="Invalid refresh token payload")

    refresh_record = get_refresh_token_record(token_id)
    if not refresh_record or refresh_record.get("revoked_at") is not None:
        raise HTTPException(status_code=401, detail="Refresh token is no longer valid")

    user = get_user_record(str(refresh_payload.get("sub") or ""))
    if not user or not user.get("is_active", True):
        raise HTTPException(status_code=401, detail="User not found or inactive")

    access_token = create_access_token(
        user_id=user["user_id"],
        username=user["username"],
        roles=user.get("roles", []),
        tenant_id=user.get("tenant_id"),
    )
    next_refresh_bundle = create_refresh_token(
        user_id=user["user_id"],
        username=user["username"],
        roles=user.get("roles", []),
        tenant_id=user.get("tenant_id"),
    )
    mark_refresh_token_used(token_id)
    revoke_refresh_token_record(
        token_id,
        replaced_by_token_id=next_refresh_bundle["token_id"],
    )

    return {
        "access_token": access_token,
        "refresh_token": next_refresh_bundle["token"],
        "token_type": "bearer",
        "expires_in": 1800,
        "refresh_expires_in": REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
        "user": {
            "user_id": user["user_id"],
            "username": user["username"],
            "roles": user.get("roles", []),
        },
    }


@app.post("/auth/logout")
async def logout_session(payload: dict):
    refresh_token = payload.get("refresh_token")
    refresh_payload = verify_token(refresh_token or "")
    token_id = str(refresh_payload.get("jti") or "").strip() if refresh_payload else ""
    if token_id:
        revoke_refresh_token_record(token_id)
    return {"success": True}


@app.get("/auth/me")
async def get_current_user_info(user=Depends(get_current_user)):
    return {
        "user_id": user.user_id,
        "username": user.username,
        "roles": user.roles,
        "tenant_id": user.tenant_id,
        "permissions": user.permissions,
    }


@app.get("/version")
async def get_version():
    return {
        "version": "v1.0.0-beta2",
        "codename": "",
        "build_time": "2026-01-15",
        "git_commit": "beta2-docs-aligned",
        "python_version": "3.11+",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.main:app",
        host=api_host,
        port=api_port,
        reload=reload_enabled,
        log_level="info",
    )
