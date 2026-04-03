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
    _users,
    create_access_token,
    get_current_user,
    init_default_users,
)
from src.api.routers.alerts import init_default_alerts, init_default_rules
from src.api.routers.devices import init_default_devices
from src.utils.config import load_config
from src.utils.health_check import init_default_checks
from src.utils.structured_logging import get_logger
logger = get_logger("api")
config = load_config()
cors_config = config.get("api", {}).get("cors", {})
allow_origins = cors_config.get("origins", ["*"]) if cors_config.get("enabled", True) else []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize lightweight runtime dependencies."""
    logger.info("Starting API service")
    init_default_users()
    init_default_checks()
    init_default_devices()
    init_default_rules()
    init_default_alerts()
    try:
        yield
    finally:
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
    "src.api.routers.health",
    "src.api.routers.devices",
    "src.api.routers.collection",
    "src.api.routers.alerts",
    "src.api.routers.analysis",
    "src.api.routers.knowledge",
    "src.api.routers.diagnosis_v2",
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
            "analysis",
            "knowledge",
            "multi-agent-diagnosis",
        ],
    }


@app.post("/auth/login")
async def login(credentials: dict):
    username = credentials.get("username")
    password = credentials.get("password")

    matched_user = None
    for _, user_data in _users.items():
        if user_data.get("username") == username:
            matched_user = user_data
            break

    if not matched_user or matched_user.get("password") != password:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_access_token(
        user_id=matched_user["user_id"],
        username=matched_user["username"],
        roles=matched_user.get("roles", []),
        tenant_id=matched_user.get("tenant_id"),
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": 1800,
        "user": {
            "user_id": matched_user["user_id"],
            "username": matched_user["username"],
            "roles": matched_user.get("roles", []),
        },
    }


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
        host="127.0.0.1",
        port=8600,
        reload=True,
        log_level="info",
    )
