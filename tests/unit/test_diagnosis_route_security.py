import asyncio

import pytest
from fastapi import HTTPException

from src.api.dependencies import UserContext
from src.api.routers import diagnosis_v2


def _resolve_route_guard(path: str):
    route = next(
        route
        for route in diagnosis_v2.router.routes
        if str(getattr(route, "path", "")).endswith(path)
    )
    return route.dependant.dependencies[0].call


def test_runtime_debug_route_requires_admin_role():
    guard = _resolve_route_guard("/runtime-debug")
    viewer = UserContext(
        user_id="viewer",
        username="viewer",
        roles=["viewer"],
        tenant_id="default",
        permissions=["data:read"],
    )
    admin = UserContext(
        user_id="admin",
        username="admin",
        roles=["admin"],
        tenant_id="default",
        permissions=["*"],
    )

    with pytest.raises(HTTPException, match="Required role: admin") as exc_info:
        asyncio.run(guard(user=viewer))

    assert exc_info.value.status_code == 403
    assert asyncio.run(guard(user=admin)) is admin


def test_model_probe_route_requires_admin_role():
    guard = _resolve_route_guard("/model-probe")
    operator = UserContext(
        user_id="operator",
        username="operator",
        roles=["operator"],
        tenant_id="default",
        permissions=["alert:read", "data:read"],
    )
    admin = UserContext(
        user_id="admin",
        username="admin",
        roles=["admin"],
        tenant_id="default",
        permissions=["*"],
    )

    with pytest.raises(HTTPException, match="Required role: admin") as exc_info:
        asyncio.run(guard(user=operator))

    assert exc_info.value.status_code == 403
    assert asyncio.run(guard(user=admin)) is admin
