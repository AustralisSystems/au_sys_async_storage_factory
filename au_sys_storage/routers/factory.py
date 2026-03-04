"""
FastAPI router for Storage Factory and Health endpoints.
"""

import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from au_sys_storage.admin_portal import AdminPortalService
from au_sys_storage.factory import AsyncStorageFactory
from au_sys_storage.routers._deps import get_admin_service, get_factory

router = APIRouter()


class ProviderInfo(BaseModel):
    name: str
    type: str
    available: bool
    failover_tier: Optional[int] = None
    backend_class: str


class FactoryHealth(BaseModel):
    status: str
    providers: list[dict[str, Any]]


class ProviderHealth(BaseModel):
    healthy: bool
    last_check: str
    details: Optional[dict[str, Any]] = None


@router.get("/providers", response_model=list[ProviderInfo])
async def list_providers(factory: AsyncStorageFactory = Depends(get_factory)) -> list[ProviderInfo]:
    """List all registered provider names and basic info."""
    providers = []
    for name, config in factory._manifest.backends.items():
        providers.append(
            ProviderInfo(
                name=name,
                type=config.type.value,
                available=name in factory._instances,
                failover_tier=None,
                backend_class=config.provider,
            )
        )
    return providers


@router.get("/providers/{name}", response_model=ProviderInfo)
async def get_provider(name: str, factory: AsyncStorageFactory = Depends(get_factory)) -> ProviderInfo:
    """Inspect a named provider."""
    config = factory._manifest.backends.get(name)
    if not config:
        raise HTTPException(status_code=404, detail=f"Provider '{name}' not found")

    return ProviderInfo(
        name=name,
        type=config.type.value,
        available=name in factory._instances,
        failover_tier=None,
        backend_class=config.provider,
    )


@router.post("/providers/{name}/ping")
async def ping_provider(name: str, factory: AsyncStorageFactory = Depends(get_factory)) -> dict[str, Any]:
    """Test connectivity to a named provider."""
    start_time = time.perf_counter()
    try:
        await factory._get_or_create_instance(name)
        ok: bool = True
    except Exception as e:
        return {"latency_ms": (time.perf_counter() - start_time) * 1000, "ok": False, "error": str(e)}

    return {"latency_ms": (time.perf_counter() - start_time) * 1000, "ok": ok}


@router.get("/health", response_model=FactoryHealth)
async def get_factory_health(admin: AdminPortalService = Depends(get_admin_service)) -> FactoryHealth:
    """Aggregate health of all providers."""
    health_data = await admin.get_all_providers_health()

    status_val = "ok"
    if any(p.get("status") == "down" for p in health_data):
        status_val = "down"
    elif any(p.get("status") == "degraded" for p in health_data):
        status_val = "degraded"

    return FactoryHealth(status=status_val, providers=health_data)


@router.get("/health/{name}", response_model=ProviderHealth)
async def get_provider_health(name: str, admin: AdminPortalService = Depends(get_admin_service)) -> ProviderHealth:
    """Deep health check for a single named provider."""
    all_health = await admin.get_all_providers_health()
    for p in all_health:
        if p.get("name") == name:
            return ProviderHealth(
                healthy=p.get("status") == "ok",
                last_check=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                details=p,
            )

    raise HTTPException(status_code=404, detail=f"Provider '{name}' not found")


@router.get("/stats")
async def get_storage_stats(admin: AdminPortalService = Depends(get_admin_service)) -> dict[str, Any]:
    """Aggregate usage stats via AdminPortalService."""
    return await admin.get_storage_stats()
