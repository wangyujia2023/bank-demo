from fastapi import APIRouter, Query
from functools import lru_cache

from backend.service.manufacturing_service import ManufacturingService

router = APIRouter()


@lru_cache
def get_mfg_svc() -> ManufacturingService:
    return ManufacturingService()


@router.post("/mfg/init")
async def mfg_init():
    return await get_mfg_svc().init_table()


@router.post("/mfg/generate")
async def mfg_generate():
    return await get_mfg_svc().generate_step()


@router.get("/mfg/overview")
async def mfg_overview():
    return await get_mfg_svc().get_overview()


@router.get("/mfg/oee-trend")
async def mfg_oee_trend():
    return await get_mfg_svc().get_oee_trend()


@router.get("/mfg/machine-status")
async def mfg_machine_status():
    return await get_mfg_svc().get_machine_status()


@router.get("/mfg/causal")
async def mfg_causal():
    return await get_mfg_svc().get_causal_analysis()


@router.post("/mfg/batch")
async def mfg_batch(steps: int = 5):
    return await get_mfg_svc().batch_generate(steps)


@router.get("/mfg/detail")
async def mfg_detail(limit: int = Query(60, ge=1, le=200)):
    return await get_mfg_svc().get_detail(limit)


@router.get("/mfg/machine-trend")
async def mfg_machine_trend(machine_id: str):
    return await get_mfg_svc().get_machine_trend(machine_id)


@router.get("/mfg/quality-stats")
async def mfg_quality_stats():
    return await get_mfg_svc().get_quality_stats()


@router.get("/mfg/energy-stats")
async def mfg_energy_stats():
    return await get_mfg_svc().get_energy_stats()


@router.get("/mfg/maintenance-stats")
async def mfg_maintenance_stats():
    return await get_mfg_svc().get_maintenance_stats()


@router.get("/mfg/process-trend")
async def mfg_process_trend():
    return await get_mfg_svc().get_process_trend()


@router.post("/mfg/reset")
async def mfg_reset():
    return await get_mfg_svc().reset()
