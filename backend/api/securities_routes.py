from fastapi import APIRouter, Query
from functools import lru_cache

from backend.service.securities_service import SecuritiesService

router = APIRouter()


@lru_cache
def get_sec_svc() -> SecuritiesService:
    return SecuritiesService()


@router.post("/sec/init")
async def sec_init():
    return await get_sec_svc().init_table()


@router.post("/sec/generate")
async def sec_generate():
    return await get_sec_svc().generate_step()


@router.post("/sec/batch")
async def sec_batch(steps: int = Query(6, ge=1, le=24)):
    return await get_sec_svc().batch_generate(steps)


@router.post("/sec/reset")
async def sec_reset():
    return await get_sec_svc().reset()


@router.get("/sec/overview")
async def sec_overview():
    return await get_sec_svc().get_overview()


@router.get("/sec/trend")
async def sec_trend():
    return await get_sec_svc().get_trend()


@router.get("/sec/trades")
async def sec_trades(limit: int = Query(60, ge=20, le=200)):
    return await get_sec_svc().get_trades(limit)


@router.get("/sec/accounts")
async def sec_accounts():
    return await get_sec_svc().get_accounts()


@router.get("/sec/positions")
async def sec_positions():
    return await get_sec_svc().get_positions()


@router.get("/sec/sector-heat")
async def sec_sector_heat():
    return await get_sec_svc().get_sector_heat()


@router.get("/sec/risk-alerts")
async def sec_risk_alerts():
    return await get_sec_svc().get_risk_alerts()


@router.get("/sec/branches")
async def sec_branches():
    return await get_sec_svc().get_branches()
