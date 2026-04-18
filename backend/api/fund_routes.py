from fastapi import APIRouter, Query
from functools import lru_cache

from backend.service.fund_service import FundService

router = APIRouter()


@lru_cache
def get_fund_svc() -> FundService:
    return FundService()


@router.post("/fund/init")
async def fund_init():
    return await get_fund_svc().init_table()


@router.post("/fund/generate")
async def fund_generate():
    return await get_fund_svc().generate_step()


@router.post("/fund/batch")
async def fund_batch(days: int = 5):
    return await get_fund_svc().batch_generate(days)


@router.post("/fund/reset")
async def fund_reset():
    return await get_fund_svc().reset()


@router.get("/fund/overview")
async def fund_overview():
    return await get_fund_svc().get_overview()


@router.get("/fund/list")
async def fund_list(sector: str = None, fund_type: str = None, risk: int = None):
    return await get_fund_svc().get_fund_list(sector, fund_type, risk)


@router.get("/fund/detail/{fund_id}")
async def fund_detail(fund_id: str):
    return await get_fund_svc().get_fund_detail(fund_id)


@router.get("/fund/nav/{fund_id}")
async def fund_nav(fund_id: str, days: int = Query(90, ge=10, le=365)):
    return await get_fund_svc().get_nav_history(fund_id, days)


@router.get("/fund/position/{fund_id}")
async def fund_position(fund_id: str):
    return await get_fund_svc().get_position(fund_id)


@router.get("/fund/manager/{manager_id}")
async def fund_manager(manager_id: str):
    return await get_fund_svc().get_manager(manager_id)


@router.get("/fund/peers/{fund_id}")
async def fund_peers(fund_id: str):
    return await get_fund_svc().get_peers(fund_id)


@router.get("/fund/sector-stats")
async def fund_sector_stats():
    return await get_fund_svc().get_sector_stats()
