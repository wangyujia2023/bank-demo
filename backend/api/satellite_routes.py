from fastapi import APIRouter, Query
from functools import lru_cache
from typing import Optional

from backend.service.satellite_service import SatelliteService

router = APIRouter()


@lru_cache
def get_sat_svc() -> SatelliteService:
    return SatelliteService()


@router.post("/satellite/init")
async def satellite_init():
    return await get_sat_svc().init_table()


@router.get("/satellite/overview")
async def satellite_overview():
    return await get_sat_svc().overview()


@router.get("/satellite/charts")
async def satellite_charts():
    return await get_sat_svc().charts()


@router.get("/satellite/query")
async def satellite_query(
    satellite_id: Optional[str] = None,
    satellite_name: Optional[str] = None,
    satellite_type: Optional[str] = None,
    data_type: Optional[str] = None,
    target_type: Optional[str] = None,
    quality_min: Optional[int] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    return await get_sat_svc().query(
        satellite_id, satellite_name, satellite_type,
        data_type, target_type, quality_min, status, page, size
    )
