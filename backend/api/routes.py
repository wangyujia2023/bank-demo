"""银行CDP平台 - FastAPI 路由聚合"""
from fastapi import APIRouter

from backend.api.benchmark_routes import router as benchmark_router
from backend.api.cdp_routes import router as cdp_router
from backend.api.core_routes import router as core_router
from backend.api.fund_routes import router as fund_router
from backend.api.manufacturing_routes import router as manufacturing_router
from backend.api.news_routes import router as news_router
from backend.api.satellite_routes import router as satellite_router
from backend.api.securities_routes import router as securities_router
from backend.api.vector_routes import router as vector_router

router = APIRouter()
router.include_router(core_router)
router.include_router(cdp_router)
router.include_router(vector_router)
router.include_router(benchmark_router)
router.include_router(satellite_router)
router.include_router(securities_router)
router.include_router(manufacturing_router)
router.include_router(fund_router)
router.include_router(news_router)
