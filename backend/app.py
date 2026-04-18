"""
银行业 CDP 平台 - FastAPI 入口
启动: uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.settings import settings
from backend.doris.connect import get_pool, close_pool
from backend.api.routes import router
from backend.middleware.request_logger import RequestLoggerMiddleware
from backend.telemetry.collector import start_writer, stop_writer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.DB_WARMUP_ON_START:
        try:
            await get_pool()
            logger.info("✅ Doris 连接池就绪")
        except Exception as e:
            logger.warning(f"⚠️ Doris 连接失败: {e}")
    else:
        logger.info("ℹ️ 跳过启动预热，Doris 连接池改为按需初始化")

    if settings.TELEMETRY_ENABLED:
        start_writer()
        logger.info("✅ Telemetry writer 已启动")
    else:
        logger.info("ℹ️ Telemetry writer 已关闭")
    yield
    if settings.TELEMETRY_ENABLED:
        stop_writer()
    await close_pool()


app = FastAPI(
    title="银行业CDP平台",
    description="基于 Apache Doris 4.0 的银行客户数据平台（HASP + AI Function）",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggerMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


@app.get("/")
async def root():
    return {"message": "银行CDP平台 API 运行中", "docs": "/docs"}
