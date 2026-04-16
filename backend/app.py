"""
银行业 CDP 平台 - FastAPI 入口
启动: uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.settings import settings
from backend.doris.connect import get_pool, close_pool, execute_query
from backend.api.routes import router
from backend.middleware.request_logger import RequestLoggerMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 CDP 平台启动，初始化 Doris 连接池...")
    try:
        await get_pool()
        logger.info("✅ Doris 连接池就绪")
    except Exception as e:
        logger.warning(f"⚠️ Doris 连接失败: {e}，使用内存数据模式运行")

    # 初始化 request_log 表
    try:
        await execute_query("""
            CREATE TABLE IF NOT EXISTS request_log (
                trace_id VARCHAR(64) NOT NULL,
                span_id VARCHAR(64) NOT NULL,
                request_time DATETIME NOT NULL,
                method VARCHAR(16) NOT NULL,
                path VARCHAR(255) NOT NULL,
                status_code INT NOT NULL,
                duration_ms INT NOT NULL,
                user_id BIGINT,
                ip_address VARCHAR(32) NOT NULL,
                user_agent VARCHAR(512),
                error_message VARCHAR(512),
                tags VARCHAR(256),
                db_calls INT,
                db_duration_ms INT,
                INDEX idx_trace_id (trace_id),
                INDEX idx_request_time (request_time),
                INDEX idx_path_time (path, request_time)
            ) ENGINE=OLAP
            DUPLICATE KEY(trace_id, span_id)
            DISTRIBUTED BY HASH(trace_id) BUCKETS 8
            PROPERTIES ("replication_num" = "1")
        """)
        logger.info("✅ request_log 表已就绪")
    except Exception as e:
        logger.warning(f"request_log 表初始化: {e}")

    yield
    logger.info("🛑 关闭 Doris 连接池")
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
