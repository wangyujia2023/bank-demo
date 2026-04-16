"""
系统运行日志 & 链路追踪采集器
- 每个 HTTP 请求生成 trace_id，记录根 span（api-gateway）
- 每次 DB 查询作为子 span（doris-connector），携带 offset_ms 和真实执行时间
- 异步队列批量写入 Doris sys_logs / sys_spans 表，不阻塞请求
"""
import asyncio
import uuid
import logging
from contextvars import ContextVar
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── 请求级 Context ────────────────────────────────────────────────
_trace_id:    ContextVar[Optional[str]] = ContextVar("trace_id",    default=None)
_trace_start: ContextVar[float]         = ContextVar("trace_start", default=0.0)  # monotonic

def new_trace_id() -> str:
    return uuid.uuid4().hex[:16]

def get_trace_id() -> Optional[str]:
    return _trace_id.get(None)

def set_trace_context(tid: str, start: float):
    _trace_id.set(tid)
    _trace_start.set(start)

def get_trace_start() -> float:
    return _trace_start.get(0.0)

# ── 写入队列 ──────────────────────────────────────────────────────
_log_q:  asyncio.Queue = asyncio.Queue(maxsize=20000)
_span_q: asyncio.Queue = asyncio.Queue(maxsize=20000)

BATCH  = 100
FLUSH_SEC = 2

# 不记录的路径（避免观测自身造成死循环）
_SKIP_PATHS = {"/api/observe/logs", "/api/observe/stats",
               "/api/observe/classify", "/api/observe/analysis",
               "/api/trace/list", "/api/system/health", "/",
               "/api/benchmark/run"}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def emit_log(trace_id: str, level: str, service: str, method: str,
             path: str, status_code: int, duration_ms: float,
             db_time_ms: float, message: str):
    try:
        _log_q.put_nowait({
            "trace_id":    trace_id,
            "log_time":    _now(),
            "level":       level,
            "service":     service,
            "method":      method,
            "path":        path[:500],
            "status_code": status_code,
            "duration_ms": round(duration_ms, 3),
            "db_time_ms":  round(db_time_ms, 3),
            "message":     message[:2000],
            "log_tag":     "",
        })
    except asyncio.QueueFull:
        pass


def emit_span(trace_id: str, span_id: str, parent_span_id: str,
              service: str, operation: str, offset_ms: float,
              duration_ms: float, status: str, db_query: str = ""):
    try:
        _span_q.put_nowait({
            "trace_id":       trace_id,
            "span_id":        span_id,
            "parent_span_id": parent_span_id,
            "span_time":      _now(),
            "service":        service,
            "operation":      operation[:200],
            "offset_ms":      round(offset_ms, 3),
            "duration_ms":    round(duration_ms, 3),
            "status":         status,
            "db_query":       db_query[:500],
        })
    except asyncio.QueueFull:
        pass


# ── 路径 → 服务映射 ───────────────────────────────────────────────
_PATH_SVC = [
    ("/api/user",         "user-service"),
    ("/api/segment",      "segment-service"),
    ("/api/behavior",     "behavior-service"),
    ("/api/dashboard",    "portal-service"),
    ("/api/vector",       "vector-service"),
    ("/api/benchmark",    "benchmark-service"),
    ("/api/report",       "report-service"),
    ("/api/metrics",      "metrics-service"),
    ("/api/observe",      "observe-service"),
    ("/api/trace",        "trace-service"),
    ("/api/tag-analysis", "tag-service"),
    ("/api/system",       "system-service"),
]

def path_to_service(path: str) -> str:
    for prefix, svc in _PATH_SVC:
        if path.startswith(prefix):
            return svc
    return "api-gateway"


# ── 批量写入 ──────────────────────────────────────────────────────
async def _flush(q: asyncio.Queue, table: str, cols: list):
    rows = []
    while not q.empty() and len(rows) < BATCH:
        try:
            rows.append(q.get_nowait())
        except asyncio.QueueEmpty:
            break
    if not rows:
        return
    from backend.doris.connect import execute_many
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    args = [tuple(r[c] for c in cols) for r in rows]
    try:
        await execute_many(sql, args)
    except Exception as e:
        logger.warning(f"telemetry flush {table} failed: {e}")


_LOG_COLS = ["trace_id","log_time","level","service","method",
             "path","status_code","duration_ms","db_time_ms","message","log_tag"]
_SPAN_COLS = ["trace_id","span_id","parent_span_id","span_time","service",
              "operation","offset_ms","duration_ms","status","db_query"]


_writer_task: Optional[asyncio.Task] = None


async def _writer_loop():
    while True:
        await asyncio.sleep(FLUSH_SEC)
        await _flush(_log_q,  "sys_logs",  _LOG_COLS)
        await _flush(_span_q, "sys_spans", _SPAN_COLS)


def start_writer():
    global _writer_task
    _writer_task = asyncio.create_task(_writer_loop())
    logger.info("telemetry writer started")


def stop_writer():
    if _writer_task:
        _writer_task.cancel()


# ── Doris 建表 SQL ────────────────────────────────────────────────
INIT_SQL = [
    """
    CREATE TABLE IF NOT EXISTS sys_logs (
        trace_id    VARCHAR(32),
        log_time    DATETIME(3),
        level       VARCHAR(10),
        service     VARCHAR(60),
        method      VARCHAR(10),
        path        VARCHAR(500),
        status_code INT,
        duration_ms DOUBLE,
        db_time_ms  DOUBLE,
        message     VARCHAR(2000),
        log_tag     VARCHAR(50)
    ) ENGINE=OLAP
    UNIQUE KEY(trace_id, log_time)
    DISTRIBUTED BY HASH(trace_id) BUCKETS 4
    PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
    """,
    """
    CREATE TABLE IF NOT EXISTS sys_spans (
        trace_id       VARCHAR(32),
        span_id        VARCHAR(32),
        parent_span_id VARCHAR(32),
        span_time      DATETIME(3),
        service        VARCHAR(60),
        operation      VARCHAR(200),
        offset_ms      DOUBLE,
        duration_ms    DOUBLE,
        status         VARCHAR(10),
        db_query       VARCHAR(500)
    ) ENGINE=OLAP
    DUPLICATE KEY(trace_id, span_id)
    DISTRIBUTED BY HASH(trace_id) BUCKETS 4
    PROPERTIES("replication_num"="1")
    """,
]
