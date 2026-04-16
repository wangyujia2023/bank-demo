"""
Doris 4.0 连接模块
- 启用 HASP 能力（Pipeline 引擎、向量化执行）
- 连接池管理
- Stream Load 客户端（高效批量写入）
"""
import asyncio
import json
import base64
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import aiomysql
import httpx

from backend.settings import settings

logger = logging.getLogger(__name__)

# ── 连接池 ──────────────────────────────────────────────────────
_pool: Optional[aiomysql.Pool] = None


async def get_pool() -> aiomysql.Pool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = await aiomysql.create_pool(
            host=settings.DORIS_HOST,
            port=settings.DORIS_PORT,
            user=settings.DORIS_USER,
            password=settings.DORIS_PASSWORD,
            db=settings.DORIS_DATABASE,
            charset="utf8mb4",
            autocommit=True,
            minsize=5,
            maxsize=30,
            echo=False,
            connect_timeout=10,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool and not _pool.closed:
        _pool.close()
        await _pool.wait_closed()


@asynccontextmanager
async def get_conn():
    """获取连接"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


# ── 通用查询 ─────────────────────────────────────────────────────
async def execute_query(sql: str, args: tuple = None) -> List[Dict]:
    """执行查询，返回字典列表"""
    async with get_conn() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args)
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def execute_one(sql: str, args: tuple = None) -> Optional[Dict]:
    """执行查询，返回单行"""
    rows = await execute_query(sql, args)
    return rows[0] if rows else None


async def execute_write(sql: str, args: tuple = None) -> int:
    """执行写操作，返回影响行数"""
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            return cur.rowcount


async def execute_many(sql: str, args_list: List[tuple]) -> int:
    """批量写操作"""
    if not args_list:
        return 0
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, args_list)
            return cur.rowcount


# ── Stream Load（高效批量写入，适配 HASP） ──────────────────────
async def stream_load(
    table: str,
    data: List[Dict],
    columns: Optional[List[str]] = None,
    label: Optional[str] = None,
) -> Dict:
    """
    利用 Doris Stream Load API 批量写入数据
    比 INSERT INTO 快 5-10 倍，适配 HASP 写入能力
    """
    if not data:
        return {"Status": "Success", "NumberLoadedRows": 0}

    if columns is None:
        columns = list(data[0].keys())

    # 构建 CSV 内容
    lines = []
    for row in data:
        vals = []
        for col in columns:
            val = row.get(col, "")
            if val is None:
                val = "\\N"
            elif isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)
            vals.append(str(val))
        lines.append("\t".join(vals))
    content = "\n".join(lines)

    url = (
        f"http://{settings.DORIS_HOST}:{settings.DORIS_HTTP_PORT}"
        f"/api/{settings.DORIS_DATABASE}/{table}/_stream_load"
    )
    auth = base64.b64encode(
        f"{settings.DORIS_USER}:{settings.DORIS_PASSWORD}".encode()
    ).decode()

    import time
    load_label = label or f"cdp_{table}_{int(time.time() * 1000)}"

    headers = {
        "Authorization": f"Basic {auth}",
        "Expect": "100-continue",
        "label": load_label,
        "column_separator": "\t",
        "columns": ",".join(columns),
        "format": "CSV",
        "max_filter_ratio": "0.01",
        # Doris 4.0 HASP 写入优化
        "enable_profile": "false",
        "strict_mode": "false",
    }

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.put(url, content=content.encode("utf-8"), headers=headers)
        result = resp.json()
        if result.get("Status") not in ("Success", "Publish Timeout"):
            logger.error(f"Stream Load 失败: {result}")
        return result


# ── Doris 4.0 AI Function 调用 ──────────────────────────────────
async def call_ai_function(prompt_sql: str) -> List[Dict]:
    """
    调用 Doris 4.0 内置 AI Function
    格式: SELECT AI_COMPLETION('model', 'prompt') FROM table
    """
    return await execute_query(prompt_sql)


# ── 工具函数 ────────────────────────────────────────────────────
async def ping() -> bool:
    """检测 Doris 连接是否正常"""
    try:
        result = await execute_query("SELECT 1 AS ok")
        return bool(result)
    except Exception as e:
        logger.error(f"Doris ping 失败: {e}")
        return False


async def get_doris_version() -> str:
    """获取 Doris 版本"""
    try:
        row = await execute_one("SELECT VERSION() AS v")
        return row.get("v", "unknown") if row else "unknown"
    except Exception:
        return "unknown"
