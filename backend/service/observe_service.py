"""日志可观测性 & 链路追踪服务"""
import asyncio
from backend.doris.connect import execute_query, execute_one


class ObserveService:
    """日志可观测性和链路追踪 - 基于request_log表"""

    async def logs(self, path: str = None, status_code: int = None, page: int = 1, size: int = 100):
        """获取日志列表"""
        where = ["1=1"]
        if path:
            where.append(f"path LIKE '%{path}%'")
        if status_code:
            where.append(f"status_code = {status_code}")

        offset = (page - 1) * size
        logs, total = await asyncio.gather(
            execute_query(f"""
                SELECT trace_id, request_time, method, path, status_code,
                       duration_ms, error_message, tags
                FROM request_log
                WHERE {' AND '.join(where)}
                ORDER BY request_time DESC
                LIMIT {size} OFFSET {offset}
            """),
            execute_one(f"SELECT COUNT(*) as cnt FROM request_log WHERE {' AND '.join(where)}")
        )
        return {"logs": logs, "total": (total or {}).get("cnt", 0)}

    async def stats(self):
        """日志统计"""
        stats, top_paths, errors = await asyncio.gather(
            execute_one("""
                SELECT
                    COUNT(*) as total_requests,
                    COUNT(IF(status_code >= 400, 1, NULL)) as error_count,
                    COUNT(IF(duration_ms > 1000, 1, NULL)) as slow_count,
                    ROUND(AVG(duration_ms), 2) as avg_duration_ms,
                    MAX(duration_ms) as max_duration_ms
                FROM request_log
                WHERE request_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """),
            execute_query("""
                SELECT path, COUNT(*) as count, ROUND(AVG(duration_ms), 2) as avg_duration
                FROM request_log
                WHERE request_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY path ORDER BY count DESC LIMIT 10
            """),
            execute_query("""
                SELECT path, COUNT(*) as error_count, status_code
                FROM request_log
                WHERE status_code >= 400 AND request_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY path, status_code ORDER BY error_count DESC LIMIT 10
            """)
        )
        return {"stats": stats or {}, "top_paths": top_paths, "error_paths": errors}

    async def traces(self, page: int = 1, size: int = 20):
        """链路列表"""
        offset = (page - 1) * size
        traces, total = await asyncio.gather(
            execute_query(f"""
                SELECT trace_id, request_time, path, method, status_code, duration_ms
                FROM request_log
                ORDER BY request_time DESC
                LIMIT {size} OFFSET {offset}
            """),
            execute_one("SELECT COUNT(DISTINCT trace_id) as cnt FROM request_log")
        )
        return {"traces": traces, "total": (total or {}).get("cnt", 0)}

    async def trace_detail(self, trace_id: str):
        """链路详情"""
        logs = await execute_query(f"""
            SELECT trace_id, span_id, method, path, status_code, duration_ms, request_time
            FROM request_log
            WHERE trace_id = '{trace_id}'
            ORDER BY request_time ASC
        """)
        return {"trace_id": trace_id, "spans": logs}
