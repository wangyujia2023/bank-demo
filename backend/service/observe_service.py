"""日志可观测性 & 链路追踪服务 - 基于内存日志"""
from backend.log_store import log_store


class ObserveService:
    """日志可观测性和链路追踪 - 基于内存request日志"""

    async def logs(self, path: str = None, status_code: int = None, page: int = 1, size: int = 100):
        """获取日志列表"""
        all_logs = log_store.get_all()

        # 筛选
        if path:
            all_logs = [log for log in all_logs if path in log.get('path', '')]
        if status_code:
            all_logs = [log for log in all_logs if log.get('status_code') == status_code]

        total = len(all_logs)
        offset = (page - 1) * size
        logs = all_logs[offset:offset + size]

        return {"logs": logs, "total": total}

    async def stats(self):
        """日志统计"""
        return log_store.get_stats()

    async def traces(self, page: int = 1, size: int = 20):
        """链路列表（按trace_id去重）"""
        all_logs = log_store.get_all()

        # 按trace_id分组，取每个trace的第一条日志作为代表
        trace_map = {}
        for log in all_logs:
            trace_id = log.get('trace_id')
            if trace_id not in trace_map:
                trace_map[trace_id] = log

        traces = list(trace_map.values())
        total = len(traces)
        offset = (page - 1) * size

        return {"traces": traces[offset:offset + size], "total": total}

    async def trace_detail(self, trace_id: str):
        """链路详情 - 获取该trace_id的所有日志"""
        spans = log_store.get_by_trace(trace_id)
        return {
            "trace_id": trace_id,
            "spans": spans,
            "duration_ms": sum(s.get('duration_ms', 0) for s in spans),
            "status": "ERROR" if any(s.get('status_code', 0) >= 400 for s in spans) else "OK"
        }
