import logging
import time
import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from backend.telemetry.collector import (
    emit_log, emit_span, set_trace_context,
    new_trace_id, get_trace_id, get_trace_start, path_to_service, _SKIP_PATHS
)

logger = logging.getLogger(__name__)


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if path in _SKIP_PATHS or path.startswith("/docs") or path.startswith("/openapi"):
            return await call_next(request)

        trace_id = request.headers.get("X-Trace-ID") or new_trace_id()
        span_id  = uuid.uuid4().hex[:16]
        start_ts = time.time()
        set_trace_context(trace_id, start_ts)

        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            dur = (time.time() - start_ts) * 1000
            svc = path_to_service(path)
            emit_log(trace_id, "ERROR", svc, request.method, path, 500, dur, 0.0, str(e)[:500])
            emit_span(trace_id, span_id, "", svc, f"{request.method} {path}", 0, dur, "ERROR")
            raise

        dur = (time.time() - start_ts) * 1000
        method = request.method
        svc = path_to_service(path)

        if status_code >= 500:
            level = "ERROR"
        elif status_code >= 400 or dur > 1000:
            level = "WARN"
        else:
            level = "INFO"

        # db_time_ms 由 connect.py 的 span 记录，这里暂为 0；observe_service 可从 sys_spans 聚合
        emit_log(trace_id, level, svc, method, path, status_code, dur, 0.0,
                 f"{method} {path} -> {status_code} ({dur:.0f}ms)")
        emit_span(trace_id, span_id, "", svc, f"{method} {path}", 0, dur,
                  "ERROR" if status_code >= 500 else "OK")

        response.headers["X-Trace-ID"] = trace_id
        return response
