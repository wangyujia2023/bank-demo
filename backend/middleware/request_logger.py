"""请求日志中间件 - 收集链路追踪数据"""
import time
import json
import uuid
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from backend.doris.connect import stream_load

logger = logging.getLogger(__name__)


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    # 排除不记录日志的路由
    EXCLUDE_PATHS = {"/api/benchmark", "/docs", "/openapi.json", "/health"}

    async def dispatch(self, request: Request, call_next) -> Response:
        # 检查是否在排除列表中
        if any(request.url.path.startswith(path) for path in self.EXCLUDE_PATHS):
            return await call_next(request)

        trace_id = request.headers.get("X-Trace-ID") or str(uuid.uuid4())
        span_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        try:
            response = await call_next(request)
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            await self._log_error(request, trace_id, span_id, duration_ms, str(e))
            raise

        duration_ms = int((time.time() - start_time) * 1000)
        await self._log_request(request, response, trace_id, span_id, duration_ms)
        response.headers["X-Trace-ID"] = trace_id
        response.headers["X-Span-ID"] = span_id

        return response

    async def _log_request(self, request: Request, response: Response, trace_id: str, span_id: str, duration_ms: int):
        """记录请求日志"""
        try:
            log_data = {
                "trace_id": trace_id,
                "span_id": span_id,
                "request_time": int(time.time() * 1000),  # 毫秒时间戳
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
                "ip_address": request.client.host if request.client else "unknown",
                "user_agent": request.headers.get("user-agent", "")[:512],
                "tags": self._get_tags(response.status_code, duration_ms),
            }
            await stream_load("request_log", [log_data])
        except Exception as e:
            logger.warning(f"日志记录失败: {e}")

    async def _log_error(self, request: Request, trace_id: str, span_id: str, duration_ms: int, error: str):
        """记录错误日志"""
        try:
            log_data = {
                "trace_id": trace_id,
                "span_id": span_id,
                "request_time": int(time.time() * 1000),
                "method": request.method,
                "path": request.url.path,
                "status_code": 500,
                "duration_ms": duration_ms,
                "ip_address": request.client.host if request.client else "unknown",
                "error_message": error[:512],
                "tags": "error",
            }
            await stream_load("request_log", [log_data])
        except Exception as e:
            logger.warning(f"错误日志记录失败: {e}")

    @staticmethod
    def _get_tags(status_code: int, duration_ms: int) -> str:
        tags = []
        if status_code >= 500:
            tags.append("error")
        elif status_code >= 400:
            tags.append("client_error")
        if duration_ms > 1000:
            tags.append("slow")
        return ",".join(tags) if tags else ""
