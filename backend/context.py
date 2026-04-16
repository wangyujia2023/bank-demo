from contextvars import ContextVar

trace_id_var: ContextVar[str] = ContextVar('trace_id', default='')
http_span_id_var: ContextVar[str] = ContextVar('http_span_id', default='')
request_start_var: ContextVar[float] = ContextVar('request_start', default=0.0)
db_time_var: ContextVar[float] = ContextVar('db_time_ms', default=0.0)
