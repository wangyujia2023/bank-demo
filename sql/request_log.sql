-- ================================================================
-- 请求日志表 - 用于可观测性和链路追踪
-- ================================================================

CREATE TABLE IF NOT EXISTS request_log (
    trace_id        VARCHAR(64)     NOT NULL    COMMENT '链路追踪ID',
    span_id         VARCHAR(64)     NOT NULL    COMMENT 'Span ID',
    request_time    DATETIME        NOT NULL    COMMENT '请求时间',
    method          VARCHAR(16)     NOT NULL    COMMENT 'HTTP方法：GET/POST/PUT/DELETE',
    path            VARCHAR(255)    NOT NULL    COMMENT '请求路径',
    status_code     INT             NOT NULL    COMMENT '响应状态码',
    duration_ms     INT             NOT NULL    COMMENT '响应耗时(毫秒)',
    user_id         BIGINT                      COMMENT '用户ID',
    ip_address      VARCHAR(32)     NOT NULL    COMMENT '客户端IP',
    user_agent      VARCHAR(512)                COMMENT '用户代理',
    request_body    TEXT                        COMMENT '请求体(截断到1KB)',
    response_body   TEXT                        COMMENT '响应体(截断到1KB)',
    error_message   VARCHAR(512)                COMMENT '错误信息',
    error_stack     TEXT                        COMMENT '错误堆栈',
    tags            VARCHAR(256)                COMMENT '标签:slow,error,cache_miss等',
    dependencies    VARCHAR(512)                COMMENT '依赖服务:user_service,order_service等',
    db_calls        INT                         COMMENT '数据库调用次数',
    db_duration_ms  INT                         COMMENT '数据库耗时(毫秒)',
    cache_hits      INT                         COMMENT '缓存命中次数',

    INDEX idx_trace_id (trace_id),
    INDEX idx_request_time (request_time),
    INDEX idx_path_time (path, request_time),
    INDEX idx_user_id (user_id),
    INDEX idx_status_code (status_code)
) ENGINE=OLAP
DUPLICATE KEY(trace_id, span_id)
DISTRIBUTED BY HASH(trace_id) BUCKETS 8
PARTITION BY RANGE(DATE(request_time)) (
    PARTITION p_default VALUES LESS THAN ('2099-01-01')
)
PROPERTIES (
    "replication_num" = "1",
    "compression" = "lz4"
);

-- 创建索引视图：日志统计
CREATE VIEW IF NOT EXISTS request_log_stats AS
SELECT
    DATE(request_time) as log_date,
    COUNT(*) as total_requests,
    COUNT(IF(status_code >= 400, 1, NULL)) as error_count,
    COUNT(IF(duration_ms > 1000, 1, NULL)) as slow_count,
    ROUND(AVG(duration_ms), 2) as avg_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    ROUND(AVG(db_duration_ms), 2) as avg_db_duration_ms
FROM request_log
GROUP BY DATE(request_time);

-- 创建索引视图：按路径统计
CREATE VIEW IF NOT EXISTS request_log_by_path AS
SELECT
    path,
    COUNT(*) as call_count,
    COUNT(IF(status_code >= 400, 1, NULL)) as error_count,
    ROUND(AVG(duration_ms), 2) as avg_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    ROUND(COUNT(IF(status_code < 400, 1, NULL)) * 100.0 / COUNT(*), 2) as success_rate
FROM request_log
WHERE request_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY path
ORDER BY call_count DESC;
