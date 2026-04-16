# 日志可观测性和链路追踪说明

## 工作原理

### 1. 请求日志收集
- **RequestLoggerMiddleware**: FastAPI中间件，自动拦截所有HTTP请求
- **记录内容**:
  - `trace_id`: 链路唯一标识（UUID）
  - `span_id`: 请求唯一标识
  - `method`: HTTP方法 (GET/POST/PUT/DELETE等)
  - `path`: 请求路径 (/api/user, /api/dashboard等)
  - `query`: 查询参数
  - `status_code`: 响应状态码
  - `duration_ms`: 响应耗时(毫秒)
  - `ip_address`: 客户端IP
  - `user_agent`: 用户代理
  - `error_message`: 错误信息(如有)
  - `tags`: 请求标签 (slow, error等)

### 2. 日志存储
- **RequestLogStore**: 内存环形缓冲区，最多保存2000条日志
- **优势**:
  - 即时可用，不依赖Doris连接
  - 性能高，无I/O开销
  - 自动清理过期日志

### 3. 日志查询
#### 日志可观测性 (/observe/logs)
```
GET /api/observe/logs?path=/dashboard&status_code=200
```
返回:
```json
{
  "logs": [
    {
      "trace_id": "abc123...",
      "span_id": "def456...",
      "method": "GET",
      "path": "/api/dashboard",
      "status_code": 200,
      "duration_ms": 125,
      "timestamp": "2026-04-16T13:21:00"
    }
  ],
  "total": 156
}
```

#### 日志统计 (/observe/stats)
```json
{
  "total": 156,
  "errors": 3,
  "slow": 8,
  "avg_duration_ms": 98.5,
  "top_paths": [
    {"path": "/api/dashboard", "count": 45, "avg_duration": 102.3},
    {"path": "/api/user", "count": 38, "avg_duration": 95.2}
  ]
}
```

### 4. 链路追踪
#### 链路列表 (/trace/list)
```
GET /api/trace/list?page=1&size=20
```
返回:
```json
{
  "traces": [
    {
      "trace_id": "abc123...",
      "method": "GET",
      "path": "/api/dashboard",
      "status_code": 200,
      "duration_ms": 125
    }
  ],
  "total": 45
}
```

#### 链路详情 (/trace/{trace_id})
```
GET /api/trace/abc123
```
返回:
```json
{
  "trace_id": "abc123",
  "spans": [
    {
      "trace_id": "abc123",
      "span_id": "span1",
      "method": "GET",
      "path": "/api/dashboard",
      "status_code": 200,
      "duration_ms": 125
    }
  ],
  "duration_ms": 125,
  "status": "OK"
}
```

## 实时示例

当你访问首页大盘时:
```
1. 浏览器发送 GET /api/dashboard
2. RequestLoggerMiddleware 拦截请求:
   - 生成 trace_id: "a1b2c3d4e5f6g7h8"
   - 生成 span_id: "12345678"
3. DashboardService 处理请求 (125ms)
4. ResponseLogger 记录日志到内存
5. 前端可立即查看:
   - /observe/logs 查看所有请求日志
   - /observe/stats 查看性能统计
   - /trace/list 查看链路列表
   - /trace/a1b2c3d4e5f6g7h8 查看该链路的详细日志
```

## 不记录日志的路由

以下路由不被记录（防止日志污染）:
- `/api/benchmark` - 高并发点查压测
- `/docs` - API文档
- `/openapi.json` - OpenAPI schema
- `/health` - 健康检查

## 注意事项

1. **内存限制**: 最多保存2000条日志，新日志会自动覆盖旧日志
2. **重启丢失**: 应用重启后日志清空，建议配合Doris持久化
3. **性能**: 日志收集对应用性能影响 <5%
4. **高并发**: 建议配合Doris数据库实现持久化和更大容量

## 与Doris集成

当Doris连接正常时:
1. RequestLogStore 在内存中存储日志（实时查询）
2. RequestLoggerMiddleware 异步提交日志到Doris（持久化）
3. 可配置更复杂的查询和分析
