# Doris 4.0 演示平台

基于 Apache Doris 4.0 的多场景演示系统，覆盖银行 CDP、证券经营与风控、基金投研、智能制造等业务。

## 开发规范（必读）

后续功能开发统一按以下规范执行：  
[DEVELOPMENT_SPEC.md](./DEVELOPMENT_SPEC.md)

重点：已内置“低 Token 开发细则”，要求先检索后读取、批量修改、最小验证、短结果输出。

## 项目结构

- `backend/`：FastAPI 后端（API、Service、Doris 访问层）
- `frontend/`：Vue 3 + Vite 前端
- `sql/`：初始化 SQL 脚本
- `start_all.sh`：本地一键启动脚本

## 快速启动

### 1) 后端

```bash
cd backend
pip install -r ../requirements.txt
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

### 2) 前端

```bash
cd frontend
npm install
npm run dev
```

### 3) 一键启动

```bash
bash start_all.sh
```

## 环境变量

参考 `.env.example`：

- `DORIS_HOST` / `DORIS_PORT` / `DORIS_USER` / `DORIS_PASSWORD` / `DORIS_DATABASE`
- `DB_WARMUP_ON_START`：是否启动时预热连接池（默认 `false`）
- `TELEMETRY_ENABLED`：是否启用 telemetry writer（默认 `false`）
- `BEHAVIOR_SCAN_DAYS`：行为分析查询默认扫描天数（默认 `120`）

## 常用命令

```bash
# 前端构建验证
cd frontend && npm run build

# 后端运行
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

## 当前功能菜单

- 首页大盘、经营管理大屏
- 用户宽表、人群圈选、行为分析、用户行为分析
- AI 日志标签、图片向量检索、卫星数据分析
- 银行报表、指标平台、日志可观测性、链路追踪、高并发点查
- 智能制造、证券实时数仓、基金投研沙盘、资讯 AI 分析、系统配置

## 开发约束摘要

- 先 `rg` 定位再读文件，禁止全盘扫描。
- 同类问题一次性批量修改，减少多轮重复改动。
- 每次改动后至少做最小可用验证（前端 build / 后端语法或运行验证）。
