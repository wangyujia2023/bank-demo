# Doris 4.0 演示平台

一个以 Apache Doris 4.0 为核心的多场景演示项目，覆盖银行 CDP、基金投研、智能制造、证券经营与风控等业务。

## 当前菜单

- 首页大盘
- 经营管理大屏
- 用户宽表
- 人群圈选
- 行为分析
- 用户行为分析
- AI 日志标签
- 图片向量检索
- 卫星数据分析
- 银行报表
- 指标平台
- 日志可观测性
- 日志标签分析
- 链路追踪
- 高并发点查
- 智能制造
- 证券实时数仓
- 基金投研沙盘
- 资讯 AI 分析
- 系统配置

## 项目结构

- `backend/`：FastAPI 后端和 Doris 访问层
- `frontend/`：Vue 3 前端
- `sql/`：初始化 SQL
- `券商.md`：证券场景设计稿

## 启动方式

### 后端

```bash
cd backend
pip install -r ../requirements.txt
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

### 前端

```bash
cd frontend
npm install
npm run dev
```

### 一键启动

```bash
bash start_all.sh
```

## 说明

- 页面内的“初始化”按钮会自动建表并写入演示数据。
- 前端默认通过 `/api` 代理访问后端。
- 证券场景和基金场景都采用固定演示数据，适合现场讲解。
