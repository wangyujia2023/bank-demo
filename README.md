# Doris 4.0 特性全景展示平台

**Apache Doris 4.0 · HASP · AI Function · 向量检索 · 数字孪生沙盘 · 基金投研**

> 本平台以"动态演练 + 数据驱动"为核心，覆盖银行 CDP、智能制造、基金投研三大场景，全面展示 Apache Doris 4.0 的核心技术能力。所有模块独立运行，互不干扰，可按需演示。

---

## 🗂 功能模块总览

### 用户画像（银行 CDP）

| 菜单 | 说明 | 核心 Doris 特性 |
|------|------|------|
| 首页大盘 | 实时 KPI、趋势图、业务概况 | 聚合查询、物化视图 |
| 经营管理大屏 | 多维度经营分析看板 | 多表 JOIN、窗口函数 |
| 宽表查询 | 用户多条件组合查询、360°视图 | 宽表模型、HASH分桶 |
| 人群圈选 | Bitmap 多标签实时圈选估算 | BITMAP_AND/OR/NOT |
| 行为分析 | 漏斗、留存、RFM、交易趋势 | window_funnel、留存矩阵 |
| 用户行为分析 | 标签交叉分析、行为路径挖掘 | 多维聚合、TopN |
| AI 日志标签 | LLM 自动打标签、标签分布 | Doris AI Function |

### HASP 场景

| 菜单 | 说明 | 核心 Doris 特性 |
|------|------|------|
| 图片向量检索 | 人脸/图片相似度检索、混合检索 | HASP 向量索引、ANN 近似最近邻 |
| 卫星数据分析 | 遥感数据采集、时序分析 | 大宽表、Stream Load |

### 数据能力

| 菜单 | 说明 | 核心 Doris 特性 |
|------|------|------|
| 银行报表 | 标准监管报表、风险报表 | 复杂聚合、报表加速 |
| 指标平台 | 通用指标查询引擎、多维对比、下钻分析 | 动态 SQL、RANK/CORR 窗口函数 |
| 日志可观测性 | 日志实时查询、分类统计、AI 标签分析 | 全文检索、倒排索引 |
| 日志标签分析 | AI 打标分布、标签共现、风险标签 | Bitmap、GROUP BY ROLLUP |
| 链路追踪 | 分布式追踪可视化、Span 瀑布图 | 嵌套数据查询 |
| 高并发点查 | 压测基准、毫秒级主键点查 | UNIQUE KEY、行存优化 |

### 智能制造

| 菜单 | 说明 | 核心 Doris 特性 |
|------|------|------|
| 数字孪生沙盘 | 6台设备×50+指标的工厂仿真演练 | DUPLICATE KEY、QUALIFY 窗口去重、Stream Load |

### 基金场景

| 菜单 | 说明 | 核心 Doris 特性 |
|------|------|------|
| 基金投研沙盘 | 30只基金×5套市场剧本的动态演练 | DUPLICATE KEY 4表、CORR() 相关性矩阵、RANK() 窗口排名 |

---

## 📐 工程结构

```
bank-cdp-doris4/
├── backend/
│   ├── app.py                          # FastAPI 入口
│   ├── settings.py                     # 全局配置（Doris连接等）
│   ├── requirements.txt
│   ├── api/
│   │   └── routes.py                   # 全部 API 路由（12+模块）
│   ├── service/
│   │   ├── user_service.py             # 用户宽表 + 人群圈选
│   │   ├── behavior_service.py         # 行为分析（漏斗/留存/RFM）
│   │   ├── dashboard_service.py        # 首页大盘
│   │   ├── management_dashboard.py     # 经营管理大屏
│   │   ├── report_service.py           # 银行报表
│   │   ├── metrics_service.py          # 指标平台（通用查询引擎）
│   │   ├── observe_service.py          # 日志可观测性
│   │   ├── tag_analysis_service.py     # AI 日志标签分析
│   │   ├── portrait_service.py         # 用户画像 + CDP
│   │   ├── benchmark_service.py        # 高并发压测
│   │   ├── vector_search_service.py    # HASP 向量检索
│   │   ├── satellite_service.py        # 卫星数据采集
│   │   ├── manufacturing_service.py    # 智能制造数字孪生（50+指标）
│   │   └── fund_service.py             # 基金投研沙盘（30只基金×4表）
│   └── doris/
│       ├── connect.py                  # 连接池 + execute_query/write/many
│       └── ai_function.py              # AI Function 调用封装
├── frontend/
│   └── src/
│       ├── App.vue                     # 根组件（侧边栏导航）
│       ├── router/index.js             # 路由表
│       ├── api/index.js                # 所有接口封装（fundApi/mfgApi等）
│       └── views/
│           ├── Dashboard.vue           # 首页大盘
│           ├── ManagementDashboard.vue # 经营管理大屏
│           ├── UserWide.vue            # 用户宽表查询
│           ├── Segment.vue             # 人群圈选
│           ├── Behavior.vue            # 行为分析
│           ├── UserTagAnalysis.vue     # 用户行为分析
│           ├── LogClassifyAnalysis.vue # AI 日志标签
│           ├── VectorSearch.vue        # 图片向量检索
│           ├── Satellite.vue           # 卫星数据分析
│           ├── BankReport.vue          # 银行报表
│           ├── MetricsPlatform.vue     # 指标平台
│           ├── LogObserve.vue          # 日志可观测性
│           ├── LogTagStats.vue         # 日志标签分析
│           ├── TraceView.vue           # 链路追踪
│           ├── Benchmark.vue           # 高并发点查
│           ├── Manufacturing.vue       # 智能制造数字孪生
│           ├── Fund.vue                # 基金投研沙盘
│           └── SysConfig.vue           # 系统配置
└── README.md
```

---

## 🚀 快速启动

### 1. 启动 Apache Doris 4.0

```bash
# Docker 快速体验（推荐）
docker run -d \
  --name doris-all-in-one \
  -p 8030:8030 -p 9030:9030 -p 8040:8040 \
  -e FE_SERVERS="fe1:127.0.0.1:9010" \
  -e BE_ADDR="127.0.0.1:9050" \
  apache/doris:2.1.3-all-in-one

# 等待约 30 秒，验证连接
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW DATABASES;"
```

### 2. 初始化数据库

```bash
mysql -h 127.0.0.1 -P 9030 -u root -e "CREATE DATABASE IF NOT EXISTS bank_cdp;"
```

> 各模块均通过页面「初始化」按钮自动建表并注入仿真数据，无需手动执行 SQL。

### 3. 启动后端

```bash
cd backend
pip install -r requirements.txt

# 配置连接（修改 settings.py 或设置环境变量）
export DORIS_HOST=127.0.0.1
export DORIS_PORT=9030
export DORIS_USER=root
export DORIS_PASSWORD=
export DORIS_DATABASE=bank_cdp

uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

- API 文档：http://localhost:8000/docs
- 健康检查：http://localhost:8000/api/system/health

### 4. 启动前端

```bash
cd frontend
npm install       # Node.js 18+ 必须
npm run dev
```

前端访问：http://localhost:5173

---

## ⚡ Doris 4.0 核心技术展示矩阵

### HASP — 混合自适应存储与处理

| 特性 | 本项目应用场景 |
|------|------|
| Pipeline 执行引擎 | 行为分析大查询、指标平台多维聚合 |
| 向量化执行（SIMD） | RFM 计算、基金收益聚合 |
| 向量索引（ANN） | 图片向量检索、用户相似推荐 |
| Runtime Filter | 基金持仓 × 行情 JOIN、宽表多条件查询 |
| 行列混存（MOW） | 高并发点查场景（毫秒级主键查找） |

### 模型选择策略

```
DUPLICATE KEY  — 智能制造（mfg_metrics_v2）、基金净值历史（fund_nav_history）
                 允许重复行，QUALIFY 窗口函数取最新版本

UNIQUE KEY     — 用户宽表（user_wide）、基金经理（fund_manager）
                 主键唯一，写时合并

AGGREGATE KEY  — 人群标签 Bitmap（user_tag）
                 BITMAP_UNION 自动聚合，圈选毫秒级响应
```

### 核心 SQL 能力展示

**① QUALIFY 窗口函数去重（智能制造 / 基金快照）**
```sql
-- 取每台设备最新一条，无需 UNIQUE KEY
SELECT * FROM mfg_metrics_v2
QUALIFY ROW_NUMBER() OVER (PARTITION BY machine_id ORDER BY ts DESC) = 1

-- 取每只基金最新快照
SELECT * FROM fund_basic
QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
```

**② RANK() 同板块基金排名（基金投研）**
```sql
SELECT fund_id, fund_name, sharpe,
    RANK() OVER (ORDER BY sharpe DESC)       AS sharpe_rank,
    RANK() OVER (ORDER BY max_drawdown DESC) AS drawdown_rank
FROM (...最新快照...) WHERE sector_tag = '半导体'
```

**③ CORR() 基金走势相关性矩阵（基金投研）**
```sql
SELECT a.fund_id, b.fund_id,
    ROUND(CORR(a.daily_return, b.daily_return), 3) AS corr
FROM fund_nav_history a
JOIN fund_nav_history b ON a.trade_date = b.trade_date
WHERE a.fund_id IN ('F001','F002','F003')
  AND a.trade_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY a.fund_id, b.fund_id
```

**④ window_funnel 行为漏斗（用户画像）**
```sql
SELECT user_id,
    window_funnel(86400, 'strict_increase', event_time,
        event_type='login', event_type='view_product',
        event_type='add_cart', event_type='payment'
    ) AS funnel_level
FROM user_event
GROUP BY user_id
```

**⑤ BITMAP 亿级用户圈选（人群圈选）**
```sql
SELECT BITMAP_COUNT(
    BITMAP_AND(
        (SELECT user_bitmap FROM user_tag WHERE tag_name='asset_level' AND tag_value='VIP钻石'),
        BITMAP_AND_NOT(
            (SELECT user_bitmap FROM user_tag WHERE tag_name='active_level' AND tag_value='活跃'),
            (SELECT user_bitmap FROM user_tag WHERE tag_name='anomaly_flag' AND tag_value='1')
        )
    )
) AS crowd_size
```

**⑥ AI Function 日志打标（AI日志标签）**
```sql
SELECT log_id,
    ai_completion('openai', 'gpt-4o-mini', 'sk-xxx',
        'https://api.openai.com/v1',
        CONCAT('分析日志风险等级：', log_content)
    ) AS ai_tag
FROM user_log_raw WHERE log_date = CURDATE()
```

---

## 🎮 动态演练模块说明

### 智能制造数字孪生

- **6 台设备**：CNC-001/002/003、WLD-001、ASM-001/002
- **50+ 指标**：物理状态(15) · 生产效能(12) · 质量工艺(10) · 能耗环境(8) · 维保告警(5)
- **5 套剧本**：黄金时段 → 设备疲劳 → 高温熔断 → 产能恢复 → 黄金时段（循环）
- **操作**：点击「+1步」推进15分钟，支持批量(5/10/20步)和自动演练

```
表：mfg_metrics_v2  DUPLICATE KEY(ts, machine_id)  BUCKETS 4
写入：Stream Load HTTP PUT（每步6行，一次批量写入）
查询：QUALIFY 窗口去重 + 分tab聚合（工艺/质量/能耗/维保）
```

### 基金投研沙盘

- **30 只基金**：覆盖半导体、新能源、消费、医药等10个板块，5种类型
- **10 位经理**：不同风格（激进成长/稳健价值/均衡配置/行业集中/量化增强）
- **4 张 Doris 表**：fund_basic / fund_nav_history / fund_position / fund_manager
- **5 套市场剧本**：科技牛市 → 熊市调整 → 板块轮动 → 震荡行情 → 黑天鹅
- **操作**：点击「+1日」推进一个交易日，每8日自动切换剧本

```
Doris 技术亮点：
  fund_basic       DUPLICATE KEY(fund_id)             — 快照多版本，QUALIFY取最新
  fund_nav_history DUPLICATE KEY(trade_date, fund_id) — 日频净值+滚动夏普
  fund_position    DUPLICATE KEY(fund_id,report_date) — 季报持仓穿透
  fund_manager     DUPLICATE KEY(manager_id)          — 经理维度画像
```

---

## 📡 API 接口清单

### 用户画像
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/user/wide` | 多条件分页查询 |
| GET | `/api/user/{id}` | 用户360°视图 |
| POST | `/api/segment/count` | Bitmap 人群估算 |
| POST | `/api/segment/create` | 创建人群包 |
| POST | `/api/behavior/funnel` | 漏斗分析 |
| POST | `/api/behavior/retention` | 留存分析 |
| GET | `/api/behavior/rfm` | RFM 分层 |

### 数据能力
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/metrics/definitions` | 指标目录 |
| POST | `/api/metrics/query` | 通用指标查询 |
| POST | `/api/metrics/compare` | 同环比对比 |
| POST | `/api/metrics/drilldown` | 维度下钻 |
| GET | `/api/observe/logs` | 日志查询 |
| GET | `/api/trace/list` | 链路追踪列表 |
| POST | `/api/benchmark/run` | 压测执行 |

### 向量 & 卫星
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/vector/init` | 向量索引初始化 |
| POST | `/api/vector/search/users` | 用户向量检索 |
| POST | `/api/vector/search/hybrid` | 混合检索 |
| GET | `/api/satellite/overview` | 卫星数据概览 |

### 智能制造
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/mfg/init` | 建表+初始化 |
| POST | `/api/mfg/generate` | 推进1步(15min) |
| POST | `/api/mfg/batch?steps=N` | 批量推进N步 |
| GET | `/api/mfg/overview` | KPI总览 |
| GET | `/api/mfg/machine-status` | 各设备最新状态 |
| GET | `/api/mfg/machine-trend` | 单设备时序 |
| GET | `/api/mfg/quality-stats` | 质量统计 |
| GET | `/api/mfg/energy-stats` | 能耗统计 |
| GET | `/api/mfg/maintenance-stats` | 维保统计 |
| GET | `/api/mfg/process-trend` | 工艺趋势 |
| POST | `/api/mfg/reset` | 清空重置 |

### 基金投研
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/fund/init` | 建表+60日历史种子 |
| POST | `/api/fund/generate` | 推进1个交易日 |
| POST | `/api/fund/batch?days=N` | 批量推进N日 |
| GET | `/api/fund/overview` | KPI总览+当前剧本 |
| GET | `/api/fund/list` | 基金列表(含筛选) |
| GET | `/api/fund/detail/{id}` | 单基金全量数据 |
| GET | `/api/fund/nav/{id}` | 净值历史时序 |
| GET | `/api/fund/position/{id}` | 持仓穿透 |
| GET | `/api/fund/manager/{id}` | 经理画像 |
| GET | `/api/fund/peers/{id}` | 竞品推荐+相关性矩阵 |
| GET | `/api/fund/sector-stats` | 板块热力统计 |
| POST | `/api/fund/reset` | 清空重置 |

---

## ⚙️ 环境变量（backend/settings.py）

```env
DORIS_HOST=127.0.0.1
DORIS_PORT=9030
DORIS_HTTP_PORT=8030
DORIS_USER=root
DORIS_PASSWORD=
DORIS_DATABASE=bank_cdp

# AI Function（可选）
AI_PROVIDER=openai
AI_API_KEY=sk-xxx
AI_API_ENDPOINT=https://api.openai.com/v1
AI_MODEL=gpt-4o-mini
```

---

## 🔒 数据安全说明

- 所有数据为**仿真模拟数据**，不含真实用户信息
- 敏感字段展示时自动脱敏：身份证 `110***8881`，手机 `138****8888`
- 基金/制造数据完全由本地仿真引擎生成，无外部依赖
- 生产环境建议接入 SSO/LDAP 权限管控
