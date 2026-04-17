# 银行业 CDP 平台 v2.0
**基于 Apache Doris 4.0 · HASP 能力 · AI Function · FileBeat 日志采集**

---

## 📐 工程结构

```
bank-cdp-doris4/
├── backend/                        # FastAPI 后端
│   ├── app.py                      # 入口
│   ├── settings.py                 # 全局配置
│   ├── requirements.txt
│   ├── api/
│   │   └── routes.py               # 全部 API 路由
│   ├── service/
│   │   ├── user_service.py         # 用户宽表 + 人群圈选
│   │   ├── behavior_service.py     # 行为分析（漏斗/留存/RFM）
│   │   ├── log_service.py          # 日志接收 + 查询 + 分析
│   │   └── dashboard_service.py    # 大盘统计
│   └── doris/
│       ├── connect.py              # Doris 连接池 + HASP 注入 + Stream Load
│       └── ai_function.py          # AI Function 调用（Doris原生 + Python降级）
├── frontend/                       # Vue3 + Element Plus 前端
│   ├── src/
│   │   ├── App.vue                 # 根组件（侧边栏布局）
│   │   ├── main.js
│   │   ├── router/index.js
│   │   ├── api/index.js            # 所有接口封装
│   │   └── views/
│   │       ├── Dashboard.vue       # 首页大盘
│   │       ├── UserWide.vue        # 用户宽表查询
│   │       ├── Segment.vue         # 人群圈选
│   │       ├── Behavior.vue        # 行为分析（漏斗/留存/趋势/RFM）
│   │       ├── LogManage.vue       # 日志管理
│   │       ├── AiTag.vue           # AI 标签管理
│   │       ├── LogAnalysis.vue     # 日志分析
│   │       └── SysConfig.vue       # 系统配置
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── sql/
│   └── doris_ddl.sql               # 全部建表 DDL + 物化视图 + AI Function 定义
├── test_data/
│   └── init_data.sql               # 测试数据（10用户 + 标签 + 日志 + AI标签）
├── filebeat/
│   ├── filebeat.yml                # FileBeat 采集配置
│   └── gen_test_logs.sh            # 测试日志生成脚本
└── README.md
```

---

## 🚀 快速启动

### 第一步：部署 Apache Doris 4.0

**方式A：Docker 快速体验（推荐开发测试）**
```bash
# 拉取 Doris 4.0 镜像
docker run -d \
  --name doris-all-in-one \
  -p 8030:8030 -p 9030:9030 -p 8040:8040 \
  -e FE_SERVERS="fe1:127.0.0.1:9010" \
  -e BE_ADDR="127.0.0.1:9050" \
  apache/doris:2.1.3-all-in-one

# 等待约 30 秒后连接测试
mysql -h 127.0.0.1 -P 9030 -u root
```

**方式B：生产部署（参考官方文档）**
```
https://doris.apache.org/zh-CN/docs/install/standard-deployment
```

### 第二步：启用 Doris 4.0 HASP & 初始化数据库

```bash
# 连接 Doris
mysql -h 127.0.0.1 -P 9030 -u root

# 开启 Pipeline 执行引擎（HASP核心）

# 初始化建表
SOURCE sql/doris_ddl.sql;

# 导入测试数据
SOURCE test_data/init_data.sql;
```

### 第三步：启动后端

```bash
cd backend

# 安装依赖
pip install -r requirements.txt

# 配置环境变量（可直接修改 settings.py 或创建 .env）
export DORIS_HOST=127.0.0.1
export DORIS_PORT=9030
export DORIS_USER=root
export DORIS_PASSWORD=
export DORIS_DATABASE=bank_cdp

# 配置 AI Function（可选，不配置则跳过 AI 打标）
export AI_API_KEY=sk-your-openai-key
export AI_MODEL=gpt-4o-mini

# 启动
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

后端访问：
- API 文档：http://localhost:8000/docs
- 健康检查：http://localhost:8000/api/system/health

### 第四步：启动前端

```bash
cd frontend

# 安装依赖（需要 Node.js 18+）
npm install

# 开发模式启动
npm run dev
```

前端访问：http://localhost:5173

### 第五步（可选）：启动 FileBeat 日志采集

```bash
# 安装 FileBeat（https://www.elastic.co/beats/filebeat）
# macOS
brew install filebeat

# 生成测试日志
bash filebeat/gen_test_logs.sh

# 启动 FileBeat
filebeat -c filebeat/filebeat.yml -e
```

---

## 🔑 Doris 4.0 核心特性说明

### HASP（Hybrid Adaptive Storage and Processing）

| 特性 | 配置项 | 作用 |
|------|--------|------|
| Pipeline 执行引擎 | `enable_pipeline_engine=true` | 多核并行，查询性能 3-5x |
| 向量化执行 | `enable_vectorized_engine=true` | SIMD 加速，聚合快 5-10x |
| 行列混存（MOW） | `store_row_column=true` | 写时合并，Upsert 性能最优 |
| Runtime Filter | `runtime_filter_mode=GLOBAL` | Join 动态过滤，减少 Shuffle |
| 物化视图 | `CREATE MATERIALIZED VIEW` | 热点查询自动加速，透明命中 |

### AI Function（Doris 4.0 原生）

```sql
-- 在 SQL 中直接调用外部 LLM，无需数据传输到应用层
SELECT
    log_id,
    ai_completion(
        'openai',                          -- AI 提供商
        'gpt-4o-mini',                     -- 模型名称
        'sk-your-api-key',                 -- API Key
        'https://api.openai.com/v1',       -- Endpoint
        CONCAT('分析日志：', log_content)   -- Prompt
    ) AS ai_tag
FROM user_log_raw
WHERE log_date = CURDATE()
LIMIT 100;
```

### Bitmap 人群圈选

```sql
-- 毫秒级亿级用户圈选
SELECT BITMAP_COUNT(
    BITMAP_AND(
        (SELECT user_bitmap FROM user_tag WHERE tag_name='asset_level' AND tag_value='VIP钻石'),
        BITMAP_AND_NOT(
            (SELECT user_bitmap FROM user_tag WHERE tag_name='active_level' AND tag_value='沉睡'),
            (SELECT user_bitmap FROM user_tag WHERE tag_name='anomaly_flag' AND tag_value='1')
        )
    )
) AS crowd_size;
```

---

## 📡 API 接口清单

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/api/user/wide` | 用户宽表多条件分页查询 |
| GET  | `/api/user/{id}` | 用户360°视图 |
| POST | `/api/segment/count` | 实时 Bitmap 人群估算 |
| POST | `/api/segment/create` | 创建保存人群包 |
| GET  | `/api/segment/list` | 人群包列表 |
| DELETE | `/api/segment/{id}` | 删除人群包 |
| POST | `/api/behavior/funnel` | 漏斗分析（window_funnel） |
| POST | `/api/behavior/retention` | 留存分析 |
| GET  | `/api/behavior/transaction` | 交易趋势分析 |
| GET  | `/api/behavior/rfm` | RFM 用户价值分层 |
| POST | `/api/log/collect` | 接收 FileBeat 日志 |
| POST | `/api/log/ai-tag` | 触发 AI 自动打标签 |
| GET  | `/api/log/query` | 日志多条件查询 |
| GET  | `/api/log/analysis` | 日志统计分析 |
| GET  | `/api/log/tag-stats` | AI 打标签统计 |
| GET  | `/api/tag/list` | 标签字典查询 |
| POST | `/api/tag/manage` | 新增标签 |
| DELETE | `/api/tag/{id}` | 删除标签 |
| GET  | `/api/dashboard` | 大盘核心数据 |
| GET  | `/api/system/health` | Doris 健康检查 |
| GET  | `/api/system/config` | 系统配置查看 |

---

## 🔒 数据安全合规

- **脱敏处理**：身份证号 `110***8881`、手机号 `138****8888`、账号 `6217****1234`
- **权限控制**：生产环境建议接入银行 SSO/LDAP
- **日志审计**：所有查询操作记录操作日志
- **数据分类**：敏感字段（AUM、贷款额）仅限授权角色查看
- **AI 合规**：日志内容不含真实身份证号、银行账号，AI 推理前预处理脱敏

---

## ⚙️ 环境变量说明（backend/.env）

```env
# Doris 连接
DORIS_HOST=127.0.0.1
DORIS_PORT=9030
DORIS_HTTP_PORT=8030
DORIS_USER=root
DORIS_PASSWORD=
DORIS_DATABASE=bank_cdp

# AI Function
AI_PROVIDER=openai
AI_API_KEY=sk-xxx
AI_API_ENDPOINT=https://api.openai.com/v1
AI_MODEL=gpt-4o-mini

# 日志采集
LOG_BATCH_SIZE=500
LOG_FLUSH_INTERVAL=5
```
