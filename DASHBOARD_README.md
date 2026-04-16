# 银行管理驾驶舱 - 快速启动

## 功能模块

| 模块 | 说明 | 数据表 |
|------|------|--------|
| 🎯 **经营指标** | 收入、成本、利润趋势 | `biz_metrics` |
| 💎 **AUM 规模** | 基金产品资产规模 | `aum_metrics` |
| 🚨 **风险管理** | 风险等级、敞口分析 | `risk_metrics` |
| 📊 **头寸配置** | 资产配置、收益率 | `position_metrics` |
| 🛍️ **产品营销** | 销售额、客户转化 | `product_marketing` |

## 初始化步骤

### 1️⃣ 创建数据库表和导入数据

```bash
# 连接 Doris
mysql -h 127.0.0.1 -P 9030 -u root

# 执行初始化脚本
source /path/to/init_dashboard_data.sql;
```

### 2️⃣ 验证数据

```sql
-- 查看表
SHOW TABLES LIKE '%metrics%';

-- 查看数据量
SELECT COUNT(*) FROM biz_metrics;
SELECT COUNT(*) FROM aum_metrics;
SELECT COUNT(*) FROM risk_metrics;
SELECT COUNT(*) FROM position_metrics;
SELECT COUNT(*) FROM product_marketing;
```

### 3️⃣ 启动应用

```bash
# 后端
uvicorn backend.app:app --reload

# 前端（新终端）
cd frontend && npm run dev
```

### 4️⃣ 访问 Dashboard

打开 `http://localhost:5173/dashboard` 查看管理驾驶舱

## API 端点

| 端点 | 功能 | 用途 |
|------|------|------|
| `GET /api/dashboard/summary` | 摘要数据 | KPI 卡片 |
| `GET /api/dashboard/biz-metrics` | 经营指标 | 趋势图表 |
| `GET /api/dashboard/aum-metrics` | AUM 规模 | 产品分布 |
| `GET /api/dashboard/risk-metrics` | 风险数据 | 风险分析 |
| `GET /api/dashboard/position-metrics` | 头寸数据 | 配置展示 |
| `GET /api/dashboard/product-marketing` | 产品数据 | 销售排行 |

## 图表说明

### 📊 经营指标趋势
- 展示收入、利润的日趋势
- 实时对比目标达成情况

### 💰 AUM 产品分布
- 各类型基金的资产占比
- 包括股票型、债券型、货币型、混合型

### 🛍️ 产品销售排行
- TOP 5 产品的销售金额排序
- 快速识别爆款产品

### 🏦 头寸配置
- 股票、债券、衍生品、现金占比
- 资产配置一目了然

### 🚨 风险等级分布
- 低/中/高风险敞口分析
- 风险管理指标

### 📈 头寸收益率
- 各资产类别的收益率走势
- 多条线图对比分析

## 数据说明

### 示例数据范围
- 日期：2024-04-01 至 2024-04-05（5 天数据）
- 经营指标：3 种类型 × 5 天 = 15 条
- AUM：4 种产品 × 5 天 = 20 条
- 风控：3 个等级 × 5 天 = 15 条
- 头寸：4 种资产 × 5 天 = 20 条
- 产品营销：5 种产品 × 4 天 = 20 条

### 自定义数据

可直接在 Doris 中插入数据：

```sql
INSERT INTO biz_metrics VALUES
('2024-04-06', '收入', 95000000, 17.5, 1.9, 85000000, NOW());
```

## 常见问题

### Q: 数据不显示？
**A:** 检查数据库连接和 .env 配置

```bash
# 检查 Doris 连接
mysql -h 127.0.0.1 -P 9030 -u root -D bank_cdp
SELECT COUNT(*) FROM biz_metrics;
```

### Q: 图表显示 NaN？
**A:** 确保数据中包含必要字段，运行初始化脚本重新导入

### Q: 想修改 KPI 数值格式？
**A:** 编辑 `Dashboard.vue` 中的 `fmt()` 函数

```javascript
const fmt = (v, inBillion = false) => {
  // 自定义格式逻辑
}
```

## 扩展建议

1. **添加实时数据推送**：使用 WebSocket 更新数据
2. **导出报表**：添加 PDF 导出功能
3. **权限管理**：按角色分配不同数据权限
4. **告警规则**：设置阈值自动告警
5. **多维分析**：添加钻取和切片功能

---

**Demo 完成！** 🎉 快速创建了银行经营驾驶舱，包括完整的数据和界面。
