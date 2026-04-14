"""金融指标平台服务 - 自定义维度×指标组合查询"""
from backend.doris.connect import execute_query

DIMENSIONS = [
    {"field": "asset_level",       "label": "资产等级",   "icon": "💰"},
    {"field": "age_group",         "label": "年龄段",     "icon": "👤"},
    {"field": "city",              "label": "城市",       "icon": "🏙️"},
    {"field": "lifecycle_stage",   "label": "生命周期",   "icon": "🔄"},
    {"field": "active_level",      "label": "活跃度",     "icon": "⚡"},
    {"field": "preferred_channel", "label": "偏好渠道",   "icon": "📱"},
    {"field": "gender",            "label": "性别",       "icon": "👥"},
    {"field": "credit_grade",      "label": "信用等级",   "icon": "⭐"},
    {"field": "risk_level",        "label": "风险等级",   "icon": "🔰"},
    {"field": "province",          "label": "省份",       "icon": "📍"},
]

MEASURES = [
    {"alias": "user_cnt",    "label": "用户数",       "fmt": "num",   "expr": "COUNT(*)"},
    {"alias": "total_aum",   "label": "总AUM(万)",    "fmt": "money", "expr": "ROUND(SUM(aum_total),2)"},
    {"alias": "avg_aum",     "label": "均AUM(万)",    "fmt": "money", "expr": "ROUND(AVG(aum_total),2)"},
    {"alias": "max_aum",     "label": "最高AUM(万)",  "fmt": "money", "expr": "ROUND(MAX(aum_total),2)"},
    {"alias": "anomaly_cnt", "label": "异常用户数",   "fmt": "num",   "expr": "SUM(anomaly_flag)"},
    {"alias": "anomaly_rate","label": "异常率%",      "fmt": "pct",   "expr": "ROUND(SUM(anomaly_flag)*100.0/COUNT(*),2)"},
    {"alias": "avg_churn",   "label": "平均流失率%",  "fmt": "pct",   "expr": "ROUND(AVG(churn_prob)*100,2)"},
    {"alias": "avg_credit",  "label": "平均信用分",   "fmt": "num",   "expr": "ROUND(AVG(credit_score),1)"},
    {"alias": "loan_total",  "label": "贷款总额(万)", "fmt": "money", "expr": "ROUND(SUM(loan_amount),2)"},
    {"alias": "fund_total",  "label": "基金总额(万)", "fmt": "money", "expr": "ROUND(SUM(fund_amount),2)"},
    {"alias": "deposit_sum", "label": "存款总额(万)", "fmt": "money", "expr": "ROUND(SUM(deposit_amount),2)"},
]

DIM_MAP     = {d["field"]: d for d in DIMENSIONS}
MEASURE_MAP = {m["alias"]: m for m in MEASURES}

ALLOWED_DIMS = set(DIM_MAP.keys())
ALLOWED_MEASURES = set(MEASURE_MAP.keys())


class MetricsService:

    async def get_definitions(self):
        return {"dimensions": DIMENSIONS, "measures": MEASURES}

    async def query(self, dim_fields: list, measure_aliases: list, limit: int = 100):
        # 安全校验
        dim_fields      = [f for f in dim_fields      if f in ALLOWED_DIMS]
        measure_aliases = [a for a in measure_aliases if a in ALLOWED_MEASURES]

        if not dim_fields and not measure_aliases:
            return {"columns": [], "rows": [], "sql": ""}

        select_parts = list(dim_fields)
        for alias in measure_aliases:
            m = MEASURE_MAP[alias]
            select_parts.append(f"{m['expr']} AS {alias}")

        group_clause  = f"GROUP BY {', '.join(dim_fields)}" if dim_fields else ""
        order_field   = measure_aliases[0] if measure_aliases else dim_fields[0]
        order_dir     = "DESC" if measure_aliases else "ASC"
        order_clause  = f"ORDER BY {order_field} {order_dir}"
        limit_clause  = f"LIMIT {min(limit, 200)}"

        sql = (
            f"SELECT {', '.join(select_parts)}\n"
            f"FROM user_wide\n"
            f"{group_clause}\n"
            f"{order_clause}\n"
            f"{limit_clause}"
        ).strip()

        rows = await execute_query(sql)
        for row in rows:
            for k, v in row.items():
                if k not in dim_fields and v is not None:
                    row[k] = float(v)

        columns = []
        for f in dim_fields:
            d = DIM_MAP[f]
            columns.append({"field": f, "label": d["label"], "type": "dim"})
        for alias in measure_aliases:
            m = MEASURE_MAP[alias]
            columns.append({"field": alias, "label": m["label"], "type": "measure", "fmt": m["fmt"]})

        return {"columns": columns, "rows": rows, "sql": sql}
