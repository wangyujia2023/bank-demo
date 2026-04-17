"""金融指标平台服务 - 通用指标分析平台"""
from backend.doris.connect import execute_query
from datetime import datetime, timedelta
import uuid
import time

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

DIM_MAP      = {d["field"]: d for d in DIMENSIONS}
MEASURE_MAP  = {m["alias"]: m for m in MEASURES}
ALLOWED_DIMS = set(DIM_MAP.keys())
ALLOWED_MEASURES = set(MEASURE_MAP.keys())

VALID_OPS = {"=", "!=", ">", ">=", "<", "<=", "LIKE", "IN"}
VALID_COLS = ALLOWED_DIMS | {
    "aum_total", "credit_score", "churn_prob", "loan_amount",
    "fund_amount", "deposit_amount", "anomaly_flag", "age",
}

TEMPLATES = [
    {
        "id": "t1", "name": "资产等级用户分布", "desc": "按资产等级统计用户数和AUM",
        "dimensions": ["asset_level"], "measures": ["user_cnt", "total_aum", "avg_aum"],
        "sort_by": "total_aum", "sort_dir": "DESC", "limit": 20,
        "filters": [], "calc_fields": [],
    },
    {
        "id": "t2", "name": "省份流失风险分析", "desc": "各省份平均流失率对比",
        "dimensions": ["province"], "measures": ["user_cnt", "avg_churn", "anomaly_cnt"],
        "sort_by": "avg_churn", "sort_dir": "DESC", "limit": 20,
        "filters": [], "calc_fields": [],
    },
    {
        "id": "t3", "name": "渠道活跃度分析", "desc": "各偏好渠道的信用评分和AUM",
        "dimensions": ["preferred_channel"], "measures": ["user_cnt", "avg_credit", "avg_aum"],
        "sort_by": "user_cnt", "sort_dir": "DESC", "limit": 20,
        "filters": [], "calc_fields": [],
    },
    {
        "id": "t4", "name": "生命周期×资产交叉", "desc": "生命周期阶段与资产等级交叉分析",
        "dimensions": ["lifecycle_stage", "asset_level"], "measures": ["user_cnt", "avg_aum"],
        "sort_by": "user_cnt", "sort_dir": "DESC", "limit": 50,
        "filters": [], "calc_fields": [],
    },
    {
        "id": "t5", "name": "高净值客户画像", "desc": "AUM>50万的优质客户结构",
        "dimensions": ["age_group", "risk_level"], "measures": ["user_cnt", "avg_aum", "avg_credit"],
        "sort_by": "avg_aum", "sort_dir": "DESC", "limit": 20,
        "filters": [{"field": "aum_total", "op": ">", "value": "50", "logic": "AND"}],
        "calc_fields": [],
    },
    {
        "id": "t6", "name": "信贷风险区域分布", "desc": "各城市贷款规模与异常率",
        "dimensions": ["city"], "measures": ["user_cnt", "loan_total", "anomaly_rate"],
        "sort_by": "loan_total", "sort_dir": "DESC", "limit": 20,
        "filters": [], "calc_fields": [],
    },
]

_SAVED_QUERIES: dict = {}
_QUERY_HISTORY: list = []


def _build_where(filters: list) -> str:
    clauses = []
    for f in filters:
        field = f.get("field", "")
        op    = f.get("op", "=")
        value = str(f.get("value", ""))
        logic = f.get("logic", "AND").upper()
        if field not in VALID_COLS or op not in VALID_OPS:
            continue
        safe_val = value.replace("'", "''")
        if op == "LIKE":
            clause = f"{field} LIKE '%{safe_val}%'"
        elif op == "IN":
            parts = [f"'{v.strip().replace(chr(39), chr(39)*2)}'" for v in safe_val.split(",")]
            clause = f"{field} IN ({', '.join(parts)})"
        else:
            try:
                float(safe_val)
                clause = f"{field} {op} {safe_val}"
            except ValueError:
                clause = f"{field} {op} '{safe_val}'"
        if clauses:
            clauses.append(f"{logic} {clause}")
        else:
            clauses.append(clause)
    return "WHERE " + " ".join(clauses) if clauses else ""


def _time_clause(time_range: str, start_date: str = None, end_date: str = None) -> str:
    today = datetime.now()
    if time_range == "today":
        d = today.strftime("%Y-%m-%d")
        return f"DATE(register_date) = '{d}'"
    elif time_range == "week":
        d = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        return f"register_date >= '{d}'"
    elif time_range == "month":
        d = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        return f"register_date >= '{d}'"
    elif time_range == "quarter":
        d = (today - timedelta(days=90)).strftime("%Y-%m-%d")
        return f"register_date >= '{d}'"
    elif time_range == "custom" and start_date and end_date:
        return f"register_date BETWEEN '{start_date}' AND '{end_date}'"
    return ""


def _build_select(dim_fields, measure_aliases, calc_fields):
    parts = list(dim_fields)
    for alias in measure_aliases:
        m = MEASURE_MAP[alias]
        parts.append(f"{m['expr']} AS {alias}")
    for cf in (calc_fields or []):
        name = cf.get("name", "").strip()
        expr = cf.get("expr", "").strip()
        allowed_chars = set("0123456789+-*/()., _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
        if name and expr and all(c in allowed_chars for c in expr):
            parts.append(f"ROUND(({expr}), 2) AS `{name}`")
    return parts


def _to_float(rows, dim_fields):
    for row in rows:
        for k, v in row.items():
            if k not in dim_fields and v is not None:
                try:
                    row[k] = float(v)
                except (ValueError, TypeError):
                    pass


def _add_history(sql: str, elapsed_ms: int, rows: int):
    _QUERY_HISTORY.append({
        "sql": sql[:400],
        "elapsed_ms": elapsed_ms,
        "rows": rows,
        "time": datetime.now().strftime("%H:%M:%S"),
    })
    if len(_QUERY_HISTORY) > 100:
        _QUERY_HISTORY.pop(0)


class MetricsService:

    async def get_definitions(self):
        return {"dimensions": DIMENSIONS, "measures": MEASURES}

    async def get_templates(self):
        return TEMPLATES

    async def query(
        self,
        dim_fields: list,
        measure_aliases: list,
        limit: int = 100,
        page: int = 1,
        filters: list = None,
        sort_by: str = None,
        sort_dir: str = "DESC",
        top_n: int = None,
        calc_fields: list = None,
        time_range: str = None,
        start_date: str = None,
        end_date: str = None,
    ):
        dim_fields      = [f for f in dim_fields      if f in ALLOWED_DIMS]
        measure_aliases = [a for a in measure_aliases if a in ALLOWED_MEASURES]
        filters         = filters or []
        calc_fields     = calc_fields or []

        if not dim_fields and not measure_aliases and not calc_fields:
            return {"columns": [], "rows": [], "sql": "", "total": 0, "elapsed_ms": 0}

        t0 = time.time()
        select_parts = _build_select(dim_fields, measure_aliases, calc_fields)

        where_clause = _build_where(filters)
        tc = _time_clause(time_range or "", start_date, end_date) if time_range else ""
        if tc:
            where_clause = (where_clause + " AND " + tc) if where_clause else f"WHERE {tc}"

        group_clause = f"GROUP BY {', '.join(dim_fields)}" if dim_fields else ""

        if sort_by and sort_by in (ALLOWED_MEASURES | ALLOWED_DIMS):
            order_field = sort_by
        elif measure_aliases:
            order_field = measure_aliases[0]
        elif dim_fields:
            order_field = dim_fields[0]
        else:
            order_field = None

        effective_limit = min(top_n or limit, 200)
        offset = (max(page, 1) - 1) * effective_limit
        order_clause = f"ORDER BY {order_field} {sort_dir.upper()}" if order_field else ""
        limit_clause = f"LIMIT {effective_limit} OFFSET {offset}"

        lines = ["SELECT " + ", ".join(select_parts), "FROM user_wide"]
        if where_clause:  lines.append(where_clause)
        if group_clause:  lines.append(group_clause)
        if order_clause:  lines.append(order_clause)
        lines.append(limit_clause)
        sql = "\n".join(lines)

        rows = await execute_query(sql)
        elapsed = round((time.time() - t0) * 1000)
        _to_float(rows, dim_fields)

        columns = []
        for f in dim_fields:
            columns.append({"field": f, "label": DIM_MAP[f]["label"], "type": "dim"})
        for alias in measure_aliases:
            m = MEASURE_MAP[alias]
            columns.append({"field": alias, "label": m["label"], "type": "measure", "fmt": m["fmt"]})
        for cf in calc_fields:
            name = cf.get("name", "").strip()
            if name:
                columns.append({"field": name, "label": cf.get("label", name), "type": "measure", "fmt": "num"})

        _add_history(sql, elapsed, len(rows))
        return {"columns": columns, "rows": rows, "sql": sql, "elapsed_ms": elapsed, "total": len(rows)}

    async def compare(
        self,
        dim_fields: list,
        measure_aliases: list,
        compare_type: str = "mom",
        current_start: str = None,
        current_end: str = None,
    ):
        dim_fields      = [f for f in dim_fields      if f in ALLOWED_DIMS]
        measure_aliases = [a for a in measure_aliases if a in ALLOWED_MEASURES]

        today = datetime.now()
        ce = datetime.strptime(current_end,   "%Y-%m-%d") if current_end   else today
        cs = datetime.strptime(current_start, "%Y-%m-%d") if current_start else (today - timedelta(days=30))
        delta = ce - cs

        if compare_type == "mom":
            pe = cs - timedelta(days=1)
            ps = pe - delta
        else:
            ps = cs.replace(year=cs.year - 1)
            pe = ce.replace(year=ce.year - 1)

        async def _q(start_dt, end_dt):
            s, e = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
            sel = list(dim_fields)
            for alias in measure_aliases:
                sel.append(f"{MEASURE_MAP[alias]['expr']} AS {alias}")
            grp = f"GROUP BY {', '.join(dim_fields)}" if dim_fields else ""
            ord_ = f"ORDER BY {measure_aliases[0]} DESC" if measure_aliases else ""
            sql = f"SELECT {', '.join(sel)}\nFROM user_wide\nWHERE register_date BETWEEN '{s}' AND '{e}'\n{grp}\n{ord_}\nLIMIT 50"
            rows = await execute_query(sql)
            _to_float(rows, dim_fields)
            return rows, sql

        cur_rows, cur_sql = await _q(cs, ce)
        prev_rows, _      = await _q(ps, pe)

        if not dim_fields:
            cur_r  = cur_rows[0]  if cur_rows  else {}
            prev_r = prev_rows[0] if prev_rows else {}
            diffs = []
            for alias in measure_aliases:
                cv = float(cur_r.get(alias) or 0)
                pv = float(prev_r.get(alias) or 0)
                chg = round((cv - pv) / pv * 100, 2) if pv else 0
                diffs.append({"alias": alias, "label": MEASURE_MAP[alias]["label"],
                               "current": cv, "previous": pv, "change_pct": chg, "up": chg >= 0})
            return {
                "type": compare_type,
                "current_period":  f"{cs.strftime('%Y-%m-%d')} ~ {ce.strftime('%Y-%m-%d')}",
                "prev_period":     f"{ps.strftime('%Y-%m-%d')} ~ {pe.strftime('%Y-%m-%d')}",
                "diffs": diffs, "rows": [], "sql": cur_sql,
                "dim_fields": [], "measure_aliases": measure_aliases,
            }

        dim_key = lambda r: tuple(r.get(f, "") for f in dim_fields)
        prev_map = {dim_key(r): r for r in prev_rows}
        result_rows = []
        for r in cur_rows:
            k = dim_key(r)
            pr = prev_map.get(k, {})
            row = {f: r.get(f) for f in dim_fields}
            for alias in measure_aliases:
                cv = float(r.get(alias) or 0)
                pv = float(pr.get(alias) or 0)
                chg = round((cv - pv) / pv * 100, 2) if pv else 0
                row[alias]          = cv
                row[f"{alias}_prev"] = pv
                row[f"{alias}_chg"]  = chg
                row[f"{alias}_up"]   = chg >= 0
            result_rows.append(row)

        return {
            "type": compare_type,
            "current_period": f"{cs.strftime('%Y-%m-%d')} ~ {ce.strftime('%Y-%m-%d')}",
            "prev_period":    f"{ps.strftime('%Y-%m-%d')} ~ {pe.strftime('%Y-%m-%d')}",
            "diffs": [], "rows": result_rows, "sql": cur_sql,
            "dim_fields": dim_fields, "measure_aliases": measure_aliases,
        }

    async def drilldown(
        self,
        parent_dim: str,
        parent_value: str,
        child_dim: str,
        measures: list,
        filters: list = None,
    ):
        measures = [a for a in measures if a in ALLOWED_MEASURES]
        if child_dim not in ALLOWED_DIMS or parent_dim not in ALLOWED_DIMS:
            return {"columns": [], "rows": [], "sql": ""}

        sel = [child_dim] + [f"{MEASURE_MAP[a]['expr']} AS {a}" for a in measures]
        safe_val = str(parent_value).replace("'", "''")
        base_where = _build_where(filters or [])
        if base_where:
            where = base_where + f" AND {parent_dim} = '{safe_val}'"
        else:
            where = f"WHERE {parent_dim} = '{safe_val}'"

        order_field = measures[0] if measures else child_dim
        sql = (
            f"SELECT {', '.join(sel)}\nFROM user_wide\n{where}\n"
            f"GROUP BY {child_dim}\nORDER BY {order_field} DESC\nLIMIT 50"
        )
        rows = await execute_query(sql)
        _to_float(rows, [child_dim])

        columns = [{"field": child_dim, "label": DIM_MAP[child_dim]["label"], "type": "dim"}]
        for a in measures:
            m = MEASURE_MAP[a]
            columns.append({"field": a, "label": m["label"], "type": "measure", "fmt": m["fmt"]})

        return {"columns": columns, "rows": rows, "sql": sql,
                "parent": {"dim": parent_dim, "value": parent_value}}

    def save_query(self, name: str, config: dict) -> dict:
        qid = str(uuid.uuid4())[:8]
        item = {"id": qid, "name": name, "config": config,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        _SAVED_QUERIES[qid] = item
        return item

    def list_saved(self) -> list:
        return sorted(_SAVED_QUERIES.values(), key=lambda x: x["created_at"], reverse=True)

    def delete_saved(self, qid: str) -> bool:
        if qid in _SAVED_QUERIES:
            del _SAVED_QUERIES[qid]
            return True
        return False

    def get_history(self, limit: int = 30) -> list:
        return list(reversed(_QUERY_HISTORY[-limit:]))
