"""
Doris 银行数据中台演示 — 纯 Python stdlib HTTP 后端
无任何第三方依赖，直接 python3 server.py 启动

端口: 8080
Doris: 10.26.20.3:19030  root/空密码
"""
import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# 添加父目录到 sys.path 以便导入 doris_client
sys.path.insert(0, os.path.dirname(__file__))
from doris_client import DorisClient, get_connection

# ------------------------------------------------------------------ #
#  Config
# ------------------------------------------------------------------ #
DORIS_HOST = os.environ.get("DORIS_HOST", "10.26.20.3")
DORIS_PORT = int(os.environ.get("DORIS_PORT", "19030"))
DORIS_USER = os.environ.get("DORIS_USER", "root")
DORIS_PASS = os.environ.get("DORIS_PASS", "")
DB_NAME = "bank_demo"
SERVER_PORT = int(os.environ.get("SERVER_PORT", "8080"))

# ------------------------------------------------------------------ #
#  DB helper
# ------------------------------------------------------------------ #

def query(sql: str, db: str = DB_NAME) -> list[dict]:
    with get_connection(DORIS_HOST, DORIS_PORT, DORIS_USER, DORIS_PASS, db) as c:
        return c.execute(sql)

def execute(sql: str, db: str = DB_NAME):
    with get_connection(DORIS_HOST, DORIS_PORT, DORIS_USER, DORIS_PASS, db) as c:
        c.execute(sql)

# ------------------------------------------------------------------ #
#  API handlers
# ------------------------------------------------------------------ #

def api_status(_):
    try:
        rows = query("SELECT VERSION() AS ver", db="information_schema")
        return {"ok": True, "version": rows[0]["ver"] if rows else "unknown", "db": DB_NAME}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_hasp_logs(params):
    risk = params.get("risk", [""])[0]
    limit = int(params.get("limit", ["100"])[0])
    where = f"WHERE risk_level = '{risk}'" if risk else ""
    sql = f"""
        SELECT
            log_id, log_time, user_id, user_name,
            operation_type, log_content, client_ip,
            result_code,
            CASE
                WHEN REGEXP(log_content, '(连续|多次|暴力|异地|境外|凌晨[0-4]|大额|可疑)') THEN '安全预警'
                WHEN REGEXP(log_content, '(超时|响应慢|高并发|内存|CPU|OOM)')           THEN '性能预警'
                WHEN REGEXP(log_content, '(异常|报错|失败|拒绝|中断)')                   THEN '系统异常'
                ELSE '正常操作'
            END AS ai_label,
            risk_level,
            ai_confidence,
            TOKENIZE(log_content, '"parser"="chinese","mode"="fine_grained"') AS tokens
        FROM hasp_logs
        {where}
        ORDER BY log_time DESC
        LIMIT {limit}
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_hasp_stats(_):
    sql = """
        SELECT
            COUNT(*)                                          AS total,
            SUM(CASE WHEN risk_level IN ('HIGH','CRITICAL') THEN 1 ELSE 0 END) AS alerts,
            ROUND(AVG(ai_confidence) * 100, 1)               AS avg_confidence,
            SUM(CASE WHEN result_code = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
            COUNT(DISTINCT user_id)                           AS active_users
        FROM hasp_logs
        WHERE log_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_metrics_kpi(_):
    sql = """
        SELECT
            SUM(txn_count)                           AS total_txn,
            ROUND(SUM(txn_amount) / 1e8, 2)          AS total_amount_yi,
            ROUND(AVG(success_rate), 2)              AS avg_success_rate,
            ROUND(AVG(avg_response_ms), 0)           AS avg_response_ms,
            SUM(CASE WHEN channel='MOBILE' THEN txn_count ELSE 0 END) AS mobile_txn,
            SUM(CASE WHEN channel='PC'     THEN txn_count ELSE 0 END) AS pc_txn,
            SUM(CASE WHEN channel='ATM'    THEN txn_count ELSE 0 END) AS atm_txn
        FROM business_metrics
        WHERE stat_date = CURDATE()
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_metrics_trend(_):
    sql = """
        SELECT
            DATE_FORMAT(stat_time, '%H:00') AS hour_label,
            SUM(txn_count)                  AS txn_count,
            ROUND(SUM(txn_amount) / 1e4, 0) AS amount_wan,
            ROUND(AVG(success_rate), 2)     AS success_rate,
            ROUND(AVG(avg_response_ms), 0)  AS avg_ms
        FROM business_metrics
        WHERE stat_date = CURDATE()
        GROUP BY DATE_FORMAT(stat_time, '%H:00')
        ORDER BY hour_label
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_metrics_channel(_):
    sql = """
        SELECT
            channel,
            SUM(txn_count)                          AS txn_count,
            ROUND(SUM(txn_amount) / 1e8, 2)         AS amount_yi,
            ROUND(AVG(success_rate), 2)             AS success_rate
        FROM business_metrics
        WHERE stat_date = CURDATE()
        GROUP BY channel
        ORDER BY txn_count DESC
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_cdp_customers(params):
    tier = params.get("tier", [""])[0]
    keyword = params.get("keyword", [""])[0]
    limit = int(params.get("limit", ["50"])[0])
    where_parts = []
    if tier:
        where_parts.append(f"customer_tier = '{tier}'")
    if keyword:
        where_parts.append(f"(customer_name LIKE '%{keyword}%' OR customer_id LIKE '%{keyword}%')")
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    sql = f"""
        SELECT
            customer_id, customer_name, age, gender,
            customer_tier, risk_profile, total_assets,
            monthly_txn_count, monthly_txn_amount,
            tags, register_date, last_login,
            credit_score, relationship_manager
        FROM customer_profile
        {where}
        ORDER BY total_assets DESC
        LIMIT {limit}
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_cdp_customer_detail(params):
    cid = params.get("id", [""])[0]
    if not cid:
        return {"ok": False, "error": "Missing id"}
    sql = f"""
        SELECT t.*, p.customer_name, p.customer_tier, p.risk_profile
        FROM customer_transactions t
        JOIN customer_profile p ON t.customer_id = p.customer_id
        WHERE t.customer_id = '{cid}'
        ORDER BY t.txn_time DESC
        LIMIT 20
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_cdp_stats(_):
    sql = """
        SELECT
            customer_tier,
            COUNT(*)                                AS cnt,
            ROUND(AVG(total_assets) / 1e4, 1)       AS avg_assets_wan,
            ROUND(AVG(credit_score), 0)             AS avg_credit,
            ROUND(AVG(monthly_txn_count), 1)        AS avg_monthly_txn
        FROM customer_profile
        GROUP BY customer_tier
        ORDER BY avg_assets_wan DESC
    """
    try:
        return {"ok": True, "data": query(sql)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_analytics_tables(_):
    sql = f"SHOW TABLES FROM {DB_NAME}"
    try:
        rows = query(sql, db=DB_NAME)
        key = f"Tables_in_{DB_NAME}"
        tables = [r[key] for r in rows if key in r]
        result = []
        for tbl in tables:
            desc_sql = f"DESC {DB_NAME}.{tbl}"
            cols = query(desc_sql)
            result.append({"table": tbl, "columns": cols})
        return {"ok": True, "data": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def api_analytics_query(params, body):
    sql = body.get("sql", "")
    if not sql.strip():
        return {"ok": False, "error": "Empty SQL"}
    # Basic safety: only SELECT
    clean = sql.strip().lstrip(";").strip()
    if not clean.upper().startswith("SELECT") and not clean.upper().startswith("SHOW") and not clean.upper().startswith("EXPLAIN") and not clean.upper().startswith("DESC"):
        return {"ok": False, "error": "Only SELECT / SHOW / EXPLAIN / DESC is allowed"}
    try:
        rows = query(clean)
        return {"ok": True, "data": rows, "rows": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ------------------------------------------------------------------ #
#  Route table
# ------------------------------------------------------------------ #

ROUTES = {
    "GET /api/status":             api_status,
    "GET /api/hasp/logs":          api_hasp_logs,
    "GET /api/hasp/stats":         api_hasp_stats,
    "GET /api/metrics/kpi":        api_metrics_kpi,
    "GET /api/metrics/trend":      api_metrics_trend,
    "GET /api/metrics/channel":    api_metrics_channel,
    "GET /api/cdp/customers":      api_cdp_customers,
    "GET /api/cdp/customer/detail":api_cdp_customer_detail,
    "GET /api/cdp/stats":          api_cdp_stats,
    "GET /api/analytics/tables":   api_analytics_tables,
    "POST /api/analytics/query":   api_analytics_query,
}


# ------------------------------------------------------------------ #
#  HTTP handler
# ------------------------------------------------------------------ #

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def _send_json(self, data: dict, code: int = 200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _handle(self, method: str):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        route_key = f"{method} {parsed.path}"

        body = {}
        if method == "POST":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                try:
                    body = json.loads(self.rfile.read(length))
                except Exception:
                    body = {}

        handler = ROUTES.get(route_key)
        if handler is None:
            self._send_json({"ok": False, "error": "Not found"}, 404)
            return
        try:
            if method == "POST":
                result = handler(params, body)
            else:
                result = handler(params)
            self._send_json(result)
        except Exception:
            tb = traceback.format_exc()
            print(tb)
            self._send_json({"ok": False, "error": "Internal error", "detail": tb[-500:]}, 500)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")


# ------------------------------------------------------------------ #
#  Main
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    print(f"🚀 Doris 银行数据中台后端启动")
    print(f"   Doris : {DORIS_HOST}:{DORIS_PORT}  user={DORIS_USER}")
    print(f"   HTTP  : http://0.0.0.0:{SERVER_PORT}")
    print(f"   数据库 : {DB_NAME}")
    print(f"\n测试连接 Doris...")
    try:
        rows = query("SELECT VERSION() AS v", db="information_schema")
        print(f"✅ Doris 连接成功！版本: {rows[0]['v']}")
    except Exception as e:
        print(f"⚠️  Doris 连接失败: {e}")
        print(f"   前端将使用 Mock 数据展示")

    httpd = HTTPServer(("0.0.0.0", SERVER_PORT), Handler)
    print(f"\n🌐 服务已就绪，访问 http://localhost:{SERVER_PORT}\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n服务停止")
