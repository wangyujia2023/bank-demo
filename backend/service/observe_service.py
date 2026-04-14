"""日志可观测性 & 链路追踪服务"""
import hashlib
from backend.doris.connect import execute_query, execute_one

# event_type → log level
LEVEL_MAP = {
    "LOGIN": "INFO",  "LOGOUT": "INFO",  "REGISTER": "INFO",
    "VIEW": "DEBUG",
    "PAYMENT": "INFO", "DEPOSIT": "INFO",
    "TRANSFER": "WARN", "WITHDRAW": "WARN",
    "ANOMALY": "ERROR",
}

# event_type → 微服务名
SERVICE_MAP = {
    "LOGIN": "auth-service",      "LOGOUT": "auth-service",
    "REGISTER": "user-service",   "VIEW": "portal-service",
    "PAYMENT": "payment-service", "TRANSFER": "payment-service",
    "DEPOSIT": "account-service", "WITHDRAW": "account-service",
    "ANOMALY": "risk-engine",
}

LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}


def _trace_id(user_id, event_time, event_type):
    raw = f"{user_id}{event_time}{event_type}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _build_message(evt, user_id, channel, amount):
    uid, ch, amt = user_id, channel or "-", float(amount or 0)
    templates = {
        "LOGIN":    f"User {uid} authenticated via {ch} session_id={uid % 9999:04x}c3f2",
        "LOGOUT":   f"User {uid} session terminated channel={ch}",
        "REGISTER": f"Registration complete user_id={uid} channel={ch}",
        "VIEW":     f"Page view user={uid} module=dashboard latency=12ms",
        "PAYMENT":  f"Payment OK user={uid} amount={amt:.2f} channel={ch} txn_id={uid % 9999:04x}",
        "TRANSFER": f"Transfer initiated user={uid} amount={amt:.2f} compliance_check=PASS",
        "DEPOSIT":  f"Deposit received user={uid} amount={amt:.2f} channel={ch}",
        "WITHDRAW": f"Withdrawal user={uid} amount={amt:.2f} channel={ch}",
        "ANOMALY":  f"[ALERT] Anomaly detected user={uid} risk_score=0.87 channel={ch} action=BLOCKED",
    }
    return templates.get(evt, f"event={evt} user={uid}")


# Span 模版：[service, operation, offset_ms, duration_ms]
SPAN_TEMPLATES = {
    "LOGIN":    [("api-gateway","HTTP POST /auth/login",0,50),("auth-service","validateCredentials",8,30),("doris-connector","SELECT user_wide",20,12),("cache-layer","setSessionCache",42,8)],
    "PAYMENT":  [("api-gateway","HTTP POST /payment",0,150),("auth-service","verifyToken",5,15),("payment-service","processPayment",25,80),("doris-connector","INSERT user_behavior",60,20),("risk-engine","realtimeRiskCheck",90,50),("cache-layer","updateBalance",145,10)],
    "TRANSFER": [("api-gateway","HTTP POST /transfer",0,240),("auth-service","verifyToken",5,15),("payment-service","initiateTransfer",25,100),("doris-connector","INSERT user_behavior",70,25),("risk-engine","complianceCheck",100,120),("doris-connector","UPDATE account",225,15)],
    "ANOMALY":  [("api-gateway","HTTP POST /behavior",0,320),("auth-service","verifyToken",5,15),("risk-engine","anomalyDetect",25,250),("doris-connector","SELECT user_behavior",50,30),("risk-engine","scoreModel",90,180),("doris-connector","UPDATE anomaly_flag",275,25),("portal-service","sendAlert",305,18)],
    "DEPOSIT":  [("api-gateway","HTTP POST /deposit",0,120),("account-service","processDeposit",10,70),("doris-connector","INSERT user_behavior",45,20),("doris-connector","UPDATE account",75,30),("cache-layer","refreshBalance",110,12)],
    "WITHDRAW": [("api-gateway","HTTP POST /withdraw",0,180),("auth-service","verifyToken",5,15),("account-service","processWithdrawal",25,100),("risk-engine","limitCheck",60,40),("doris-connector","INSERT user_behavior",110,20),("doris-connector","UPDATE account",140,30)],
}
DEFAULT_SPANS = [("api-gateway","HTTP GET /api",0,80),("user-service","handleRequest",8,50),("doris-connector","SELECT query",20,25),("cache-layer","cacheRead",55,12)]


class ObserveService:

    # ─── 日志查询 ────────────────────────────────────────────────

    async def query_logs(self, search=None, level=None, service=None, page=1, size=50):
        conditions = ["1=1"]
        if search:
            safe = search.replace("'", "''")
            conditions.append(f"(event_type LIKE '%{safe}%' OR channel LIKE '%{safe}%')")

        if level == "ERROR":
            conditions.append("event_type IN ('ANOMALY')")
        elif level == "WARN":
            conditions.append("event_type IN ('TRANSFER','WITHDRAW','ANOMALY')")
        elif level == "INFO":
            conditions.append("event_type IN ('LOGIN','LOGOUT','PAYMENT','DEPOSIT','REGISTER')")
        elif level == "DEBUG":
            conditions.append("event_type = 'VIEW'")

        if service and service in SERVICE_MAP.values():
            evts = [k for k, v in SERVICE_MAP.items() if v == service]
            types_str = "','".join(evts)
            conditions.append(f"event_type IN ('{types_str}')")

        offset = (page - 1) * size
        rows = await execute_query(
            f"SELECT user_id, event_type, event_time, channel, amount "
            f"FROM user_behavior WHERE {' AND '.join(conditions)} "
            f"ORDER BY event_time DESC LIMIT {size} OFFSET {offset}"
        )

        logs = []
        for r in rows:
            evt = r.get("event_type", "")
            uid = r.get("user_id")
            ch  = r.get("channel", "")
            amt = float(r.get("amount") or 0)
            logs.append({
                "trace_id":  _trace_id(uid, r.get("event_time", ""), evt),
                "timestamp": str(r.get("event_time", "")),
                "level":     LEVEL_MAP.get(evt, "INFO"),
                "service":   SERVICE_MAP.get(evt, "api-gateway"),
                "event":     evt,
                "user_id":   uid,
                "channel":   ch,
                "amount":    amt,
                "message":   _build_message(evt, uid, ch, amt),
            })
        return logs

    async def log_stats(self):
        """各 level / service 的计数（用于侧边栏 facets）"""
        rows = await execute_query(
            "SELECT event_type, COUNT(*) AS cnt FROM user_behavior GROUP BY event_type"
        )
        level_counts = {}
        svc_counts   = {}
        for r in rows:
            evt = r["event_type"]
            cnt = int(r["cnt"])
            lv  = LEVEL_MAP.get(evt, "INFO")
            svc = SERVICE_MAP.get(evt, "api-gateway")
            level_counts[lv]  = level_counts.get(lv, 0)  + cnt
            svc_counts[svc]   = svc_counts.get(svc, 0)   + cnt

        histogram = await execute_query(
            "SELECT event_type, COUNT(*) AS cnt FROM user_behavior GROUP BY event_type ORDER BY cnt DESC"
        )
        return {
            "level_counts": [{"level": k, "cnt": v} for k, v in sorted(level_counts.items(), key=lambda x: -LEVEL_ORDER.get(x[0],0))],
            "svc_counts":   [{"service": k, "cnt": v} for k, v in sorted(svc_counts.items(), key=lambda x: -x[1])],
            "histogram":    histogram,
            "services":     sorted(set(SERVICE_MAP.values())),
        }

    # ─── 链路追踪 ────────────────────────────────────────────────

    async def trace_list(self, service=None, status=None, page=1, size=20):
        conditions = ["1=1"]
        if service and service in SERVICE_MAP.values():
            evts = [k for k, v in SERVICE_MAP.items() if v == service]
            types_str = "','".join(evts)
            conditions.append(f"event_type IN ('{types_str}')")
        if status == "ERROR":
            conditions.append("event_type = 'ANOMALY'")

        offset = (page - 1) * size
        rows = await execute_query(
            f"SELECT user_id, event_type, event_time, channel, amount "
            f"FROM user_behavior WHERE {' AND '.join(conditions)} "
            f"ORDER BY event_time DESC LIMIT {size} OFFSET {offset}"
        )

        traces = []
        for r in rows:
            evt   = r.get("event_type", "")
            uid   = r.get("user_id")
            tid   = _trace_id(uid, r.get("event_time", ""), evt)
            spans = SPAN_TEMPLATES.get(evt, DEFAULT_SPANS)
            total = max(s[2] + s[3] for s in spans) + int(tid[:2], 16) % 30
            traces.append({
                "trace_id":     tid,
                "root_service": SERVICE_MAP.get(evt, "api-gateway"),
                "operation":    evt.lower().replace("_", "-"),
                "start_time":   str(r.get("event_time", "")),
                "duration_ms":  total,
                "status":       "ERROR" if evt == "ANOMALY" else "OK",
                "span_count":   len(spans),
                "user_id":      uid,
                "channel":      r.get("channel", ""),
            })
        return traces

    async def trace_detail(self, trace_id: str):
        """根据 trace_id 确定性还原 span 时间线"""
        seed = int(trace_id[:4], 16)

        # 用 seed 推算是什么 event_type（在已知模版中选择）
        evt_list = list(SPAN_TEMPLATES.keys())
        evt = evt_list[seed % len(evt_list)]
        spans_tpl = SPAN_TEMPLATES.get(evt, DEFAULT_SPANS)

        jitter = seed % 15
        spans = []
        for i, (svc, op, offset, dur) in enumerate(spans_tpl):
            j = (seed * (i + 3)) % 20 - 8
            spans.append({
                "span_id":        f"{trace_id[:8]}-{i:02d}",
                "parent_span_id": f"{trace_id[:8]}-00" if i > 0 else None,
                "service":        svc,
                "operation":      op,
                "offset_ms":      max(0, offset + j),
                "duration_ms":    max(1, dur + jitter),
                "status":         "ERROR" if (evt == "ANOMALY" and i == 4) else "OK",
                "db":             "doris" if "doris" in svc else None,
            })

        total_ms = max(s["offset_ms"] + s["duration_ms"] for s in spans)
        return {
            "trace_id":        trace_id,
            "operation":       evt,
            "total_duration_ms": total_ms,
            "spans":           spans,
            "services":        list(dict.fromkeys(s["service"] for s in spans)),
        }
