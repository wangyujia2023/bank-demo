import asyncio
from backend.log_store import log_store, db_call_store
from backend.doris.connect import execute_query, execute_one, execute_write


class ObserveService:

    async def logs(self, path=None, level=None, service=None, page=1, size=100):
        try:
            where, args = "WHERE 1=1", []
            if path:
                where += " AND path LIKE %s"; args.append(f"%{path}%")
            if level:
                where += " AND level = %s"; args.append(level.upper())
            if service:
                where += " AND service = %s"; args.append(service)
            offset = (page - 1) * size
            cnt = await execute_one(f"SELECT COUNT(*) AS cnt FROM sys_logs {where}", tuple(args) or None)
            total = cnt.get("cnt", 0) if cnt else 0
            rows = await execute_query(
                f"SELECT trace_id,log_time AS timestamp,level,service,method,path,"
                f"status_code,duration_ms,db_time_ms,message,log_tag "
                f"FROM sys_logs {where} ORDER BY log_time DESC LIMIT %s OFFSET %s",
                tuple(args + [size, offset])
            )
            if total > 0 or rows:
                return {"logs": rows, "total": total, "source": "doris"}
        except Exception:
            pass
        all_logs = log_store.get_all()
        if path: all_logs = [l for l in all_logs if path in l.get("path", "")]
        if level: all_logs = [l for l in all_logs if l.get("level") == level.upper()]
        if service: all_logs = [l for l in all_logs if l.get("service") == service]
        offset = (page - 1) * size
        return {"logs": all_logs[offset:offset+size], "total": len(all_logs), "source": "memory"}

    async def stats(self):
        try:
            summary, level_rows, svc_rows, top_paths = await asyncio.gather(
                execute_one("SELECT COUNT(*) AS total,"
                    "SUM(CASE WHEN level='ERROR' THEN 1 ELSE 0 END) AS errors,"
                    "SUM(CASE WHEN level='WARN' THEN 1 ELSE 0 END) AS warns,"
                    "SUM(CASE WHEN log_tag LIKE '%slow%' THEN 1 ELSE 0 END) AS slow,"
                    "ROUND(AVG(duration_ms),2) AS avg_duration_ms,"
                    "ROUND(AVG(db_time_ms),2) AS avg_db_ms FROM sys_logs"),
                execute_query("SELECT level, COUNT(*) AS cnt FROM sys_logs GROUP BY level ORDER BY cnt DESC"),
                execute_query("SELECT service, COUNT(*) AS cnt FROM sys_logs GROUP BY service ORDER BY cnt DESC"),
                execute_query("SELECT path, COUNT(*) AS count, ROUND(AVG(duration_ms),2) AS avg_duration "
                              "FROM sys_logs GROUP BY path ORDER BY count DESC LIMIT 10"),
            )
            if summary and summary.get("total", 0) > 0:
                return {"total": summary.get("total",0), "errors": summary.get("errors",0),
                        "warns": summary.get("warns",0), "slow": summary.get("slow",0),
                        "avg_duration_ms": summary.get("avg_duration_ms",0),
                        "avg_db_ms": summary.get("avg_db_ms",0),
                        "level_counts": level_rows or [], "svc_counts": svc_rows or [],
                        "top_paths": top_paths or [], "source": "doris"}
        except Exception:
            pass
        return {**log_store.get_stats(), "source": "memory"}

    async def traces(self, page=1, size=20):
        try:
            offset = (page - 1) * size
            rows = await execute_query(
                "SELECT trace_id,service,method,path,status_code,duration_ms,"
                "db_time_ms,level,log_time AS start_time "
                "FROM sys_logs ORDER BY log_time DESC LIMIT %s OFFSET %s", (size, offset)
            )
            if rows is not None:
                tids = [r["trace_id"] for r in rows]
                span_counts = {}
                if tids:
                    ph = ",".join(["%s"]*len(tids))
                    sc = await execute_query(
                        f"SELECT trace_id, COUNT(*) AS cnt FROM sys_spans "
                        f"WHERE trace_id IN ({ph}) GROUP BY trace_id", tuple(tids))
                    span_counts = {r["trace_id"]: r["cnt"] for r in sc}
                return [{
                    "trace_id": r.get("trace_id",""),
                    "operation": f"{r.get('method','')} {r.get('path','')}",
                    "service": r.get("service","CDP后台"),
                    "duration_ms": r.get("duration_ms",0),
                    "db_time_ms": r.get("db_time_ms",0),
                    "status": "ERROR" if r.get("level")=="ERROR" else "OK",
                    "span_count": span_counts.get(r.get("trace_id",""), 1),
                    "start_time": str(r.get("start_time","")),
                } for r in rows]
        except Exception:
            pass
        all_logs = log_store.get_all()
        db_counts = db_call_store.count_by_trace()
        offset = (page - 1) * size
        return [{"trace_id": r.get("trace_id",""),
                 "operation": f"{r.get('method','')} {r.get('path','')}",
                 "service": r.get("service","CDP后台"),
                 "duration_ms": r.get("duration_ms",0), "db_time_ms": r.get("db_time_ms",0),
                 "status": "ERROR" if r.get("level")=="ERROR" else "OK",
                 "span_count": 1+db_counts.get(r.get("trace_id",""),0),
                 "start_time": r.get("timestamp","")}
                for r in all_logs[offset:offset+size]]

    async def trace_detail(self, trace_id: str):
        try:
            spans = await execute_query(
                "SELECT span_id,parent_span_id,service,operation,"
                "offset_ms,duration_ms,status,db_query "
                "FROM sys_spans WHERE trace_id=%s ORDER BY offset_ms", (trace_id,)
            )
            if spans is not None:
                total_ms = max((s.get("duration_ms",0)+s.get("offset_ms",0) for s in spans), default=0)
                return {"trace_id": trace_id, "total_duration_ms": total_ms,
                        "services": list(dict.fromkeys(s["service"] for s in spans)),
                        "spans": [{"span_id": s.get("span_id",""),
                                   "parent_span_id": s.get("parent_span_id",""),
                                   "service": s.get("service",""),
                                   "operation": s.get("operation",""),
                                   "offset_ms": s.get("offset_ms",0),
                                   "duration_ms": s.get("duration_ms",0),
                                   "status": s.get("status","OK"),
                                   "db": bool(s.get("db_query","")),
                                   "detail": s.get("db_query","")[:200]} for s in spans]}
        except Exception:
            pass
        svc_rows = log_store.get_by_trace(trace_id)
        db_rows = sorted(db_call_store.get_by_trace(trace_id), key=lambda x: x.get("offset_ms",0))
        spans = []
        total_ms = 0
        for r in svc_rows:
            dur = r.get("duration_ms",0); total_ms = max(total_ms, dur)
            spans.append({"span_id": r.get("span_id",""), "parent_span_id": "",
                          "service": r.get("service","CDP后台"),
                          "operation": f"{r.get('method','')} {r.get('path','')}",
                          "offset_ms": 0, "duration_ms": dur,
                          "status": "ERROR" if r.get("status_code",0)>=400 else "OK",
                          "db": False, "detail": ""})
        for r in db_rows:
            spans.append({"span_id": r.get("span_id",""), "parent_span_id": "",
                          "service": "Doris", "operation": r.get("operation",""),
                          "offset_ms": r.get("offset_ms",0), "duration_ms": r.get("duration_ms",0),
                          "status": r.get("status","OK"), "db": True,
                          "detail": r.get("db_query","")})
        return {"trace_id": trace_id, "total_duration_ms": total_ms,
                "services": list(dict.fromkeys(s["service"] for s in spans)), "spans": spans}

    async def classify_logs(self):
        """用 AI_CLASSIFY(qwen_llm) 对未打标签的 sys_logs.message 打标签，写回 log_tag"""
        try:
            sql = """UPDATE sys_logs
                SET log_tag = AI_CLASSIFY('qwen_llm', message,
                    ARRAY('慢请求','服务异常','正常请求','数据库超时','认证失败','业务错误','高频访问','安全告警'))
                WHERE log_tag IS NULL OR log_tag = ''"""
            affected = await execute_write(sql)
            return {"status": "success", "updated": affected}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def tag_stats(self):
        """对 sys_logs.log_tag 做统计分析"""
        try:
            tag_dist, level_tag, svc_tag, perf_tag = await asyncio.gather(
                execute_query(
                    "SELECT log_tag, COUNT(*) AS cnt,"
                    "ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS pct "
                    "FROM sys_logs WHERE log_tag IS NOT NULL AND log_tag!='' "
                    "GROUP BY log_tag ORDER BY cnt DESC"),
                execute_query(
                    "SELECT log_tag, level, COUNT(*) AS cnt "
                    "FROM sys_logs WHERE log_tag IS NOT NULL AND log_tag!='' "
                    "GROUP BY log_tag, level ORDER BY log_tag, cnt DESC"),
                execute_query(
                    "SELECT log_tag, service, COUNT(*) AS cnt "
                    "FROM sys_logs WHERE log_tag IS NOT NULL AND log_tag!='' "
                    "GROUP BY log_tag, service ORDER BY log_tag, cnt DESC"),
                execute_query(
                    "SELECT log_tag,ROUND(AVG(duration_ms),2) AS avg_ms,"
                    "MAX(duration_ms) AS max_ms, COUNT(*) AS cnt "
                    "FROM sys_logs WHERE log_tag IS NOT NULL AND log_tag!='' "
                    "GROUP BY log_tag ORDER BY avg_ms DESC"),
            )
            total_tagged = await execute_one(
                "SELECT COUNT(*) AS tagged, "
                "(SELECT COUNT(*) FROM sys_logs) AS total FROM sys_logs "
                "WHERE log_tag IS NOT NULL AND log_tag!=''")
            return {
                "tag_distribution": tag_dist,
                "level_by_tag": level_tag,
                "svc_by_tag": svc_tag,
                "perf_by_tag": perf_tag,
                "tagged": total_tagged.get("tagged", 0) if total_tagged else 0,
                "total": total_tagged.get("total", 0) if total_tagged else 0,
            }
        except Exception as e:
            return {"tag_distribution": [], "level_by_tag": [], "svc_by_tag": [], "perf_by_tag": [], "tagged": 0, "total": 0}

    async def query_logs(self, search=None, level=None, service=None, page=1, size=50):
        return await self.logs(path=search, level=level, service=service, page=page, size=size)

    async def log_stats(self):
        return await self.stats()

    async def trace_list(self, service=None, status=None, page=1, size=20):
        return await self.traces(page=page, size=size)
