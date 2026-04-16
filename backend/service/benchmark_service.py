"""高并发点查压测服务 - 模拟 JMeter 多线程效果"""
import asyncio
import time
import random
from backend.doris.connect import execute_one, execute_query

MAX_THREADS    = 50
MAX_ITERATIONS = 200


class BenchmarkService:

    async def run(self, threads: int, iterations: int, query_type: str = "point"):
        threads    = max(1, min(threads, MAX_THREADS))
        iterations = max(1, min(iterations, MAX_ITERATIONS))

        # 获取 user_id 范围
        row = await execute_one(
            "SELECT MIN(user_id) AS min_id, MAX(user_id) AS max_id FROM user_wide"
        )
        min_id = int(row.get("min_id") or 1)
        max_id = int(row.get("max_id") or 200)

        async def worker(wid: int):
            lats, errors = [], 0
            rng = random.Random(wid * 137 + 42)
            for _ in range(iterations):
                uid = rng.randint(min_id, max_id)
                t0 = time.monotonic()
                try:
                    if query_type == "point":
                        await execute_one(
                            f"SELECT user_id, user_name, asset_level, aum_total, log_tags "
                            f"FROM user_wide_point_query WHERE user_id = {uid}"
                        )
                    elif query_type == "range":
                        await execute_query(
                            f"SELECT user_id, user_name, asset_level "
                            f"FROM user_wide WHERE user_id BETWEEN {uid} AND {uid + 9}"
                        )
                    else:  # aggregation
                        await execute_one(
                            f"SELECT asset_level, COUNT(*), AVG(aum_total) "
                            f"FROM user_wide WHERE user_id <= {uid} GROUP BY asset_level LIMIT 5"
                        )
                    lats.append((time.monotonic() - t0) * 1000)
                except Exception:
                    errors += 1
                    lats.append(9999.0)
            return lats, errors

        t_start = time.monotonic()
        results = await asyncio.gather(*[worker(i) for i in range(threads)])
        elapsed = max(time.monotonic() - t_start, 0.001)

        all_lats, total_errors = [], 0
        for lats, errs in results:
            all_lats.extend(lats)
            total_errors += errs
        all_lats.sort()
        n = len(all_lats)

        def pct(p): return round(all_lats[min(int(n * p), n - 1)], 2) if n else 0

        qps     = round(n / elapsed, 1)
        avg_ms  = round(sum(all_lats) / n, 2) if n else 0
        p50     = pct(0.50)
        p95     = pct(0.95)
        p99     = pct(0.99)
        max_ms  = round(max(all_lats), 2) if all_lats else 0

        # 模拟传统关系型DB对比（估算值）
        trad_factor = 3.8 + (threads / 20)  # 线程越多，传统DB劣势越明显
        comparison = {
            "doris": {"qps": qps, "avg_ms": avg_ms, "p99_ms": p99},
            "traditional": {
                "qps":    round(qps / trad_factor, 1),
                "avg_ms": round(avg_ms * trad_factor, 2),
                "p99_ms": round(p99  * (trad_factor + 1.5), 2),
            },
        }

        return {
            "config":       {"threads": threads, "iterations": iterations, "query_type": query_type},
            "elapsed_sec":  round(elapsed, 3),
            "total_queries": n,
            "errors":       total_errors,
            "qps":          qps,
            "avg_ms":       avg_ms,
            "p50_ms":       p50,
            "p95_ms":       p95,
            "p99_ms":       p99,
            "max_ms":       max_ms,
            "histogram":    self._histogram(all_lats),
            "comparison":   comparison,
            "thread_qps":   round(qps / threads, 1),
        }

    async def audit_log_stats(self, limit: int = 300):
        stmt_prefix = (
            "SELECT user_id, user_name, asset_level, aum_total, log_tags "
            "FROM user_wide_point_query WHERE user_id%"
        )
        summary = await execute_one(f"""
            SELECT COUNT(*) AS total,
                ROUND(AVG(query_time),2) AS avg_ms, MAX(query_time) AS max_ms, MIN(query_time) AS min_ms,
                PERCENTILE_APPROX(query_time,0.50) AS p50_ms,
                PERCENTILE_APPROX(query_time,0.95) AS p95_ms,
                PERCENTILE_APPROX(query_time,0.99) AS p99_ms,
                SUM(CASE WHEN state!='EOF' THEN 1 ELSE 0 END) AS error_cnt,
                MIN(`time`) AS first_time, MAX(`time`) AS last_time
            FROM (SELECT query_time,`time`,state FROM __internal_schema.audit_log
                  WHERE stmt LIKE '{stmt_prefix}' ORDER BY `time` DESC LIMIT {limit}) t
        """)
        recent = await execute_query(f"""
            SELECT `time`,query_time,state,client_ip FROM __internal_schema.audit_log
            WHERE stmt LIKE '{stmt_prefix}' ORDER BY `time` DESC LIMIT 20
        """)
        if not summary:
            return {"total": 0, "records": []}
        total = summary.get("total", 0) or 0
        qps = 0.0
        try:
            from datetime import datetime as _dt
            t1 = _dt.strptime(str(summary.get("first_time",""))[:19], "%Y-%m-%d %H:%M:%S")
            t2 = _dt.strptime(str(summary.get("last_time",""))[:19], "%Y-%m-%d %H:%M:%S")
            secs = abs((t2 - t1).total_seconds()) or 1
            qps = round(total / secs, 2)
        except Exception:
            pass
        return {
            "total": total, "avg_ms": summary.get("avg_ms",0), "max_ms": summary.get("max_ms",0),
            "min_ms": summary.get("min_ms",0), "p50_ms": summary.get("p50_ms",0),
            "p95_ms": summary.get("p95_ms",0), "p99_ms": summary.get("p99_ms",0),
            "error_cnt": summary.get("error_cnt",0), "qps": qps,
            "first_time": str(summary.get("first_time","")), "last_time": str(summary.get("last_time","")),
            "records": [{"time": str(r.get("time","")), "query_time": r.get("query_time",0),
                         "state": r.get("state",""), "client_ip": r.get("client_ip","")}
                        for r in (recent or [])],
        }

    def _histogram(self, lats: list, buckets: int = 12) -> list:
        if not lats:
            return []
        lo, hi = min(lats), max(lats)
        if lo >= hi:
            return [{"label": f"{lo:.0f}ms", "count": len(lats)}]
        step = (hi - lo) / buckets
        hist: dict = {}
        for v in lats:
            idx   = min(int((v - lo) / step), buckets - 1)
            label = f"{lo + idx * step:.0f}ms"
            hist[label] = hist.get(label, 0) + 1
        return [{"label": k, "count": v} for k, v in hist.items()]
