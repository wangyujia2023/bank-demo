"""
用户行为分析服务
- 漏斗分析（window_funnel）
- 留存分析（7/30/90天）
- 交易频次/金额分析
- 依托 Doris 4.0 HASP 内置分析函数
- 注：使用模拟数据，不按日期过滤，全量分析
"""
import logging
from typing import Dict, List, Optional
from backend.doris.connect import execute_query, execute_one

logger = logging.getLogger(__name__)


class BehaviorService:

    # ─── 漏斗分析 ────────────────────────────────────────────────
    async def funnel_analysis(
        self,
        steps: Optional[List[str]] = None,
        window_seconds: int = 86400,
        channel: Optional[str] = None,
        segment_id: Optional[int] = None,
    ) -> Dict:
        if steps is None:
            steps = ["REGISTER", "LOGIN", "BROWSE_PRODUCT", "APPLY", "TRANSACTION"]

        step_conditions = ", ".join([f"event_type = '{s}'" for s in steps])
        channel_filter = f"AND channel = '{channel}'" if channel else ""
        segment_filter = ""
        if segment_id:
            segment_filter = f"""
                AND user_id IN (
                    SELECT * FROM BITMAP_TO_ARRAY(
                        (SELECT segment_bitmap FROM user_segment WHERE segment_id = {segment_id} LIMIT 1)
                    )
                )
            """

        sql = f"""
            WITH funnel_raw AS (
                SELECT
                    user_id,
                    window_funnel(
                        {window_seconds},
                        'default',
                        event_time,
                        {step_conditions}
                    ) AS funnel_level
                FROM user_behavior
                WHERE 1=1
                  {channel_filter}
                  {segment_filter}
                GROUP BY user_id
            )
            SELECT funnel_level AS step, COUNT(1) AS user_count
            FROM funnel_raw
            WHERE funnel_level > 0
            GROUP BY funnel_level
            ORDER BY funnel_level
        """
        rows = await execute_query(sql)

        step_map = {int(r["step"]): int(r["user_count"]) for r in rows}
        result_steps = []
        for i, name in enumerate(steps, 1):
            cnt = step_map.get(i, 0)
            prev_cnt = result_steps[-1]["user_count"] if result_steps else cnt
            base_cnt = result_steps[0]["user_count"] if result_steps else cnt
            result_steps.append({
                "step": i,
                "step_name": name,
                "user_count": cnt,
                "conversion_rate": round(cnt * 100.0 / prev_cnt, 2) if prev_cnt else 0,
                "overall_rate": round(cnt * 100.0 / base_cnt, 2) if base_cnt else 0,
            })

        return {
            "steps": result_steps,
            "total_users": result_steps[0]["user_count"] if result_steps else 0,
            "completed_users": result_steps[-1]["user_count"] if result_steps else 0,
        }

    # ─── 留存分析 ────────────────────────────────────────────────
    async def retention_analysis(
        self,
        cohort_event: str = "REGISTER",
        return_event: str = "LOGIN",
        retention_days: Optional[List[int]] = None,
    ) -> Dict:
        if retention_days is None:
            retention_days = [1, 3, 7, 14, 30]

        day_list = ", ".join(str(d) for d in retention_days)
        sql = f"""
            WITH cohort AS (
                SELECT user_id, MIN(event_date) AS cohort_date
                FROM user_behavior
                WHERE event_type = '{cohort_event}'
                GROUP BY user_id
            ),
            retention_raw AS (
                SELECT
                    c.cohort_date,
                    DATEDIFF(b.event_date, c.cohort_date) AS day_diff,
                    COUNT(DISTINCT b.user_id) AS retained_users,
                    COUNT(DISTINCT c.user_id) AS cohort_size
                FROM cohort c
                LEFT JOIN user_behavior b
                    ON c.user_id = b.user_id
                    AND b.event_type = '{return_event}'
                    AND DATEDIFF(b.event_date, c.cohort_date) IN ({day_list})
                GROUP BY c.cohort_date, day_diff
            )
            SELECT
                cohort_date, day_diff, cohort_size, retained_users,
                ROUND(retained_users * 100.0 / NULLIF(cohort_size, 0), 2) AS retention_rate
            FROM retention_raw
            WHERE day_diff IS NOT NULL
            ORDER BY cohort_date, day_diff
        """
        rows = await execute_query(sql)

        matrix: Dict[str, Dict[int, float]] = {}
        cohort_sizes: Dict[str, int] = {}
        for r in rows:
            cd = str(r["cohort_date"])
            dd = int(r["day_diff"]) if r["day_diff"] is not None else 0
            if cd not in matrix:
                matrix[cd] = {}
                cohort_sizes[cd] = int(r["cohort_size"])
            matrix[cd][dd] = float(r["retention_rate"] or 0)

        result_rows = []
        for cohort_date in sorted(matrix.keys()):
            row = {"cohort_date": cohort_date, "cohort_size": cohort_sizes.get(cohort_date, 0)}
            for d in retention_days:
                row[f"d{d}"] = matrix[cohort_date].get(d, 0)
            result_rows.append(row)

        return {"retention_days": retention_days, "rows": result_rows}

    # ─── 交易频次/金额分析 ────────────────────────────────────────
    async def transaction_analysis(self, channel: Optional[str] = None) -> Dict:
        channel_filter = f"AND channel = '{channel}'" if channel else ""
        sql = f"""
            SELECT
                event_date,
                COUNT(DISTINCT user_id)                          AS dau,
                COUNT(IF(event_type='TRANSACTION', 1, NULL))     AS tx_count,
                ROUND(SUM(IF(event_type='TRANSACTION', amount, 0)) / 10000, 2) AS tx_amount_wan
            FROM user_behavior
            WHERE 1=1 {channel_filter}
            GROUP BY event_date
            ORDER BY event_date
        """
        rows = await execute_query(sql)
        return {"rows": rows}

    # ─── RFM 分析 ────────────────────────────────────────────────
    async def rfm_analysis(self) -> List[Dict]:
        sql = """
            WITH rfm_raw AS (
                SELECT
                    user_id,
                    DATEDIFF(MAX(event_date), MIN(event_date))   AS recency_days,
                    COUNT(IF(event_type='TRANSACTION', 1, NULL)) AS frequency,
                    SUM(IF(event_type='TRANSACTION', amount, 0)) AS monetary
                FROM user_behavior
                GROUP BY user_id
            ),
            rfm_score AS (
                SELECT *,
                    NTILE(3) OVER (ORDER BY recency_days DESC)  AS r_score,
                    NTILE(3) OVER (ORDER BY frequency)          AS f_score,
                    NTILE(3) OVER (ORDER BY monetary)           AS m_score
                FROM rfm_raw
            )
            SELECT
                CASE
                    WHEN r_score=3 AND f_score=3 AND m_score=3 THEN '重要价值客户'
                    WHEN r_score=3 AND m_score=3               THEN '重要发展客户'
                    WHEN f_score=3 AND m_score=3               THEN '重要保持客户'
                    WHEN m_score=3                             THEN '重要挽留客户'
                    WHEN r_score=3 AND f_score=3               THEN '一般价值客户'
                    WHEN r_score=3                             THEN '一般发展客户'
                    ELSE                                            '一般挽留客户'
                END AS rfm_segment,
                COUNT(1) AS user_count,
                ROUND(AVG(recency_days), 1) AS avg_recency,
                ROUND(AVG(frequency), 1) AS avg_frequency,
                ROUND(AVG(monetary), 2) AS avg_monetary
            FROM rfm_score
            GROUP BY rfm_segment
            ORDER BY user_count DESC
        """
        return await execute_query(sql)
