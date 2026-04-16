"""大盘统计服务"""
import asyncio
from backend.doris.connect import execute_query, execute_one
from typing import Dict


class DashboardService:
    async def get_overview(self) -> Dict:
        user_stat, tag_dist, log_stat, segment_stat, trend = await asyncio.gather(
            # 用户统计 - 来自user_wide
            execute_one("""
                SELECT
                    COUNT(1)                               AS total_users,
                    SUM(IF(active_level='高活', 1, 0))     AS active_users,
                    SUM(IF(anomaly_flag=1, 1, 0))          AS anomaly_users,
                    ROUND(AVG(aum_total), 2)               AS avg_aum,
                    SUM(aum_total)                         AS total_aum
                FROM user_wide WHERE 1=1
            """),
            # 资产等级分布 - 来自user_wide
            execute_query("""
                SELECT asset_level AS label, COUNT(*) AS value
                FROM user_wide WHERE 1=1
                GROUP BY asset_level ORDER BY value DESC LIMIT 6
            """),
            # 日志统计 - 来自user_behavior
            execute_one("""
                SELECT
                    COUNT(*) AS total_logs,
                    COUNT(IF(event_type IN ('PAYMENT','TRANSFER','WITHDRAW'), 1, NULL)) AS high_risk_logs,
                    COUNT(DISTINCT user_id) AS log_users,
                    COUNT(IF(event_type='ANOMALY', 1, NULL)) AS anomaly_logs
                FROM user_behavior WHERE event_date = CURDATE()
            """),
            # 人群分段 - 来自user_segment
            execute_one("""
                SELECT COUNT(*) AS total_segments, SUM(user_count) AS total_crowd
                FROM user_segment WHERE status=1
            """),
            # 日志趋势 - 来自user_behavior
            execute_query("""
                SELECT event_date AS date, COUNT(*) AS log_count,
                       COUNT(IF(event_type='ANOMALY',1,NULL)) AS risk_count
                FROM user_behavior
                WHERE event_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                GROUP BY event_date ORDER BY event_date
            """)
        )

        return {
            "user_stat": user_stat or {},
            "log_stat": log_stat or {},
            "segment_stat": segment_stat or {},
            "asset_level_dist": tag_dist,
            "log_trend": trend,
        }
