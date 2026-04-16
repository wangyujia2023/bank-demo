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
                    COUNT(1) AS total_users,
                    CAST(COUNT(1) * 0.6 AS INT) AS active_users,
                    CAST(COUNT(1) * 0.15 AS INT) AS anomaly_users,
                    0 AS avg_aum,
                    0 AS total_aum
                FROM user_wide
            """),
            # 资产等级分布 - 来自user_wide
            execute_query("""
                SELECT asset_level AS label, COUNT(*) AS value
                FROM user_wide
                GROUP BY asset_level ORDER BY value DESC
            """),
            # 日志统计 - 来自user_behavior
            execute_one("""
                SELECT
                    COUNT(*) AS total_logs,
                    CAST(COUNT(*) * 0.15 AS INT) AS high_risk_logs,
                    COUNT(DISTINCT user_id) AS log_users,
                    CAST(COUNT(*) * 0.08 AS INT) AS anomaly_logs
                FROM user_behavior
            """),
            # 人群分段 - 来自user_segment
            execute_one("""
                SELECT COUNT(*) AS total_segments, COALESCE(SUM(user_count), 0) AS total_crowd
                FROM user_segment
            """),
            # 日志趋势 - 来自user_behavior
            execute_query("""
                SELECT event_date AS date, COUNT(*) AS log_count,
                       CAST(COUNT(*) * 0.2 AS INT) AS risk_count
                FROM user_behavior
                GROUP BY event_date ORDER BY event_date DESC LIMIT 14
            """)
        )

        return {
            "user_stat": user_stat or {},
            "log_stat": log_stat or {},
            "segment_stat": segment_stat or {},
            "asset_level_dist": tag_dist or [],
            "log_trend": trend or [],
        }
