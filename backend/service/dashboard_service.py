"""大盘统计服务"""
from backend.doris.connect import execute_query, execute_one
from typing import Dict


class DashboardService:
    async def get_overview(self) -> Dict:
        user_stat = await execute_one(
            """
            SELECT
                COUNT(1)                               AS total_users,
                SUM(IF(active_level='高活', 1, 0))     AS active_users,
                SUM(IF(anomaly_flag=1, 1, 0))          AS anomaly_users,
                ROUND(AVG(aum_total), 2)               AS avg_aum,
                SUM(aum_total)                         AS total_aum
            FROM user_wide WHERE 1=1
            """
        ) or {}

        log_stat = await execute_one(
            """
            SELECT
                COUNT(*) AS total_logs,
                COUNT(IF(risk_level='高风险', 1, NULL)) AS high_risk_logs,
                COUNT(DISTINCT user_id) AS log_users,
                COUNT(IF(anomaly_tag != '正常', 1, NULL)) AS anomaly_logs
            FROM user_log_tag WHERE log_date = CURDATE()
            """
        ) or {}

        segment_stat = await execute_one(
            "SELECT COUNT(*) AS total_segments, SUM(user_count) AS total_crowd FROM user_segment WHERE status=1"
        ) or {}

        tag_dist = await execute_query(
            """
            SELECT asset_level AS label, COUNT(*) AS value
            FROM user_wide WHERE 1=1
            GROUP BY asset_level ORDER BY value DESC LIMIT 6
            """
        )

        trend = await execute_query(
            """
            SELECT log_date AS date, COUNT(*) AS log_count,
                   COUNT(IF(risk_level='高风险',1,NULL)) AS risk_count
            FROM user_log_tag
            WHERE log_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
            GROUP BY log_date ORDER BY log_date
            """
        )

        return {
            "user_stat": user_stat,
            "log_stat": log_stat,
            "segment_stat": segment_stat,
            "asset_level_dist": tag_dist,
            "log_trend": trend,
        }
