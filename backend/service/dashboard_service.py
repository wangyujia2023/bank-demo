"""大盘统计服务"""
import asyncio
from backend.doris.connect import execute_query, execute_one
from typing import Dict


class DashboardService:
    async def get_overview(self) -> Dict:
        # 并行执行三个独立的查询组，而不是五个串行查询
        user_data, log_data, segment_data = await asyncio.gather(
            self._get_user_data(),
            self._get_log_data(),
            self._get_segment_data()
        )

        return {
            "user_stat": user_data["stat"],
            "log_stat": log_data["stat"],
            "segment_stat": segment_data,
            "asset_level_dist": user_data["dist"],
            "log_trend": log_data["trend"],
        }

    async def _get_user_data(self) -> Dict:
        """合并查询：用户统计 + 资产等级分布"""
        results = await execute_query(
            """
            SELECT 'stat' AS type,
                   COUNT(1) AS total_users,
                   SUM(IF(active_level='高活', 1, 0)) AS active_users,
                   SUM(IF(anomaly_flag=1, 1, 0)) AS anomaly_users,
                   ROUND(AVG(aum_total), 2) AS avg_aum,
                   SUM(aum_total) AS total_aum,
                   NULL AS label, NULL AS value
            FROM user_wide
            UNION ALL
            SELECT 'dist' AS type, NULL, NULL, NULL, NULL, NULL,
                   asset_level AS label, COUNT(*) AS value
            FROM user_wide
            GROUP BY asset_level ORDER BY value DESC LIMIT 6
            """
        )

        stat_data = {}
        dist_data = []
        for row in results:
            if row.get('type') == 'stat':
                stat_data = {k: v for k, v in row.items() if k != 'type' and v is not None}
            else:
                dist_data.append({'label': row['label'], 'value': row['value']})

        return {"stat": stat_data or {}, "dist": dist_data}

    async def _get_log_data(self) -> Dict:
        """合并查询：日志统计 + 日志趋势"""
        results = await execute_query(
            """
            SELECT 'stat' AS type,
                   COUNT(*) AS total_logs,
                   COUNT(IF(risk_level='高风险', 1, NULL)) AS high_risk_logs,
                   COUNT(DISTINCT user_id) AS log_users,
                   COUNT(IF(anomaly_tag != '正常', 1, NULL)) AS anomaly_logs,
                   NULL AS date, NULL AS log_count, NULL AS risk_count
            FROM user_log_tag WHERE log_date = CURDATE()
            UNION ALL
            SELECT 'trend' AS type, NULL, NULL, NULL, NULL,
                   log_date AS date, COUNT(*) AS log_count,
                   COUNT(IF(risk_level='高风险', 1, NULL)) AS risk_count
            FROM user_log_tag
            WHERE log_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
            GROUP BY log_date ORDER BY log_date
            """
        )

        stat_data = {}
        trend_data = []
        for row in results:
            if row.get('type') == 'stat':
                stat_data = {k: v for k, v in row.items() if k != 'type' and v is not None}
            else:
                trend_data.append({'date': row['date'], 'log_count': row['log_count'], 'risk_count': row['risk_count']})

        return {"stat": stat_data or {}, "trend": trend_data}

    async def _get_segment_data(self) -> Dict:
        """人群分段统计"""
        result = await execute_one(
            "SELECT COUNT(*) AS total_segments, SUM(user_count) AS total_crowd FROM user_segment WHERE status=1"
        )
        return result or {}
