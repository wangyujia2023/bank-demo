"""银行报表服务"""
from backend.doris.connect import execute_query, execute_one


class ReportService:

    async def business_overview(self):
        stats = await execute_one("""
            SELECT
                COUNT(*)                                  AS total_users,
                COUNT(DISTINCT city)                      AS city_count,
                ROUND(SUM(aum_total), 2)                  AS total_aum,
                ROUND(AVG(aum_total), 2)                  AS avg_aum,
                SUM(anomaly_flag)                         AS anomaly_users,
                ROUND(AVG(churn_prob) * 100, 2)           AS avg_churn_pct,
                SUM(CASE WHEN lifecycle_stage='新客期' THEN 1 ELSE 0 END) AS new_users
            FROM user_wide
        """)
        asset_dist = await execute_query("""
            SELECT asset_level,
                   COUNT(*) AS user_cnt,
                   ROUND(SUM(aum_total), 0) AS total_aum,
                   ROUND(AVG(aum_total), 1) AS avg_aum
            FROM user_wide
            GROUP BY asset_level
            ORDER BY total_aum DESC
        """)
        lifecycle_dist = await execute_query("""
            SELECT lifecycle_stage, COUNT(*) AS cnt
            FROM user_wide GROUP BY lifecycle_stage ORDER BY cnt DESC
        """)
        city_dist = await execute_query("""
            SELECT city, COUNT(*) AS user_cnt,
                   ROUND(SUM(aum_total), 0) AS total_aum,
                   SUM(anomaly_flag) AS anomaly
            FROM user_wide
            GROUP BY city ORDER BY total_aum DESC LIMIT 10
        """)
        active_dist = await execute_query("""
            SELECT active_level, COUNT(*) AS cnt FROM user_wide
            GROUP BY active_level ORDER BY cnt DESC
        """)
        for k, v in (stats or {}).items():
            stats[k] = round(float(v or 0), 2)
        for row in asset_dist:
            for k, v in row.items():
                row[k] = float(v or 0) if k != 'asset_level' else v
        return {
            "stats": stats or {},
            "asset_dist": asset_dist,
            "lifecycle_dist": lifecycle_dist,
            "city_dist": city_dist,
            "active_dist": active_dist,
        }

    async def transaction_report(self):
        channel_stats = await execute_query("""
            SELECT channel,
                   COUNT(*) AS tx_count,
                   ROUND(SUM(amount), 2) AS total_amount,
                   ROUND(AVG(amount), 2) AS avg_amount,
                   COUNT(DISTINCT user_id) AS user_count
            FROM user_behavior
            WHERE event_type IN ('PAYMENT','TRANSFER','WITHDRAW','DEPOSIT')
            GROUP BY channel ORDER BY total_amount DESC
        """)
        event_dist = await execute_query("""
            SELECT event_type, COUNT(*) AS cnt,
                   ROUND(SUM(amount), 2) AS total_amount
            FROM user_behavior WHERE amount > 0
            GROUP BY event_type ORDER BY cnt DESC
        """)
        hourly_trend = await execute_query("""
            SELECT HOUR(event_time) AS hour_val, COUNT(*) AS cnt
            FROM user_behavior
            GROUP BY HOUR(event_time) ORDER BY hour_val
        """)
        for row in channel_stats:
            for k, v in row.items():
                row[k] = float(v or 0) if k != 'channel' else v
        for row in event_dist:
            row['cnt'] = int(row.get('cnt') or 0)
            row['total_amount'] = float(row.get('total_amount') or 0)
        return {
            "channel_stats": channel_stats,
            "event_dist": event_dist,
            "hourly_trend": hourly_trend,
        }

    async def risk_report(self):
        risk_stats = await execute_one("""
            SELECT
                SUM(anomaly_flag) AS anomaly_count,
                COUNT(*) AS total,
                ROUND(AVG(churn_prob)*100, 2) AS avg_churn_pct,
                ROUND(SUM(CASE WHEN anomaly_flag=1 THEN aum_total ELSE 0 END), 0) AS anomaly_aum
            FROM user_wide
        """)
        risk_by_asset = await execute_query("""
            SELECT asset_level,
                   COUNT(*) AS total,
                   SUM(anomaly_flag) AS anomaly,
                   ROUND(SUM(anomaly_flag)*100.0/COUNT(*), 1) AS anomaly_rate,
                   ROUND(AVG(churn_prob)*100, 1) AS avg_churn
            FROM user_wide GROUP BY asset_level ORDER BY anomaly_rate DESC
        """)
        risk_by_city = await execute_query("""
            SELECT city,
                   SUM(anomaly_flag) AS anomaly_count,
                   COUNT(*) AS total,
                   ROUND(SUM(anomaly_flag)*100.0/COUNT(*), 1) AS rate
            FROM user_wide GROUP BY city HAVING anomaly_count > 0
            ORDER BY anomaly_count DESC LIMIT 10
        """)
        credit_dist = await execute_query("""
            SELECT credit_grade, COUNT(*) AS cnt,
                   ROUND(AVG(credit_score), 1) AS avg_score
            FROM user_wide WHERE credit_grade IS NOT NULL
            GROUP BY credit_grade ORDER BY avg_score DESC
        """)
        for k, v in (risk_stats or {}).items():
            risk_stats[k] = float(v or 0)
        for row in risk_by_asset:
            for k, v in row.items():
                row[k] = float(v or 0) if k != 'asset_level' else v
        return {
            "risk_stats": risk_stats or {},
            "risk_by_asset": risk_by_asset,
            "risk_by_city": risk_by_city,
            "credit_dist": credit_dist,
        }
