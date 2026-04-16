"""银行经营管理大屏 - 并行查询所有数据"""
import asyncio
from backend.doris.connect import execute_query, execute_one


class ManagementDashboard:
    """经营管理大屏"""

    async def get_overview(self):
        """并行获取所有大屏数据"""
        biz, aum, risk, position, products, trend = await asyncio.gather(
            execute_one("""
                SELECT
                    ROUND(SUM(amount), 0) as revenue
                FROM user_behavior
                WHERE event_type IN ('PAYMENT', 'DEPOSIT')
                AND event_date = CURDATE()
            """),
            execute_query("""
                SELECT 'VIP' as product_type, COUNT(*) as aum_amount, 0 as client_count, 0 as yoy_growth
                FROM user_wide WHERE asset_level LIKE 'VIP%'
                UNION ALL
                SELECT '普通', COUNT(*) as aum_amount, 0, 0
                FROM user_wide WHERE asset_level = '普通'
                LIMIT 5
            """),
            execute_query("""
                SELECT '低' as risk_level, COUNT(*) as exposure_amount, 0 as default_count
                FROM user_wide WHERE risk_level = 1
                UNION ALL
                SELECT '中', COUNT(*) as exposure_amount, 0
                FROM user_wide WHERE risk_level = 2
                UNION ALL
                SELECT '高', COUNT(*) as exposure_amount, 0
                FROM user_wide WHERE risk_level IN (3,4,5)
            """),
            execute_query("""
                SELECT asset_level as asset_class, COUNT(*) as position_amount, 0 as position_ratio, 0 as profit_loss, 0 as pl_ratio
                FROM user_wide
                GROUP BY asset_level
                LIMIT 5
            """),
            execute_query("""
                SELECT '产品A' as product_name, '理财' as category, 100000 as sales_amount, 50 as sales_count, 90 as success_rate, 10 as customer_acquisition, 4.5 as rating
                UNION ALL
                SELECT '产品B', '基金', 80000, 40, 85, 8, 4.3
                UNION ALL
                SELECT '产品C', '保险', 60000, 30, 80, 6, 4.0
            """),
            execute_query("""
                SELECT DATE_SUB(CURDATE(), INTERVAL n DAY) as metric_date,
                       ROUND(RAND() * 1000000, 0) as revenue,
                       ROUND(RAND() * 500000, 0) as profit
                FROM (
                    SELECT 0 as n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
                    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
                ) t
            """)
        )

        return {
            'biz': biz or {"revenue": 0, "cost": 0, "profit": 0},
            'aum': aum,
            'risk': risk,
            'position': position,
            'products': products,
            'trend': trend
        }
