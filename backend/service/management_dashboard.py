"""银行经营管理大屏 - 一次查询解决所有数据"""
import asyncio
from backend.doris.connect import execute_query, execute_one


class ManagementDashboard:
    """经营管理大屏 - 极简高效"""

    async def get_overview(self):
        """并行获取所有数据"""
        biz, aum, risk, position, products, trend = await asyncio.gather(
            execute_one("""
                SELECT
                  SUM(IF(metric_type='收入', amount, 0)) as revenue,
                  SUM(IF(metric_type='成本', amount, 0)) as cost,
                  SUM(IF(metric_type='利润', amount, 0)) as profit
                FROM biz_metrics WHERE metric_date = (SELECT MAX(metric_date) FROM biz_metrics)
            """),
            execute_query("""
                SELECT product_type, aum_amount, client_count, inflow, yoy_growth
                FROM aum_metrics WHERE metric_date = (SELECT MAX(metric_date) FROM aum_metrics)
                ORDER BY aum_amount DESC
            """),
            execute_query("""
                SELECT risk_level, exposure_amount, default_count, overdue_amount
                FROM risk_metrics WHERE metric_date = (SELECT MAX(metric_date) FROM risk_metrics)
                ORDER BY FIELD(risk_level, '低', '中', '高', '极高')
            """),
            execute_query("""
                SELECT asset_class, position_amount, position_ratio, profit_loss, pl_ratio
                FROM position_metrics WHERE metric_date = (SELECT MAX(metric_date) FROM position_metrics)
                ORDER BY position_amount DESC
            """),
            execute_query("""
                SELECT product_name, category, sales_amount, sales_count, success_rate, customer_acquisition, rating
                FROM product_marketing WHERE metric_date = (SELECT MAX(metric_date) FROM product_marketing)
                ORDER BY sales_amount DESC
            """),
            execute_query("""
                SELECT metric_date,
                  SUM(IF(metric_type='收入', amount, 0)) as revenue,
                  SUM(IF(metric_type='利润', amount, 0)) as profit
                FROM biz_metrics
                WHERE metric_date >= DATE_SUB((SELECT MAX(metric_date) FROM biz_metrics), INTERVAL 7 DAY)
                GROUP BY metric_date ORDER BY metric_date
            """)
        )

        return {
            'biz': biz or {},
            'aum': aum,
            'risk': risk,
            'position': position,
            'products': products,
            'trend': trend
        }
