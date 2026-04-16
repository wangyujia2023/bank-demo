"""银行经营管理大屏 - 演示数据"""


class ManagementDashboard:
    """经营管理大屏"""

    async def get_overview(self):
        """获取经营管理大屏数据"""
        return {
            'biz': {
                'revenue': 1250000,
                'cost': 620000,
                'profit': 630000
            },
            'aum': [
                {'product_type': '理财产品', 'aum_amount': 5200000000, 'client_count': 1240, 'yoy_growth': 12},
                {'product_type': '基金产品', 'aum_amount': 3800000000, 'client_count': 980, 'yoy_growth': 18},
                {'product_type': '保险产品', 'aum_amount': 2100000000, 'client_count': 650, 'yoy_growth': 8},
            ],
            'risk': [
                {'risk_level': '低', 'exposure_amount': 4200000000, 'default_count': 12},
                {'risk_level': '中', 'exposure_amount': 3100000000, 'default_count': 28},
                {'risk_level': '高', 'exposure_amount': 1800000000, 'default_count': 45},
                {'risk_level': '极高', 'exposure_amount': 800000000, 'default_count': 18},
            ],
            'position': [
                {'asset_class': 'VIP私行', 'position_amount': 3500000000, 'position_ratio': 35, 'profit_loss': 125000000, 'pl_ratio': 3.6},
                {'asset_class': 'VIP钻石', 'position_amount': 2800000000, 'position_ratio': 28, 'profit_loss': 98000000, 'pl_ratio': 3.5},
                {'asset_class': 'VIP铂金', 'position_amount': 2100000000, 'position_ratio': 21, 'profit_loss': 72000000, 'pl_ratio': 3.4},
                {'asset_class': 'VIP黄金', 'position_amount': 1600000000, 'position_ratio': 16, 'profit_loss': 52000000, 'pl_ratio': 3.2},
            ],
            'products': [
                {'product_name': '智投+基金组合', 'category': '基金', 'sales_amount': 450000000, 'sales_count': 1250, 'success_rate': 94, 'customer_acquisition': 380, 'rating': 4.8},
                {'product_name': '稳健理财计划', 'category': '理财', 'sales_amount': 380000000, 'sales_count': 890, 'success_rate': 91, 'customer_acquisition': 320, 'rating': 4.6},
                {'product_name': '保障保险套餐', 'category': '保险', 'sales_amount': 280000000, 'sales_count': 640, 'success_rate': 88, 'customer_acquisition': 210, 'rating': 4.4},
            ],
            'trend': [
                {'metric_date': '2026-04-10', 'revenue': 1100000, 'profit': 520000},
                {'metric_date': '2026-04-11', 'revenue': 1180000, 'profit': 580000},
                {'metric_date': '2026-04-12', 'revenue': 1240000, 'profit': 610000},
                {'metric_date': '2026-04-13', 'revenue': 1320000, 'profit': 650000},
                {'metric_date': '2026-04-14', 'revenue': 1250000, 'profit': 630000},
                {'metric_date': '2026-04-15', 'revenue': 1380000, 'profit': 680000},
                {'metric_date': '2026-04-16', 'revenue': 1250000, 'profit': 630000},
            ]
        }
