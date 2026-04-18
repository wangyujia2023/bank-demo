"""
基金数字沙盘 — Doris DUPLICATE KEY 多表方案
表: fund_basic / fund_nav_history / fund_position / fund_manager
"""
import random, math
from datetime import datetime, timedelta, date
from backend.doris.connect import execute_query, execute_write, execute_many

# ── 常量 ───────────────────────────────────────────────────
SECTORS = ["半导体", "新能源", "消费", "医药", "金融", "军工", "地产", "传媒", "化工", "食品饮料"]
FUND_TYPES = ["股票型", "混合型", "指数型", "债券型", "FOF"]
RISK_LEVELS = ["低风险", "中低风险", "中风险", "中高风险", "高风险"]
STYLE_TAGS = ["激进成长", "稳健价值", "均衡配置", "行业集中", "量化增强"]

# ── 基金经理（10位）───────────────────────────────────────
MANAGERS = [
    {"id":"MGR001","name":"张伟","tenure":8.2,"aum":156.3,"style":"激进成长","turnover":1.82},
    {"id":"MGR002","name":"李静","tenure":12.5,"aum":289.7,"style":"稳健价值","turnover":0.65},
    {"id":"MGR003","name":"王磊","tenure":5.1,"aum":98.4,"style":"行业集中","turnover":2.10},
    {"id":"MGR004","name":"陈芳","tenure":9.8,"aum":342.1,"style":"均衡配置","turnover":0.88},
    {"id":"MGR005","name":"刘洋","tenure":3.4,"aum":67.2,"style":"量化增强","turnover":3.56},
    {"id":"MGR006","name":"赵明","tenure":14.1,"aum":512.8,"style":"稳健价值","turnover":0.42},
    {"id":"MGR007","name":"孙华","tenure":6.7,"aum":178.5,"style":"激进成长","turnover":2.34},
    {"id":"MGR008","name":"周敏","tenure":11.3,"aum":267.9,"style":"均衡配置","turnover":0.71},
    {"id":"MGR009","name":"吴涛","tenure":4.2,"aum":45.6,"style":"行业集中","turnover":2.88},
    {"id":"MGR010","name":"郑雪","tenure":7.9,"aum":198.3,"style":"量化增强","turnover":1.65},
]

# ── 基金列表（30只）──────────────────────────────────────
FUNDS = [
    {"id":"F001","name":"鑫瑞半导体精选","type":"股票型","sector":"半导体","mgr":"MGR001","risk":4,"fee":1.5},
    {"id":"F002","name":"汇华半导体龙头","type":"指数型","sector":"半导体","mgr":"MGR003","risk":4,"fee":0.5},
    {"id":"F003","name":"博远新能源主题","type":"股票型","sector":"新能源","mgr":"MGR002","risk":4,"fee":1.5},
    {"id":"F004","name":"景行新能源ETF","type":"指数型","sector":"新能源","mgr":"MGR005","risk":4,"fee":0.5},
    {"id":"F005","name":"惠民消费升级","type":"混合型","sector":"消费","mgr":"MGR004","risk":3,"fee":1.2},
    {"id":"F006","name":"国泰消费龙头","type":"股票型","sector":"消费","mgr":"MGR006","risk":3,"fee":1.5},
    {"id":"F007","name":"鑫康医药健康","type":"股票型","sector":"医药","mgr":"MGR007","risk":4,"fee":1.5},
    {"id":"F008","name":"汇康医疗创新","type":"混合型","sector":"医药","mgr":"MGR002","risk":3,"fee":1.2},
    {"id":"F009","name":"华夏金融地产","type":"混合型","sector":"金融","mgr":"MGR008","risk":3,"fee":1.0},
    {"id":"F010","name":"建信价值精选","type":"股票型","sector":"金融","mgr":"MGR006","risk":3,"fee":1.5},
    {"id":"F011","name":"富国军工主题","type":"股票型","sector":"军工","mgr":"MGR009","risk":5,"fee":1.5},
    {"id":"F012","name":"南方军工ETF","type":"指数型","sector":"军工","mgr":"MGR005","risk":5,"fee":0.5},
    {"id":"F013","name":"广发地产精选","type":"混合型","sector":"地产","mgr":"MGR004","risk":3,"fee":1.2},
    {"id":"F014","name":"嘉实传媒互联","type":"股票型","sector":"传媒","mgr":"MGR007","risk":4,"fee":1.5},
    {"id":"F015","name":"工银化工周期","type":"混合型","sector":"化工","mgr":"MGR010","risk":3,"fee":1.0},
    {"id":"F016","name":"景顺食品饮料","type":"股票型","sector":"食品饮料","mgr":"MGR006","risk":3,"fee":1.5},
    {"id":"F017","name":"鑫瑞科技成长","type":"股票型","sector":"半导体","mgr":"MGR001","risk":5,"fee":1.5},
    {"id":"F018","name":"博远绿色能源","type":"混合型","sector":"新能源","mgr":"MGR003","risk":4,"fee":1.2},
    {"id":"F019","name":"汇华均衡优选","type":"FOF","sector":"消费","mgr":"MGR008","risk":2,"fee":0.8},
    {"id":"F020","name":"惠民稳健债基","type":"债券型","sector":"金融","mgr":"MGR002","risk":1,"fee":0.6},
    {"id":"F021","name":"大成半导体50","type":"指数型","sector":"半导体","mgr":"MGR010","risk":4,"fee":0.5},
    {"id":"F022","name":"富国医疗器械","type":"股票型","sector":"医药","mgr":"MGR009","risk":4,"fee":1.5},
    {"id":"F023","name":"南方新能源车","type":"股票型","sector":"新能源","mgr":"MGR007","risk":4,"fee":1.5},
    {"id":"F024","name":"广发消费精选","type":"混合型","sector":"消费","mgr":"MGR004","risk":3,"fee":1.2},
    {"id":"F025","name":"华夏科创板50","type":"指数型","sector":"半导体","mgr":"MGR005","risk":5,"fee":0.5},
    {"id":"F026","name":"工银军工精选","type":"股票型","sector":"军工","mgr":"MGR001","risk":5,"fee":1.5},
    {"id":"F027","name":"建信化工龙头","type":"混合型","sector":"化工","mgr":"MGR008","risk":3,"fee":1.0},
    {"id":"F028","name":"嘉实传媒新经济","type":"股票型","sector":"传媒","mgr":"MGR003","risk":4,"fee":1.5},
    {"id":"F029","name":"景顺均衡FOF","type":"FOF","sector":"消费","mgr":"MGR006","risk":2,"fee":0.8},
    {"id":"F030","name":"鑫康创新药","type":"股票型","sector":"医药","mgr":"MGR010","risk":4,"fee":1.5},
]
FUND_MAP = {f["id"]: f for f in FUNDS}

# ── 持仓股票池（每板块8只）────────────────────────────────
STOCKS = {
    "半导体": [
        ("688981","中芯国际",0.22),("688012","中微公司",0.18),("603501","韦尔股份",0.15),
        ("688256","寒武纪",0.12),("688551","华海清科",0.10),("002371","北方华创",0.09),
        ("688425","澜起科技",0.08),("688041","海光信息",0.06),
    ],
    "新能源": [
        ("300750","宁德时代",0.25),("002594","比亚迪",0.20),("601012","隆基绿能",0.16),
        ("688599","天合光能",0.12),("002129","中环股份",0.10),("688223","晶科能源",0.08),
        ("601877","正泰电器",0.05),("300274","阳光电源",0.04),
    ],
    "消费": [
        ("600519","贵州茅台",0.25),("000858","五粮液",0.18),("002304","洋河股份",0.15),
        ("600887","伊利股份",0.14),("603288","海天味业",0.10),("000651","格力电器",0.08),
        ("600690","海尔智家",0.06),("002032","苏泊尔",0.04),
    ],
    "医药": [
        ("300760","迈瑞医疗",0.22),("002415","海康威视",0.18),("600276","恒瑞医药",0.16),
        ("603259","药明康德",0.14),("002001","新和成",0.10),("688180","君实生物",0.09),
        ("600436","片仔癀",0.07),("002252","上海莱士",0.04),
    ],
    "金融": [
        ("601318","中国平安",0.28),("600036","招商银行",0.22),("601166","兴业银行",0.16),
        ("600030","中信证券",0.12),("601688","华泰证券",0.10),("000001","平安银行",0.06),
        ("601628","中国人寿",0.04),("600000","浦发银行",0.02),
    ],
    "军工": [
        ("600760","中航沈飞",0.25),("002399","海格通信",0.18),("600893","航发动力",0.16),
        ("300547","川环科技",0.14),("002013","中航机电",0.10),("600316","洪都航空",0.08),
        ("002025","航天电子",0.06),("600297","广汇能源",0.03),
    ],
    "地产": [
        ("000002","万科A",0.28),("600048","保利发展",0.22),("001979","招商蛇口",0.18),
        ("600606","绿地控股",0.12),("002244","滨江集团",0.10),("000069","华侨城A",0.06),
        ("600895","张江高科",0.03),("001914","招商积余",0.01),
    ],
    "传媒": [
        ("002027","分众传媒",0.28),("000633","合金投资",0.20),("300251","光线传媒",0.16),
        ("002292","奥飞娱乐",0.12),("603444","吉比特",0.10),("002174","游族网络",0.08),
        ("300005","探路者",0.04),("002352","顺丰控股",0.02),
    ],
    "化工": [
        ("000792","盐湖股份",0.24),("600309","万华化学",0.22),("002648","卫星化学",0.16),
        ("600426","华鲁恒升",0.14),("002696","百洋股份",0.10),("603127","昭衍新药",0.08),
        ("002408","齐翔腾达",0.04),("603260","合盛硅业",0.02),
    ],
    "食品饮料": [
        ("600519","贵州茅台",0.30),("000858","五粮液",0.22),("600887","伊利股份",0.18),
        ("002304","洋河股份",0.12),("603288","海天味业",0.08),("600597","光明乳业",0.05),
        ("002557","洽洽食品",0.03),("600779","水井坊",0.02),
    ],
}

# ── 市场剧本（5套）──────────────────────────────────────
SCRIPTS = [
    {
        "name":"科技牛市","color":"green",
        "sectors_up":["半导体","新能源","军工"],
        "sectors_down":["地产","金融"],
        "market_ret":(0.008,0.025),   # 日涨幅区间
        "volatility":0.012,
        "capital_bias": 1.4,          # 资金净流入倍数
        "alpha_r":(0.003,0.012),
        "hint":"科技板块强势上涨，资金大幅净流入，IOPV普遍偏高",
    },
    {
        "name":"熊市调整","color":"red",
        "sectors_up":["金融"],
        "sectors_down":["半导体","新能源","医药","军工","传媒"],
        "market_ret":(-0.022,-0.005),
        "volatility":0.022,
        "capital_bias": 0.3,
        "alpha_r":(-0.008,0.001),
        "hint":"市场全面下跌，IOPV偏离为负，大额资金持续流出",
    },
    {
        "name":"板块轮动","color":"yellow",
        "sectors_up":["消费","食品饮料","医药"],
        "sectors_down":["半导体","新能源"],
        "market_ret":(-0.005,0.012),
        "volatility":0.018,
        "capital_bias": 0.9,
        "alpha_r":(-0.002,0.008),
        "hint":"科技退潮消费接力，同板块基金大幅分化，相关性矩阵明显变化",
    },
    {
        "name":"震荡行情","color":"yellow",
        "sectors_up":["金融","化工"],
        "sectors_down":["传媒","地产"],
        "market_ret":(-0.008,0.010),
        "volatility":0.028,
        "capital_bias": 0.8,
        "alpha_r":(-0.004,0.006),
        "hint":"高波动低回报，Sortino远低于Sharpe，Alpha离散度高",
    },
    {
        "name":"黑天鹅","color":"red",
        "sectors_up":[],
        "sectors_down":["半导体","新能源","医药","军工","传媒","化工","消费","食品饮料"],
        "market_ret":(-0.055,-0.025),
        "volatility":0.045,
        "capital_bias": 0.05,
        "alpha_r":(-0.020,-0.008),
        "hint":"⚠️ 黑天鹅事件：全板块急跌，IOPV熔断预警触发，大额资金恐慌出逃",
    },
]

_current_step = 0
_current_script_idx = 0
_base_date = date(2024, 1, 2)

# ── helper ──────────────────────────────────────────────
def _rnd(lo, hi): return round(random.uniform(lo, hi), 4)
def _rndi(lo, hi): return random.randint(lo, hi)

async def _query(sql):
    rows = await execute_query(sql)
    return rows or []

async def _write(sql, params=None):
    await execute_write(sql, params)

async def _many(sql, rows):
    await execute_many(sql, rows)


class FundService:

    # ── 建表 ───────────────────────────────────────────────
    async def init_table(self):
        global _current_step, _current_script_idx

        for tbl in ["fund_nav_history","fund_position","fund_basic","fund_manager"]:
            await _write(f"DROP TABLE IF EXISTS {tbl}")

        await _write("""
            CREATE TABLE fund_manager (
                manager_id      VARCHAR(10)   NOT NULL,
                name            VARCHAR(20),
                tenure_years    FLOAT,
                aum_bn          FLOAT         COMMENT '在管规模(亿)',
                style_tag       VARCHAR(20),
                turnover_rate   FLOAT         COMMENT '年均换手率',
                avg_alpha       FLOAT,
                total_return    FLOAT,
                max_drawdown    FLOAT,
                sharpe          FLOAT
            ) DUPLICATE KEY(manager_id)
            DISTRIBUTED BY HASH(manager_id) BUCKETS 2
            PROPERTIES("replication_num"="1")
        """)

        await _write("""
            CREATE TABLE fund_basic (
                fund_id         VARCHAR(10)   NOT NULL,
                fund_name       VARCHAR(50),
                fund_type       VARCHAR(20),
                sector_tag      VARCHAR(20),
                manager_id      VARCHAR(10),
                risk_level      INT,
                fee_rate        FLOAT,
                inception_date  DATE,
                nav_yesterday   DOUBLE        COMMENT '昨日单位净值',
                cumulative_nav  DOUBLE        COMMENT '累计净值',
                realtime_iopv   DOUBLE        COMMENT '盘中实时估值',
                iopv_deviation  DOUBLE        COMMENT 'IOPV偏离度%',
                capital_inflow  DOUBLE        COMMENT '今日资金流入(万)',
                capital_outflow DOUBLE        COMMENT '今日资金流出(万)',
                capital_net     DOUBLE        COMMENT '净流入(万)',
                trade_status    VARCHAR(10),
                ret_1m          FLOAT,
                ret_3m          FLOAT,
                ret_6m          FLOAT,
                ret_1y          FLOAT,
                ret_inception   FLOAT,
                sharpe          FLOAT,
                sortino         FLOAT,
                alpha           FLOAT,
                beta            FLOAT,
                max_drawdown    FLOAT,
                volatility      FLOAT,
                update_ts       DATETIME
            ) DUPLICATE KEY(fund_id)
            DISTRIBUTED BY HASH(fund_id) BUCKETS 4
            PROPERTIES("replication_num"="1")
        """)

        await _write("""
            CREATE TABLE fund_nav_history (
                trade_date      DATE          NOT NULL,
                fund_id         VARCHAR(10)   NOT NULL,
                nav             DOUBLE,
                cumulative_nav  DOUBLE,
                daily_return    FLOAT,
                benchmark_ret   FLOAT,
                excess_ret      FLOAT,
                drawdown        FLOAT,
                rolling_sharpe  FLOAT,
                rolling_alpha   FLOAT,
                script_name     VARCHAR(20),
                script_color    VARCHAR(10)
            ) DUPLICATE KEY(trade_date, fund_id)
            DISTRIBUTED BY HASH(fund_id) BUCKETS 4
            PROPERTIES("replication_num"="1")
        """)

        await _write("""
            CREATE TABLE fund_position (
                fund_id         VARCHAR(10)   NOT NULL,
                report_date     DATE          NOT NULL,
                stock_code      VARCHAR(10),
                stock_name      VARCHAR(20),
                sector_l1       VARCHAR(20),
                weight_pct      FLOAT,
                market_value_mn FLOAT         COMMENT '市值(百万)',
                price_contrib   FLOAT         COMMENT '价格贡献度%',
                alpha_contrib   FLOAT
            ) DUPLICATE KEY(fund_id, report_date)
            DISTRIBUTED BY HASH(fund_id) BUCKETS 4
            PROPERTIES("replication_num"="1")
        """)

        _current_step = 0
        _current_script_idx = 0
        await self._seed_static()
        await self._seed_history(60)
        return {"msg": "初始化完成：30只基金 · 60日历史净值 · 持仓数据已就绪"}

    # ── 静态数据种子 ──────────────────────────────────────
    async def _seed_static(self):
        # 经理
        mgr_rows = []
        for m in MANAGERS:
            alpha = _rnd(0.02, 0.12) if m["style"] in ("激进成长","行业集中") else _rnd(-0.01, 0.06)
            total_ret = _rnd(0.15, 2.8)
            sharpe = _rnd(0.6, 1.8)
            mdd = _rnd(-0.35, -0.08)
            mgr_rows.append((
                m["id"], m["name"], m["tenure"], m["aum"],
                m["style"], m["turnover"],
                round(alpha, 4), round(total_ret, 4),
                round(mdd, 4), round(sharpe, 4)
            ))
        await _many("""INSERT INTO fund_manager VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", mgr_rows)

        # 基金基础
        fund_rows = []
        inception_base = date(2015, 1, 1)
        for f in FUNDS:
            inc = inception_base + timedelta(days=_rndi(0, 2500))
            nav0 = _rnd(0.85, 3.20)
            cum_nav = nav0 * _rnd(1.0, 1.8)
            iopv = round(nav0 * (1 + _rnd(-0.02, 0.02)), 4)
            dev = round((iopv - nav0) / nav0 * 100, 3)
            inflow = _rnd(500, 8000)
            outflow = _rnd(300, 7000)
            cnet = round(inflow - outflow, 2)
            ret_1m  = _rnd(-0.08, 0.15)
            ret_3m  = _rnd(-0.15, 0.35)
            ret_6m  = _rnd(-0.20, 0.55)
            ret_1y  = _rnd(-0.25, 0.80)
            ret_inc = _rnd(-0.10, 2.50)
            sharpe  = _rnd(0.3, 1.9)
            sortino = sharpe * _rnd(0.8, 1.3)
            alpha   = _rnd(-0.05, 0.15)
            beta    = _rnd(0.55, 1.35)
            mdd     = _rnd(-0.45, -0.05)
            vol     = _rnd(0.08, 0.32)
            fund_rows.append((
                f["id"], f["name"], f["type"], f["sector"], f["mgr"],
                f["risk"], f["fee"], str(inc),
                round(nav0,4), round(cum_nav,4),
                round(iopv,4), round(dev,3),
                round(inflow,2), round(outflow,2), round(cnet,2),
                "开放",
                round(ret_1m,4), round(ret_3m,4), round(ret_6m,4),
                round(ret_1y,4), round(ret_inc,4),
                round(sharpe,4), round(sortino,4),
                round(alpha,4), round(beta,4),
                round(mdd,4), round(vol,4),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
        await _many("""INSERT INTO fund_basic VALUES(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", fund_rows)

        # 持仓（季报）
        pos_rows = []
        report_dt = str(date(2024, 9, 30))
        for f in FUNDS:
            stocks = STOCKS.get(f["sector"], list(STOCKS.values())[0])
            nav_v = 100.0  # 百万
            for code, sname, base_w in stocks:
                w = round(base_w * _rnd(0.7, 1.3), 4)
                w = min(w, 0.30)
                mv = round(nav_v * w * _rnd(0.8, 1.2), 2)
                pc = round(w * _rnd(-0.02, 0.08), 4)
                ac = round(pc * _rnd(0.3, 1.2) - 0.01, 4)
                pos_rows.append((f["id"], report_dt, code, sname, f["sector"], round(w,4), mv, pc, ac))
        await _many("INSERT INTO fund_position VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", pos_rows)

    # ── 历史净值种子（60日）────────────────────────────────
    async def _seed_history(self, days: int):
        nav_rows = []
        for f in FUNDS:
            nav = _rnd(0.85, 3.20)
            cum = nav * _rnd(1.0, 1.5)
            drawdown_peak = nav
            daily_rets = []
            for i in range(days):
                d = _base_date + timedelta(days=i)
                script = SCRIPTS[i % len(SCRIPTS)]
                sec_ret = _rnd(*script["market_ret"])
                if f["sector"] in script["sectors_up"]:
                    sec_ret += _rnd(0.005, 0.018)
                elif f["sector"] in script["sectors_down"]:
                    sec_ret -= _rnd(0.005, 0.020)
                noise = random.gauss(0, script["volatility"] * 0.5)
                dr = round(sec_ret + noise, 5)
                daily_rets.append(dr)
                nav = round(nav * (1 + dr), 4)
                cum = round(cum * (1 + dr * 0.9), 4)
                bench_ret = round(_rnd(*script["market_ret"]) * 0.8 + random.gauss(0, 0.006), 5)
                excess = round(dr - bench_ret, 5)
                drawdown_peak = max(drawdown_peak, nav)
                dd = round((nav - drawdown_peak) / drawdown_peak * 100, 3)
                # rolling sharpe (last 20 days)
                window = daily_rets[-20:] if len(daily_rets) >= 20 else daily_rets
                rs_mean = sum(window) / len(window)
                rs_std  = (sum((x - rs_mean)**2 for x in window) / max(len(window)-1,1)) ** 0.5
                rs = round(rs_mean / rs_std * (252**0.5), 3) if rs_std > 0 else 0.0
                alpha_d = round(_rnd(*script["alpha_r"]), 5)
                nav_rows.append((
                    str(d), f["id"], nav, cum, round(dr,5),
                    round(bench_ret,5), round(excess,5),
                    dd, rs, round(alpha_d,5),
                    script["name"], script["color"]
                ))
        await _many("""INSERT INTO fund_nav_history VALUES(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", nav_rows)

    # ── 推进一个交易日 ────────────────────────────────────
    async def generate_step(self):
        global _current_step, _current_script_idx
        _current_step += 1
        # 每8步切换剧本
        _current_script_idx = (_current_step // 8) % len(SCRIPTS)
        script = SCRIPTS[_current_script_idx]
        trade_dt = _base_date + timedelta(days=60 + _current_step)

        # 取当前净值（取最新一条）
        latest = await _query("""
            SELECT fund_id, nav, cumulative_nav
            FROM fund_nav_history
            QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY trade_date DESC) = 1
        """)
        nav_map = {r["fund_id"]: (float(r["nav"]), float(r["cumulative_nav"])) for r in latest}

        nav_rows = []
        basic_rows = []

        for f in FUNDS:
            old_nav, old_cum = nav_map.get(f["id"], (1.0, 1.0))
            sec_ret = _rnd(*script["market_ret"])
            if f["sector"] in script["sectors_up"]:   sec_ret += _rnd(0.008, 0.022)
            elif f["sector"] in script["sectors_down"]: sec_ret -= _rnd(0.008, 0.025)
            noise = random.gauss(0, script["volatility"] * 0.6)
            dr = round(sec_ret + noise, 5)
            nav  = round(old_nav * (1 + dr), 4)
            cum  = round(old_cum * (1 + dr * 0.9), 4)
            bench_ret  = round(_rnd(*script["market_ret"]) * 0.8 + random.gauss(0, 0.006), 5)
            excess_ret = round(dr - bench_ret, 5)
            dd = round(_rnd(-0.25, 0.0), 3)
            rs = round(_rnd(0.2, 1.8), 3)
            alpha_d = round(_rnd(*script["alpha_r"]), 5)

            nav_rows.append((
                str(trade_dt), f["id"], nav, cum,
                round(dr,5), round(bench_ret,5), round(excess_ret,5),
                dd, rs, alpha_d,
                script["name"], script["color"]
            ))

            # 更新 fund_basic
            iopv = round(nav * (1 + _rnd(-0.008, 0.008)), 4)
            dev  = round((iopv - old_nav) / old_nav * 100, 3)
            cap_base = _rnd(1000, 12000)
            bias = script["capital_bias"]
            inflow  = round(cap_base * bias * _rnd(0.8, 1.2), 2)
            outflow = round(cap_base / bias * _rnd(0.8, 1.2), 2)
            cnet    = round(inflow - outflow, 2)

            # 简化更新：只覆写实时字段（DUPLICATE KEY — 插入新行，查询时取MAX(update_ts)那行）
            basic_rows.append((
                f["id"], f["name"], f["type"], f["sector"], f["mgr"],
                f["risk"], f["fee"],
                str(_base_date + timedelta(days=_rndi(0,2500))),
                round(old_nav,4), round(cum,4),
                iopv, dev, inflow, outflow, cnet, "开放",
                round(_rnd(-0.08,0.15),4), round(_rnd(-0.20,0.40),4),
                round(_rnd(-0.25,0.55),4), round(_rnd(-0.30,0.80),4),
                round(_rnd(-0.10,2.5),4),
                round(rs,4), round(rs*_rnd(0.8,1.3),4),
                round(alpha_d*20,4), round(_rnd(0.6,1.3),4),
                round(dd,4), round(_rnd(0.08,0.32),4),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

        await _many("""INSERT INTO fund_nav_history VALUES(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", nav_rows)
        await _many("""INSERT INTO fund_basic VALUES(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", basic_rows)

        return {"msg": f"交易日 {trade_dt} 推进完成（{script['name']}）", "script": script["name"], "color": script["color"]}

    async def batch_generate(self, days: int):
        days = min(days, 20)
        for _ in range(days):
            await self.generate_step()
        return {"msg": f"已推进 {days} 个交易日"}

    # ── 总览 KPI ──────────────────────────────────────────
    async def get_overview(self):
        global _current_step, _current_script_idx
        rows = await _query("""
            SELECT COUNT(DISTINCT fund_id) AS fund_cnt,
                   ROUND(AVG(realtime_iopv),4) AS avg_iopv,
                   ROUND(AVG(iopv_deviation),3) AS avg_dev,
                   ROUND(SUM(capital_net)/10000,2) AS total_net_bn,
                   ROUND(AVG(sharpe),3) AS avg_sharpe,
                   ROUND(AVG(max_drawdown)*100,2) AS avg_mdd,
                   ROUND(AVG(alpha)*100,2) AS avg_alpha,
                   SUM(CASE WHEN iopv_deviation > 2 OR iopv_deviation < -2 THEN 1 ELSE 0 END) AS iopv_alerts,
                   MAX(update_ts) AS last_ts
            FROM (
                SELECT fund_id, realtime_iopv, iopv_deviation, capital_net,
                       sharpe, max_drawdown, alpha, update_ts
                FROM fund_basic
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
            ) t
        """)
        if not rows:
            return {"empty": True}
        r = dict(rows[0])
        script = SCRIPTS[_current_script_idx]
        r["current_step"]   = _current_step
        r["script_name"]    = script["name"]
        r["script_color"]   = script["color"]
        r["script_hint"]    = script["hint"]
        r["empty"]          = r.get("fund_cnt", 0) == 0
        return r

    # ── 基金列表（含实时字段）────────────────────────────
    async def get_fund_list(self, sector: str = None, fund_type: str = None, risk: int = None):
        where = ""
        conds = []
        if sector:    conds.append(f"sector_tag = '{sector}'")
        if fund_type: conds.append(f"fund_type = '{fund_type}'")
        if risk:      conds.append(f"risk_level = {risk}")
        if conds:     where = "WHERE " + " AND ".join(conds)
        return await _query(f"""
            SELECT t.*, m.name AS manager_name, m.style_tag AS manager_style
            FROM (
                SELECT *
                FROM fund_basic
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
            ) t
            LEFT JOIN fund_manager m ON t.manager_id = m.manager_id
            {where}
            ORDER BY t.sector_tag, t.fund_id
        """)

    # ── 单基金全量 ────────────────────────────────────────
    async def get_fund_detail(self, fund_id: str):
        rows = await _query(f"""
            SELECT t.*, m.name AS manager_name, m.style_tag, m.tenure_years,
                   m.aum_bn, m.turnover_rate, m.avg_alpha AS mgr_alpha
            FROM (
                SELECT * FROM fund_basic
                WHERE fund_id = '{fund_id}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
            ) t
            LEFT JOIN fund_manager m ON t.manager_id = m.manager_id
        """)
        return rows[0] if rows else {}

    # ── 净值历史 ──────────────────────────────────────────
    async def get_nav_history(self, fund_id: str, days: int = 90):
        return await _query(f"""
            SELECT trade_date, nav, cumulative_nav, daily_return,
                   benchmark_ret, excess_ret, drawdown,
                   rolling_sharpe, rolling_alpha, script_name, script_color
            FROM fund_nav_history
            WHERE fund_id = '{fund_id}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY trade_date) = 1
            ORDER BY trade_date ASC
            LIMIT {days}
        """)

    # ── 持仓穿透 ──────────────────────────────────────────
    async def get_position(self, fund_id: str):
        return await _query(f"""
            SELECT stock_code, stock_name, sector_l1,
                   weight_pct, market_value_mn, price_contrib, alpha_contrib
            FROM fund_position
            WHERE fund_id = '{fund_id}'
              AND report_date = (SELECT MAX(report_date) FROM fund_position WHERE fund_id = '{fund_id}')
            ORDER BY weight_pct DESC
        """)

    # ── 经理画像 ──────────────────────────────────────────
    async def get_manager(self, manager_id: str):
        mgr = await _query(f"SELECT * FROM fund_manager WHERE manager_id = '{manager_id}'")
        if not mgr:
            return {}
        funds = await _query(f"""
            SELECT t.fund_id, t.fund_name, t.sector_tag, t.ret_1y, t.sharpe, t.max_drawdown, t.alpha
            FROM (
                SELECT fund_id, fund_name, sector_tag, ret_1y, sharpe, max_drawdown, alpha
                FROM fund_basic
                WHERE manager_id = '{manager_id}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
            ) t
            ORDER BY t.ret_1y DESC
        """)
        return {"manager": mgr[0], "funds": funds}

    # ── 竞品推荐 + 相关性矩阵 ────────────────────────────
    async def get_peers(self, fund_id: str):
        # 取当前基金板块
        info = await _query(f"SELECT sector_tag FROM fund_basic WHERE fund_id='{fund_id}' LIMIT 1")
        if not info:
            return {"peers": [], "correlation": []}
        sector = info[0]["sector_tag"]

        # 同板块基金排名（窗口函数 RANK）
        peers = await _query(f"""
            SELECT fund_id, fund_name, sector_tag, ret_1y, sharpe, max_drawdown,
                   alpha, volatility, manager_id,
                   RANK() OVER (ORDER BY sharpe DESC)       AS sharpe_rank,
                   RANK() OVER (ORDER BY max_drawdown DESC) AS mdd_rank,
                   RANK() OVER (ORDER BY ret_1y DESC)       AS ret_rank
            FROM (
                SELECT fund_id, fund_name, sector_tag, ret_1y, sharpe,
                       max_drawdown, alpha, volatility, manager_id
                FROM fund_basic
                WHERE sector_tag = '{sector}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
            ) t
            ORDER BY sharpe DESC
            LIMIT 6
        """)

        # peer fund_ids（含自身）
        peer_ids = [r["fund_id"] for r in peers]
        if not peer_ids:
            return {"peers": peers, "correlation": []}
        ids_str = ",".join(f"'{x}'" for x in peer_ids)

        # 相关性矩阵（CORR聚合 — Doris 核心特性展示）
        corr = await _query(f"""
            SELECT a.fund_id AS fund_a, b.fund_id AS fund_b,
                   ROUND(CORR(a.daily_return, b.daily_return), 3) AS corr
            FROM fund_nav_history a
            JOIN fund_nav_history b ON a.trade_date = b.trade_date
            WHERE a.fund_id IN ({ids_str}) AND b.fund_id IN ({ids_str})
              AND a.trade_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
            GROUP BY a.fund_id, b.fund_id
            ORDER BY a.fund_id, b.fund_id
        """)
        return {"peers": peers, "correlation": corr, "sector": sector}

    # ── 板块热力 ──────────────────────────────────────────
    async def get_sector_stats(self):
        return await _query("""
            SELECT sector_tag,
                   COUNT(DISTINCT fund_id)       AS fund_cnt,
                   ROUND(AVG(ret_1y)*100, 2)     AS avg_ret_1y,
                   ROUND(AVG(iopv_deviation), 3) AS avg_dev,
                   ROUND(SUM(capital_net)/10000, 2) AS net_flow_bn,
                   ROUND(AVG(sharpe), 3)         AS avg_sharpe,
                   ROUND(AVG(max_drawdown)*100, 2) AS avg_mdd
            FROM (
                SELECT fund_id, sector_tag, ret_1y, iopv_deviation,
                       capital_net, sharpe, max_drawdown
                FROM fund_basic
                QUALIFY ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY update_ts DESC) = 1
            ) t
            GROUP BY sector_tag
            ORDER BY avg_ret_1y DESC
        """)

    # ── 重置 ─────────────────────────────────────────────
    async def reset(self):
        global _current_step, _current_script_idx
        _current_step = 0
        _current_script_idx = 0
        for t in ["fund_nav_history","fund_position","fund_basic","fund_manager"]:
            await _write(f"TRUNCATE TABLE {t}")
        await self._seed_static()
        await self._seed_history(60)
        return {"msg": "已重置"}
