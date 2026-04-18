"""
证券实时数仓沙盘
场景: 券商经纪 + 财富管理 + 两融风控
"""
import random
from datetime import datetime, timedelta

from backend.doris.connect import execute_many, execute_query, execute_write

ACCOUNT_TABLE = "sec_account_snapshot"
POSITION_TABLE = "sec_position_snapshot"
RISK_TABLE = "sec_risk_snapshot"
MARKET_TABLE = "sec_market_minute"
TRADE_TABLE = "sec_trade_detail"
BRANCH_TABLE = "sec_branch_metrics"

BASE_DATE = datetime(2025, 3, 3, 9, 30, 0)

BRANCHES = [
    {"id": "B001", "name": "上海陆家嘴营业部", "region": "华东"},
    {"id": "B002", "name": "深圳福田营业部", "region": "华南"},
    {"id": "B003", "name": "北京金融街营业部", "region": "华北"},
    {"id": "B004", "name": "杭州钱江营业部", "region": "华东"},
]

SYMBOLS = [
    {"symbol": "600519", "name": "贵州茅台", "sector": "白酒消费", "price": 1688.0, "beta": 0.7},
    {"symbol": "300750", "name": "宁德时代", "sector": "新能源", "price": 213.0, "beta": 1.2},
    {"symbol": "600036", "name": "招商银行", "sector": "银行", "price": 36.5, "beta": 0.5},
    {"symbol": "600030", "name": "中信证券", "sector": "券商", "price": 25.8, "beta": 1.0},
    {"symbol": "601318", "name": "中国平安", "sector": "保险", "price": 52.4, "beta": 0.8},
    {"symbol": "688981", "name": "中芯国际", "sector": "半导体", "price": 52.0, "beta": 1.5},
    {"symbol": "002594", "name": "比亚迪", "sector": "新能源", "price": 241.5, "beta": 1.3},
    {"symbol": "603259", "name": "药明康德", "sector": "创新药", "price": 46.8, "beta": 1.1},
    {"symbol": "601127", "name": "赛力斯", "sector": "智能汽车", "price": 96.2, "beta": 1.8},
    {"symbol": "601688", "name": "华泰证券", "sector": "券商", "price": 17.2, "beta": 0.9},
    {"symbol": "000333", "name": "美的集团", "sector": "家电消费", "price": 63.4, "beta": 0.7},
    {"symbol": "600276", "name": "恒瑞医药", "sector": "创新药", "price": 47.5, "beta": 0.9},
]

PHASES = [
    {
        "name": "开盘试盘",
        "market_bias": 0.0015,
        "buy_bias": 0.56,
        "favored": {"券商": 0.008, "银行": 0.004, "保险": 0.004},
        "volume_mul": 1.05,
    },
    {
        "name": "题材拉升",
        "market_bias": 0.0038,
        "buy_bias": 0.63,
        "favored": {"半导体": 0.010, "新能源": 0.008, "智能汽车": 0.012},
        "volume_mul": 1.28,
    },
    {
        "name": "午后震荡",
        "market_bias": -0.0008,
        "buy_bias": 0.49,
        "favored": {"白酒消费": 0.003, "家电消费": 0.002, "创新药": 0.001},
        "volume_mul": 0.92,
    },
    {
        "name": "尾盘风控",
        "market_bias": -0.0035,
        "buy_bias": 0.41,
        "favored": {"银行": 0.004, "保险": 0.003, "券商": -0.003, "新能源": -0.006, "智能汽车": -0.007},
        "volume_mul": 1.18,
    },
]

CLIENTS = [
    ("A0001", "周明", "B001", "张晨", "私行", 5, 4200000, 600000),
    ("A0002", "王悦", "B001", "张晨", "钻石", 4, 2600000, 240000),
    ("A0003", "赵峰", "B001", "李岩", "金卡", 3, 1280000, 0),
    ("A0004", "陈琳", "B001", "李岩", "成长", 2, 620000, 0),
    ("A0005", "孙宁", "B001", "高远", "钻石", 4, 3100000, 420000),
    ("A0006", "刘洋", "B001", "高远", "金卡", 3, 1420000, 0),
    ("A0007", "吴涛", "B002", "许航", "私行", 5, 5600000, 1200000),
    ("A0008", "郭璇", "B002", "许航", "钻石", 4, 3300000, 380000),
    ("A0009", "朱蕾", "B002", "韩冰", "金卡", 3, 1360000, 0),
    ("A0010", "何俊", "B002", "韩冰", "成长", 2, 760000, 0),
    ("A0011", "蒋欣", "B002", "林枫", "钻石", 4, 2900000, 300000),
    ("A0012", "沈波", "B002", "林枫", "金卡", 3, 1650000, 0),
    ("A0013", "钱程", "B003", "顾斌", "私行", 5, 6100000, 1600000),
    ("A0014", "徐岚", "B003", "顾斌", "钻石", 4, 3520000, 450000),
    ("A0015", "唐薇", "B003", "苏晨", "金卡", 3, 1540000, 0),
    ("A0016", "马腾", "B003", "苏晨", "成长", 2, 700000, 0),
    ("A0017", "邓超", "B003", "周航", "钻石", 4, 2800000, 260000),
    ("A0018", "谢颖", "B003", "周航", "金卡", 3, 1180000, 0),
    ("A0019", "梁静", "B004", "陆川", "私行", 5, 4700000, 850000),
    ("A0020", "韩雪", "B004", "陆川", "钻石", 4, 3200000, 330000),
    ("A0021", "黄杰", "B004", "孟舟", "金卡", 3, 1490000, 0),
    ("A0022", "姜楠", "B004", "孟舟", "成长", 2, 680000, 0),
    ("A0023", "曹宇", "B004", "任泽", "钻石", 4, 2450000, 180000),
    ("A0024", "袁菲", "B004", "任泽", "金卡", 3, 1320000, 0),
]


def _branch_name(branch_id: str) -> str:
    return next((b["name"] for b in BRANCHES if b["id"] == branch_id), branch_id)


def _symbol_map():
    return {s["symbol"]: s for s in SYMBOLS}


def _phase_for_step(step: int):
    return PHASES[(step // 12) % len(PHASES)]


def _ts_for_step(step: int) -> datetime:
    day_offset = step // 48
    slot = step % 48
    if slot < 24:
        hour = 9 + (30 + slot * 5) // 60
        minute = (30 + slot * 5) % 60
    else:
        slot -= 24
        hour = 13 + (slot * 5) // 60
        minute = (slot * 5) % 60
    dt = BASE_DATE + timedelta(days=day_offset)
    return dt.replace(hour=hour, minute=minute, second=0)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _tier_rank(tier: str) -> int:
    return {"私行": 4, "钻石": 3, "金卡": 2, "成长": 1}.get(tier, 1)


class SecuritiesService:
    async def init_table(self):
        tables = [BRANCH_TABLE, TRADE_TABLE, MARKET_TABLE, RISK_TABLE, POSITION_TABLE, ACCOUNT_TABLE]
        # for tbl in tables:
        #     await execute_write(f"DROP TABLE IF EXISTS {tbl}")

        ddls = [
            f"""
            CREATE TABLE IF NOT EXISTS  IF NOT EXISTS {ACCOUNT_TABLE} (
                account_id           VARCHAR(20),
                client_name          VARCHAR(20),
                branch_id            VARCHAR(10),
                branch_name          VARCHAR(50),
                rm_name              VARCHAR(20),
                client_tier          VARCHAR(20),
                risk_level           INT,
                total_asset          DOUBLE,
                cash_amt             DOUBLE,
                market_value         DOUBLE,
                unrealized_pnl       DOUBLE,
                pnl_pct              DOUBLE,
                margin_debt          DOUBLE,
                maintenance_ratio    DOUBLE,
                concentration_pct    DOUBLE,
                position_count       INT,
                trade_count_today    INT,
                turnover_today       DOUBLE,
                last_trade_ts        DATETIME,
                update_ts            DATETIME
            ) UNIQUE KEY(account_id)
            DISTRIBUTED BY HASH(account_id) BUCKETS 4
            PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {POSITION_TABLE} (
                account_id         VARCHAR(20),
                symbol             VARCHAR(10),
                security_name      VARCHAR(30),
                sector_name        VARCHAR(30),
                branch_id          VARCHAR(10),
                branch_name        VARCHAR(50),
                qty                BIGINT,
                available_qty      BIGINT,
                avg_cost           DOUBLE,
                last_price         DOUBLE,
                market_value       DOUBLE,
                cost_amount        DOUBLE,
                unrealized_pnl     DOUBLE,
                pnl_pct            DOUBLE,
                weight_pct         DOUBLE,
                update_ts          DATETIME
            ) UNIQUE KEY(account_id, symbol)
            DISTRIBUTED BY HASH(account_id) BUCKETS 4
            PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
            """,
            f"""
            CREATE TABLE IF NOT EXISTS  {RISK_TABLE} (
                alert_id              VARCHAR(40),
                account_id            VARCHAR(20),
                client_name           VARCHAR(20),
                branch_id             VARCHAR(10),
                branch_name           VARCHAR(50),
                alert_type            VARCHAR(40),
                risk_level            VARCHAR(10),
                metric_value          DOUBLE,
                threshold_value       DOUBLE,
                position_symbol       VARCHAR(10),
                position_name         VARCHAR(30),
                suggestion            VARCHAR(100),
                status                VARCHAR(20),
                update_ts             DATETIME
            ) UNIQUE KEY(alert_id)
            DISTRIBUTED BY HASH(alert_id) BUCKETS 4
            PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
            """,
            f"""
            CREATE TABLE IF NOT EXISTS  {MARKET_TABLE} (
                ts                DATETIME,
                symbol            VARCHAR(10),
                security_name     VARCHAR(30),
                sector_name       VARCHAR(30),
                last_price        DOUBLE,
                change_pct        DOUBLE,
                volume_lot        BIGINT,
                turnover_amt      DOUBLE,
                net_inflow_amt    DOUBLE,
                phase_name        VARCHAR(20)
            ) UNIQUE KEY(ts, symbol)
            DISTRIBUTED BY HASH(symbol) BUCKETS 4
            PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
            """,
            f"""
            CREATE TABLE IF NOT EXISTS  {TRADE_TABLE} (
                trade_id          VARCHAR(40),
                ts                DATETIME,
                account_id        VARCHAR(20),
                client_name       VARCHAR(20),
                branch_id         VARCHAR(10),
                branch_name       VARCHAR(50),
                rm_name           VARCHAR(20),
                symbol            VARCHAR(10),
                security_name     VARCHAR(30),
                sector_name       VARCHAR(30),
                side              VARCHAR(10),
                price             DOUBLE,
                qty               BIGINT,
                amount            DOUBLE,
                fee               DOUBLE,
                channel           VARCHAR(20),
                phase_name        VARCHAR(20)
            ) UNIQUE KEY(trade_id)
            DISTRIBUTED BY HASH(trade_id) BUCKETS 4
            PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
            """,
            f"""
            CREATE TABLE IF NOT EXISTS  {BRANCH_TABLE} (
                ts                DATETIME,
                branch_id         VARCHAR(10),
                branch_name       VARCHAR(50),
                turnover_amt      DOUBLE,
                commission_amt    DOUBLE,
                buy_amt           DOUBLE,
                sell_amt          DOUBLE,
                net_inflow_amt    DOUBLE,
                active_clients    INT,
                margin_clients    INT,
                avg_maintenance   DOUBLE,
                phase_name        VARCHAR(20)
            ) UNIQUE KEY(ts, branch_id)
            DISTRIBUTED BY HASH(branch_id) BUCKETS 4
            PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")
            """,
        ]
        for ddl in ddls:
            await execute_write(ddl)

        await self._seed_initial_snapshot()
        return {"success": True, "msg": "证券实时数仓表已初始化，已写入账户与持仓基线"}

    async def _seed_initial_snapshot(self):
        rng = random.Random(20250303)
        symbol_map = _symbol_map()
        init_ts = _fmt(BASE_DATE - timedelta(minutes=5))
        positions = []
        accounts = []

        for idx, row in enumerate(CLIENTS):
            account_id, client_name, branch_id, rm_name, tier, risk_level, base_asset, margin_debt = row
            branch_name = _branch_name(branch_id)
            candidates = SYMBOLS[idx % len(SYMBOLS):] + SYMBOLS[:idx % len(SYMBOLS)]
            hold_num = 4 if _tier_rank(tier) >= 3 else 3
            selected = candidates[:hold_num]
            cash_amt = round(base_asset * (0.28 if risk_level >= 4 else 0.36), 2)
            cost_total = 0.0
            market_total = 0.0
            pos_rows = []
            for j, sec in enumerate(selected):
                weight = 0.18 + 0.05 * ((j + risk_level) % 3)
                if sec["sector"] in ("半导体", "新能源", "智能汽车") and risk_level >= 4:
                    weight += 0.05
                budget = base_asset * weight
                lot_price = sec["price"] * 100
                qty = max(100, int(budget / lot_price) * 100)
                avg_cost = round(sec["price"] * rng.uniform(0.94, 1.04), 2)
                cost_amount = round(qty * avg_cost, 2)
                market_value = round(qty * sec["price"], 2)
                pnl = round(market_value - cost_amount, 2)
                cost_total += cost_amount
                market_total += market_value
                pos_rows.append({
                    "account_id": account_id,
                    "symbol": sec["symbol"],
                    "security_name": sec["name"],
                    "sector_name": sec["sector"],
                    "branch_id": branch_id,
                    "branch_name": branch_name,
                    "qty": qty,
                    "available_qty": qty,
                    "avg_cost": avg_cost,
                    "last_price": sec["price"],
                    "market_value": market_value,
                    "cost_amount": cost_amount,
                    "unrealized_pnl": pnl,
                    "pnl_pct": round(pnl / cost_amount * 100, 2) if cost_amount else 0.0,
                    "weight_pct": 0.0,
                    "update_ts": init_ts,
                })
            total_asset = round(cash_amt + market_total, 2)
            for pos in pos_rows:
                pos["weight_pct"] = round(pos["market_value"] / total_asset * 100, 2) if total_asset else 0.0
                positions.append(pos)
            concentration = max((p["weight_pct"] for p in pos_rows), default=0.0)
            pnl_total = round(market_total - cost_total, 2)
            accounts.append((
                account_id, client_name, branch_id, branch_name, rm_name, tier, risk_level,
                total_asset, round(cash_amt, 2), round(market_total, 2), pnl_total,
                round(pnl_total / max(cost_total, 1) * 100, 2), margin_debt,
                round(total_asset / max(margin_debt, 1) * 100, 2) if margin_debt > 0 else 999.0,
                concentration, len(pos_rows), 0, 0.0, init_ts, init_ts,
            ))

        await execute_many(
            f"""INSERT INTO {POSITION_TABLE}
                (account_id, symbol, security_name, sector_name, branch_id, branch_name, qty, available_qty,
                 avg_cost, last_price, market_value, cost_amount, unrealized_pnl, pnl_pct, weight_pct, update_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [(
                p["account_id"], p["symbol"], p["security_name"], p["sector_name"], p["branch_id"], p["branch_name"],
                p["qty"], p["available_qty"], p["avg_cost"], p["last_price"], p["market_value"], p["cost_amount"],
                p["unrealized_pnl"], p["pnl_pct"], p["weight_pct"], p["update_ts"],
            ) for p in positions]
        )
        await execute_many(
            f"""INSERT INTO {ACCOUNT_TABLE}
                (account_id, client_name, branch_id, branch_name, rm_name, client_tier, risk_level, total_asset,
                 cash_amt, market_value, unrealized_pnl, pnl_pct, margin_debt, maintenance_ratio, concentration_pct,
                 position_count, trade_count_today, turnover_today, last_trade_ts, update_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            accounts
        )
        await self._refresh_risks(init_ts)

    async def get_current_step(self) -> int:
        rows = await execute_query(f"SELECT COUNT(*) AS cnt FROM {MARKET_TABLE}")
        cnt = int(rows[0].get("cnt", 0) if rows else 0)
        return cnt // max(len(SYMBOLS), 1)

    async def _latest_accounts(self):
        return await execute_query(f"SELECT * FROM {ACCOUNT_TABLE} ORDER BY account_id")

    async def _latest_positions(self):
        return await execute_query(f"SELECT * FROM {POSITION_TABLE} ORDER BY account_id, symbol")

    async def _latest_price_map(self):
        rows = await execute_query(f"""
            SELECT symbol, last_price
            FROM {MARKET_TABLE}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) = 1
        """)
        if rows:
            return {r["symbol"]: float(r["last_price"]) for r in rows}
        return {s["symbol"]: s["price"] for s in SYMBOLS}

    async def generate_step(self):
        step = await self.get_current_step()
        phase = _phase_for_step(step)
        ts = _fmt(_ts_for_step(step))
        rng = random.Random(91000 + step)
        symbol_map = _symbol_map()
        price_map = await self._latest_price_map()
        market_rows = []

        for sec in SYMBOLS:
            prev = float(price_map.get(sec["symbol"], sec["price"]))
            sector_alpha = phase["favored"].get(sec["sector"], 0.0)
            beta_k = sec["beta"] * 0.0022
            noise = rng.uniform(-0.006, 0.006)
            change_pct = round(phase["market_bias"] + sector_alpha + noise * beta_k * 100, 4)
            last_price = round(max(prev * (1 + change_pct), sec["price"] * 0.78), 2)
            volume = int(rng.randint(1200, 4800) * phase["volume_mul"] * (1 + sec["beta"] * 0.25))
            turnover = round(last_price * volume * 100, 2)
            net_inflow = round(turnover * (change_pct * 2.8 + rng.uniform(-0.05, 0.08)), 2)
            market_rows.append((
                ts, sec["symbol"], sec["name"], sec["sector"], last_price, round(change_pct * 100, 2),
                volume, turnover, net_inflow, phase["name"],
            ))
            price_map[sec["symbol"]] = last_price

        await execute_many(
            f"""INSERT INTO {MARKET_TABLE}
                (ts, symbol, security_name, sector_name, last_price, change_pct, volume_lot, turnover_amt, net_inflow_amt, phase_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            market_rows
        )

        accounts = await self._latest_accounts()
        positions = await self._latest_positions()
        if not accounts:
            await self.init_table()
            accounts = await self._latest_accounts()
            positions = await self._latest_positions()
        acct_map = {a["account_id"]: dict(a) for a in accounts}
        pos_map = {(p["account_id"], p["symbol"]): dict(p) for p in positions}

        trade_rows = []
        today_trade_cnt = {aid: int(acct_map[aid].get("trade_count_today", 0) or 0) for aid in acct_map}
        today_turnover = {aid: float(acct_map[aid].get("turnover_today", 0) or 0) for aid in acct_map}
        trade_total = int(24 * phase["volume_mul"]) + rng.randint(6, 14)

        for seq in range(trade_total):
            aid = rng.choice(list(acct_map.keys()))
            acct = acct_map[aid]
            branch_id = acct["branch_id"]
            branch_name = acct["branch_name"]
            symbol = rng.choice(SYMBOLS)["symbol"]
            sec = symbol_map[symbol]
            key = (aid, symbol)
            pos = pos_map.get(key)
            target_buy = rng.random() < phase["buy_bias"]
            if not pos or int(pos.get("qty", 0) or 0) < 200:
                side = "买入"
            else:
                side = "买入" if target_buy else "卖出"
            qty = rng.choice([100, 200, 300, 500, 800, 1000]) * max(1, _tier_rank(acct["client_tier"]) - 1)
            price = round(price_map[symbol] * rng.uniform(0.998, 1.002), 2)
            amount = round(price * qty, 2)
            fee = max(5.0, round(amount * 0.00028, 2))
            if side == "卖出" and pos:
                qty = min(qty, int(pos["qty"] // 100 * 100))
                if qty < 100:
                    side = "买入"
                    qty = 100
                    amount = round(price * qty, 2)
                    fee = max(5.0, round(amount * 0.00028, 2))
            trade_rows.append((
                f"T{step:03d}{seq:03d}{aid[-2:]}{symbol[-2:]}", ts, aid, acct["client_name"], branch_id, branch_name,
                acct["rm_name"], symbol, sec["name"], sec["sector"], side, price, qty, amount, fee,
                rng.choice(["APP", "投顾终端", "柜台"]), phase["name"],
            ))
            today_trade_cnt[aid] += 1
            today_turnover[aid] += amount
            acct["cash_amt"] = float(acct.get("cash_amt", 0) or 0)

            if key not in pos_map:
                pos_map[key] = {
                    "account_id": aid, "symbol": symbol, "security_name": sec["name"], "sector_name": sec["sector"],
                    "branch_id": branch_id, "branch_name": branch_name, "qty": 0, "available_qty": 0,
                    "avg_cost": price, "last_price": price, "market_value": 0.0, "cost_amount": 0.0,
                    "unrealized_pnl": 0.0, "pnl_pct": 0.0, "weight_pct": 0.0, "update_ts": ts,
                }
            pos = pos_map[key]
            old_qty = int(pos.get("qty", 0) or 0)
            old_cost = float(pos.get("cost_amount", 0) or 0)
            if side == "买入":
                new_qty = old_qty + qty
                new_cost = old_cost + amount + fee
                acct["cash_amt"] -= amount + fee
            else:
                sell_cost = old_cost * qty / max(old_qty, 1)
                new_qty = max(0, old_qty - qty)
                new_cost = max(0.0, old_cost - sell_cost)
                acct["cash_amt"] += amount - fee
            pos["qty"] = new_qty
            pos["available_qty"] = new_qty
            pos["cost_amount"] = round(new_cost, 2)
            pos["avg_cost"] = round(new_cost / new_qty, 4) if new_qty else 0.0
            pos["last_price"] = price_map[symbol]
            pos["update_ts"] = ts

        await execute_many(
            f"""INSERT INTO {TRADE_TABLE}
                (trade_id, ts, account_id, client_name, branch_id, branch_name, rm_name, symbol, security_name,
                 sector_name, side, price, qty, amount, fee, channel, phase_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            trade_rows
        )

        pos_rows = []
        account_rows = []
        for aid, acct in acct_map.items():
            acct_positions = [p for (account_id, _), p in pos_map.items() if account_id == aid and int(p.get("qty", 0) or 0) > 0]
            total_mv = 0.0
            total_pnl = 0.0
            for pos in acct_positions:
                last_price = float(price_map.get(pos["symbol"], pos["last_price"]))
                market_value = round(last_price * int(pos["qty"]), 2)
                pnl = round(market_value - float(pos.get("cost_amount", 0) or 0), 2)
                pos["last_price"] = last_price
                pos["market_value"] = market_value
                pos["unrealized_pnl"] = pnl
                pos["pnl_pct"] = round(pnl / max(float(pos.get("cost_amount", 0) or 1), 1) * 100, 2)
                total_mv += market_value
                total_pnl += pnl
            total_asset = round(float(acct["cash_amt"]) + total_mv, 2)
            for pos in acct_positions:
                pos["weight_pct"] = round(pos["market_value"] / total_asset * 100, 2) if total_asset else 0.0
                pos_rows.append((
                    pos["account_id"], pos["symbol"], pos["security_name"], pos["sector_name"], pos["branch_id"], pos["branch_name"],
                    int(pos["qty"]), int(pos["available_qty"]), float(pos["avg_cost"]), float(pos["last_price"]),
                    float(pos["market_value"]), float(pos["cost_amount"]), float(pos["unrealized_pnl"]),
                    float(pos["pnl_pct"]), float(pos["weight_pct"]), ts,
                ))
            concentration = max((p["weight_pct"] for p in acct_positions), default=0.0)
            margin_debt = float(acct.get("margin_debt", 0) or 0)
            if margin_debt > 0:
                margin_debt = round(max(50000.0, margin_debt * (1 + rng.uniform(-0.01, 0.015))), 2)
            maintenance = round(total_asset / max(margin_debt, 1) * 100, 2) if margin_debt > 0 else 999.0
            account_rows.append((
                aid, acct["client_name"], acct["branch_id"], acct["branch_name"], acct["rm_name"], acct["client_tier"], int(acct["risk_level"]),
                total_asset, round(float(acct["cash_amt"]), 2), round(total_mv, 2), round(total_pnl, 2),
                round(total_pnl / max(total_asset - float(acct["cash_amt"]), 1) * 100, 2) if total_mv > 0 else 0.0,
                margin_debt, maintenance, concentration, len(acct_positions), today_trade_cnt[aid], round(today_turnover[aid], 2), ts, ts,
            ))

        await execute_many(
            f"""INSERT INTO {POSITION_TABLE}
                (account_id, symbol, security_name, sector_name, branch_id, branch_name, qty, available_qty,
                 avg_cost, last_price, market_value, cost_amount, unrealized_pnl, pnl_pct, weight_pct, update_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            pos_rows
        )
        await execute_many(
            f"""INSERT INTO {ACCOUNT_TABLE}
                (account_id, client_name, branch_id, branch_name, rm_name, client_tier, risk_level, total_asset,
                 cash_amt, market_value, unrealized_pnl, pnl_pct, margin_debt, maintenance_ratio, concentration_pct,
                 position_count, trade_count_today, turnover_today, last_trade_ts, update_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            account_rows
        )
        await self._refresh_branch_metrics(ts, phase["name"])
        await self._refresh_risks(ts)
        return {
            "success": True,
            "step": step + 1,
            "ts": ts,
            "phase": phase["name"],
            "msg": f"已推进至 {phase['name']}，成交、持仓、两融风险同步刷新",
        }

    async def _refresh_branch_metrics(self, ts: str, phase_name: str):
        rows = await execute_query(f"""
            SELECT a.branch_id, a.branch_name,
                   ROUND(SUM(t.amount), 2) AS turnover_amt,
                   ROUND(SUM(t.fee), 2) AS commission_amt,
                   ROUND(SUM(CASE WHEN t.side='买入' THEN t.amount ELSE 0 END), 2) AS buy_amt,
                   ROUND(SUM(CASE WHEN t.side='卖出' THEN t.amount ELSE 0 END), 2) AS sell_amt,
                   COUNT(DISTINCT t.account_id) AS active_clients,
                   SUM(CASE WHEN a.margin_debt > 0 THEN 1 ELSE 0 END) AS margin_clients,
                   ROUND(AVG(a.maintenance_ratio), 2) AS avg_maintenance
            FROM {TRADE_TABLE} t
            JOIN {ACCOUNT_TABLE} a ON t.account_id = a.account_id
            WHERE t.ts = '{ts}'
            GROUP BY a.branch_id, a.branch_name
        """)
        metric_rows = []
        rows_map = {r["branch_id"]: r for r in rows}
        for br in BRANCHES:
            r = rows_map.get(br["id"], {})
            buy_amt = float(r.get("buy_amt", 0) or 0)
            sell_amt = float(r.get("sell_amt", 0) or 0)
            metric_rows.append((
                ts, br["id"], br["name"],
                float(r.get("turnover_amt", 0) or 0), float(r.get("commission_amt", 0) or 0),
                buy_amt, sell_amt, round(buy_amt - sell_amt, 2),
                int(r.get("active_clients", 0) or 0), int(r.get("margin_clients", 0) or 0),
                float(r.get("avg_maintenance", 0) or 0), phase_name,
            ))
        await execute_many(
            f"""INSERT INTO {BRANCH_TABLE}
                (ts, branch_id, branch_name, turnover_amt, commission_amt, buy_amt, sell_amt,
                 net_inflow_amt, active_clients, margin_clients, avg_maintenance, phase_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            metric_rows
        )

    async def _refresh_risks(self, ts: str):
        accounts = await execute_query(f"SELECT * FROM {ACCOUNT_TABLE}")
        positions = await execute_query(f"SELECT * FROM {POSITION_TABLE}")
        pos_by_account = {}
        for p in positions:
            pos_by_account.setdefault(p["account_id"], []).append(p)
        risk_rows = []
        for acct in accounts:
            acct_positions = pos_by_account.get(acct["account_id"], [])
            top_pos = max(acct_positions, key=lambda x: float(x.get("weight_pct", 0) or 0), default=None)
            maintenance = float(acct.get("maintenance_ratio", 999) or 999)
            concentration = float(acct.get("concentration_pct", 0) or 0)
            pnl_pct = float(acct.get("pnl_pct", 0) or 0)
            margin_status = "ACTIVE" if acct.get("margin_debt", 0) and maintenance < 165 else "RESOLVED"
            margin_level = "高" if maintenance < 145 else "中" if maintenance < 165 else "低"
            risk_rows.append((
                f"{acct['account_id']}_margin", acct["account_id"], acct["client_name"], acct["branch_id"], acct["branch_name"],
                "两融维保预警", margin_level, maintenance, 165.0, top_pos["symbol"] if top_pos else "",
                top_pos["security_name"] if top_pos else "", "建议降低融资占用或补足担保品", margin_status, ts,
            ))
            conc_status = "ACTIVE" if concentration >= 38 else "RESOLVED"
            conc_level = "高" if concentration >= 52 else "中" if concentration >= 38 else "低"
            risk_rows.append((
                f"{acct['account_id']}_concentration", acct["account_id"], acct["client_name"], acct["branch_id"], acct["branch_name"],
                "单票集中度", conc_level, concentration, 38.0, top_pos["symbol"] if top_pos else "",
                top_pos["security_name"] if top_pos else "", "建议分散至低相关板块，控制单票仓位", conc_status, ts,
            ))
            dd_status = "ACTIVE" if pnl_pct <= -7 else "RESOLVED"
            dd_level = "高" if pnl_pct <= -12 else "中" if pnl_pct <= -7 else "低"
            risk_rows.append((
                f"{acct['account_id']}_drawdown", acct["account_id"], acct["client_name"], acct["branch_id"], acct["branch_name"],
                "客户回撤异常", dd_level, pnl_pct, -7.0, top_pos["symbol"] if top_pos else "",
                top_pos["security_name"] if top_pos else "", "建议投顾回访并复核止损纪律", dd_status, ts,
            ))
        await execute_many(
            f"""INSERT INTO {RISK_TABLE}
                (alert_id, account_id, client_name, branch_id, branch_name, alert_type, risk_level, metric_value,
                 threshold_value, position_symbol, position_name, suggestion, status, update_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            risk_rows
        )

    async def batch_generate(self, steps: int = 6):
        steps = max(1, min(int(steps or 1), 24))
        last = {}
        for _ in range(steps):
            last = await self.generate_step()
        return {
            "success": True,
            "steps_generated": steps,
            "step": last.get("step", 0),
            "ts": last.get("ts", ""),
            "phase": last.get("phase", ""),
            "msg": f"已批量推进 {steps} 个时间片",
        }

    async def reset(self):
        await self.init_table()
        return {"success": True, "msg": "证券沙盘已重置到开盘前基线"}

    async def get_overview(self):
        row = await execute_query(f"""
            SELECT
                COUNT(*) AS account_cnt,
                ROUND(SUM(total_asset), 2) AS total_aum,
                ROUND(SUM(market_value), 2) AS total_mv,
                ROUND(SUM(turnover_today), 2) AS turnover_today,
                SUM(trade_count_today) AS trade_count,
                ROUND(SUM(turnover_today) * 0.00028, 2) AS commission_income,
                ROUND(AVG(maintenance_ratio), 2) AS avg_maintenance,
                SUM(CASE WHEN margin_debt > 0 THEN 1 ELSE 0 END) AS margin_account_cnt,
                SUM(CASE WHEN concentration_pct >= 38 THEN 1 ELSE 0 END) AS concentration_alert_cnt
            FROM {ACCOUNT_TABLE}
        """)
        if not row:
            return {"empty": True}
        risk = await execute_query(f"SELECT COUNT(*) AS cnt FROM {RISK_TABLE} WHERE status='ACTIVE'")
        branch = await execute_query(f"""
            SELECT branch_name, turnover_amt
            FROM {BRANCH_TABLE}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY ts DESC) = 1
            ORDER BY turnover_amt DESC
            LIMIT 1
        """)
        sector = await execute_query(f"""
            SELECT sector_name, ROUND(SUM(net_inflow_amt), 2) AS net_inflow
            FROM {MARKET_TABLE}
            GROUP BY sector_name
            ORDER BY net_inflow DESC
            LIMIT 1
        """)
        last = await execute_query(f"SELECT MAX(ts) AS last_ts, MAX(phase_name) AS phase_name FROM {MARKET_TABLE}")
        tier_dist = await execute_query(f"""
            SELECT client_tier, COUNT(*) AS cnt
            FROM {ACCOUNT_TABLE}
            GROUP BY client_tier
            ORDER BY COUNT(*) DESC
        """)
        return {
            **row[0],
            "risk_alert_cnt": int(risk[0]["cnt"]) if risk else 0,
            "top_branch": branch[0]["branch_name"] if branch else "-",
            "hot_sector": sector[0]["sector_name"] if sector else "-",
            "hot_sector_inflow": sector[0]["net_inflow"] if sector else 0,
            "last_ts": last[0]["last_ts"] if last else None,
            "phase_name": last[0]["phase_name"] if last else "开盘前",
            "tier_dist": tier_dist,
            "empty": False,
        }

    async def get_trend(self):
        return await execute_query(f"""
            SELECT ts,
                   ROUND(SUM(turnover_amt)/100000000, 2) AS turnover_bn,
                   ROUND(SUM(commission_amt)/10000, 2) AS commission_wan,
                   SUM(active_clients) AS active_clients,
                   ROUND(AVG(avg_maintenance), 2) AS avg_maintenance
            FROM {BRANCH_TABLE}
            GROUP BY ts
            ORDER BY ts
            LIMIT 120
        """)

    async def get_trades(self, limit: int = 60):
        limit = max(20, min(limit, 200))
        return await execute_query(f"""
            SELECT trade_id, ts, account_id, client_name, branch_name, rm_name,
                   symbol, security_name, sector_name, side, price, qty, amount, fee, channel, phase_name
            FROM {TRADE_TABLE}
            ORDER BY ts DESC, amount DESC
            LIMIT {limit}
        """)

    async def get_accounts(self):
        return await execute_query(f"""
            SELECT account_id, client_name, branch_name, rm_name, client_tier, risk_level,
                   ROUND(total_asset/10000, 2) AS total_asset_wan,
                   ROUND(market_value/10000, 2) AS market_value_wan,
                   ROUND(cash_amt/10000, 2) AS cash_amt_wan,
                   ROUND(unrealized_pnl/10000, 2) AS unrealized_pnl_wan,
                   pnl_pct, maintenance_ratio, concentration_pct, position_count, trade_count_today,
                   ROUND(turnover_today/10000, 2) AS turnover_today_wan, update_ts
            FROM {ACCOUNT_TABLE}
            ORDER BY total_asset DESC
            LIMIT 30
        """)

    async def get_positions(self):
        sector_stats = await execute_query(f"""
            SELECT sector_name,
                   ROUND(SUM(market_value)/10000, 2) AS market_value_wan,
                   ROUND(SUM(unrealized_pnl)/10000, 2) AS pnl_wan,
                   ROUND(AVG(weight_pct), 2) AS avg_weight_pct,
                   COUNT(*) AS holding_accounts
            FROM {POSITION_TABLE}
            WHERE qty > 0
            GROUP BY sector_name
            ORDER BY market_value_wan DESC
        """)
        top_positions = await execute_query(f"""
            SELECT account_id, branch_name, symbol, security_name, sector_name,
                   qty, ROUND(last_price, 2) AS last_price,
                   ROUND(market_value/10000, 2) AS market_value_wan,
                   ROUND(unrealized_pnl/10000, 2) AS pnl_wan,
                   pnl_pct, weight_pct, update_ts
            FROM {POSITION_TABLE}
            WHERE qty > 0
            ORDER BY market_value DESC
            LIMIT 30
        """)
        return {"sector_stats": sector_stats, "top_positions": top_positions}

    async def get_sector_heat(self):
        return await execute_query(f"""
            SELECT sector_name,
                   ROUND(AVG(change_pct), 2) AS avg_change_pct,
                   ROUND(SUM(turnover_amt)/100000000, 2) AS turnover_bn,
                   ROUND(SUM(net_inflow_amt)/100000000, 2) AS net_inflow_bn
            FROM {MARKET_TABLE}
            GROUP BY sector_name
            ORDER BY net_inflow_bn DESC, avg_change_pct DESC
        """)

    async def get_risk_alerts(self):
        return await execute_query(f"""
            SELECT alert_id, account_id, client_name, branch_name, alert_type, risk_level,
                   metric_value, threshold_value, position_symbol, position_name, suggestion, update_ts
            FROM {RISK_TABLE}
            WHERE status = 'ACTIVE'
            ORDER BY
                CASE risk_level WHEN '高' THEN 3 WHEN '中' THEN 2 ELSE 1 END DESC,
                update_ts DESC
            LIMIT 50
        """)

    async def get_branches(self):
        return await execute_query(f"""
            SELECT branch_id, branch_name, turnover_amt, commission_amt, buy_amt, sell_amt, net_inflow_amt,
                   active_clients, margin_clients, avg_maintenance, phase_name, ts
            FROM {BRANCH_TABLE}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY ts DESC) = 1
            ORDER BY turnover_amt DESC
        """)
