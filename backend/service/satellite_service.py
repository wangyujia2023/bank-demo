"""卫星军工数据查询分析服务"""
import random
import math
from datetime import datetime, timedelta
from typing import Optional
from backend.doris.connect import execute_query, execute_one, execute_write

# ── DDL ─────────────────────────────────────────────────────────────────────
_DDL = [
    """
    CREATE TABLE IF NOT EXISTS satellite_info (
        satellite_id   INT           NOT NULL,
        satellite_name VARCHAR(50)   NOT NULL,
        satellite_type VARCHAR(20),
        orbit_type     VARCHAR(10),
        launch_date    DATE,
        status         VARCHAR(10),
        operator       VARCHAR(60),
        country        VARCHAR(20),
        altitude_km    FLOAT,
        inclination_deg FLOAT,
        period_min     FLOAT,
        payload_type   VARCHAR(60),
        mass_kg        FLOAT
    ) DUPLICATE KEY(satellite_id)
    DISTRIBUTED BY HASH(satellite_id) BUCKETS 4
    PROPERTIES ("replication_num" = "1")
    """,
    """
    CREATE TABLE IF NOT EXISTS satellite_task (
        task_id        BIGINT        NOT NULL,
        satellite_id   INT,
        task_type      VARCHAR(20),
        task_time      DATETIME,
        target_area    VARCHAR(50),
        priority       INT,
        status         VARCHAR(10),
        duration_min   INT,
        data_volume_gb FLOAT,
        resolution_m   FLOAT,
        coverage_km2   FLOAT
    ) DUPLICATE KEY(task_id)
    DISTRIBUTED BY HASH(task_id) BUCKETS 8
    PROPERTIES ("replication_num" = "1")
    """,
    """
    CREATE TABLE IF NOT EXISTS satellite_telemetry (
        record_id         BIGINT   NOT NULL,
        satellite_id      INT,
        record_time       DATETIME,
        battery_pct       FLOAT,
        solar_power_w     FLOAT,
        cpu_temp_c        FLOAT,
        signal_strength_db FLOAT,
        orbit_altitude_km FLOAT,
        attitude_roll     FLOAT,
        attitude_pitch    FLOAT,
        attitude_yaw      FLOAT,
        anomaly_flag      INT DEFAULT 0
    ) DUPLICATE KEY(record_id)
    DISTRIBUTED BY HASH(record_id) BUCKETS 8
    PROPERTIES ("replication_num" = "1")
    """,
    """
    CREATE TABLE IF NOT EXISTS ground_station (
        station_id        INT          NOT NULL,
        station_name      VARCHAR(50),
        location          VARCHAR(50),
        latitude          FLOAT,
        longitude         FLOAT,
        station_type      VARCHAR(20),
        status            VARCHAR(10),
        antenna_count     INT,
        coverage_radius_km INT,
        daily_contacts    INT,
        uptime_pct        FLOAT
    ) DUPLICATE KEY(station_id)
    DISTRIBUTED BY HASH(station_id) BUCKETS 1
    PROPERTIES ("replication_num" = "1")
    """,
]

# ── 静态参考数据 ──────────────────────────────────────────────────────────────
_SATELLITES = [
    (1,  "天眼-1",   "遥感",  "LEO", "2020-03-15", "在轨", "国家航天局", "中国",  500,  97.4, 94.7,  "光学成像",    1200),
    (2,  "天眼-2",   "遥感",  "LEO", "2021-06-20", "在轨", "国家航天局", "中国",  520,  97.6, 95.1,  "SAR雷达",     1350),
    (3,  "北斗-3G1", "导航",  "GEO", "2018-11-01", "在轨", "北斗系统",   "中国",  35786, 0.0, 1436.1,"导航信号",    4500),
    (4,  "北斗-3M1", "导航",  "MEO", "2019-04-20", "在轨", "北斗系统",   "中国",  21528, 55.0, 760.5, "导航信号",    980),
    (5,  "北斗-3M2", "导航",  "MEO", "2019-04-20", "在轨", "北斗系统",   "中国",  21528, 55.0, 760.5, "导航信号",    980),
    (6,  "通信-A1",  "通信",  "GEO", "2017-08-05", "在轨", "中国卫通",   "中国",  35786, 0.1, 1436.1,"Ka/Ku频段",   5200),
    (7,  "通信-A2",  "通信",  "GEO", "2019-12-10", "在轨", "中国卫通",   "中国",  35786, 0.2, 1436.1,"宽带通信",    5400),
    (8,  "气象-FY4", "气象",  "GEO", "2016-12-11", "在轨", "气象局",     "中国",  35786, 0.0, 1436.1,"多光谱成像",  5400),
    (9,  "气象-FY3", "气象",  "LEO", "2017-11-15", "在轨", "气象局",     "中国",  836,  98.75, 101.5,"微波探测",    2250),
    (10, "海洋-HY2", "遥感",  "LEO", "2018-10-25", "在轨", "国家海洋局", "中国",  971,  99.3,  104.3,"微波散射",    1575),
    (11, "资源-ZY5", "遥感",  "LEO", "2021-09-26", "在轨", "国家测绘局", "中国",  506,  97.5,  94.9, "高光谱",      1100),
    (12, "预警-GJ1", "预警",  "GEO", "2015-07-23", "在轨", "军方",       "中国",  35786, 0.0, 1436.1,"红外探测",    4800),
    (13, "侦察-JB1", "侦察",  "LEO", "2022-01-17", "在轨", "军方",       "中国",  490,  97.3,  94.5, "高分光学",    850),
    (14, "侦察-JB2", "侦察",  "LEO", "2022-08-04", "在轨", "军方",       "中国",  480,  97.2,  94.3, "SAR+光学",    900),
    (15, "中继-TH1", "中继",  "HEO", "2016-08-06", "在轨", "航天科技",   "中国",  2000, 63.0,  127.5,"数据中继",    3000),
    (16, "天眼-3",   "遥感",  "LEO", "2023-05-21", "在轨", "国家航天局", "中国",  510,  97.5,  95.0, "高分SAR",     1450),
    (17, "导弹预警1","预警",  "GEO", "2014-03-31", "故障", "军方",       "中国",  35786, 0.0, 1436.1,"红外阵列",    4200),
    (18, "科学-KX1", "科学",  "LEO", "2023-10-15", "在轨", "科学院",     "中国",  550,  43.0,  95.8, "空间科学",    650),
]

_STATIONS = [
    (1, "北京航天飞控中心", "北京",  39.9,  116.4, "指挥中心", "正常", 12, 5000, 48, 99.8),
    (2, "西安卫星测控中心", "西安",  34.3,  108.9, "测控",     "正常",  8, 3000, 32, 99.5),
    (3, "喀什测控站",       "新疆",  39.5,  75.9,  "测控",     "正常",  6, 2500, 28, 98.9),
    (4, "三亚数据接收站",   "三亚",  18.3,  109.5, "数据接收", "正常",  4, 2000, 22, 99.1),
    (5, "密云数据接收站",   "密云",  40.4,  116.8, "数据接收", "维护",  3, 1500, 18, 85.0),
    (6, "长春光机所",       "长春",  43.9,  125.3, "数据接收", "正常",  5, 2200, 24, 98.7),
    (7, "南极中山站",       "南极", -69.4,  76.4,  "测控",     "正常",  2, 1800, 12, 97.5),
    (8, "纳米比亚测控站",   "纳米比亚",-22.6, 17.1, "测控",    "正常",  2, 1500, 10, 96.8),
]

_TASK_TYPES  = ["成像侦察", "通信中继", "导航定位", "气象探测", "海洋监测", "预警探测", "科学实验", "轨道维护"]
_AREAS       = ["东海区域", "南海区域", "西太平洋", "印度洋",   "中东地区", "欧洲北部", "北极区域", "非洲东部",
                "东南亚",   "朝鲜半岛", "台湾海峡", "波斯湾",   "黑海区域", "日本列岛", "菲律宾海"]


class SatelliteService:

    async def init_tables(self):
        """建表 + 插入模拟数据（幂等）"""
        results = []

        # 建表
        for ddl in _DDL:
            try:
                await execute_write(ddl.strip())
                results.append("table ok")
            except Exception as e:
                results.append(f"table err: {e}")

        # 检查是否已有数据
        row = await execute_one("SELECT COUNT(*) AS cnt FROM satellite_info")
        if row and int(row.get("cnt") or 0) > 0:
            return {"status": "already_initialized", "results": results}

        # 插入卫星基础信息
        for s in _SATELLITES:
            sql = (
                f"INSERT INTO satellite_info VALUES "
                f"({s[0]},'{s[1]}','{s[2]}','{s[3]}','{s[4]}','{s[5]}','{s[6]}',"
                f"'{s[7]}',{s[8]},{s[9]},{s[10]},'{s[11]}',{s[12]})"
            )
            try:
                await execute_write(sql)
            except Exception as e:
                results.append(f"satellite insert err: {e}")

        # 插入地面站
        for st in _STATIONS:
            sql = (
                f"INSERT INTO ground_station VALUES "
                f"({st[0]},'{st[1]}','{st[2]}',{st[3]},{st[4]},'{st[5]}',"
                f"'{st[6]}',{st[7]},{st[8]},{st[9]},{st[10]})"
            )
            try:
                await execute_write(sql)
            except Exception as e:
                results.append(f"station insert err: {e}")

        # 生成任务数据（近60天，~1200条）
        rng = random.Random(42)
        now = datetime.now()
        task_id = 1
        active_sats = [s for s in _SATELLITES if s[5] == "在轨"]
        task_rows = []
        for day in range(60, 0, -1):
            base_dt = now - timedelta(days=day)
            daily_tasks = rng.randint(15, 25)
            for _ in range(daily_tasks):
                sat = rng.choice(active_sats)
                sat_id = sat[0]
                task_type = rng.choice(_TASK_TYPES)
                area = rng.choice(_AREAS)
                priority = rng.choices([1, 2, 3, 4, 5], weights=[5, 15, 40, 30, 10])[0]
                hour = rng.randint(0, 23)
                minute = rng.randint(0, 59)
                t = base_dt.replace(hour=hour, minute=minute, second=0)
                duration = rng.randint(5, 120)
                status = rng.choices(["已完成", "已完成", "已完成", "异常", "执行中"],
                                     weights=[70, 10, 5, 10, 5])[0]
                data_vol = round(rng.uniform(0.5, 80.0), 2) if task_type in ("成像侦察", "气象探测") else round(rng.uniform(0.1, 5.0), 2)
                res = round(rng.uniform(0.1, 5.0), 2) if task_type == "成像侦察" else 0.0
                coverage = round(rng.uniform(500, 50000), 0) if res > 0 else 0.0
                task_rows.append(
                    f"({task_id},{sat_id},'{task_type}','{t.strftime('%Y-%m-%d %H:%M:%S')}',"
                    f"'{area}',{priority},'{status}',{duration},{data_vol},{res},{coverage})"
                )
                task_id += 1

        # 批量插入任务（每批200）
        for i in range(0, len(task_rows), 200):
            batch = task_rows[i:i+200]
            sql = "INSERT INTO satellite_task VALUES " + ",".join(batch)
            try:
                await execute_write(sql)
            except Exception as e:
                results.append(f"task batch err: {e}")

        # 生成遥测数据（最近7天，每颗在轨卫星每小时一条）
        record_id = 1
        tele_rows = []
        active_sats_full = [s for s in _SATELLITES if s[5] == "在轨"]
        for sat in active_sats_full:
            sat_id = sat[0]
            base_alt = float(sat[8])
            rng2 = random.Random(sat_id * 31)
            for h in range(7 * 24, 0, -1):
                t = now - timedelta(hours=h)
                hour_phase = math.sin(2 * math.pi * h / 24)
                battery = round(max(20, min(100, 75 + 20 * hour_phase + rng2.uniform(-5, 5))), 1)
                solar = round(max(0, 350 + 200 * max(0, hour_phase) + rng2.uniform(-30, 30)), 1)
                cpu_temp = round(25 + rng2.uniform(-3, 8) + (1 if battery < 40 else 0) * 5, 1)
                signal = round(-80 + rng2.uniform(-10, 15), 1)
                alt = round(base_alt + rng2.uniform(-2, 2), 1)
                roll  = round(rng2.uniform(-1.5, 1.5), 3)
                pitch = round(rng2.uniform(-1.5, 1.5), 3)
                yaw   = round(rng2.uniform(-2.0, 2.0), 3)
                anomaly = 1 if (battery < 30 or cpu_temp > 45 or signal < -90) else 0
                tele_rows.append(
                    f"({record_id},{sat_id},'{t.strftime('%Y-%m-%d %H:%M:%S')}',"
                    f"{battery},{solar},{cpu_temp},{signal},{alt},{roll},{pitch},{yaw},{anomaly})"
                )
                record_id += 1

        for i in range(0, len(tele_rows), 500):
            batch = tele_rows[i:i+500]
            sql = "INSERT INTO satellite_telemetry VALUES " + ",".join(batch)
            try:
                await execute_write(sql)
            except Exception as e:
                results.append(f"tele batch err: {e}")

        return {"status": "initialized", "tasks": task_id - 1, "telemetry": record_id - 1, "results": results}

    # ── 概况 ──────────────────────────────────────────────────────────────────
    async def overview(self):
        stats = await execute_one("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status='在轨' THEN 1 ELSE 0 END) AS active,
                   SUM(CASE WHEN status='故障' THEN 1 ELSE 0 END) AS fault,
                   COUNT(DISTINCT satellite_type) AS type_count
            FROM satellite_info
        """)
        task_today = await execute_one("""
            SELECT COUNT(*) AS cnt,
                   SUM(CASE WHEN status='异常' THEN 1 ELSE 0 END) AS err
            FROM satellite_task
            WHERE DATE(task_time) = CURDATE()
        """)
        task_week = await execute_one("""
            SELECT COUNT(*) AS cnt,
                   ROUND(SUM(data_volume_gb),1) AS total_gb
            FROM satellite_task
            WHERE task_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)
        type_dist = await execute_query("""
            SELECT satellite_type, COUNT(*) AS cnt
            FROM satellite_info GROUP BY satellite_type ORDER BY cnt DESC
        """)
        orbit_dist = await execute_query("""
            SELECT orbit_type, COUNT(*) AS cnt
            FROM satellite_info GROUP BY orbit_type ORDER BY cnt DESC
        """)
        status_dist = await execute_query("""
            SELECT status, COUNT(*) AS cnt
            FROM satellite_info GROUP BY status
        """)
        anomaly_count = await execute_one("""
            SELECT COUNT(DISTINCT satellite_id) AS cnt
            FROM satellite_telemetry
            WHERE anomaly_flag=1 AND record_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """)
        station_ok = await execute_one("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status='正常' THEN 1 ELSE 0 END) AS ok
            FROM ground_station
        """)
        return {
            "stats": {
                **(stats or {}),
                "today_tasks":  int((task_today or {}).get("cnt") or 0),
                "today_errors": int((task_today or {}).get("err") or 0),
                "week_tasks":   int((task_week or {}).get("cnt") or 0),
                "week_data_gb": float((task_week or {}).get("total_gb") or 0),
                "anomaly_sats": int((anomaly_count or {}).get("cnt") or 0),
                "station_ok":   int((station_ok or {}).get("ok") or 0),
                "station_total":int((station_ok or {}).get("total") or 0),
            },
            "type_dist":   type_dist,
            "orbit_dist":  orbit_dist,
            "status_dist": status_dist,
        }

    # ── 卫星列表 ──────────────────────────────────────────────────────────────
    async def satellite_list(
        self,
        satellite_type: Optional[str] = None,
        orbit_type:     Optional[str] = None,
        status:         Optional[str] = None,
    ):
        conds = ["1=1"]
        if satellite_type: conds.append(f"satellite_type = '{satellite_type}'")
        if orbit_type:     conds.append(f"orbit_type = '{orbit_type}'")
        if status:         conds.append(f"status = '{status}'")
        where = " AND ".join(conds)
        rows = await execute_query(f"""
            SELECT s.*,
                   t.task_cnt, t.last_task,
                   tl.latest_battery, tl.latest_signal
            FROM satellite_info s
            LEFT JOIN (
                SELECT satellite_id,
                       COUNT(*) AS task_cnt,
                       MAX(task_time) AS last_task
                FROM satellite_task GROUP BY satellite_id
            ) t ON s.satellite_id = t.satellite_id
            LEFT JOIN (
                SELECT tele.satellite_id,
                       tele.battery_pct       AS latest_battery,
                       tele.signal_strength_db AS latest_signal
                FROM satellite_telemetry tele
                INNER JOIN (
                    SELECT satellite_id, MAX(record_time) AS max_time
                    FROM satellite_telemetry GROUP BY satellite_id
                ) mx ON tele.satellite_id = mx.satellite_id
                     AND tele.record_time  = mx.max_time
            ) tl ON s.satellite_id = tl.satellite_id
            WHERE {where}
            ORDER BY s.satellite_id
        """)
        return {"rows": rows, "total": len(rows)}

    # ── 任务分析 ──────────────────────────────────────────────────────────────
    async def task_analysis(self):
        by_type = await execute_query("""
            SELECT task_type,
                   COUNT(*) AS cnt,
                   ROUND(AVG(duration_min),1) AS avg_duration,
                   ROUND(SUM(data_volume_gb),1) AS total_gb,
                   SUM(CASE WHEN status='异常' THEN 1 ELSE 0 END) AS err_cnt
            FROM satellite_task GROUP BY task_type ORDER BY cnt DESC
        """)
        by_priority = await execute_query("""
            SELECT priority,
                   COUNT(*) AS cnt,
                   SUM(CASE WHEN status='异常' THEN 1 ELSE 0 END) AS err_cnt
            FROM satellite_task GROUP BY priority ORDER BY priority
        """)
        daily_trend = await execute_query("""
            SELECT DATE(task_time) AS dt,
                   COUNT(*) AS cnt,
                   SUM(CASE WHEN status='异常' THEN 1 ELSE 0 END) AS err_cnt,
                   ROUND(SUM(data_volume_gb),1) AS total_gb
            FROM satellite_task
            WHERE task_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY DATE(task_time) ORDER BY dt
        """)
        by_area = await execute_query("""
            SELECT target_area,
                   COUNT(*) AS cnt,
                   SUM(CASE WHEN priority >= 4 THEN 1 ELSE 0 END) AS high_priority
            FROM satellite_task GROUP BY target_area ORDER BY cnt DESC LIMIT 12
        """)
        by_sat = await execute_query("""
            SELECT s.satellite_name, s.satellite_type,
                   COUNT(t.task_id) AS task_cnt,
                   SUM(CASE WHEN t.status='异常' THEN 1 ELSE 0 END) AS err_cnt,
                   ROUND(SUM(t.data_volume_gb),1) AS total_gb
            FROM satellite_info s
            LEFT JOIN satellite_task t ON s.satellite_id = t.satellite_id
            WHERE s.status = '在轨'
            GROUP BY s.satellite_name, s.satellite_type
            ORDER BY task_cnt DESC
        """)
        return {
            "by_type":     by_type,
            "by_priority": by_priority,
            "daily_trend": daily_trend,
            "by_area":     by_area,
            "by_sat":      by_sat,
        }

    # ── 遥测监控 ──────────────────────────────────────────────────────────────
    async def telemetry_latest(self):
        """每颗卫星最新一条遥测 + 最近24h平均"""
        latest = await execute_query("""
            SELECT t.satellite_id, s.satellite_name, s.satellite_type,
                   t.record_time, t.battery_pct, t.solar_power_w,
                   t.cpu_temp_c, t.signal_strength_db,
                   t.orbit_altitude_km, t.anomaly_flag
            FROM satellite_telemetry t
            INNER JOIN (
                SELECT satellite_id, MAX(record_time) AS max_time
                FROM satellite_telemetry GROUP BY satellite_id
            ) mx ON t.satellite_id = mx.satellite_id
                 AND t.record_time  = mx.max_time
            JOIN satellite_info s ON t.satellite_id = s.satellite_id
            ORDER BY t.satellite_id
        """)
        anomaly_trend = await execute_query("""
            SELECT DATE_FORMAT(record_time,'%Y-%m-%d %H:00:00') AS hour_slot,
                   COUNT(*) AS total,
                   SUM(anomaly_flag) AS anomaly
            FROM satellite_telemetry
            WHERE record_time >= DATE_SUB(NOW(), INTERVAL 48 HOUR)
            GROUP BY hour_slot ORDER BY hour_slot
        """)
        return {"latest": latest, "anomaly_trend": anomaly_trend}

    async def telemetry_series(self, satellite_id: int, hours: int = 48):
        """单颗卫星遥测时间序列"""
        rows = await execute_query(f"""
            SELECT record_time, battery_pct, solar_power_w,
                   cpu_temp_c, signal_strength_db, orbit_altitude_km, anomaly_flag
            FROM satellite_telemetry
            WHERE satellite_id = {satellite_id}
              AND record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
            ORDER BY record_time
        """)
        return {"satellite_id": satellite_id, "series": rows}

    # ── 地面站 ────────────────────────────────────────────────────────────────
    async def station_status(self):
        stations = await execute_query("SELECT * FROM ground_station ORDER BY station_id")
        contacts = await execute_query("""
            SELECT gs.station_name,
                   COUNT(st.task_id) AS contacts
            FROM ground_station gs
            LEFT JOIN satellite_task st
              ON st.task_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY gs.station_name ORDER BY gs.station_id
        """)
        return {"stations": stations, "contacts": contacts}
