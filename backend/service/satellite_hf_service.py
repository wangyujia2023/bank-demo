"""
卫星高频遥测指标分析服务
- 采样粒度：5 分钟
- 时间跨度：近 3 天（~14700 条记录）
- 指标：9 个连续量 + 1 个异常标志
- 分析能力：健康评分 / 指标趋势+滚动均值+2σ带 / Z-score异常检测 / 多星对比
"""
import math, random
from datetime import datetime, timedelta
from typing import Optional
from backend.doris.connect import execute_query, execute_one, execute_write

# ── DDL ─────────────────────────────────────────────────────────────────────
_HF_DDL = """
CREATE TABLE IF NOT EXISTS satellite_telemetry_hf (
    record_id        BIGINT    NOT NULL,
    satellite_id     INT       NOT NULL,
    record_time      DATETIME  NOT NULL,
    battery_pct      FLOAT     COMMENT '电池电量%',
    solar_power_w    FLOAT     COMMENT '太阳能功率W',
    cpu_temp_c       FLOAT     COMMENT 'CPU温度℃',
    thermal_payload_c FLOAT    COMMENT '载荷温度℃',
    signal_strength_db FLOAT   COMMENT '信号强度dB',
    link_snr_db      FLOAT     COMMENT '链路信噪比dB',
    orbit_altitude_km FLOAT    COMMENT '轨道高度km',
    attitude_error_deg FLOAT   COMMENT '姿态误差°',
    data_buffer_pct  FLOAT     COMMENT '星上存储占用%',
    anomaly_flag     INT       DEFAULT 0
) DUPLICATE KEY(record_id)
DISTRIBUTED BY HASH(satellite_id) BUCKETS 8
PROPERTIES ("replication_num" = "1")
"""

# 在轨卫星参数：(id, 名称, 类型, 轨道高度, 轨道周期min, 是否GEO)
_ACTIVE_SATS = [
    (1,  "天眼-1",   "遥感",  500,   94.7, False),
    (2,  "天眼-2",   "遥感",  520,   95.1, False),
    (3,  "北斗-3G1", "导航",  35786, 1436, True ),
    (4,  "北斗-3M1", "导航",  21528, 760,  False),
    (5,  "北斗-3M2", "导航",  21528, 760,  False),
    (6,  "通信-A1",  "通信",  35786, 1436, True ),
    (7,  "通信-A2",  "通信",  35786, 1436, True ),
    (8,  "气象-FY4", "气象",  35786, 1436, True ),
    (9,  "气象-FY3", "气象",  836,   101,  False),
    (10, "海洋-HY2", "遥感",  971,   104,  False),
    (11, "资源-ZY5", "遥感",  506,   94.9, False),
    (12, "预警-GJ1", "预警",  35786, 1436, True ),
    (13, "侦察-JB1", "侦察",  490,   94.5, False),
    (14, "侦察-JB2", "侦察",  480,   94.3, False),
    (15, "中继-TH1", "中继",  2000,  127,  False),
    (16, "天眼-3",   "遥感",  510,   95.0, False),
    (18, "科学-KX1", "科学",  550,   95.8, False),
]

# 注入特定异常场景
_ANOMALY_SAT = {
    2:  "thermal",    # 天眼-2: 载荷温度异常（第2天一次过热事件）
    13: "battery",    # 侦察-JB1: 电池衰退趋势（持续偏低）
    7:  "signal",     # 通信-A2: 信号周期性丢失
    16: "attitude",   # 天眼-3: 姿态不稳定
}


def _gen_sat_data(sat_id, sat_name, sat_type, base_alt, orbit_period, is_geo,
                  start_time, n_points, anomaly_type=None):
    """为一颗卫星生成 n_points 条5分钟采样遥测"""
    rng = random.Random(sat_id * 997 + 7)
    rows = []
    battery = rng.uniform(75, 92)   # 初始电量
    buffer_pct = rng.uniform(20, 60)

    eclipse_ratio = 0.37  # LEO：约37%时间在地影（约35/95 min）
    charge_rate   = 2.8   # 出太阳：每5min充电约2.8%
    discharge_rate= 1.5   # 入地影：每5min放电约1.5%

    for i in range(n_points):
        t = start_time + timedelta(minutes=5 * i)
        elapsed_min = i * 5

        # ── 基础物理模型 ──────────────────────────────────────────
        if is_geo:
            # GEO：稳定，电量高，温度低
            battery = max(82, min(98, battery + rng.uniform(-0.3, 0.5)))
            solar   = round(rng.uniform(390, 430), 1)
            orbit_phase = 1.0  # 始终在阳光侧
        else:
            # LEO：轨道周期内充放电循环
            phase_in_orbit = (elapsed_min % orbit_period) / orbit_period
            in_sunlight = phase_in_orbit > eclipse_ratio
            if in_sunlight:
                battery = min(97, battery + charge_rate + rng.uniform(-0.5, 0.5))
                solar   = round(350 + 180 * math.sin(math.pi * (phase_in_orbit - eclipse_ratio) / (1 - eclipse_ratio)) + rng.uniform(-20, 20), 1)
            else:
                battery = max(55, battery - discharge_rate - rng.uniform(0, 0.5))
                solar   = round(max(0, rng.uniform(-10, 40)), 1)
            orbit_phase = phase_in_orbit

        battery_r = round(battery, 1)
        solar_r   = max(0, solar)

        # CPU温度：与充电功率正相关
        base_temp = 22 + (solar_r / 430) * 12 + rng.uniform(-2, 3)
        cpu_temp  = round(base_temp, 1)

        # 载荷温度：略滞后于CPU
        thermal_payload = round(base_temp - 3 + rng.uniform(-2, 2), 1)

        # 信号强度
        signal = round(-78 + rng.uniform(-8, 12), 1)
        link_snr = round(18 + (signal + 78) * 0.4 + rng.uniform(-2, 2), 1)

        # 轨道高度：缓慢衰减 + 小扰动
        alt = round(base_alt - elapsed_min * 0.00015 + rng.uniform(-0.8, 0.8), 1)

        # 姿态误差
        attitude_err = round(abs(rng.gauss(0.15, 0.08)), 3)

        # 星上存储
        buffer_pct = max(5, min(95, buffer_pct + rng.uniform(-1.5, 2.0)))
        buffer_r = round(buffer_pct, 1)

        # ── 注入异常 ─────────────────────────────────────────────
        anomaly = 0
        day_idx = elapsed_min // 1440  # 第几天

        if anomaly_type == "thermal" and day_idx == 1:
            # 第2天(day_idx=1)第8-10小时：载荷过热事件
            hour_in_day = (elapsed_min % 1440) // 60
            if 8 <= hour_in_day <= 10:
                thermal_payload = round(thermal_payload + rng.uniform(15, 28), 1)
                cpu_temp = round(cpu_temp + rng.uniform(8, 15), 1)
                anomaly = 1

        elif anomaly_type == "battery":
            # 全程电池容量衰退（基准降低20%）
            battery_r = round(max(35, battery_r - 20 - elapsed_min * 0.005), 1)
            if battery_r < 45:
                anomaly = 1

        elif anomaly_type == "signal":
            # 每3.5小时出现一次信号丢失（约10分钟）
            phase_210 = elapsed_min % 210
            if 0 <= phase_210 < 12:
                signal = round(rng.uniform(-98, -93), 1)
                link_snr = round(max(0, link_snr - 14), 1)
                anomaly = 1

        elif anomaly_type == "attitude":
            # 姿态误差偏大，偶发超限
            attitude_err = round(abs(rng.gauss(0.4, 0.25)), 3)
            if attitude_err > 0.8:
                anomaly = 1

        # 通用阈值检测
        if (battery_r < 40 or cpu_temp > 50 or
                thermal_payload > 52 or signal < -92):
            anomaly = 1

        rows.append((
            sat_id, t.strftime('%Y-%m-%d %H:%M:%S'),
            battery_r, solar_r, cpu_temp, thermal_payload,
            signal, link_snr, alt, attitude_err, buffer_r, anomaly
        ))

    return rows


class SatelliteHFService:

    async def init_hf_data(self):
        """建 HF 表 + 插入模拟高频遥测（幂等）"""
        results = []
        try:
            await execute_write(_HF_DDL.strip())
            results.append("hf table ok")
        except Exception as e:
            results.append(f"hf table err: {e}")

        row = await execute_one("SELECT COUNT(*) AS cnt FROM satellite_telemetry_hf")
        if row and int(row.get("cnt") or 0) > 0:
            return {"status": "already_initialized", "count": int(row["cnt"])}

        now   = datetime.now().replace(second=0, microsecond=0)
        # 对齐到5分钟整点
        now   = now - timedelta(minutes=now.minute % 5)
        start = now - timedelta(days=3)
        n_pts = 3 * 24 * 12   # 864 per satellite

        record_id = 1
        total = 0
        for (sat_id, name, sat_type, alt, period, is_geo) in _ACTIVE_SATS:
            anomaly_type = _ANOMALY_SAT.get(sat_id)
            rows = _gen_sat_data(sat_id, name, sat_type, alt, period, is_geo,
                                 start, n_pts, anomaly_type)
            vals = [
                f"({record_id + j},{r[0]},'{r[1]}',{r[2]},{r[3]},{r[4]},"
                f"{r[5]},{r[6]},{r[7]},{r[8]},{r[9]},{r[10]},{r[11]})"
                for j, r in enumerate(rows)
            ]
            record_id += len(rows)
            total     += len(rows)
            # 每颗卫星一批插入
            for i in range(0, len(vals), 500):
                batch = vals[i:i+500]
                sql = "INSERT INTO satellite_telemetry_hf VALUES " + ",".join(batch)
                try:
                    await execute_write(sql)
                except Exception as e:
                    results.append(f"hf insert {sat_id} err: {e}")

        return {"status": "initialized", "total": total, "results": results}

    # ── 健康评分 ─────────────────────────────────────────────────────────────
    async def health_scores(self):
        """过去24h各卫星综合健康评分（0-100）"""
        rows = await execute_query("""
            SELECT
                si.satellite_id, si.satellite_name, si.satellite_type,
                ROUND(AVG(t.battery_pct),   1) AS avg_battery,
                ROUND(AVG(t.cpu_temp_c),    1) AS avg_cpu_temp,
                ROUND(AVG(t.thermal_payload_c), 1) AS avg_payload_temp,
                ROUND(AVG(t.signal_strength_db),1) AS avg_signal,
                ROUND(AVG(t.attitude_error_deg),3) AS avg_attitude,
                ROUND(AVG(t.link_snr_db),   1) AS avg_snr,
                ROUND(MIN(t.battery_pct),   1) AS min_battery,
                ROUND(MAX(t.cpu_temp_c),    1) AS max_cpu_temp,
                ROUND(MAX(t.thermal_payload_c),1) AS max_payload_temp,
                ROUND(MIN(t.signal_strength_db),1) AS min_signal,
                ROUND(STDDEV_POP(t.battery_pct),  2) AS std_battery,
                ROUND(STDDEV_POP(t.attitude_error_deg), 3) AS std_attitude,
                SUM(t.anomaly_flag)   AS anomaly_cnt,
                COUNT(*)              AS total_cnt
            FROM satellite_info si
            JOIN satellite_telemetry_hf t ON si.satellite_id = t.satellite_id
            WHERE si.status = '在轨'
              AND t.record_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            GROUP BY si.satellite_id, si.satellite_name, si.satellite_type
            ORDER BY si.satellite_id
        """)

        for r in rows:
            batt   = float(r.get("avg_battery") or 0)
            temp   = float(r.get("avg_cpu_temp") or 0)
            sig    = float(r.get("avg_signal") or -100)
            att    = float(r.get("avg_attitude") or 0)
            anom   = int(r.get("anomaly_cnt") or 0)
            total  = max(1, int(r.get("total_cnt") or 1))

            score_batt  = min(30, batt * 0.30)
            score_temp  = 25 if temp < 30 else 20 if temp < 38 else 12 if temp < 45 else 4
            score_sig   = 25 if sig > -76 else 20 if sig > -83 else 12 if sig > -90 else 4
            score_att   = 12 if att < 0.25 else 8 if att < 0.5 else 4 if att < 0.8 else 1
            score_anom  = max(0, 8 - round(anom / total * 100))
            r["health_score"] = round(min(100, score_batt + score_temp + score_sig + score_att + score_anom), 1)

        return sorted(rows, key=lambda x: -x["health_score"])

    # ── 指标趋势（含滚动均值 + 2σ 带） ────────────────────────────────────────
    async def metric_trend(self, satellite_id: int, metric: str, hours: int = 24):
        ALLOWED = {"battery_pct","solar_power_w","cpu_temp_c","thermal_payload_c",
                   "signal_strength_db","link_snr_db","orbit_altitude_km",
                   "attitude_error_deg","data_buffer_pct"}
        if metric not in ALLOWED:
            metric = "battery_pct"

        rows = await execute_query(f"""
            SELECT record_time, {metric} AS val, anomaly_flag
            FROM satellite_telemetry_hf
            WHERE satellite_id = {satellite_id}
              AND record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
            ORDER BY record_time
        """)
        if not rows:
            return {"times": [], "values": [], "rolling": [], "upper": [], "lower": [], "anomalies": []}

        vals   = [float(r["val"] or 0) for r in rows]
        flags  = [int(r["anomaly_flag"] or 0) for r in rows]
        times  = [str(r["record_time"]) for r in rows]

        # 滚动统计：窗口 = 12 点（1 小时）
        W = 12
        rolling, upper, lower = [], [], []
        for i, v in enumerate(vals):
            window = vals[max(0, i - W + 1): i + 1]
            mu  = sum(window) / len(window)
            variance = sum((x - mu) ** 2 for x in window) / len(window)
            sigma = variance ** 0.5
            rolling.append(round(mu, 3))
            upper.append(round(mu + 2 * sigma, 3))
            lower.append(round(mu - 2 * sigma, 3))

        # 统计摘要
        n     = len(vals)
        mu    = sum(vals) / n
        std   = (sum((v - mu) ** 2 for v in vals) / n) ** 0.5
        stats = {
            "min":   round(min(vals), 2),
            "max":   round(max(vals), 2),
            "avg":   round(mu, 2),
            "std":   round(std, 2),
            "anomaly_cnt": sum(flags),
            "total": n,
        }

        return {
            "times":     times,
            "values":    [round(v, 3) for v in vals],
            "rolling":   rolling,
            "upper":     upper,
            "lower":     lower,
            "anomalies": [{"t": times[i], "v": vals[i]} for i, f in enumerate(flags) if f == 1],
            "stats":     stats,
        }

    # ── Z-score 异常检测 ──────────────────────────────────────────────────────
    async def anomaly_report(self, hours: int = 48):
        """全星座各指标 Z-score > 2.5 的异常点汇总"""
        METRICS = [
            ("battery_pct",       "电池电量%"),
            ("cpu_temp_c",        "CPU温度"),
            ("thermal_payload_c", "载荷温度"),
            ("signal_strength_db","信号强度"),
            ("attitude_error_deg","姿态误差"),
        ]
        summary_by_metric = []
        detail_rows = []

        for col, label in METRICS:
            # 用 Doris 计算全星座均值+标准差，再找异常点
            stat = await execute_one(f"""
                SELECT AVG({col}) AS mean_v, STDDEV_POP({col}) AS std_v
                FROM satellite_telemetry_hf
                WHERE record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
            """)
            mean_v = float(stat.get("mean_v") or 0)
            std_v  = float(stat.get("std_v") or 1) or 1

            outliers = await execute_query(f"""
                SELECT si.satellite_name, si.satellite_type,
                       t.satellite_id, t.record_time,
                       t.{col} AS val
                FROM satellite_telemetry_hf t
                JOIN satellite_info si ON t.satellite_id = si.satellite_id
                WHERE t.record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
                  AND ABS(t.{col} - {mean_v}) / {std_v} > 2.5
                ORDER BY ABS(t.{col} - {mean_v}) DESC
                LIMIT 50
            """)

            for row in outliers:
                z = round(abs(float(row["val"]) - mean_v) / std_v, 2)
                detail_rows.append({
                    "satellite":  row["satellite_name"],
                    "sat_type":   row["satellite_type"],
                    "metric":     label,
                    "metric_key": col,
                    "time":       str(row["record_time"]),
                    "value":      round(float(row["val"]), 2),
                    "mean":       round(mean_v, 2),
                    "z_score":    z,
                    "severity":   "严重" if z > 4 else "警告" if z > 3 else "提示",
                })

            summary_by_metric.append({
                "metric": label,
                "col":    col,
                "count":  len(outliers),
                "mean":   round(mean_v, 2),
                "std":    round(std_v, 2),
            })

        # 按卫星汇总异常数
        by_sat = await execute_query(f"""
            SELECT si.satellite_name, SUM(t.anomaly_flag) AS cnt
            FROM satellite_telemetry_hf t
            JOIN satellite_info si ON t.satellite_id = si.satellite_id
            WHERE t.record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
            GROUP BY si.satellite_name ORDER BY cnt DESC
        """)

        # 按小时分布
        hourly = await execute_query(f"""
            SELECT DATE_FORMAT(record_time,'%m-%d %H:00') AS slot,
                   SUM(anomaly_flag) AS cnt
            FROM satellite_telemetry_hf
            WHERE record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
            GROUP BY slot ORDER BY slot
        """)

        detail_rows.sort(key=lambda x: -x["z_score"])
        return {
            "summary_by_metric": summary_by_metric,
            "detail":            detail_rows[:100],
            "by_sat":            by_sat,
            "hourly":            hourly,
        }

    # ── 多星指标对比 ──────────────────────────────────────────────────────────
    async def multi_compare(self, hours: int = 24):
        """全星座过去N小时各指标均值 + 极值对比"""
        rows = await execute_query(f"""
            SELECT si.satellite_id, si.satellite_name, si.satellite_type,
                   ROUND(AVG(t.battery_pct),        1) AS avg_battery,
                   ROUND(AVG(t.cpu_temp_c),          1) AS avg_cpu_temp,
                   ROUND(AVG(t.thermal_payload_c),   1) AS avg_payload_temp,
                   ROUND(AVG(t.signal_strength_db),  1) AS avg_signal,
                   ROUND(AVG(t.link_snr_db),         1) AS avg_snr,
                   ROUND(AVG(t.attitude_error_deg),  3) AS avg_attitude,
                   ROUND(AVG(t.data_buffer_pct),     1) AS avg_buffer,
                   ROUND(STDDEV_POP(t.battery_pct),  2) AS std_battery,
                   ROUND(STDDEV_POP(t.cpu_temp_c),   2) AS std_cpu_temp,
                   ROUND(STDDEV_POP(t.signal_strength_db), 2) AS std_signal,
                   ROUND(MIN(t.battery_pct),         1) AS min_battery,
                   ROUND(MAX(t.cpu_temp_c),          1) AS max_cpu_temp,
                   ROUND(MIN(t.signal_strength_db),  1) AS min_signal,
                   SUM(t.anomaly_flag)   AS anomaly_cnt,
                   COUNT(*)              AS total_cnt
            FROM satellite_info si
            JOIN satellite_telemetry_hf t ON si.satellite_id = t.satellite_id
            WHERE si.status = '在轨'
              AND t.record_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
            GROUP BY si.satellite_id, si.satellite_name, si.satellite_type
            ORDER BY si.satellite_id
        """)
        return {"rows": rows, "hours": hours}

    # ── 指标总览（全星座最新一批统计） ──────────────────────────────────────────
    async def metric_overview(self):
        """全指标实时快照：均值 / σ / 分位数"""
        METRICS = [
            ("battery_pct",       "电池电量",  "%"),
            ("cpu_temp_c",        "CPU温度",   "℃"),
            ("thermal_payload_c", "载荷温度",  "℃"),
            ("signal_strength_db","信号强度",  "dB"),
            ("link_snr_db",       "信噪比",    "dB"),
            ("attitude_error_deg","姿态误差",  "°"),
            ("solar_power_w",     "太阳能功率","W"),
            ("data_buffer_pct",   "存储占用",  "%"),
        ]
        result = []
        for col, label, unit in METRICS:
            stat = await execute_one(f"""
                SELECT
                    ROUND(AVG({col}),2)          AS mean_v,
                    ROUND(STDDEV_POP({col}),2)   AS std_v,
                    ROUND(MIN({col}),2)           AS min_v,
                    ROUND(MAX({col}),2)           AS max_v,
                    ROUND(PERCENTILE_APPROX({col},0.25),2) AS p25,
                    ROUND(PERCENTILE_APPROX({col},0.50),2) AS p50,
                    ROUND(PERCENTILE_APPROX({col},0.75),2) AS p75
                FROM satellite_telemetry_hf
                WHERE record_time >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            """)
            result.append({"col": col, "label": label, "unit": unit, **(stat or {})})
        return result
