"""
智能制造数字孪生沙盘 — 50+ 指标全覆盖
涵盖：物理状态 / 生产效能 / 工艺质量 / 能耗环境 / 预测维保
"""
import random, math
from datetime import datetime, timedelta
from backend.doris.connect import execute_query, execute_write, execute_many

# ── 设备定义 ────────────────────────────────────────────
MACHINES = [
    {"id": "CNC-001", "line": "产线A", "type": "CNC加工中心"},
    {"id": "CNC-002", "line": "产线A", "type": "CNC加工中心"},
    {"id": "CNC-003", "line": "产线B", "type": "CNC加工中心"},
    {"id": "WLD-001", "line": "产线B", "type": "焊接工作站"},
    {"id": "ASM-001", "line": "产线C", "type": "装配工位"},
    {"id": "ASM-002", "line": "产线C", "type": "装配工位"},
]
M_IDS = [m["id"] for m in MACHINES]
M_MAP = {m["id"]: m for m in MACHINES}

# ── 剧本（黄金→疲劳→熔断→恢复循环）────────────────────
SCRIPTS = [
    {
        "name": "黄金时段", "color": "green",
        "oee_r":        (0.88, 0.96), "avail_r":   (0.93, 0.99),
        "perf_r":       (0.91, 0.97), "qual_r":    (0.97, 0.99),
        "temp_r":       (56,   68),   "bear_r":    (43,  54),
        "cool_t_r":     (17,   24),   "vib_r":     (0.8, 2.0),
        "noise_r":      (63,   72),   "spindle_r": (2900, 3200),
        "feed_r":       (160, 200),   "force_r":   (275, 360),
        "torque_r":     (42,   62),   "curr_r":    (15,  22),
        "power_r":      (11,   16),   "hydra_r":   (88,  96),
        "air_r":        (0.56, 0.65), "flow_r":    (9,   13),
        "tool_r":       (4,    28),   "plan_out":  220,
        "yield_r":      (0.97, 0.995),"fpy_r":     (0.96, 0.99),
        "cpk_r":        (1.40, 1.85), "rough_r":   (0.5, 1.1),
        "dim_r":        (2,    7),    "hard_r":    (58,  62),
        "tens_r":       (820, 890),   "energy_r":  (11,  16),
        "co2f":         0.72,         "env_t_r":   (21,  25),
        "humi_r":       (45,  55),    "cool_c_r":  (0.8, 1.4),
        "air_m3_r":     (2.2, 3.0),   "water_r":   (0.5, 0.9),
        "alarm_r":      (0,   1),     "down_r":    (0,   3),
        "mtbf_r":       (160, 250),   "cycle_r":   (57,  64),
        "plan_cycle":   62,
    },
    {
        "name": "设备疲劳", "color": "yellow",
        "oee_r":        (0.68, 0.80), "avail_r":   (0.76, 0.87),
        "perf_r":       (0.78, 0.88), "qual_r":    (0.87, 0.94),
        "temp_r":       (72,   85),   "bear_r":    (58,  72),
        "cool_t_r":     (28,   38),   "vib_r":     (2.8, 5.2),
        "noise_r":      (76,   86),   "spindle_r": (2100, 2650),
        "feed_r":       (95,  142),   "force_r":   (420, 580),
        "torque_r":     (72,   98),   "curr_r":    (26,  36),
        "power_r":      (22,   30),   "hydra_r":   (73,  85),
        "air_r":        (0.44, 0.55), "flow_r":    (4.5, 8),
        "tool_r":       (44,   73),   "plan_out":  220,
        "yield_r":      (0.89, 0.95), "fpy_r":     (0.86, 0.92),
        "cpk_r":        (0.88, 1.30), "rough_r":   (1.8, 2.9),
        "dim_r":        (11,   25),   "hard_r":    (53,  58),
        "tens_r":       (740, 820),   "energy_r":  (22,  30),
        "co2f":         0.88,         "env_t_r":   (25,  30),
        "humi_r":       (55,  68),    "cool_c_r":  (1.8, 2.8),
        "air_m3_r":     (3.5, 5.0),   "water_r":   (1.2, 1.8),
        "alarm_r":      (2,   6),     "down_r":    (8,   22),
        "mtbf_r":       (50,  100),   "cycle_r":   (70,  83),
        "plan_cycle":   62,
    },
    {
        "name": "高温熔断", "color": "red",
        "oee_r":        (0.26, 0.52), "avail_r":   (0.36, 0.60),
        "perf_r":       (0.50, 0.72), "qual_r":    (0.60, 0.80),
        "temp_r":       (90,  112),   "bear_r":    (78,  98),
        "cool_t_r":     (44,   60),   "vib_r":     (5.8, 9.8),
        "noise_r":      (88,  100),   "spindle_r": (1200, 1900),
        "feed_r":       (42,   85),   "force_r":   (640, 870),
        "torque_r":     (116, 160),   "curr_r":    (40,  56),
        "power_r":      (36,   50),   "hydra_r":   (56,  72),
        "air_r":        (0.30, 0.44), "flow_r":    (1.2, 4.0),
        "tool_r":       (78,   99),   "plan_out":  220,
        "yield_r":      (0.68, 0.82), "fpy_r":     (0.62, 0.76),
        "cpk_r":        (0.42, 0.82), "rough_r":   (3.5, 6.2),
        "dim_r":        (30,   60),   "hard_r":    (44,  52),
        "tens_r":       (620, 730),   "energy_r":  (36,  52),
        "co2f":         1.18,         "env_t_r":   (30,  38),
        "humi_r":       (68,  80),    "cool_c_r":  (3.5, 5.5),
        "air_m3_r":     (5.5, 8.0),   "water_r":   (2.0, 3.2),
        "alarm_r":      (8,   20),    "down_r":    (28,  58),
        "mtbf_r":       (10,  38),    "cycle_r":   (90, 118),
        "plan_cycle":   62,
    },
    {
        "name": "产能恢复", "color": "yellow",
        "oee_r":        (0.75, 0.84), "avail_r":   (0.80, 0.90),
        "perf_r":       (0.82, 0.90), "qual_r":    (0.91, 0.96),
        "temp_r":       (66,   78),   "bear_r":    (52,  64),
        "cool_t_r":     (24,   32),   "vib_r":     (2.0, 3.8),
        "noise_r":      (70,   80),   "spindle_r": (2400, 2800),
        "feed_r":       (120, 165),   "force_r":   (360, 480),
        "torque_r":     (60,   82),   "curr_r":    (22,  30),
        "power_r":      (18,   24),   "hydra_r":   (80,  90),
        "air_r":        (0.50, 0.58), "flow_r":    (6.5, 10),
        "tool_r":       (30,   55),   "plan_out":  220,
        "yield_r":      (0.92, 0.97), "fpy_r":     (0.90, 0.95),
        "cpk_r":        (1.10, 1.45), "rough_r":   (1.2, 2.0),
        "dim_r":        (8,    18),   "hard_r":    (55,  60),
        "tens_r":       (770, 840),   "energy_r":  (18,  24),
        "co2f":         0.80,         "env_t_r":   (23,  28),
        "humi_r":       (50,  62),    "cool_c_r":  (1.2, 2.2),
        "air_m3_r":     (2.8, 4.2),   "water_r":   (0.8, 1.4),
        "alarm_r":      (1,   4),     "down_r":    (5,   15),
        "mtbf_r":       (80,  140),   "cycle_r":   (64,  76),
        "plan_cycle":   62,
    },
    {
        "name": "黄金时段", "color": "green",   # 第5段重回黄金
        "oee_r":        (0.86, 0.95), "avail_r":   (0.92, 0.98),
        "perf_r":       (0.90, 0.96), "qual_r":    (0.96, 0.99),
        "temp_r":       (55,   67),   "bear_r":    (42,  52),
        "cool_t_r":     (16,   23),   "vib_r":     (0.7, 1.9),
        "noise_r":      (62,   71),   "spindle_r": (2850, 3200),
        "feed_r":       (158, 200),   "force_r":   (270, 355),
        "torque_r":     (41,   60),   "curr_r":    (14,  21),
        "power_r":      (10,   15),   "hydra_r":   (89,  97),
        "air_r":        (0.57, 0.66), "flow_r":    (9.5, 13.5),
        "tool_r":       (3,    25),   "plan_out":  220,
        "yield_r":      (0.975, 0.998),"fpy_r":    (0.965, 0.992),
        "cpk_r":        (1.45, 1.90), "rough_r":   (0.45, 1.0),
        "dim_r":        (2,    6),    "hard_r":    (59,  63),
        "tens_r":       (830, 895),   "energy_r":  (10,  15),
        "co2f":         0.70,         "env_t_r":   (20,  24),
        "humi_r":       (43,  53),    "cool_c_r":  (0.7, 1.3),
        "air_m3_r":     (2.1, 2.9),   "water_r":   (0.4, 0.8),
        "alarm_r":      (0,   1),     "down_r":    (0,   2),
        "mtbf_r":       (170, 260),   "cycle_r":   (56,  63),
        "plan_cycle":   62,
    },
]

# ── DDL（新建/覆盖）─────────────────────────────────────
INIT_SQL = """
CREATE TABLE IF NOT EXISTS mfg_metrics_v2 (
    ts                  DATETIME      COMMENT '采集时间',
    machine_id          VARCHAR(20)   COMMENT '设备编号',
    line_id             VARCHAR(10)   COMMENT '产线',
    machine_type        VARCHAR(20)   COMMENT '设备类型',
    script_name         VARCHAR(20)   COMMENT '剧本状态',
    script_color        VARCHAR(10)   COMMENT '状态色',
    -- 物理状态（15指标）
    temperature         FLOAT         COMMENT '主轴温度°C',
    bearing_temp        FLOAT         COMMENT '轴承温度°C',
    coolant_temp        FLOAT         COMMENT '冷却液温度°C',
    vibration           FLOAT         COMMENT '震动mm/s',
    noise_level         FLOAT         COMMENT '噪音dB',
    spindle_speed       INT           COMMENT '主轴转速rpm',
    feed_rate           FLOAT         COMMENT '进给速率mm/min',
    cutting_force       FLOAT         COMMENT '切削力N',
    torque              FLOAT         COMMENT '扭矩N·m',
    motor_current       FLOAT         COMMENT '电机电流A',
    power_kw            FLOAT         COMMENT '实时功耗kW',
    hydraulic_bar       FLOAT         COMMENT '液压bar',
    air_pressure        FLOAT         COMMENT '气压MPa',
    coolant_flow        FLOAT         COMMENT '冷却液流量L/min',
    tool_wear_pct       FLOAT         COMMENT '刀具磨损%',
    -- 生产效能（12指标）
    oee                 FLOAT         COMMENT '综合设备效率',
    availability        FLOAT         COMMENT '可用率',
    performance_rate    FLOAT         COMMENT '性能率',
    quality_rate        FLOAT         COMMENT '质量率',
    output_count        INT           COMMENT '实际产出件',
    planned_output      INT           COMMENT '计划产出件',
    defect_count        INT           COMMENT '缺陷件',
    scrap_count         INT           COMMENT '报废件',
    rework_count        INT           COMMENT '返工件',
    cycle_time_s        FLOAT         COMMENT '实际节拍s',
    plan_cycle_s        FLOAT         COMMENT '计划节拍s',
    run_hours           FLOAT         COMMENT '累计运行h',
    -- 质量工艺（10指标）
    yield_rate          FLOAT         COMMENT '综合良品率',
    first_pass_yield    FLOAT         COMMENT '首次通过率',
    scrap_rate          FLOAT         COMMENT '废品率',
    rework_rate         FLOAT         COMMENT '返工率',
    cpk_value           FLOAT         COMMENT '过程能力Cpk',
    ppm_value           INT           COMMENT '百万缺陷PPM',
    surface_roughness   FLOAT         COMMENT '表面粗糙度Ra μm',
    dimensional_error   FLOAT         COMMENT '尺寸偏差μm',
    hardness_hrc        FLOAT         COMMENT '硬度HRC',
    tensile_strength    FLOAT         COMMENT '抗拉强度MPa',
    -- 能耗环境（8指标）
    energy_kwh          FLOAT         COMMENT '单步能耗kWh',
    energy_per_unit     FLOAT         COMMENT '单件能耗kWh',
    co2_kg              FLOAT         COMMENT '碳排放kg',
    env_temp            FLOAT         COMMENT '环境温度°C',
    env_humidity        FLOAT         COMMENT '环境湿度%',
    coolant_consumed_l  FLOAT         COMMENT '冷却液消耗L',
    compressed_air_m3   FLOAT         COMMENT '压缩空气m³',
    water_l             FLOAT         COMMENT '用水L',
    -- 维保告警（5指标）
    tool_change_cnt     INT           COMMENT '换刀次数',
    alarm_count         INT           COMMENT '告警次数',
    unplanned_down_min  FLOAT         COMMENT '非计划停机min',
    planned_down_min    FLOAT         COMMENT '计划停机min',
    mtbf_h              FLOAT         COMMENT '平均故障间隔h'
) DUPLICATE KEY(ts, machine_id)
  DISTRIBUTED BY HASH(machine_id) BUCKETS 4
  PROPERTIES ("replication_num" = "1");
"""

TABLE = "mfg_metrics_v2"

# ── 辅助 ────────────────────────────────────────────────
def _r(lo, hi, dec=2):
    return round(random.uniform(lo, hi), dec)

async def _exec(sql, params=None):
    if params:
        await execute_many(sql, params)
    else:
        await execute_write(sql)

async def _query(sql):
    return await execute_query(sql)


class ManufacturingService:

    async def init_table(self):
        # 若旧表结构与新表不兼容，先 DROP 再建
        await execute_write(f"DROP TABLE IF EXISTS {TABLE}")
        await execute_write(INIT_SQL)
        return {"success": True, "msg": "数字孪生沙盘初始化完成（50指标模型已就绪）"}

    async def get_current_step(self) -> int:
        rows = await _query(f"SELECT COUNT(DISTINCT ts) AS cnt FROM {TABLE}")
        return int(rows[0].get("cnt", 0) if rows else 0) // len(MACHINES)

    async def generate_step(self):
        await execute_write(INIT_SQL)  # CREATE IF NOT EXISTS
        step   = await self.get_current_step()
        sc     = SCRIPTS[step % len(SCRIPTS)]
        base_t = datetime(2025, 1, 1, 8, 0, 0)
        ts     = base_t + timedelta(minutes=15 * step)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for m in MACHINES:
            mid = m["id"]
            # 核心效能
            oee   = _r(*sc["oee_r"], 3)
            avail = _r(*sc["avail_r"], 3)
            perf  = _r(*sc["perf_r"], 3)
            qual  = _r(*sc["qual_r"], 3)
            out   = int(sc["plan_out"] * oee * _r(0.96, 1.04))
            def_  = max(0, int(out * (1 - _r(*sc["qual_r"]))))
            scrap = max(0, int(def_ * _r(0.3, 0.6)))
            rew   = def_ - scrap
            yr    = round(1 - def_ / max(out, 1), 4)
            fpy   = _r(*sc["fpy_r"], 4)
            # 物理
            temp  = _r(*sc["temp_r"], 1)
            bear  = _r(*sc["bear_r"], 1)
            ct    = _r(*sc["cool_t_r"], 1)
            vib   = _r(*sc["vib_r"], 2)
            noi   = _r(*sc["noise_r"], 1)
            sp    = random.randint(*sc["spindle_r"])
            feed  = _r(*sc["feed_r"], 1)
            force = _r(*sc["force_r"], 1)
            torq  = _r(*sc["torque_r"], 2)
            curr  = _r(*sc["curr_r"], 1)
            pwr   = _r(*sc["power_r"], 2)
            hyd   = _r(*sc["hydra_r"], 1)
            air   = _r(*sc["air_r"], 3)
            flow  = _r(*sc["flow_r"], 2)
            wear  = _r(*sc["tool_r"], 1)
            cyc   = _r(*sc["cycle_r"], 1)
            rh    = round(step * 0.25 + _r(0, 0.1), 2)
            # 质量工艺
            cpk   = _r(*sc["cpk_r"], 3)
            rough = _r(*sc["rough_r"], 2)
            dim_e = _r(*sc["dim_r"], 1)
            hard  = _r(*sc["hard_r"], 1)
            tens  = _r(*sc["tens_r"], 0)
            ppm   = max(0, int((1 - yr) * 1_000_000))
            scr_r = round(scrap / max(out, 1) * 100, 2)
            rew_r = round(rew   / max(out, 1) * 100, 2)
            # 能耗
            enk   = _r(*sc["energy_r"], 3)
            epu   = round(enk / max(out, 1) * 100, 4) if out else 0
            co2   = round(enk * sc["co2f"], 3)
            env_t = _r(*sc["env_t_r"], 1)
            humi  = _r(*sc["humi_r"], 1)
            cc    = _r(*sc["cool_c_r"], 2)
            ca    = _r(*sc["air_m3_r"], 2)
            wt    = _r(*sc["water_r"], 2)
            # 维保
            tc    = 1 if wear > 80 else 0
            alm   = random.randint(*sc["alarm_r"])
            udt   = _r(*sc["down_r"], 1)
            pdt   = round(_r(0, 5), 1) if sc["color"] == "green" else 0
            mtbf  = _r(*sc["mtbf_r"], 1)

            rows.append((
                ts_str, mid, m["line"], m["type"], sc["name"], sc["color"],
                temp, bear, ct, vib, noi, sp, feed, force, torq, curr, pwr, hyd, air, flow, wear,
                oee, avail, perf, qual,
                out, sc["plan_out"], def_, scrap, rew, cyc, float(sc["plan_cycle"]), rh,
                yr, fpy, scr_r, rew_r, cpk, ppm, rough, dim_e, hard, tens,
                enk, epu, co2, env_t, humi, cc, ca, wt,
                tc, alm, udt, pdt, mtbf,
            ))

        sql = f"""INSERT INTO {TABLE}
            (ts, machine_id, line_id, machine_type, script_name, script_color,
             temperature, bearing_temp, coolant_temp, vibration, noise_level,
             spindle_speed, feed_rate, cutting_force, torque, motor_current,
             power_kw, hydraulic_bar, air_pressure, coolant_flow, tool_wear_pct,
             oee, availability, performance_rate, quality_rate,
             output_count, planned_output, defect_count, scrap_count, rework_count,
             cycle_time_s, plan_cycle_s, run_hours,
             yield_rate, first_pass_yield, scrap_rate, rework_rate,
             cpk_value, ppm_value, surface_roughness, dimensional_error, hardness_hrc, tensile_strength,
             energy_kwh, energy_per_unit, co2_kg, env_temp, env_humidity,
             coolant_consumed_l, compressed_air_m3, water_l,
             tool_change_cnt, alarm_count, unplanned_down_min, planned_down_min, mtbf_h)
            VALUES (%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s)"""
        await execute_many(sql, rows)
        return {
            "success": True, "step": step + 1,
            "ts": ts_str, "script": sc["name"], "script_color": sc["color"],
            "msg": f"数据生成成功（{sc['name']}），看板已实时刷新",
        }

    async def batch_generate(self, steps: int = 5):
        results = []
        for _ in range(min(steps, 20)):
            r = await self.generate_step()
            results.append(r)
        last = results[-1] if results else {}
        return {
            "success": True, "steps_generated": len(results),
            "step": last.get("step", 0), "ts": last.get("ts", ""),
            "script": last.get("script", ""), "script_color": last.get("script_color", ""),
            "msg": f"已生成 {len(results)} 步仿真数据（{len(results)*15} 分钟）",
        }

    async def get_overview(self):
        rows = await _query(f"""
            SELECT
                COUNT(DISTINCT machine_id)              AS machine_count,
                ROUND(AVG(oee), 3)                      AS avg_oee,
                ROUND(AVG(availability), 3)             AS avg_avail,
                ROUND(AVG(performance_rate), 3)         AS avg_perf,
                ROUND(AVG(quality_rate), 3)             AS avg_qual,
                SUM(output_count)                       AS total_output,
                SUM(planned_output)                     AS total_planned,
                SUM(defect_count)                       AS total_defect,
                SUM(scrap_count)                        AS total_scrap,
                SUM(rework_count)                       AS total_rework,
                ROUND(AVG(temperature), 1)              AS avg_temp,
                ROUND(AVG(vibration), 2)                AS avg_vib,
                ROUND(AVG(cpk_value), 3)                AS avg_cpk,
                ROUND(AVG(first_pass_yield)*100, 2)     AS avg_fpy,
                ROUND(SUM(energy_kwh), 2)               AS total_energy,
                ROUND(SUM(co2_kg), 2)                   AS total_co2,
                ROUND(AVG(tool_wear_pct), 1)            AS avg_tool_wear,
                SUM(alarm_count)                        AS total_alarms,
                MAX(ts)                                 AS last_ts,
                MAX(script_name)                        AS script_name,
                MAX(script_color)                       AS script_color
            FROM {TABLE}
        """)
        if not rows or rows[0]["machine_count"] is None:
            return {"empty": True}
        r = rows[0]
        r["yield_rate"] = round(1 - int(r.get("total_defect") or 0) / max(int(r.get("total_output") or 1), 1), 4)
        r["current_step"] = await self.get_current_step()
        return r

    async def get_machine_status(self):
        """各设备最新一条数据（全字段），QUALIFY 保证每台设备严格一行"""
        return await _query(f"""
            SELECT *
            FROM {TABLE}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY machine_id ORDER BY ts DESC) = 1
            ORDER BY line_id, machine_id
        """)

    async def get_oee_trend(self):
        return await _query(f"""
            SELECT ts, line_id,
                   ROUND(AVG(oee)*100, 1)           AS oee,
                   ROUND(AVG(availability)*100, 1)  AS availability,
                   ROUND(AVG(performance_rate)*100,1)AS performance_rate,
                   ROUND(AVG(quality_rate)*100, 1)  AS quality_rate,
                   ROUND(AVG(temperature), 1)       AS temperature
            FROM {TABLE}
            GROUP BY ts, line_id
            ORDER BY ts ASC
            LIMIT 600
        """)

    async def get_machine_trend(self, machine_id: str):
        """单台设备的历史时序（所有效能+物理指标）"""
        safe = machine_id.replace("'", "")
        return await _query(f"""
            SELECT ts,
                   ROUND(oee*100,1) AS oee,
                   ROUND(availability*100,1) AS availability,
                   ROUND(performance_rate*100,1) AS performance_rate,
                   temperature, vibration, power_kw, tool_wear_pct,
                   cpk_value, noise_level, spindle_speed, feed_rate
            FROM {TABLE}
            WHERE machine_id = '{safe}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ts ORDER BY ts) = 1
            ORDER BY ts ASC
            LIMIT 100
        """)

    async def get_quality_stats(self):
        """质量分析聚合"""
        return await _query(f"""
            SELECT machine_id, line_id,
                   ROUND(AVG(cpk_value), 3)               AS cpk,
                   ROUND(AVG(first_pass_yield)*100, 2)     AS fpy,
                   ROUND(AVG(surface_roughness), 3)        AS roughness,
                   ROUND(AVG(dimensional_error), 2)        AS dim_err,
                   ROUND(AVG(hardness_hrc), 1)             AS hardness,
                   ROUND(AVG(tensile_strength), 0)         AS tensile,
                   ROUND(AVG(scrap_rate), 2)               AS scrap_rate,
                   ROUND(AVG(rework_rate), 2)              AS rework_rate,
                   ROUND(AVG(ppm_value), 0)                AS ppm,
                   SUM(scrap_count)                        AS total_scrap,
                   SUM(rework_count)                       AS total_rework
            FROM {TABLE}
            GROUP BY machine_id, line_id
            ORDER BY machine_id
        """)

    async def get_energy_stats(self):
        """能耗环境聚合"""
        return await _query(f"""
            SELECT machine_id, line_id,
                   ROUND(SUM(energy_kwh), 2)             AS total_energy,
                   ROUND(AVG(energy_per_unit), 4)        AS avg_epu,
                   ROUND(SUM(co2_kg), 2)                 AS total_co2,
                   ROUND(AVG(env_temp), 1)               AS env_temp,
                   ROUND(AVG(env_humidity), 1)           AS humidity,
                   ROUND(SUM(coolant_consumed_l), 2)     AS coolant_l,
                   ROUND(SUM(compressed_air_m3), 2)      AS air_m3,
                   ROUND(SUM(water_l), 2)                AS water_l,
                   ROUND(AVG(power_kw), 2)               AS avg_power
            FROM {TABLE}
            GROUP BY machine_id, line_id
            ORDER BY machine_id
        """)

    async def get_maintenance_stats(self):
        """维保告警聚合"""
        return await _query(f"""
            SELECT machine_id, line_id, machine_type,
                   ROUND(AVG(tool_wear_pct), 1)          AS avg_tool_wear,
                   MAX(tool_wear_pct)                     AS max_tool_wear,
                   SUM(tool_change_cnt)                   AS tool_changes,
                   SUM(alarm_count)                       AS total_alarms,
                   ROUND(SUM(unplanned_down_min), 1)      AS total_down,
                   ROUND(AVG(mtbf_h), 1)                  AS avg_mtbf,
                   MAX(ts)                                AS last_ts
            FROM {TABLE}
            GROUP BY machine_id, line_id, machine_type
            ORDER BY avg_tool_wear DESC
        """)

    async def get_process_trend(self):
        """工艺指标趋势（全产线）"""
        return await _query(f"""
            SELECT ts,
                   ROUND(AVG(cycle_time_s), 1)           AS cycle_time,
                   ROUND(AVG(plan_cycle_s), 1)           AS plan_cycle,
                   ROUND(AVG(spindle_speed), 0)          AS spindle_speed,
                   ROUND(AVG(cutting_force), 1)          AS cutting_force,
                   ROUND(AVG(torque), 2)                 AS torque,
                   ROUND(AVG(cpk_value), 3)              AS cpk,
                   ROUND(SUM(output_count), 0)           AS output,
                   ROUND(SUM(planned_output), 0)         AS planned
            FROM {TABLE}
            GROUP BY ts
            ORDER BY ts ASC
            LIMIT 200
        """)

    async def get_causal_analysis(self):
        return await _query(f"""
            SELECT machine_id, temperature, vibration,
                   oee, yield_rate, cpk_value, power_kw,
                   tool_wear_pct, script_color, ts
            FROM {TABLE}
            ORDER BY ts DESC
            LIMIT 400
        """)

    async def get_detail(self, limit: int = 60):
        return await _query(f"""
            SELECT ts, machine_id, line_id, script_name, script_color,
                   oee, availability, performance_rate, quality_rate,
                   temperature, vibration, power_kw, tool_wear_pct,
                   output_count, defect_count, scrap_count, rework_count,
                   cpk_value, ppm_value, first_pass_yield,
                   energy_kwh, co2_kg, alarm_count, unplanned_down_min
            FROM {TABLE}
            ORDER BY ts DESC, machine_id
            LIMIT {min(limit, 200)}
        """)

    async def reset(self):
        await execute_write(f"TRUNCATE TABLE {TABLE}")
        return {"success": True, "msg": "数据已重置"}
