"""
AI 日志分类分析服务 + API 路由
基于 bank_system_log_tagged 表（AI_CLASSIFY 结果）
"""
from typing import Optional, List, Dict
from fastapi import APIRouter, Query
from backend.doris.connect import execute_query, execute_one, execute_write
import logging
import asyncio

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/log-classify", tags=["AI日志分类分析"])


# ── 触发 AI_CLASSIFY（INSERT INTO SELECT）──────────────────────
@router.post("/run")
async def run_ai_classify(
    log_date: str = Query(..., description="日志日期 YYYY-MM-DD"),
    use_fallback: bool = Query(False, description="True=跳过AI直接用规则模拟（调试用）"),
):
    """
    触发 AI_CLASSIFY，将分类结果写入 bank_system_log_tagged
    正式使用：use_fallback=false（需配置 qwen_llm RESOURCE）
    调试使用：use_fallback=true（基于规则模拟标签）
    """
    if use_fallback:
        sql = _build_fallback_insert(log_date)
    else:
        sql = _build_ai_classify_insert(log_date)

    try:
        affected = await execute_write(sql)
        return {
            "status": "success",
            "log_date": log_date,
            "method": "FALLBACK" if use_fallback else "AI_CLASSIFY",
            "rows_inserted": affected,
        }
    except Exception as e:
        logger.error(f"AI_CLASSIFY 执行失败: {e}")
        return {"status": "error", "message": str(e)}


def _build_ai_classify_insert(log_date: str) -> str:
    """构建 AI_CLASSIFY INSERT INTO SELECT SQL"""
    return f"""
    INSERT INTO bank_system_log_tagged
    (log_id, log_date, log_time, user_id, user_name, log_source, operation,
     amount, risk_score, log_content, ai_tag, ai_tag_group,
     is_exception, is_risk, classify_time, classify_method)
    SELECT
        s.log_id, s.log_date, s.log_time, s.user_id, s.user_name,
        s.log_source, s.operation, s.amount, s.risk_score, s.log_content,

        AI_CLASSIFY(
            'qwen_llm',
            s.log_content,
            ['登录成功','登录异常','注册成功','注册失败',
             '交易成功','交易失败','支付成功','支付失败','风险预警']
        ) AS ai_tag,

        CASE AI_CLASSIFY(
            'qwen_llm', s.log_content,
            ['登录成功','登录异常','注册成功','注册失败',
             '交易成功','交易失败','支付成功','支付失败','风险预警'])
            WHEN '登录成功' THEN '登录' WHEN '登录异常' THEN '登录'
            WHEN '注册成功' THEN '注册' WHEN '注册失败' THEN '注册'
            WHEN '交易成功' THEN '交易' WHEN '交易失败' THEN '交易'
            WHEN '支付成功' THEN '支付' WHEN '支付失败' THEN '支付'
            ELSE '风险'
        END AS ai_tag_group,

        IF(AI_CLASSIFY('qwen_llm', s.log_content,
            ['登录成功','登录异常','注册成功','注册失败',
             '交易成功','交易失败','支付成功','支付失败','风险预警'])
            IN ('登录异常','注册失败','交易失败','支付失败','风险预警'), 1, 0) AS is_exception,

        IF(AI_CLASSIFY('qwen_llm', s.log_content,
            ['登录成功','登录异常','注册成功','注册失败',
             '交易成功','交易失败','支付成功','支付失败','风险预警'])
            = '风险预警' OR s.risk_score > 70, 1, 0) AS is_risk,

        NOW(), 'AI_CLASSIFY'
    FROM bank_system_log s
    WHERE s.log_date = '{log_date}'
      AND s.log_id NOT IN (
          SELECT log_id FROM bank_system_log_tagged WHERE log_date = '{log_date}'
      )
    """


def _build_fallback_insert(log_date: str) -> str:
    """调试用规则模拟（无需 qwen RESOURCE）"""
    return f"""
    INSERT INTO bank_system_log_tagged
    (log_id, log_date, log_time, user_id, user_name, log_source, operation,
     amount, risk_score, log_content, ai_tag, ai_tag_group,
     is_exception, is_risk, classify_time, classify_method)
    SELECT
        log_id, log_date, log_time, user_id, user_name, log_source, operation,
        amount, risk_score, log_content,
        CASE
            WHEN operation='LOGIN'    AND result_code='0000' THEN '登录成功'
            WHEN operation='LOGIN'    AND result_code!='0000' THEN '登录异常'
            WHEN operation='REGISTER' AND result_code='0000' THEN '注册成功'
            WHEN operation='REGISTER' AND result_code!='0000' THEN '注册失败'
            WHEN operation='TRANSFER' AND result_code='0000' THEN '交易成功'
            WHEN operation='TRANSFER' AND result_code!='0000' THEN '交易失败'
            WHEN operation='PAYMENT'  AND result_code='0000' THEN '支付成功'
            WHEN operation='PAYMENT'  AND result_code!='0000' THEN '支付失败'
            ELSE '风险预警'
        END AS ai_tag,
        CASE
            WHEN operation IN ('LOGIN','REGISTER') THEN
                IF(operation='LOGIN','登录','注册')
            WHEN operation='TRANSFER' THEN '交易'
            WHEN operation='PAYMENT'  THEN '支付'
            ELSE '风险'
        END AS ai_tag_group,
        IF(result_code!='0000', 1, 0) AS is_exception,
        IF(risk_score>70, 1, 0)       AS is_risk,
        NOW(), 'FALLBACK'
    FROM bank_system_log
    WHERE log_date='{log_date}'
      AND log_id NOT IN (
          SELECT log_id FROM bank_system_log_tagged WHERE log_date='{log_date}'
      )
    """


# ── 统计概览 ─────────────────────────────────────────────────────
@router.get("/overview")
async def get_overview(
    start_date: str = Query(...),
    end_date: str   = Query(...),
):
    """大盘：总量、各标签数量、异常数、风险数"""
    summary = await execute_one(f"""
        SELECT
            COUNT(*)                                    AS total,
            COUNT(DISTINCT user_id)                     AS unique_users,
            SUM(is_exception)                           AS exception_count,
            SUM(is_risk)                                AS risk_count,
            ROUND(SUM(is_exception)*100.0/COUNT(*), 2)  AS exception_rate,
            ROUND(SUM(is_risk)*100.0/COUNT(*), 2)       AS risk_rate
        FROM bank_system_log_tagged
        WHERE log_date BETWEEN '{start_date}' AND '{end_date}'
    """)

    tag_dist = await execute_query(f"""
        SELECT
            ai_tag,
            ai_tag_group,
            COUNT(*) AS cnt,
            ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS pct
        FROM bank_system_log_tagged
        WHERE log_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ai_tag, ai_tag_group
        ORDER BY cnt DESC
    """)

    group_dist = await execute_query(f"""
        SELECT
            ai_tag_group                                           AS tag_group,
            COUNT(*)                                               AS total,
            SUM(is_exception)                                      AS exception_cnt,
            SUM(IF(is_exception=0,1,0))                           AS success_cnt,
            ROUND(SUM(is_exception)*100.0/COUNT(*), 2)            AS exception_rate
        FROM bank_system_log_tagged
        WHERE log_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ai_tag_group
        ORDER BY total DESC
    """)

    return {
        "summary": summary or {},
        "tag_distribution": tag_dist,
        "group_distribution": group_dist,
    }


# ── 每日趋势 ─────────────────────────────────────────────────────
@router.get("/trend")
async def get_trend(
    start_date: str = Query(...),
    end_date:   str = Query(...),
    granularity: str = Query("day", description="day / hour"),
):
    """日志分类趋势（按日或按小时）"""
    if granularity == "hour":
        time_col = "DATE_FORMAT(log_time, '%Y-%m-%d %H:00')"
    else:
        time_col = "log_date"

    rows = await execute_query(f"""
        SELECT
            {time_col}                            AS ts,
            ai_tag,
            COUNT(*)                              AS cnt,
            SUM(is_exception)                     AS exception_cnt,
            SUM(is_risk)                          AS risk_cnt
        FROM bank_system_log_tagged
        WHERE log_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ts, ai_tag
        ORDER BY ts, cnt DESC
    """)
    return {"granularity": granularity, "rows": rows}


# ── 风险用户排行 ─────────────────────────────────────────────────
@router.get("/risk-users")
async def get_risk_users(
    start_date: str = Query(...),
    end_date:   str = Query(...),
    limit: int  = Query(10, ge=1, le=100),
):
    """风险/异常次数 TOP N 用户"""
    return await execute_query(f"""
        SELECT
            t.user_id,
            t.user_name,
            COUNT(*)                                    AS total_logs,
            SUM(t.is_exception)                         AS exception_cnt,
            SUM(t.is_risk)                              AS risk_cnt,
            ROUND(AVG(s.risk_score), 2)                 AS avg_risk_score,
            MAX(s.risk_score)                           AS max_risk_score,
            GROUP_CONCAT(DISTINCT t.ai_tag ORDER BY t.ai_tag SEPARATOR '|') AS ai_tags
        FROM bank_system_log_tagged t
        JOIN bank_system_log s ON t.log_id = s.log_id AND t.log_date = s.log_date
        WHERE t.log_date BETWEEN '{start_date}' AND '{end_date}'
          AND (t.is_exception = 1 OR t.is_risk = 1)
          AND t.user_id NOT IN ('UNKNOWN','')
        GROUP BY t.user_id, t.user_name
        ORDER BY risk_cnt DESC, exception_cnt DESC
        LIMIT {limit}
    """)


# ── 日志明细查询 ──────────────────────────────────────────────────
@router.get("/detail")
async def get_detail(
    start_date:  Optional[str] = None,
    end_date:    Optional[str] = None,
    ai_tag:      Optional[str] = None,
    ai_tag_group: Optional[str] = None,
    is_exception: Optional[int] = None,
    is_risk:     Optional[int]  = None,
    user_id:     Optional[str]  = None,
    keyword:     Optional[str]  = None,
    page:        int = Query(1, ge=1),
    page_size:   int = Query(20, ge=1, le=100),
):
    """标签结果明细查询（分页）"""
    conds, args = ["1=1"], []
    if start_date:
        conds.append(f"log_date >= '{start_date}'")
    if end_date:
        conds.append(f"log_date <= '{end_date}'")
    if ai_tag:
        conds.append(f"ai_tag = '{ai_tag}'")
    if ai_tag_group:
        conds.append(f"ai_tag_group = '{ai_tag_group}'")
    if is_exception is not None:
        conds.append(f"is_exception = {is_exception}")
    if is_risk is not None:
        conds.append(f"is_risk = {is_risk}")
    if user_id:
        conds.append(f"user_id = '{user_id}'")
    if keyword:
        conds.append(f"log_content LIKE '%{keyword}%'")

    where = " AND ".join(conds)
    offset = (page - 1) * page_size

    # 并行执行 COUNT 和 SELECT，减少数据库往返
    total_row, rows = await asyncio.gather(
        execute_one(f"SELECT COUNT(*) AS total FROM bank_system_log_tagged WHERE {where}"),
        execute_query(f"""
            SELECT
                log_id, log_date, log_time, user_id, user_name,
                log_source, operation, amount, risk_score,
                ai_tag, ai_tag_group, is_exception, is_risk,
                classify_time, classify_method,
                SUBSTR(log_content, 1, 120) AS log_content_short
            FROM bank_system_log_tagged
            WHERE {where}
            ORDER BY log_time DESC
            LIMIT {page_size} OFFSET {offset}
        """)
    )
    return {
        "total": total_row["total"] if total_row else 0,
        "page": page, "page_size": page_size,
        "rows": rows,
    }


# ── 标签对比：两个时间段横向对比 ─────────────────────────────────
@router.get("/compare")
async def compare_periods(
    period1_start: str = Query(...),
    period1_end:   str = Query(...),
    period2_start: str = Query(...),
    period2_end:   str = Query(...),
):
    """两个时段的 AI 标签分布对比"""
    async def _query(s, e):
        return await execute_query(f"""
            SELECT ai_tag, COUNT(*) AS cnt,
                   ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) AS pct
            FROM bank_system_log_tagged
            WHERE log_date BETWEEN '{s}' AND '{e}'
            GROUP BY ai_tag ORDER BY cnt DESC
        """)

    p1 = await _query(period1_start, period1_end)
    p2 = await _query(period2_start, period2_end)

    # 合并为对比结构
    p1_map = {r["ai_tag"]: r for r in p1}
    p2_map = {r["ai_tag"]: r for r in p2}
    all_tags = sorted(set(list(p1_map.keys()) + list(p2_map.keys())))

    compare = []
    for tag in all_tags:
        compare.append({
            "ai_tag":    tag,
            "period1_cnt": p1_map.get(tag, {}).get("cnt", 0),
            "period1_pct": p1_map.get(tag, {}).get("pct", 0),
            "period2_cnt": p2_map.get(tag, {}).get("cnt", 0),
            "period2_pct": p2_map.get(tag, {}).get("pct", 0),
        })
    return {"period1": f"{period1_start}~{period1_end}",
            "period2": f"{period2_start}~{period2_end}",
            "data": compare}


# ── 风险评分分布 ──────────────────────────────────────────────────
@router.get("/risk-score-dist")
async def risk_score_dist(start_date: str, end_date: str):
    """风险评分区间分布（结合 AI 标签）"""
    return await execute_query(f"""
        SELECT
            ai_tag,
            CASE
                WHEN s.risk_score < 20  THEN '0-20（低风险）'
                WHEN s.risk_score < 50  THEN '20-50（中低）'
                WHEN s.risk_score < 70  THEN '50-70（中高）'
                WHEN s.risk_score < 90  THEN '70-90（高风险）'
                ELSE '90-100（极高风险）'
            END AS risk_band,
            COUNT(*) AS cnt
        FROM bank_system_log_tagged t
        JOIN bank_system_log s ON t.log_id = s.log_id AND t.log_date = s.log_date
        WHERE t.log_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ai_tag, risk_band
        ORDER BY ai_tag, risk_band
    """)


# ── 原始日志状态 ─────────────────────────────────────────────────
@router.get("/raw-status")
async def raw_status(log_date: str = Query(...)):
    """原始日志 vs 已标签数量对比"""
    raw = await execute_one(
        f"SELECT COUNT(*) AS total FROM bank_system_log WHERE log_date='{log_date}'"
    )
    tagged = await execute_one(
        f"SELECT COUNT(*) AS total, COUNT(DISTINCT classify_method) AS methods "
        f"FROM bank_system_log_tagged WHERE log_date='{log_date}'"
    )
    return {
        "log_date": log_date,
        "raw_total": raw["total"] if raw else 0,
        "tagged_total": tagged["total"] if tagged else 0,
        "pending": (raw["total"] if raw else 0) - (tagged["total"] if tagged else 0),
    }
