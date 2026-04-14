"""
Doris 4.0 AI Function 调用模块
支持：日志自动打标签、用户行为意图识别、异常检测
"""
import json
import logging
import re
from typing import Dict, List, Optional

import httpx

from backend.settings import settings
from backend.doris.connect import execute_query, execute_write, execute_many

logger = logging.getLogger(__name__)


# ── Doris 4.0 AI Function SQL 模板 ──────────────────────────────
# Doris 4.0 通过 ai_function() 内置函数调用外部 LLM
# 语法: ai_completion(provider, model, api_key, endpoint, prompt)

AI_TAG_SQL_TEMPLATE = """
INSERT INTO user_log_tag (log_id, user_id, log_time, log_type, intent_tag, 
                           anomaly_tag, risk_level, ai_raw_result, tag_source, created_at)
SELECT
    log_id,
    user_id,
    log_time,
    -- Doris 4.0 AI Function：日志类型分类
    ai_completion(
        '{provider}',
        '{model}',
        '{api_key}',
        '{endpoint}',
        CONCAT(
            '请对以下银行操作日志进行分类，只返回JSON格式。\\n',
            '日志内容：', log_content, '\\n',
            '返回格式：{{"log_type":"登录日志|交易日志|查询日志|转账日志|异常日志","confidence":0.95}}'
        )
    ) AS log_type_raw,
    -- 用户操作意图识别
    ai_completion(
        '{provider}',
        '{model}',
        '{api_key}',
        '{endpoint}',
        CONCAT(
            '识别银行用户操作意图，只返回JSON。\\n',
            '日志：', log_content, '\\n',
            '返回：{{"intent":"查询余额|转账汇款|购买理财|申请贷款|账户设置|其他","amount_hint":null}}'
        )
    ) AS intent_raw,
    -- 异常行为检测
    ai_completion(
        '{provider}',
        '{model}',
        '{api_key}',
        '{endpoint}',
        CONCAT(
            '检测银行日志中的异常行为，只返回JSON。\\n',
            '日志：', log_content, '\\n',
            '用户历史操作地区：', IFNULL(user_region, '未知'), '\\n',
            '返回：{{"anomaly_type":"正常|异地登录|大额转账|频繁操作|可疑账户","risk_score":0}}'
        )
    ) AS anomaly_raw,
    'AI_AUTO' AS tag_source,
    NOW() AS created_at
FROM user_log_raw
WHERE log_date = '{date}'
  AND log_id NOT IN (SELECT log_id FROM user_log_tag WHERE log_date = '{date}')
LIMIT {batch_size}
"""


class AITagService:
    """日志 AI 自动打标签服务"""

    def __init__(self):
        self.provider = settings.AI_FUNCTION_PROVIDER
        self.model = settings.AI_MODEL
        self.api_key = settings.AI_API_KEY
        self.endpoint = settings.AI_API_ENDPOINT

    async def trigger_ai_tagging(
        self,
        date: str,
        batch_size: int = 100,
        use_doris_ai_function: bool = True,
    ) -> Dict:
        """
        触发 AI 打标签
        优先使用 Doris 4.0 原生 AI Function（推入数据库层执行）
        降级时使用 Python 调用外部 API
        """
        if use_doris_ai_function:
            return await self._tag_via_doris_ai_function(date, batch_size)
        else:
            return await self._tag_via_python_api(date, batch_size)

    async def _tag_via_doris_ai_function(self, date: str, batch_size: int) -> Dict:
        """
        使用 Doris 4.0 AI Function 在数据库层直接执行 AI 推理
        无需将数据传输到应用层，性能最优
        """
        sql = AI_TAG_SQL_TEMPLATE.format(
            provider=self.provider,
            model=self.model,
            api_key=self.api_key,
            endpoint=self.endpoint,
            date=date,
            batch_size=batch_size,
        )
        try:
            affected = await execute_write(sql)
            # 解析 AI 原始结果，更新结构化字段
            await self._parse_and_update_ai_results(date)
            return {
                "method": "doris_ai_function",
                "tagged_count": affected,
                "date": date,
                "status": "success",
            }
        except Exception as e:
            logger.warning(f"Doris AI Function 调用失败，降级到 Python API: {e}")
            return await self._tag_via_python_api(date, batch_size)

    async def _tag_via_python_api(self, date: str, batch_size: int) -> Dict:
        """
        降级方案：Python 调用外部 AI API，批量打标签后写入 Doris
        """
        # 获取待打标签的日志
        logs = await execute_query(
            """
            SELECT log_id, user_id, log_time, log_content, user_region
            FROM user_log_raw
            WHERE log_date = %s
              AND log_id NOT IN (
                  SELECT log_id FROM user_log_tag WHERE log_date = %s
              )
            LIMIT %s
            """,
            (date, date, batch_size),
        )

        if not logs:
            return {"method": "python_api", "tagged_count": 0, "date": date}

        tagged = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for log in logs:
                try:
                    result = await self._call_ai_api(client, log["log_content"], log.get("user_region"))
                    tagged.append({
                        "log_id": log["log_id"],
                        "user_id": log["user_id"],
                        "log_time": str(log["log_time"]),
                        "log_date": date,
                        "log_type": result.get("log_type", "其他"),
                        "intent_tag": result.get("intent", "其他"),
                        "anomaly_tag": result.get("anomaly_type", "正常"),
                        "risk_level": self._score_to_level(result.get("risk_score", 0)),
                        "ai_raw_result": json.dumps(result, ensure_ascii=False),
                        "tag_source": "PYTHON_API",
                    })
                except Exception as e:
                    logger.error(f"AI 打标签失败 log_id={log['log_id']}: {e}")

        # 批量写入
        if tagged:
            cols = list(tagged[0].keys())
            sql = f"""
                INSERT INTO user_log_tag ({','.join(cols)}, created_at)
                VALUES ({','.join(['%s'] * len(cols))}, NOW())
            """
            args = [tuple(r[c] for c in cols) for r in tagged]
            await execute_many(sql, args)

        return {
            "method": "python_api",
            "tagged_count": len(tagged),
            "date": date,
            "status": "success",
        }

    async def _call_ai_api(self, client: httpx.AsyncClient, log_content: str, user_region: str = None) -> Dict:
        """调用外部 AI API（OpenAI 兼容接口）"""
        prompt = f"""分析以下银行操作日志，返回纯JSON（无多余内容）：

日志内容：{log_content}
用户历史所在地区：{user_region or '未知'}

返回格式：
{{
  "log_type": "登录日志|交易日志|查询日志|转账日志|异常日志",
  "intent": "查询余额|转账汇款|购买理财|申请贷款|账户设置|其他",
  "anomaly_type": "正常|异地登录|大额转账|频繁操作|可疑账户",
  "risk_score": 0
}}"""

        resp = await client.post(
            f"{self.endpoint}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": settings.AI_TEMPERATURE,
                "max_tokens": settings.AI_MAX_TOKENS,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        # 提取 JSON
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            return json.loads(match.group())
        return {"log_type": "其他", "intent": "其他", "anomaly_type": "正常", "risk_score": 0}

    async def _parse_and_update_ai_results(self, date: str):
        """解析 Doris AI Function 返回的原始 JSON 字符串，更新结构化字段"""
        sql = """
            UPDATE user_log_tag
            SET
                log_type = JSON_EXTRACT(ai_raw_result, '$.log_type'),
                intent_tag = JSON_EXTRACT(ai_raw_result, '$.intent'),
                anomaly_tag = JSON_EXTRACT(ai_raw_result, '$.anomaly_type'),
                risk_level = CASE
                    WHEN CAST(JSON_EXTRACT(ai_raw_result, '$.risk_score') AS DOUBLE) >= 0.8 THEN '高风险'
                    WHEN CAST(JSON_EXTRACT(ai_raw_result, '$.risk_score') AS DOUBLE) >= 0.5 THEN '中风险'
                    ELSE '低风险'
                END
            WHERE log_date = %s AND log_type IS NULL
        """
        await execute_write(sql, (date,))

    @staticmethod
    def _score_to_level(score: float) -> str:
        if score >= 0.8:
            return "高风险"
        elif score >= 0.5:
            return "中风险"
        return "低风险"

    async def get_tag_stats(self, date: str) -> Dict:
        """获取当日打标签统计"""
        rows = await execute_query(
            """
            SELECT
                log_type,
                intent_tag,
                anomaly_tag,
                risk_level,
                COUNT(*) AS cnt
            FROM user_log_tag
            WHERE log_date = %s
            GROUP BY log_type, intent_tag, anomaly_tag, risk_level
            ORDER BY cnt DESC
            """,
            (date,),
        )
        total_raw = await execute_query(
            "SELECT COUNT(*) AS total FROM user_log_raw WHERE log_date = %s", (date,)
        )
        total_tagged = await execute_query(
            "SELECT COUNT(*) AS total FROM user_log_tag WHERE log_date = %s", (date,)
        )
        return {
            "date": date,
            "total_raw": total_raw[0]["total"] if total_raw else 0,
            "total_tagged": total_tagged[0]["total"] if total_tagged else 0,
            "details": rows,
        }
