"""
AI 日志标签分析服务
基于 user_wide.log_tags（JSON 数组）进行多维分析
"""
import json
from backend.doris.connect import execute_query, execute_one, execute_write

# 已知标签元数据
KNOWN_TAGS = [
    {"tag": "高净值",  "category": "资产特征", "risk": False, "color": "#f7c948"},
    {"tag": "基金偏好", "category": "投资偏好", "risk": False, "color": "#409eff"},
    {"tag": "理财偏好", "category": "投资偏好", "risk": False, "color": "#67c23a"},
    {"tag": "保险偏好", "category": "投资偏好", "risk": False, "color": "#1abc9c"},
    {"tag": "稳健型",  "category": "风险偏好", "risk": False, "color": "#95a5a6"},
    {"tag": "贵宾",   "category": "客户级别", "risk": False, "color": "#e67e22"},
    {"tag": "新客",   "category": "生命周期", "risk": False, "color": "#3498db"},
    {"tag": "高频交易", "category": "行为特征", "risk": False, "color": "#9b59b6"},
    {"tag": "频繁操作", "category": "风险特征", "risk": True,  "color": "#e6a23c"},
    {"tag": "贷款需求", "category": "产品需求", "risk": False, "color": "#2ecc71"},
    {"tag": "异地登录", "category": "风险特征", "risk": True,  "color": "#f56c6c"},
    {"tag": "大额转账", "category": "风险特征", "risk": True,  "color": "#c0392b"},
]

TAG_NAMES = [t["tag"] for t in KNOWN_TAGS]
TAG_META  = {t["tag"]: t for t in KNOWN_TAGS}


class TagAnalysisService:

    async def overview(self):
        """各标签覆盖用户数 + 总体统计"""
        # 每个标签的用户数（LIKE 匹配 JSON 数组内容）
        union_parts = "\nUNION ALL\n".join(
            f"SELECT '{t['tag']}' AS tag_name, '{t['category']}' AS category, "
            f"{int(t['risk'])} AS is_risk, "
            f"COUNT(*) AS user_count "
            f"FROM user_wide WHERE log_tags LIKE '%{t['tag']}%'"
            for t in KNOWN_TAGS
        )
        tag_dist = await execute_query(
            f"SELECT * FROM ({union_parts}) t ORDER BY user_count DESC"
        )
        for r in tag_dist:
            r["color"] = TAG_META.get(r["tag_name"], {}).get("color", "#909399")
            r["user_count"] = int(r["user_count"])
            r["is_risk"] = int(r["is_risk"])

        # 汇总
        summary = await execute_one("""
            SELECT
                COUNT(*) AS total_users,
                SUM(IF(log_tags != '[]' AND log_tags IS NOT NULL AND log_tags != '', 1, 0)) AS tagged_users,
                SUM(anomaly_flag) AS risk_users,
                COUNT(DISTINCT asset_level) AS asset_levels
            FROM user_wide
        """)
        for k, v in (summary or {}).items():
            summary[k] = int(v or 0)

        return {"tag_distribution": tag_dist, "summary": summary}

    async def tag_user_list(self, tag_name: str = None, is_risk: int = None):
        """按标签筛选用户列表"""
        conditions = ["1=1"]
        if tag_name:
            conditions.append(f"log_tags LIKE '%{tag_name}%'")
        if is_risk is not None:
            conditions.append(f"anomaly_flag = {is_risk}")
        where = " AND ".join(conditions)
        rows = await execute_query(f"""
            SELECT user_id, user_name, phone, age_group, city,
                   asset_level, aum_total, active_level, lifecycle_stage,
                   log_tags, anomaly_flag, churn_prob
            FROM user_wide
            WHERE {where}
            ORDER BY anomaly_flag DESC, aum_total DESC
            LIMIT 50
        """)
        for r in rows:
            r["aum_total"] = float(r.get("aum_total") or 0)
            r["churn_prob"] = float(r.get("churn_prob") or 0)
        return rows

    async def risk_tag_analysis(self):
        """风险标签关联的用户异常统计"""
        risk_tags = [t["tag"] for t in KNOWN_TAGS if t["risk"]]
        rows = []
        for tag in risk_tags:
            r = await execute_one(f"""
                SELECT
                    '{tag}' AS tag_name,
                    COUNT(*) AS total_users,
                    SUM(anomaly_flag) AS anomaly_users,
                    ROUND(AVG(aum_total), 1) AS avg_aum,
                    ROUND(AVG(churn_prob) * 100, 1) AS avg_churn_pct
                FROM user_wide
                WHERE log_tags LIKE '%{tag}%'
            """)
            if r:
                r["color"] = TAG_META[tag]["color"]
                r["total_users"] = int(r.get("total_users") or 0)
                r["anomaly_users"] = int(r.get("anomaly_users") or 0)
                r["avg_aum"] = float(r.get("avg_aum") or 0)
                r["avg_churn_pct"] = float(r.get("avg_churn_pct") or 0)
                rows.append(r)
        return rows

    async def tag_asset_cross(self):
        """各标签下的资产等级分布（交叉分析）"""
        rows = []
        for t in KNOWN_TAGS:
            tag = t["tag"]
            dist = await execute_query(f"""
                SELECT asset_level, COUNT(*) AS cnt
                FROM user_wide
                WHERE log_tags LIKE '%{tag}%'
                GROUP BY asset_level
                ORDER BY cnt DESC
            """)
            if dist:
                rows.append({
                    "tag_name": tag,
                    "category": t["category"],
                    "color": t["color"],
                    "asset_dist": [{"level": d["asset_level"], "cnt": int(d["cnt"])} for d in dist]
                })
        return rows

    async def run_classify(self):
        """
        模拟 Doris AI_CLASSIFY 打标签：
        根据用户属性规则推断标签，写入 user_wide.log_tags。
        实际生产中此处替换为 AI_CLASSIFY SQL 一键执行。
        """
        users = await execute_query("""
            SELECT user_id, asset_level, aum_total, active_level,
                   lifecycle_stage, anomaly_flag, preferred_channel
            FROM user_wide
            LIMIT 500
        """)

        tagged_count = 0
        samples = []

        for u in users:
            aum      = float(u.get("aum_total") or 0)
            asset    = u.get("asset_level") or ""
            active   = u.get("active_level") or ""
            lifecycle = u.get("lifecycle_stage") or ""
            anomaly  = int(u.get("anomaly_flag") or 0)
            channel  = u.get("preferred_channel") or ""

            tags = []
            if aum > 80 or asset in ("VIP私行", "VIP钻石"):
                tags.append("高净值")
            if asset in ("VIP私行", "VIP钻石", "VIP铂金"):
                tags.append("贵宾")
            if lifecycle == "新客期":
                tags.append("新客")
            if active == "高活跃":
                tags.append("高频交易")
            if anomaly == 1:
                tags.append("频繁操作")
                if aum > 50:
                    tags.append("大额转账")
                tags.append("异地登录")
            if aum < 8:
                tags.append("贷款需求")
            if "基金" in channel:
                tags.append("基金偏好")
            elif "理财" in channel or aum > 20:
                tags.append("理财偏好")
            if active in ("低活跃",) and aum < 30:
                tags.append("稳健型")
            if "保险" in channel:
                tags.append("保险偏好")

            if tags:
                tags_json = json.dumps(list(dict.fromkeys(tags)), ensure_ascii=False)
                await execute_write(
                    f"UPDATE user_wide SET log_tags = '{tags_json}' WHERE user_id = {u['user_id']}"
                )
                tagged_count += 1
                if len(samples) < 5:
                    samples.append({"user_id": u["user_id"], "tags": tags[:4]})

        return {
            "status": "ok",
            "total": len(users),
            "tagged": tagged_count,
            "samples": samples,
        }

    async def tag_cooccurrence(self):
        """标签共现矩阵（两两同时出现的用户数）"""
        matrix = []
        for i, t1 in enumerate(KNOWN_TAGS):
            for t2 in KNOWN_TAGS[i + 1:]:
                r = await execute_one(
                    f"SELECT COUNT(*) AS cnt FROM user_wide "
                    f"WHERE log_tags LIKE '%{t1['tag']}%' AND log_tags LIKE '%{t2['tag']}%'"
                )
                cnt = int(r["cnt"]) if r else 0
                if cnt > 0:
                    matrix.append({"tag_a": t1["tag"], "tag_b": t2["tag"], "count": cnt})
        return sorted(matrix, key=lambda x: -x["count"])
