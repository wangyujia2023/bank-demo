"""
用户宽表查询 & 人群圈选服务
- 用户宽表多条件查询（HASP 加速）
- Bitmap 人群圈选（不依赖日期过滤，适配模拟数据）
- 数据脱敏
"""
import json
import logging
from typing import Dict, List, Optional

from backend.doris.connect import execute_query, execute_one, execute_write
from backend.settings import settings

logger = logging.getLogger(__name__)


# ─────────────────────────── 数据脱敏 ─────────────────────────────
def mask_id_card(v: str) -> str:
    if not v or len(v) < 8:
        return v
    return v[:3] + "***" + v[-4:]


def mask_phone(v: str) -> str:
    if not v or len(v) < 8:
        return v
    return v[:3] + "****" + v[-4:]


def desensitize(row: Dict) -> Dict:
    r = dict(row)
    if settings.MASK_ID_CARD and r.get("id_card"):
        r["id_card"] = mask_id_card(r["id_card"])
    if settings.MASK_PHONE and r.get("phone"):
        r["phone"] = mask_phone(r["phone"])
    return r


# ─────────────────────────── 用户宽表 ─────────────────────────────
class UserService:

    async def query_wide(
        self,
        user_name: Optional[str] = None,
        id_card: Optional[str] = None,
        phone: Optional[str] = None,
        asset_level: Optional[str] = None,
        active_level: Optional[str] = None,
        lifecycle_stage: Optional[str] = None,
        preferred_channel: Optional[str] = None,
        age_min: Optional[int] = None,
        age_max: Optional[int] = None,
        aum_min: Optional[float] = None,
        aum_max: Optional[float] = None,
        anomaly_flag: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict:
        conditions = ["1=1"]
        args = []

        if user_name:
            conditions.append("user_name LIKE %s")
            args.append(f"%{user_name}%")
        if id_card:
            conditions.append("id_card = %s")
            args.append(id_card)
        if phone:
            conditions.append("phone = %s")
            args.append(phone)
        if asset_level:
            conditions.append("asset_level = %s")
            args.append(asset_level)
        if active_level:
            conditions.append("active_level = %s")
            args.append(active_level)
        if lifecycle_stage:
            conditions.append("lifecycle_stage = %s")
            args.append(lifecycle_stage)
        if preferred_channel:
            conditions.append("preferred_channel = %s")
            args.append(preferred_channel)
        if age_min is not None:
            conditions.append("age >= %s")
            args.append(age_min)
        if age_max is not None:
            conditions.append("age <= %s")
            args.append(age_max)
        if aum_min is not None:
            conditions.append("aum_total >= %s")
            args.append(aum_min)
        if aum_max is not None:
            conditions.append("aum_total <= %s")
            args.append(aum_max)
        if anomaly_flag is not None:
            conditions.append("anomaly_flag = %s")
            args.append(anomaly_flag)

        where = " AND ".join(conditions)

        count_sql = f"SELECT COUNT(1) AS total FROM user_wide WHERE {where}"
        count_result = await execute_one(count_sql, tuple(args) if args else None)
        total = count_result["total"] if count_result else 0

        offset = (page - 1) * page_size
        data_sql = f"""
            SELECT
                user_id, user_name, id_card, phone, gender, age, age_group,
                city, province, asset_level, aum_total, deposit_amount,
                fund_amount, loan_amount, wm_amount,
                has_credit_card, credit_score, credit_grade, risk_level,
                preferred_channel, app_login_30d, active_level,
                lifecycle_stage, churn_prob, anomaly_flag, log_tags,
                register_date, updated_at
            FROM user_wide
            WHERE {where}
            ORDER BY aum_total DESC
            LIMIT %s OFFSET %s
        """
        rows = await execute_query(data_sql, tuple(args) + (page_size, offset))
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "rows": [desensitize(r) for r in rows],
        }

    async def get_user_detail(self, user_id: int) -> Optional[Dict]:
        row = await execute_one(
            "SELECT * FROM user_wide WHERE user_id = %s LIMIT 1",
            (user_id,),
        )
        return desensitize(row) if row else None


# ─────────────────────────── 人群圈选 ─────────────────────────────
class SegmentService:

    async def count_segment(self, rules: List[Dict]) -> Dict:
        """
        实时估算人群规模（Bitmap 圈选，不按日期过滤）
        rules 之间默认 AND
        """
        bitmap_expr = self._build_bitmap_expr(rules)
        sql = f"SELECT BITMAP_COUNT({bitmap_expr}) AS crowd_size"
        result = await execute_one(sql)
        return {
            "crowd_size": int(result["crowd_size"]) if result else 0,
            "rules": rules,
        }

    async def create_segment(
        self,
        segment_name: str,
        rules: List[Dict],
        description: str = "",
        created_by: str = "system",
    ) -> Dict:
        import time
        segment_id = int(time.time() * 1000)
        bitmap_expr = self._build_bitmap_expr(rules)

        sql = f"""
            INSERT INTO user_segment
                (segment_id, segment_name, segment_desc, rule_config,
                 segment_type, snap_date, segment_bitmap, user_count,
                 status, created_by, created_at, updated_at)
            SELECT
                {segment_id},
                '{segment_name}',
                '{description}',
                '{json.dumps(rules, ensure_ascii=False).replace("'", "''")}',
                'BITMAP',
                CURDATE(),
                {bitmap_expr},
                BITMAP_COUNT({bitmap_expr}),
                1,
                '{created_by}',
                NOW(),
                NOW()
        """
        await execute_write(sql)

        count_result = await execute_one(
            "SELECT user_count FROM user_segment WHERE segment_id = %s", (segment_id,)
        )
        return {
            "segment_id": segment_id,
            "segment_name": segment_name,
            "user_count": int(count_result["user_count"]) if count_result else 0,
            "status": "success",
        }

    async def list_segments(self) -> List[Dict]:
        rows = await execute_query(
            """
            SELECT segment_id, segment_name, segment_desc, rule_config, segment_type,
                   snap_date, user_count, status, created_by, created_at, updated_at
            FROM user_segment
            WHERE status = 1
            ORDER BY created_at DESC
            LIMIT 100
            """
        )
        for r in rows:
            if r.get("rule_config"):
                try:
                    r["rule_config"] = json.loads(r["rule_config"])
                except Exception:
                    pass
            r["user_count"] = int(r.get("user_count") or 0)
        return rows

    async def delete_segment(self, segment_id: int) -> bool:
        n = await execute_write(
            "UPDATE user_segment SET status = 0, updated_at = NOW() WHERE segment_id = %s",
            (segment_id,),
        )
        return n >= 0

    async def get_segment_users(self, segment_id: int, page: int = 1, size: int = 20) -> Dict:
        offset = (page - 1) * size
        rows = await execute_query(
            f"""
            SELECT u.user_id, u.user_name, u.phone, u.age, u.age_group,
                   u.city, u.asset_level, u.aum_total, u.active_level,
                   u.lifecycle_stage, u.credit_grade, u.risk_level,
                   u.preferred_channel, u.churn_prob, u.anomaly_flag
            FROM user_wide u
            JOIN (SELECT segment_bitmap FROM user_segment WHERE segment_id = %s LIMIT 1) s
              ON BITMAP_CONTAINS(s.segment_bitmap, u.user_id) = 1
            ORDER BY u.aum_total DESC
            LIMIT {size} OFFSET {offset}
            """,
            (segment_id,),
        )
        return {"segment_id": segment_id, "rows": [desensitize(r) for r in rows]}

    async def get_segment_stats(self, segment_id: int) -> Dict:
        """人群分布统计"""
        base = f"""
            SELECT u.*
            FROM user_wide u
            JOIN (SELECT segment_bitmap FROM user_segment WHERE segment_id = {segment_id} LIMIT 1) s
              ON BITMAP_CONTAINS(s.segment_bitmap, u.user_id) = 1
        """
        asset_dist = await execute_query(
            f"SELECT asset_level, COUNT(*) AS cnt FROM ({base}) t GROUP BY asset_level ORDER BY cnt DESC"
        )
        active_dist = await execute_query(
            f"SELECT active_level, COUNT(*) AS cnt FROM ({base}) t GROUP BY active_level ORDER BY cnt DESC"
        )
        lifecycle_dist = await execute_query(
            f"SELECT lifecycle_stage, COUNT(*) AS cnt FROM ({base}) t GROUP BY lifecycle_stage ORDER BY cnt DESC"
        )
        channel_dist = await execute_query(
            f"SELECT preferred_channel, COUNT(*) AS cnt FROM ({base}) t GROUP BY preferred_channel ORDER BY cnt DESC"
        )
        aum_stat = await execute_one(
            f"SELECT AVG(aum_total) AS avg_aum, MAX(aum_total) AS max_aum, MIN(aum_total) AS min_aum FROM ({base}) t"
        )
        return {
            "asset_dist": asset_dist,
            "active_dist": active_dist,
            "lifecycle_dist": lifecycle_dist,
            "channel_dist": channel_dist,
            "aum_stat": {k: float(v or 0) for k, v in (aum_stat or {}).items()},
        }

    def _build_bitmap_expr(self, rules: List[Dict]) -> str:
        if not rules:
            return "BITMAP_EMPTY()"

        sub_exprs = []
        for rule in rules:
            tag_name = rule["tag_name"]
            tag_values = rule.get("tag_values", [])
            logic = rule.get("op", "OR").upper()

            if not tag_values:
                continue

            if len(tag_values) == 1:
                sub = self._single_bitmap_query(tag_name, tag_values[0])
            else:
                inner = [self._single_bitmap_query(tag_name, v) for v in tag_values]
                if logic == "AND":
                    sub = f"BITMAP_AND({', '.join(inner)})"
                else:
                    sub = f"BITMAP_OR({', '.join(inner)})"
            sub_exprs.append((sub, rule.get("exclude", False)))

        if not sub_exprs:
            return "BITMAP_EMPTY()"

        include = [e for e, excl in sub_exprs if not excl]
        exclude = [e for e, excl in sub_exprs if excl]

        if not include:
            return "BITMAP_EMPTY()"

        result = f"BITMAP_AND({', '.join(include)})" if len(include) > 1 else include[0]

        if exclude:
            excl_bitmap = f"BITMAP_OR({', '.join(exclude)})" if len(exclude) > 1 else exclude[0]
            result = f"BITMAP_AND_NOT({result}, {excl_bitmap})"

        return result

    @staticmethod
    def _single_bitmap_query(tag_name: str, tag_value: str) -> str:
        # 不按 tag_date 过滤，直接按 tag_name + tag_value 聚合
        return (
            f"(SELECT IFNULL(BITMAP_UNION(user_bitmap), BITMAP_EMPTY()) "
            f"FROM user_tag "
            f"WHERE tag_name = '{tag_name}' AND tag_value = '{tag_value}')"
        )
