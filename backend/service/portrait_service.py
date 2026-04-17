"""
人群画像 & 人群包扩展服务
- TGI 指数：目标人群 vs 全体基准
- 交叉分析：两标签分类的共现矩阵
- 地域分布：province/city 分布（from user_wide）
- 人群包：内存存储 + 标签权重 + 对比
- 异常检测：anomaly_flag 用户的标签画像
"""
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional

from backend.doris.connect import execute_query, execute_one
from backend.service.cdp_service import TAG_DEFS, _TAG_BY_ID, _TAG_BY_COL, _TAG_COLS

logger = logging.getLogger(__name__)

# ── 内存人群包存储 ─────────────────────────────────────────────
_CROWD_STORE: Dict[str, Dict] = {}


# ══════════════════════════════════════════════════════
# 人群画像
# ══════════════════════════════════════════════════════
class PortraitService:

    async def tgi_analysis(self, include_tag_ids: List[int]) -> List[Dict]:
        """
        TGI = (目标人群该标签占比 / 全体基准占比) × 100
        include_tag_ids 为空时返回全体自身 TGI（均为100）
        """
        total_sql = f"""
            SELECT COUNT(*) AS total,
                   {', '.join(f'SUM({col}) AS {col}' for _, col, _, _ in TAG_DEFS)}
            FROM bank.user_tag_wide
        """
        base_row = await execute_one(total_sql)
        base_total = int(base_row.get("total") or 0)
        if base_total == 0:
            return []

        # 目标人群条件
        seg_conditions = []
        for tid in include_tag_ids:
            info = _TAG_BY_ID.get(tid)
            if info:
                seg_conditions.append(f"{info[0]} = 1")
        where = " AND ".join(seg_conditions) if seg_conditions else "1=1"

        seg_sql = f"""
            SELECT COUNT(*) AS total,
                   {', '.join(f'SUM({col}) AS {col}' for _, col, _, _ in TAG_DEFS)}
            FROM bank.user_tag_wide
            WHERE {where}
        """
        seg_row = await execute_one(seg_sql)
        seg_total = int(seg_row.get("total") or 0)

        result = []
        for tid, col, label, cat in TAG_DEFS:
            if col in (seg_conditions or []):
                continue  # 跳过用于圈选的标签
            base_cnt = int(base_row.get(col) or 0)
            seg_cnt  = int(seg_row.get(col) or 0)
            base_pct = base_cnt / base_total * 100 if base_total > 0 else 0
            seg_pct  = seg_cnt  / seg_total  * 100 if seg_total  > 0 else 0
            tgi = round(seg_pct / base_pct * 100, 1) if base_pct > 0 else 0

            result.append({
                "tag_id":    tid,
                "col":       col,
                "label":     label,
                "category":  cat,
                "base_pct":  round(base_pct, 2),
                "seg_pct":   round(seg_pct, 2),
                "base_cnt":  base_cnt,
                "seg_cnt":   seg_cnt,
                "tgi":       tgi,
                "seg_total": seg_total,
            })

        return sorted(result, key=lambda x: x["tgi"], reverse=True)

    async def cross_analysis(self, cat1: str, cat2: str) -> Dict:
        """
        两个标签分类交叉共现矩阵
        cat1/cat2 为 category 名称（如"资产"、"行为"）
        """
        tags1 = [(tid, col, label) for tid, col, label, cat in TAG_DEFS if cat == cat1]
        tags2 = [(tid, col, label) for tid, col, label, cat in TAG_DEFS if cat == cat2]

        if not tags1 or not tags2:
            return {"tags1": [], "tags2": [], "matrix": []}

        # 一次查询算出所有组合
        select_parts = []
        for _, c1, _ in tags1:
            for _, c2, _ in tags2:
                select_parts.append(
                    f"SUM(IF({c1}=1 AND {c2}=1, 1, 0)) AS `{c1}_x_{c2}`"
                )

        sql = f"SELECT COUNT(*) AS total, {', '.join(select_parts)} FROM bank.user_tag_wide"
        row = await execute_one(sql)
        total = int(row.get("total") or 0)

        matrix = []
        for _, c1, l1 in tags1:
            row_data = {"label": l1, "cells": []}
            for _, c2, l2 in tags2:
                val = int(row.get(f"{c1}_x_{c2}") or 0)
                pct = round(val / total * 100, 1) if total > 0 else 0
                row_data["cells"].append({"count": val, "pct": pct})
            matrix.append(row_data)

        return {
            "tags1": [l for _, _, l in tags1],
            "tags2": [l for _, _, l in tags2],
            "matrix": matrix,
            "total": total,
        }

    async def geo_distribution(self, include_tag_ids: List[int] = None) -> List[Dict]:
        """省份分布（JOIN user_wide.province & user_tag_wide）"""
        if include_tag_ids:
            # 先拿出 customer_id 集合（用宽表条件过滤）
            conditions = []
            for tid in include_tag_ids:
                info = _TAG_BY_ID.get(tid)
                if info:
                    conditions.append(f"t.{info[0]} = 1")
            tag_where = " AND ".join(conditions) if conditions else "1=1"
            sql = f"""
                SELECT w.province, w.city, COUNT(*) AS cnt
                FROM bank.user_tag_wide t
                JOIN user_wide w ON t.customer_id = w.user_id
                WHERE {tag_where}
                GROUP BY w.province, w.city
                ORDER BY cnt DESC
                LIMIT 50
            """
        else:
            sql = """
                SELECT province, city, COUNT(*) AS cnt
                FROM user_wide
                WHERE province IS NOT NULL AND province != ''
                GROUP BY province, city
                ORDER BY cnt DESC
                LIMIT 50
            """
        rows = await execute_query(sql)
        # 聚合到省份级别
        province_map: Dict[str, int] = {}
        city_list = []
        for r in rows:
            p = r.get("province") or "未知"
            c = r.get("city") or ""
            cnt = int(r.get("cnt") or 0)
            province_map[p] = province_map.get(p, 0) + cnt
            city_list.append({"province": p, "city": c, "cnt": cnt})

        province_list = [{"province": k, "cnt": v} for k, v in
                         sorted(province_map.items(), key=lambda x: x[1], reverse=True)]
        return {"province_list": province_list, "city_list": city_list}

    async def targeting_distribution(self, provinces: List[str]) -> Dict:
        """投放地域人群预估（按省份圈选）"""
        if not provinces:
            return {"total": 0, "detail": []}
        prov_in = ", ".join(f"'{p}'" for p in provinces)
        sql = f"""
            SELECT w.province, COUNT(*) AS cnt,
                   ROUND(AVG(w.aum_total), 2) AS avg_aum,
                   SUM(w.anomaly_flag) AS anomaly_cnt
            FROM user_wide w
            WHERE w.province IN ({prov_in})
            GROUP BY w.province
            ORDER BY cnt DESC
        """
        rows = await execute_query(sql)
        total = sum(int(r.get("cnt") or 0) for r in rows)
        return {"total": total, "detail": rows}


# ══════════════════════════════════════════════════════
# 人群包管理
# ══════════════════════════════════════════════════════
class CrowdPackageService:

    def save(
        self,
        name: str,
        desc: str,
        include_tag_ids: List[int],
        exclude_tag_ids: List[int],
        crowd_size: int,
    ) -> Dict:
        cid = str(uuid.uuid4())[:8]
        pkg = {
            "crowd_id":       cid,
            "name":           name,
            "desc":           desc,
            "include_tag_ids": include_tag_ids,
            "exclude_tag_ids": exclude_tag_ids,
            "crowd_size":     crowd_size,
            "include_labels": [_TAG_BY_ID[t][1] for t in include_tag_ids if t in _TAG_BY_ID],
            "exclude_labels": [_TAG_BY_ID[t][1] for t in exclude_tag_ids if t in _TAG_BY_ID],
            "created_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        _CROWD_STORE[cid] = pkg
        return pkg

    def list_all(self) -> List[Dict]:
        return sorted(_CROWD_STORE.values(), key=lambda x: x["created_at"], reverse=True)

    def get(self, crowd_id: str) -> Optional[Dict]:
        return _CROWD_STORE.get(crowd_id)

    def delete(self, crowd_id: str) -> bool:
        if crowd_id in _CROWD_STORE:
            del _CROWD_STORE[crowd_id]
            return True
        return False

    async def compare(self, id_a: str, id_b: str) -> Dict:
        """对比两个人群包在各标签上的命中率"""
        pkg_a = _CROWD_STORE.get(id_a)
        pkg_b = _CROWD_STORE.get(id_b)
        if not pkg_a or not pkg_b:
            return {"error": "人群包不存在"}

        async def _distribution(tag_ids, exclude_ids):
            conditions = [f"{_TAG_BY_ID[t][0]} = 1" for t in tag_ids if t in _TAG_BY_ID]
            ex_conds   = [f"{_TAG_BY_ID[t][0]} = 0" for t in exclude_ids if t in _TAG_BY_ID]
            all_conds  = conditions + ex_conds
            where = " AND ".join(all_conds) if all_conds else "1=1"
            cols  = ", ".join(f"ROUND(AVG({col})*100, 1) AS {col}" for _, col, _, _ in TAG_DEFS)
            sql   = f"SELECT COUNT(*) AS total, {cols} FROM bank.user_tag_wide WHERE {where}"
            return await execute_one(sql)

        row_a = await _distribution(pkg_a["include_tag_ids"], pkg_a["exclude_tag_ids"])
        row_b = await _distribution(pkg_b["include_tag_ids"], pkg_b["exclude_tag_ids"])

        diffs = []
        for tid, col, label, cat in TAG_DEFS:
            pct_a = float(row_a.get(col) or 0)
            pct_b = float(row_b.get(col) or 0)
            diffs.append({
                "tag_id":  tid,
                "label":   label,
                "category": cat,
                "pct_a":   pct_a,
                "pct_b":   pct_b,
                "diff":    round(pct_a - pct_b, 1),
            })

        diffs.sort(key=lambda x: abs(x["diff"]), reverse=True)
        return {
            "pkg_a":   {"name": pkg_a["name"], "size": pkg_a["crowd_size"]},
            "pkg_b":   {"name": pkg_b["name"], "size": pkg_b["crowd_size"]},
            "total_a": int(row_a.get("total") or 0),
            "total_b": int(row_b.get("total") or 0),
            "diffs":   diffs[:30],  # Top30差异标签
        }


# ══════════════════════════════════════════════════════
# 标签权重 & 异常检测
# ══════════════════════════════════════════════════════
class TagAnalysisService:

    async def tag_weight(self, include_tag_ids: List[int]) -> List[Dict]:
        """
        目标人群中各标签的权重 = TGI
        同时输出排序、分类汇总
        """
        svc = PortraitService()
        items = await svc.tgi_analysis(include_tag_ids)
        # 过滤掉 TGI=0 的（数据不足）
        items = [x for x in items if x["tgi"] > 0]
        # 分类汇总
        cat_map: Dict[str, List] = {}
        for item in items:
            cat_map.setdefault(item["category"], []).append(item)
        categories = [
            {
                "category": cat,
                "avg_tgi":  round(sum(x["tgi"] for x in lst) / len(lst), 1),
                "tags":     lst[:10],
            }
            for cat, lst in cat_map.items()
        ]
        categories.sort(key=lambda x: x["avg_tgi"], reverse=True)
        return {"items": items[:40], "categories": categories}

    async def anomaly_detect(self) -> Dict:
        """
        异常用户（user_wide.anomaly_flag=1）的标签画像
        与正常用户对比，计算各标签 TGI
        """
        # 异常用户 customer_id 列表
        ids_row = await execute_query(
            "SELECT user_id FROM user_wide WHERE anomaly_flag = 1 LIMIT 5000"
        )
        anomaly_ids = [r["user_id"] for r in ids_row]

        total_sql = f"""
            SELECT COUNT(*) AS total,
                   {', '.join(f'SUM({col}) AS {col}' for _, col, _, _ in TAG_DEFS)}
            FROM bank.user_tag_wide
        """
        base_row = await execute_one(total_sql)
        base_total = int(base_row.get("total") or 0)

        if not anomaly_ids or base_total == 0:
            return {"anomaly_count": 0, "tags": []}

        ids_str = ", ".join(str(i) for i in anomaly_ids)
        anom_sql = f"""
            SELECT COUNT(*) AS total,
                   {', '.join(f'SUM({col}) AS {col}' for _, col, _, _ in TAG_DEFS)}
            FROM bank.user_tag_wide
            WHERE customer_id IN ({ids_str})
        """
        anom_row = await execute_one(anom_sql)
        anom_total = int(anom_row.get("total") or 0)

        tags = []
        for tid, col, label, cat in TAG_DEFS:
            base_cnt = int(base_row.get(col) or 0)
            anom_cnt = int(anom_row.get(col) or 0)
            base_pct = base_cnt / base_total * 100 if base_total > 0 else 0
            anom_pct = anom_cnt / anom_total  * 100 if anom_total  > 0 else 0
            tgi = round(anom_pct / base_pct * 100, 1) if base_pct > 0 else 0
            tags.append({
                "tag_id": tid, "label": label, "category": cat,
                "base_pct": round(base_pct, 2), "anom_pct": round(anom_pct, 2),
                "anom_cnt": anom_cnt, "tgi": tgi,
            })

        tags.sort(key=lambda x: x["tgi"], reverse=True)
        return {
            "anomaly_count": len(anomaly_ids),
            "total_users":   base_total,
            "tags":          tags[:30],
        }
