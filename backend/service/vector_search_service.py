"""
HASP 向量检索服务 - 用户图片识别标签场景
基于 Doris cosine_distance() + 向量化执行引擎（HASP）
向量维度: 8 （活跃度/智力/幽默/冒险/科技/运动/艺术/社交）
"""
from backend.doris.connect import execute_query, execute_write, execute_one
import json
import io
import math
import random

DIM = 8
DIM_LABELS = ["活跃度", "智力", "幽默感", "冒险精神", "科技属性", "运动属性", "艺术感", "社交性"]

# 描述关键词 → 维度映射（用于文本转向量）
KEYWORD_DIM_MAP = {
    "活跃": 0, "活泼": 0, "积极": 0, "热情": 0, "能量": 0, "激情": 0, "热血": 0,
    "智慧": 1, "聪明": 1, "智力": 1, "理性": 1, "思维": 1, "学习": 1, "分析": 1,
    "幽默": 2, "搞笑": 2, "风趣": 2, "有趣": 2, "逗": 2, "乐观": 2,
    "冒险": 3, "勇敢": 3, "探索": 3, "进取": 3, "挑战": 3, "无畏": 3,
    "科技": 4, "技术": 4, "数字": 4, "创新": 4, "智能": 4, "程序": 4, "AI": 4,
    "运动": 5, "健身": 5, "体育": 5, "活力": 5, "锻炼": 5, "跑步": 5,
    "艺术": 6, "创意": 6, "审美": 6, "文艺": 6, "设计": 6, "美术": 6, "音乐": 6,
    "社交": 7, "友善": 7, "合群": 7, "外向": 7, "交际": 7, "热心": 7, "开朗": 7,
}

# 维度含义：[活跃度, 智力, 幽默感, 冒险精神, 科技属性, 运动属性, 艺术感, 社交性]
# 稀疏向量：每个角色仅在 2-3 个峰值维度高 (0.88-0.95)，其余接近 0 (0.05-0.15)
# 这样 cosine_distance 在不同"类型"间差异显著 (>0.5)，同类型相近 (<0.15)

CARTOON_USERS = [
    {"user_id": 1001, "user_name": "皮卡丘",    "avatar_style": "pikachu",
     "description": "活泼电系精灵，能量充沛",
     # 活跃度↑ 冒险精神↑ 社交性↑
     "embedding": [0.94, 0.07, 0.12, 0.88, 0.06, 0.10, 0.08, 0.92],
     "labels": ["活跃达人", "娱乐消费"]},
    {"user_id": 1002, "user_name": "哆啦A梦",   "avatar_style": "doraemon",
     "description": "来自未来的科技猫型机器人",
     # 智力↑ 科技属性↑
     "embedding": [0.10, 0.95, 0.12, 0.08, 0.93, 0.06, 0.15, 0.12],
     "labels": ["科技爱好者", "高价值客户"]},
    {"user_id": 1003, "user_name": "鸣人",       "avatar_style": "naruto",
     "description": "忍者村热血少年，永不放弃",
     # 活跃度↑ 冒险精神↑ 运动属性↑
     "embedding": [0.93, 0.08, 0.10, 0.95, 0.07, 0.90, 0.06, 0.12],
     "labels": ["活跃达人", "冒险进取"]},
    {"user_id": 1004, "user_name": "海绵宝宝",  "avatar_style": "spongebob",
     "description": "深海厨师，乐观开朗无极限",
     # 幽默感↑ 社交性↑ 活跃度↑
     "embedding": [0.80, 0.07, 0.95, 0.12, 0.05, 0.10, 0.18, 0.90],
     "labels": ["娱乐消费", "年轻潮流"]},
    {"user_id": 1005, "user_name": "樱桃小丸子", "avatar_style": "maruko",
     "description": "爱幻想的小学生，慵懒可爱",
     # 艺术感↑ 幽默感↑
     "embedding": [0.15, 0.20, 0.75, 0.10, 0.06, 0.08, 0.93, 0.25],
     "labels": ["学生群体", "娱乐消费"]},
    {"user_id": 1006, "user_name": "蜡笔小新",  "avatar_style": "shinchan",
     "description": "搞笑幼稚园小孩，鬼马精灵",
     # 幽默感↑ 活跃度↑
     "embedding": [0.75, 0.08, 0.95, 0.35, 0.05, 0.12, 0.10, 0.40],
     "labels": ["学生群体", "娱乐消费"]},
    {"user_id": 1007, "user_name": "柯南",       "avatar_style": "conan",
     "description": "高中侦探被缩小，智力超群",
     # 智力↑ 冒险精神↑
     "embedding": [0.12, 0.95, 0.08, 0.88, 0.25, 0.07, 0.10, 0.15],
     "labels": ["科技爱好者", "高价值客户"]},
    {"user_id": 1008, "user_name": "路飞",       "avatar_style": "luffy",
     "description": "海贼王梦想者，橡皮恶魔果实",
     # 冒险精神↑ 活跃度↑ 运动属性↑
     "embedding": [0.88, 0.07, 0.12, 0.95, 0.06, 0.88, 0.08, 0.15],
     "labels": ["冒险进取", "活跃达人"]},
    {"user_id": 1009, "user_name": "美加子",     "avatar_style": "misae",
     "description": "家庭主妇，持家有道爱购物",
     # 艺术感↑ 社交性↑
     "embedding": [0.18, 0.35, 0.55, 0.12, 0.08, 0.22, 0.90, 0.88],
     "labels": ["理财达人", "稳健保守"]},
    {"user_id": 1010, "user_name": "大雄",       "avatar_style": "nobita",
     "description": "懒散小学生，爱哭鬼依赖型",
     # 全维度偏低，无明显特长
     "embedding": [0.12, 0.10, 0.30, 0.08, 0.05, 0.06, 0.22, 0.18],
     "labels": ["学生群体", "稳健保守"]},
]

AVATAR_LABELS = [
    {"label_id": 1, "label_name": "活跃达人",  "category": "行为特征", "color": "#f56c6c",
     "label_desc": "高频登录、交互活跃",
     # 活跃度↑ 社交性↑
     "embedding": [0.95, 0.08, 0.15, 0.20, 0.06, 0.15, 0.08, 0.92]},
    {"label_id": 2, "label_name": "高价值客户", "category": "资产特征", "color": "#e6a23c",
     "label_desc": "AUM高、持有多款产品",
     # 智力↑ 科技属性↑
     "embedding": [0.10, 0.92, 0.12, 0.15, 0.88, 0.08, 0.18, 0.12]},
    {"label_id": 3, "label_name": "年轻潮流",  "category": "人群特征", "color": "#409eff",
     "label_desc": "18-30岁，追求新鲜感",
     # 活跃度↑ 幽默感↑ 社交性↑
     "embedding": [0.85, 0.12, 0.82, 0.25, 0.18, 0.12, 0.28, 0.88]},
    {"label_id": 4, "label_name": "理财达人",  "category": "资产特征", "color": "#67c23a",
     "label_desc": "偏好理财基金，风险意识强",
     # 智力↑ 艺术感↑
     "embedding": [0.12, 0.92, 0.08, 0.10, 0.25, 0.06, 0.78, 0.18]},
    {"label_id": 5, "label_name": "科技爱好者", "category": "兴趣特征", "color": "#9b59b6",
     "label_desc": "APP超级用户，科技早采用者",
     # 科技属性↑ 智力↑
     "embedding": [0.10, 0.88, 0.08, 0.12, 0.95, 0.07, 0.10, 0.12]},
    {"label_id": 6, "label_name": "运动健康",  "category": "兴趣特征", "color": "#1abc9c",
     "label_desc": "健身消费、运动场景活跃",
     # 运动属性↑ 冒险精神↑
     "embedding": [0.22, 0.08, 0.06, 0.78, 0.08, 0.95, 0.06, 0.18]},
    {"label_id": 7, "label_name": "娱乐消费",  "category": "消费特征", "color": "#e74c3c",
     "label_desc": "视频、游戏、演出高消费",
     # 幽默感↑ 社交性↑ 活跃度↑
     "embedding": [0.80, 0.07, 0.93, 0.18, 0.10, 0.12, 0.38, 0.88]},
    {"label_id": 8, "label_name": "稳健保守",  "category": "风险特征", "color": "#95a5a6",
     "label_desc": "偏好存款和保本产品",
     # 全维度偏低，无强特征
     "embedding": [0.10, 0.38, 0.08, 0.05, 0.18, 0.06, 0.32, 0.12]},
    {"label_id": 9, "label_name": "冒险进取",  "category": "风险特征", "color": "#e67e22",
     "label_desc": "高风险偏好，积极型投资",
     # 冒险精神↑ 运动属性↑ 活跃度↑
     "embedding": [0.88, 0.10, 0.12, 0.95, 0.07, 0.88, 0.06, 0.18]},
    {"label_id": 10, "label_name": "学生群体",  "category": "人群特征", "color": "#3498db",
     "label_desc": "在校学生，信用卡起步期",
     # 幽默感↑ 艺术感↑
     "embedding": [0.22, 0.30, 0.72, 0.18, 0.12, 0.18, 0.68, 0.32]},
]


def _vec_str(vec: list) -> str:
    return "[" + ",".join(f"{v:.4f}" for v in vec) + "]"


class VectorSearchService:

    async def init_tables(self) -> dict:
        """建表 + 写入示例数据"""
        ddl_user = """
            CREATE TABLE IF NOT EXISTS user_avatar (
                user_id       BIGINT       NOT NULL COMMENT '用户ID',
                user_name     VARCHAR(64)           COMMENT '用户名',
                avatar_style  VARCHAR(32)           COMMENT '头像风格标识',
                description   VARCHAR(256)          COMMENT '角色描述',
                photo_embedding ARRAY<FLOAT>        COMMENT '照片特征向量(8维)',
                labels        VARCHAR(256)          COMMENT '标签JSON',
                create_time   DATETIME              COMMENT '创建时间'
            ) ENGINE=OLAP
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 4
            PROPERTIES ("replication_num"="1")
        """
        ddl_label = """
            CREATE TABLE IF NOT EXISTS avatar_label (
                label_id        BIGINT       NOT NULL COMMENT '标签ID',
                label_name      VARCHAR(64)           COMMENT '标签名',
                category        VARCHAR(32)           COMMENT '标签分类',
                color           VARCHAR(16)           COMMENT '展示颜色',
                label_desc      VARCHAR(256)          COMMENT '标签描述',
                label_embedding ARRAY<FLOAT>          COMMENT '标签特征向量(8维)',
                user_count      INT                   COMMENT '关联用户数',
                create_time     DATETIME              COMMENT '创建时间'
            ) ENGINE=OLAP
            DUPLICATE KEY(label_id)
            DISTRIBUTED BY HASH(label_id) BUCKETS 4
            PROPERTIES ("replication_num"="1")
        """
        await execute_write(ddl_user)
        await execute_write(ddl_label)

        # 清空旧数据
        await execute_write("TRUNCATE TABLE user_avatar")
        await execute_write("TRUNCATE TABLE avatar_label")

        # 写入用户数据
        for u in CARTOON_USERS:
            labels_json = json.dumps(u["labels"], ensure_ascii=False)
            vec = _vec_str(u["embedding"])
            sql = f"""
                INSERT INTO user_avatar
                  (user_id, user_name, avatar_style, description, photo_embedding, labels, create_time)
                VALUES
                  ({u['user_id']}, '{u['user_name']}', '{u['avatar_style']}',
                   '{u['description']}', '{vec}',
                   '{labels_json}', NOW())
            """
            await execute_write(sql)

        # 写入标签数据
        for lb in AVATAR_LABELS:
            vec = _vec_str(lb["embedding"])
            sql = f"""
                INSERT INTO avatar_label
                  (label_id, label_name, category, color, label_desc, label_embedding, user_count, create_time)
                VALUES
                  ({lb['label_id']}, '{lb['label_name']}', '{lb['category']}',
                   '{lb['color']}', '{lb['label_desc']}', '{vec}',
                   0, NOW())
            """
            await execute_write(sql)

        # 更新标签 user_count
        await execute_write("""
            UPDATE avatar_label al
            SET user_count = (
                SELECT COUNT(*) FROM user_avatar ua
                WHERE JSON_CONTAINS(ua.labels, CONCAT('"', al.label_name, '"'))
            )
        """)

        return {"status": "ok", "users": len(CARTOON_USERS), "labels": len(AVATAR_LABELS)}

    async def get_users(self) -> list:
        rows = await execute_query(
            "SELECT user_id, user_name, avatar_style, description, photo_embedding, labels "
            "FROM user_avatar ORDER BY user_id"
        )
        for r in rows:
            try:
                r["labels"] = json.loads(r["labels"]) if r["labels"] else []
            except Exception:
                r["labels"] = []
            try:
                emb = r["photo_embedding"]
                if isinstance(emb, str):
                    emb = emb.strip()
                    if emb.startswith("["):
                        r["photo_embedding"] = [float(x) for x in emb[1:-1].split(",")]
                    else:
                        r["photo_embedding"] = []
            except Exception:
                r["photo_embedding"] = []
        return rows

    async def get_labels(self) -> list:
        rows = await execute_query(
            "SELECT label_id, label_name, category, color, label_desc, label_embedding, user_count "
            "FROM avatar_label ORDER BY label_id"
        )
        for r in rows:
            try:
                emb = r["label_embedding"]
                if isinstance(emb, str):
                    emb = emb.strip()
                    if emb.startswith("["):
                        r["label_embedding"] = [float(x) for x in emb[1:-1].split(",")]
                    else:
                        r["label_embedding"] = []
            except Exception:
                r["label_embedding"] = []
        return rows

    async def search_similar_users(self, query_vector: list, top_k: int = 5) -> list:
        """以向量检索最相似用户（cosine_distance，HASP 向量化加速）"""
        vec = _vec_str(query_vector)
        sql = f"""
            SELECT
                user_id, user_name, avatar_style, description, labels,
                ROUND(cosine_distance(photo_embedding, CAST('{vec}' AS ARRAY<FLOAT>)), 6) AS distance,
                ROUND(1 - cosine_distance(photo_embedding, CAST('{vec}' AS ARRAY<FLOAT>)), 4) AS similarity
            FROM user_avatar
            ORDER BY distance ASC
            LIMIT {top_k}
        """
        rows = await execute_query(sql)
        for r in rows:
            try:
                r["labels"] = json.loads(r["labels"]) if r["labels"] else []
            except Exception:
                r["labels"] = []
            r["distance"] = float(r.get("distance", 1))
            r["similarity"] = float(r.get("similarity", 0))
        return rows

    async def search_similar_labels(self, query_vector: list, top_k: int = 5) -> list:
        """以向量检索最相似标签"""
        vec = _vec_str(query_vector)
        sql = f"""
            SELECT
                label_id, label_name, category, color, label_desc, user_count,
                ROUND(cosine_distance(label_embedding, CAST('{vec}' AS ARRAY<FLOAT>)), 6) AS distance,
                ROUND(1 - cosine_distance(label_embedding, CAST('{vec}' AS ARRAY<FLOAT>)), 4) AS similarity
            FROM avatar_label
            ORDER BY distance ASC
            LIMIT {top_k}
        """
        rows = await execute_query(sql)
        for r in rows:
            r["distance"] = float(r.get("distance", 1))
            r["similarity"] = float(r.get("similarity", 0))
            r["user_count"] = int(r.get("user_count", 0))
        return rows

    async def get_dim_labels(self) -> list:
        return DIM_LABELS

    # ─── HASP 混合检索 ────────────────────────────────────────────

    def _text_to_vector(self, description: str) -> list:
        """文本描述 → 8 维特征向量（关键词权重映射）"""
        dims = [0.0] * 8
        for kw, idx in KEYWORD_DIM_MAP.items():
            if kw in description:
                dims[idx] += 1.0
        if max(dims) == 0:
            return [0.125] * 8   # 无法解析时返回均匀向量
        # 稀疏化：峰值→高，其余→低
        f_max = max(dims)
        result = []
        for v in dims:
            norm = v / f_max
            result.append(round(0.75 + norm * 0.20 if norm >= 0.5 else 0.05 + norm * 0.30, 4))
        return result

    def _build_sql(self, query_vector=None, label_filters=None, top_k=5) -> str:
        """动态构建 HASP 混合检索 SQL"""
        vec_str = _vec_str(query_vector) if query_vector else None
        where_parts = []

        if label_filters:
            for lb in label_filters:
                safe_lb = lb.replace("'", "''")
                where_parts.append(f"labels LIKE '%{safe_lb}%'")

        where_clause = ("WHERE " + "\n  AND ".join(where_parts)) if where_parts else ""

        if vec_str:
            dist_expr = f"cosine_distance(photo_embedding, CAST('{vec_str}' AS ARRAY<FLOAT>))"
            sql = (
                f"SELECT user_id, user_name, avatar_style, description, labels,\n"
                f"       ROUND(1 - {dist_expr}, 4) AS similarity,\n"
                f"       ROUND({dist_expr}, 6)      AS distance\n"
                f"FROM user_avatar\n"
                f"{where_clause}\n"
                f"ORDER BY {dist_expr} ASC\n"
                f"LIMIT {top_k}"
            )
        else:
            sql = (
                f"SELECT user_id, user_name, avatar_style, description, labels\n"
                f"FROM user_avatar\n"
                f"{where_clause}\n"
                f"LIMIT {top_k}"
            )
        return sql.strip()

    async def hybrid_search(
        self,
        query_vector: list = None,
        label_filters: list = None,
        description: str = None,
        top_k: int = 5,
    ) -> dict:
        """
        HASP 混合检索：
        - query_vector: 照片 embedding（可选）
        - label_filters: 标签标量过滤（可选）
        - description: 文本描述 → 自动转 embedding（可选，优先级低于 query_vector）
        """
        vec = query_vector
        if not vec and description:
            vec = self._text_to_vector(description)

        sql = self._build_sql(vec, label_filters, top_k)
        rows = await execute_query(sql)

        for r in rows:
            try:
                r["labels"] = json.loads(r["labels"]) if r["labels"] else []
            except Exception:
                r["labels"] = []
            r["distance"]   = float(r.get("distance", 1))
            r["similarity"] = float(r.get("similarity", 0))

        # 模式判断（用于前端展示）
        mode = "hybrid"
        if vec and not label_filters:
            mode = "vector"
        elif label_filters and not vec:
            mode = "scalar"

        return {
            "mode":    mode,
            "sql":     sql,
            "results": rows,
            "query_vector": vec,
            "label_filters": label_filters or [],
        }

    async def search_by_photo(
        self,
        image_bytes: bytes,
        label_filters: list = None,
        description: str = None,
        top_k: int = 5,
    ) -> dict:
        """上传照片 → 提取向量 → HASP 混合检索"""
        embedding = self._extract_embedding(image_bytes)
        result = await self.hybrid_search(embedding, label_filters, description, top_k)
        result["photo_embedding"] = embedding
        return result

    def _extract_embedding(self, image_bytes: bytes) -> list:
        """
        从图片提取 8 维特征向量（模拟 Embedding 大模型）
        维度：活跃度/智力/幽默感/冒险精神/科技属性/运动属性/艺术感/社交性
        使用稀疏化策略：峰值维度高 (0.75-0.95)，非峰值接近 0 (0.05-0.20)
        """
        try:
            from PIL import Image
            img = Image.open(io.BytesIO(image_bytes)).convert("RGB").resize((64, 64))
            pixels = list(img.getdata())
            n = len(pixels)

            r_mean = sum(p[0] for p in pixels) / (n * 255)
            g_mean = sum(p[1] for p in pixels) / (n * 255)
            b_mean = sum(p[2] for p in pixels) / (n * 255)
            eps = 1e-6

            r_var = sum((p[0] / 255 - r_mean) ** 2 for p in pixels) / n
            g_var = sum((p[1] / 255 - g_mean) ** 2 for p in pixels) / n
            b_var = sum((p[2] / 255 - b_mean) ** 2 for p in pixels) / n
            total_var = r_var + g_var + b_var
            brightness = 0.299 * r_mean + 0.587 * g_mean + 0.114 * b_mean
            saturation = math.sqrt(total_var / 3 + eps)

            raw = [
                brightness * 1.4 + saturation * 0.6,                        # 活跃度
                b_mean * 1.3 + (1 - r_mean) * 0.5,                          # 智力
                math.sqrt(total_var) * 3.5 + saturation * 0.8,              # 幽默感
                r_mean * 1.5 + saturation * 1.5,                            # 冒险精神
                (b_mean + g_mean * 0.4) / (r_mean + 0.3),                  # 科技属性
                g_mean * 1.5 + brightness * 0.3,                            # 运动属性
                saturation * 2.5 + math.sqrt(r_var * g_var + eps) * 8,     # 艺术感
                (r_mean + g_mean * 0.7) / (r_mean + g_mean + b_mean + eps), # 社交性
            ]
        except Exception:
            # Pillow 未安装或图片解析失败：生成随机稀疏向量
            peaks = random.sample(range(8), 3)
            raw = [random.uniform(0.8, 1.0) if i in peaks else random.uniform(0.0, 0.2) for i in range(8)]

        # 稀疏化：峰值维度映射到 [0.75,0.95]，其余映射到 [0.05,0.20]
        f_min, f_max = min(raw), max(raw)
        f_range = max(f_max - f_min, eps)
        result = []
        for v in raw:
            norm = (v - f_min) / f_range   # 0..1
            if norm >= 0.60:
                result.append(round(0.75 + norm * 0.20, 4))   # 0.75-0.95
            else:
                result.append(round(0.05 + norm * 0.25, 4))   # 0.05-0.20
        return result

    async def add_user_from_image(
        self,
        user_name: str,
        description: str,
        avatar_style: str,
        labels: list,
        image_bytes: bytes,
    ) -> dict:
        """上传照片 → 提取向量 → 写入 user_avatar"""
        embedding = self._extract_embedding(image_bytes)

        row = await execute_one(
            "SELECT IFNULL(MAX(user_id), 1000) + 1 AS next_id FROM user_avatar"
        )
        user_id = int(row["next_id"]) if row else 1011

        labels_json = json.dumps(labels, ensure_ascii=False)
        vec = _vec_str(embedding)
        await execute_write(f"""
            INSERT INTO user_avatar
              (user_id, user_name, avatar_style, description, photo_embedding, labels, create_time)
            VALUES
              ({user_id}, '{user_name}', '{avatar_style}',
               '{description}', '{vec}', '{labels_json}', NOW())
        """)

        # 更新关联标签的 user_count
        for lb in labels:
            await execute_write(
                f"UPDATE avatar_label SET user_count = user_count + 1 "
                f"WHERE label_name = '{lb}'"
            )

        return {
            "status": "ok",
            "user_id": user_id,
            "user_name": user_name,
            "embedding": embedding,
        }
