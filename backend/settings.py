"""
银行业CDP平台 - 全局配置
基于 Apache Doris 4.0
"""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Doris 连接 ──────────────────────────────────────────────
    DORIS_HOST: str = os.getenv("DORIS_HOST", "127.0.0.1")
    DORIS_PORT: int = int(os.getenv("DORIS_PORT", "9030"))      # MySQL 协议端口
    DORIS_HTTP_PORT: int = int(os.getenv("DORIS_HTTP_PORT", "8030"))  # HTTP 端口
    DORIS_USER: str = os.getenv("DORIS_USER", "root")
    DORIS_PASSWORD: str = os.getenv("DORIS_PASSWORD", "")
    DORIS_DATABASE: str = os.getenv("DORIS_DATABASE", "bank_cdp")

    # ── Doris 4.0 HASP 配置 ────────────────────────────────────
    # HASP = Hybrid Adaptive Storage and Processing
    DORIS_HASP_ENABLED: bool = True
    DORIS_SESSION_VARS: dict = {
        # "enable_pipeline_engine": "true",           # 启用 Pipeline 执行引擎
        # "enable_vectorized_engine": "true",          # 向量化执行引擎
        # "parallel_pipeline_task_num": "8",           # 并行任务数
        # "enable_adaptive_pipeline_task_serial_read_on_limit": "true",
        # "enable_short_circuit_query_access_column_store": "true",  # HASP列存加速
        # "enable_common_expr_pushdown": "true",       # 表达式下推
        # "enable_function_pushdown": "true",           # 函数下推
        # "runtime_filter_mode": "GLOBAL",             # 全局 Runtime Filter
        # "enable_runtime_filter_prune": "true",
        # "query_timeout": "300",
        # "insert_timeout": "600",
    }

    # ── AI Function 配置 ────────────────────────────────────────
    AI_FUNCTION_PROVIDER: str = os.getenv("AI_PROVIDER", "openai")  # openai / azure / custom
    AI_API_KEY: str = os.getenv("AI_API_KEY", "sk-your-key-here")
    AI_API_ENDPOINT: str = os.getenv("AI_API_ENDPOINT", "https://api.openai.com/v1")
    AI_MODEL: str = os.getenv("AI_MODEL", "gpt-4o-mini")
    AI_MAX_TOKENS: int = 512
    AI_TEMPERATURE: float = 0.1

    # ── 日志采集 ───────────────────────────────────────────────
    LOG_COLLECT_HOST: str = "0.0.0.0"
    LOG_COLLECT_PORT: int = 5044      # FileBeat → 后端 HTTP 端口
    LOG_BATCH_SIZE: int = 500         # 批量写入 Doris 的日志条数
    LOG_FLUSH_INTERVAL: int = 5       # 秒

    # ── FastAPI ────────────────────────────────────────────────
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    APP_DEBUG: bool = True
    CORS_ORIGINS: list = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"]

    # ── 数据安全（脱敏） ────────────────────────────────────────
    MASK_ID_CARD: bool = True       # 身份证号脱敏
    MASK_PHONE: bool = True         # 手机号脱敏
    MASK_ACCOUNT: bool = True       # 账号脱敏

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
