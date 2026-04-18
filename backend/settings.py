"""
银行业CDP平台 - 全局配置
基于 Apache Doris 4.0
"""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Doris 连接 ──────────────────────────────────────────────
    DORIS_HOST: str = os.getenv("DORIS_HOST", "10.26.20.3")
    DORIS_PORT: int = int(os.getenv("DORIS_PORT", "19030"))     # MySQL 协议端口
    DORIS_USER: str = os.getenv("DORIS_USER", "root")
    DORIS_PASSWORD: str = os.getenv("DORIS_PASSWORD", "")
    DORIS_DATABASE: str = os.getenv("DORIS_DATABASE", "bank_cdp")

    # ── Doris 4.0 HASP 配置 ────────────────────────────────────
    # HASP = Hybrid Adaptive Storage and Processing
    DORIS_HASP_ENABLED: bool = True

    CORS_ORIGINS: list = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"]

    # ── 数据安全（脱敏） ────────────────────────────────────────
    MASK_ID_CARD: bool = True       # 身份证号脱敏
    MASK_PHONE: bool = True         # 手机号脱敏
    MASK_ACCOUNT: bool = True       # 账号脱敏

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
