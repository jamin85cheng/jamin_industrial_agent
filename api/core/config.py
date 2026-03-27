"""
API 配置管理

作者: Backend Team
职责: 集中管理所有配置项
"""

import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class Settings(BaseSettings):
    """应用配置"""
    
    # ==================== 基础配置 ====================
    DEBUG: bool = Field(default=False, description="调试模式")
    API_HOST: str = Field(default="0.0.0.0", description="API 监听地址")
    API_PORT: int = Field(default=8000, description="API 端口")
    
    # ==================== 安全配置 ====================
    SECRET_KEY: str = Field(
        default="your-secret-key-change-in-production",
        description="JWT 密钥"
    )
    JWT_ALGORITHM: str = Field(default="HS256", description="JWT 算法")
    JWT_EXPIRE_HOURS: int = Field(default=24, description="JWT 过期时间(小时)")
    
    # ==================== CORS配置 ====================
    CORS_ORIGINS: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:5173"],
        description="允许的跨域来源"
    )
    
    # ==================== 数据库配置 ====================
    # InfluxDB
    INFLUXDB_URL: str = Field(default="http://localhost:8086")
    INFLUXDB_TOKEN: str = Field(default="")
    INFLUXDB_ORG: str = Field(default="jamin")
    INFLUXDB_BUCKET: str = Field(default="industrial_data")
    
    # SQLite (元数据)
    SQLITE_PATH: str = Field(default="data/metadata.db")
    
    # Redis (缓存/消息队列)
    REDIS_URL: str = Field(default="redis://localhost:6379/0")
    
    # ==================== AI模型配置 ====================
    # LLM
    LLM_PROVIDER: str = Field(default="ollama", description="LLM提供商")
    LLM_API_KEY: Optional[str] = Field(default=None)
    LLM_BASE_URL: str = Field(default="http://localhost:11434")
    LLM_MODEL: str = Field(default="qwen2.5:3b")
    
    # Embedding
    EMBEDDING_MODEL: str = Field(default="shibing624/text2vec-base-chinese")
    
    # ==================== 采集配置 ====================
    PLC_HOST: str = Field(default="192.168.1.100")
    PLC_PORT: int = Field(default=102)
    PLC_TYPE: str = Field(default="s7")
    SCAN_INTERVAL: int = Field(default=10, description="采集间隔(秒)")
    
    # ==================== 告警配置 ====================
    ALERT_SUPPRESSION_MINUTES: int = Field(default=15)
    ALERT_CHANNELS: List[str] = Field(default=["log"])
    
    # 飞书
    FEISHU_WEBHOOK: Optional[str] = Field(default=None)
    
    # ==================== 日志配置 ====================
    LOG_LEVEL: str = Field(default="INFO")
    LOG_FILE: str = Field(default="logs/api.log")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
    
    @validator("CORS_ORIGINS", pre=True)
    def parse_cors_origins(cls, v):
        """解析 CORS 来源 (支持逗号分隔的字符串)"""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    @validator("ALERT_CHANNELS", pre=True)
    def parse_alert_channels(cls, v):
        """解析告警通道"""
        if isinstance(v, str):
            return [channel.strip() for channel in v.split(",")]
        return v


# 全局配置实例
settings = Settings()
