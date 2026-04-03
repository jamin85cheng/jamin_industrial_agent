"""
结构化日志模块

DevOps工程师修复: 统一结构化日志，支持敏感信息脱敏
"""

import json
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from dataclasses import dataclass, asdict
from functools import wraps
import hashlib
import re
from loguru import logger


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LogLevel(Enum):
    """日志级别"""
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# 敏感字段模式
SENSITIVE_PATTERNS = [
    re.compile(r'password', re.IGNORECASE),
    re.compile(r'secret', re.IGNORECASE),
    re.compile(r'token', re.IGNORECASE),
    re.compile(r'api_key', re.IGNORECASE),
    re.compile(r'apikey', re.IGNORECASE),
    re.compile(r'credential', re.IGNORECASE),
    re.compile(r'private_key', re.IGNORECASE),
]

# 敏感值模式（需要脱敏）
SENSITIVE_VALUE_PATTERNS = [
    re.compile(r'^[A-Za-z0-9]{32,}$'),  # API密钥
    re.compile(r'^[A-Za-z0-9+/=]{40,}$'),  # Base64编码的密钥
]


def redact_sensitive(value: Any, key: str = "") -> Any:
    """
    脱敏敏感信息
    
    Args:
        value: 原始值
        key: 字段名
        
    Returns:
        脱敏后的值
    """
    if isinstance(value, dict):
        return {k: redact_sensitive(v, k) for k, v in value.items()}
    
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    
    if not isinstance(value, str):
        return value
    
    # 检查键名是否敏感
    for pattern in SENSITIVE_PATTERNS:
        if pattern.search(key):
            return redact_value(value)
    
    # 检查值是否匹配敏感模式
    for pattern in SENSITIVE_VALUE_PATTERNS:
        if pattern.match(value):
            return redact_value(value)
    
    return value


def redact_value(value: str) -> str:
    """脱敏单个值"""
    if len(value) <= 8:
        return "****"
    return f"{value[:4]}...{value[-4:]}"


@dataclass
class StructuredLog:
    """结构化日志"""
    timestamp: str
    level: str
    message: str
    logger_name: str
    module: str
    function: str
    line: int
    extra: Dict[str, Any]
    exception: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(asdict(self), ensure_ascii=False)


class StructuredLogger:
    """
    结构化日志器
    
    特性:
    - JSON格式输出
    - 敏感信息自动脱敏
    - 支持追踪ID
    - 性能指标记录
    """
    
    def __init__(self, name: str, redact: bool = True):
        self.name = name
        self.redact = redact
        self._context: Dict[str, Any] = {}
        
        # 配置loguru
        logger.remove()  # 移除默认处理器
        
        # 添加JSON格式处理器
        logger.add(
            sys.stdout,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
            level="DEBUG",
            serialize=False
        )
        
        # 添加文件处理器
        logger.add(
            "logs/app_{time:YYYY-MM-DD}.jsonl",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
            level="INFO",
            rotation="100 MB",
            retention="30 days",
            compression="gz",
            serialize=False
        )
    
    def _format_message(self, record: dict) -> str:
        """格式化消息"""
        # 脱敏extra字段
        extra = record.get('extra', {})
        if self.redact:
            extra = redact_sensitive(extra)
        
        log = StructuredLog(
            timestamp=record['time'].isoformat(),
            level=record['level'].name,
            message=record['message'],
            logger_name=self.name,
            module=record['module'],
            function=record['function'],
            line=record['line'],
            extra=extra,
            exception=record.get('exception'),
            trace_id=extra.get('trace_id'),
            span_id=extra.get('span_id')
        )
        
        return log.to_json()
    
    def bind(self, **kwargs):
        """绑定上下文"""
        self._context.update(kwargs)
        return logger.bind(**kwargs)
    
    def trace(self, message: str, **kwargs):
        """TRACE级别日志"""
        kwargs.update(self._context)
        logger.bind(**kwargs).trace(message)
    
    def debug(self, message: str, **kwargs):
        """DEBUG级别日志"""
        kwargs.update(self._context)
        logger.bind(**kwargs).debug(message)
    
    def info(self, message: str, **kwargs):
        """INFO级别日志"""
        kwargs.update(self._context)
        logger.bind(**kwargs).info(message)
    
    def warning(self, message: str, **kwargs):
        """WARNING级别日志"""
        kwargs.update(self._context)
        logger.bind(**kwargs).warning(message)
    
    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """ERROR级别日志"""
        kwargs.update(self._context)
        if exception:
            kwargs['exception_type'] = type(exception).__name__
            kwargs['exception_message'] = str(exception)
            logger.bind(**kwargs).error(message)
        else:
            logger.bind(**kwargs).error(message)
    
    def critical(self, message: str, **kwargs):
        """CRITICAL级别日志"""
        kwargs.update(self._context)
        logger.bind(**kwargs).critical(message)
    
    def exception(self, message: str, **kwargs):
        """异常日志（包含堆栈）"""
        kwargs.update(self._context)
        logger.bind(**kwargs).exception(message)
    
    def log_performance(self, operation: str, duration_ms: float, **kwargs):
        """记录性能指标"""
        self.info(
            f"性能指标: {operation}",
            operation=operation,
            duration_ms=duration_ms,
            metric_type="performance",
            **kwargs
        )
    
    def log_audit(self, action: str, user: str, resource: str, result: str, **kwargs):
        """记录审计日志"""
        self.info(
            f"审计: {action} by {user}",
            audit=True,
            action=action,
            user=user,
            resource=resource,
            result=result,
            **kwargs
        )


def log_execution(logger_instance: StructuredLogger, operation: str = ""):
    """
    执行日志装饰器
    
    自动记录函数执行时间和异常
    """
    def decorator(func):
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            start_time = utc_now()
            
            try:
                result = func(*args, **kwargs)
                duration_ms = (utc_now() - start_time).total_seconds() * 1000
                logger_instance.log_performance(
                    op_name,
                    duration_ms,
                    status="success"
                )
                return result
                
            except Exception as e:
                duration_ms = (utc_now() - start_time).total_seconds() * 1000
                logger_instance.error(
                    f"{op_name} 失败",
                    exception=e,
                    duration_ms=duration_ms,
                    status="error"
                )
                raise
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            start_time = utc_now()
            
            try:
                result = await func(*args, **kwargs)
                duration_ms = (utc_now() - start_time).total_seconds() * 1000
                logger_instance.log_performance(
                    op_name,
                    duration_ms,
                    status="success"
                )
                return result
                
            except Exception as e:
                duration_ms = (utc_now() - start_time).total_seconds() * 1000
                logger_instance.error(
                    f"{op_name} 失败",
                    exception=e,
                    duration_ms=duration_ms,
                    status="error"
                )
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


# 全局日志实例
_loggers: Dict[str, StructuredLogger] = {}


def get_logger(name: str = "app", redact: bool = True) -> StructuredLogger:
    """获取日志器实例"""
    if name not in _loggers:
        _loggers[name] = StructuredLogger(name, redact=redact)
    return _loggers[name]


# 使用示例
if __name__ == "__main__":
    # 创建日志器
    log = get_logger("test_app")
    
    # 绑定追踪ID
    log = log.bind(trace_id="trace-123", span_id="span-456")
    
    # 普通日志
    log.info("系统启动", version="1.0.0")
    
    # 敏感信息自动脱敏
    log.info("用户登录", username="admin", password="secret123", api_key="sk-1234567890abcdef")
    
    # 性能日志
    log.log_performance("数据库查询", 45.2, query_count=10)
    
    # 审计日志
    log.log_audit("delete_record", "admin", "user/123", "success")
    
    # 异常日志
    try:
        1 / 0
    except Exception as e:
        log.error("计算错误", exception=e)
    
    # 装饰器使用
    @log_execution(log, "calculate")
    def calculate(x, y):
        return x + y
    
    calculate(1, 2)
