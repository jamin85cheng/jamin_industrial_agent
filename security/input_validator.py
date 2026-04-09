"""
输入验证模块

安全工程师修复: 统一的输入验证机制
防止SQL注入、XSS、命令注入等攻击
"""

import re
import html
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass
from loguru import logger


class ValidationError(Exception):
    """验证错误"""
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


class InputValidator:
    """
    输入验证器
    
    提供统一的输入验证机制，防止各类注入攻击
    """
    
    # 测量名称允许的字符: 字母、数字、下划线
    MEASUREMENT_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    # 标签键允许的字符
    TAG_KEY_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    # 危险字符（SQL注入/XSS）
    DANGEROUS_CHARS = [';', '--', '/*', '*/', '<script', 'javascript:', 'onerror=', 'onload=']
    
    # 最大长度限制
    MAX_STRING_LENGTH = 1000
    MAX_TAG_VALUE_LENGTH = 255
    MAX_FIELDS_COUNT = 100
    MAX_TAGS_COUNT = 50

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)
    
    @classmethod
    def validate_measurement(cls, measurement: str) -> str:
        """
        验证测量名称
        
        Args:
            measurement: 测量名称
            
        Returns:
            验证后的测量名称
            
        Raises:
            ValidationError: 验证失败
        """
        if not measurement:
            raise ValidationError("measurement", "不能为空")
        
        if len(measurement) > cls.MAX_STRING_LENGTH:
            raise ValidationError("measurement", f"长度不能超过{cls.MAX_STRING_LENGTH}")
        
        if not cls.MEASUREMENT_PATTERN.match(measurement):
            raise ValidationError(
                "measurement", 
                "只能包含字母、数字、下划线，且必须以字母或下划线开头"
            )
        
        return measurement
    
    @classmethod
    def validate_tags(cls, tags: Dict[str, str]) -> Dict[str, str]:
        """
        验证标签字典
        
        Args:
            tags: 标签字典
            
        Returns:
            验证后的标签字典
        """
        if not isinstance(tags, dict):
            raise ValidationError("tags", "必须是字典类型")
        
        if len(tags) > cls.MAX_TAGS_COUNT:
            raise ValidationError("tags", f"标签数量不能超过{cls.MAX_TAGS_COUNT}")
        
        validated = {}
        for key, value in tags.items():
            # 验证键
            if not cls.TAG_KEY_PATTERN.match(key):
                raise ValidationError(f"tags.{key}", "标签键包含非法字符")
            
            # 验证值
            if not isinstance(value, str):
                value = str(value)
            
            if len(value) > cls.MAX_TAG_VALUE_LENGTH:
                raise ValidationError(f"tags.{key}", f"标签值长度不能超过{cls.MAX_TAG_VALUE_LENGTH}")
            
            # 转义HTML特殊字符（防止XSS）
            value = html.escape(value)
            
            # 检查危险字符
            cls._check_dangerous_chars(value, f"tags.{key}")
            
            validated[key] = value
        
        return validated
    
    @classmethod
    def validate_fields(cls, fields: Dict[str, Any]) -> Dict[str, Union[int, float, bool, str]]:
        """
        验证字段字典
        
        Args:
            fields: 字段字典
            
        Returns:
            验证后的字段字典
        """
        if not isinstance(fields, dict):
            raise ValidationError("fields", "必须是字典类型")
        
        if len(fields) > cls.MAX_FIELDS_COUNT:
            raise ValidationError("fields", f"字段数量不能超过{cls.MAX_FIELDS_COUNT}")
        
        validated = {}
        for key, value in fields.items():
            # 验证键
            if not cls.TAG_KEY_PATTERN.match(key):
                raise ValidationError(f"fields.{key}", "字段键包含非法字符")
            
            # 验证值类型
            if isinstance(value, (int, float, bool)):
                validated[key] = value
            elif isinstance(value, str):
                # 字符串长度限制
                if len(value) > cls.MAX_STRING_LENGTH:
                    raise ValidationError(f"fields.{key}", f"字段值长度不能超过{cls.MAX_STRING_LENGTH}")
                
                # 转义HTML
                value = html.escape(value)
                cls._check_dangerous_chars(value, f"fields.{key}")
                validated[key] = value
            else:
                # 其他类型转为字符串
                validated[key] = str(value)
        
        return validated
    
    @classmethod
    def validate_timestamp(cls, timestamp: Any) -> datetime:
        """
        验证时间戳
        
        Args:
            timestamp: 时间戳
            
        Returns:
            datetime对象
        """
        if timestamp is None:
            return cls._utc_now()
        
        if isinstance(timestamp, datetime):
            return timestamp
        
        if isinstance(timestamp, str):
            try:
                # 支持ISO格式
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                raise ValidationError("timestamp", "无效的时间戳格式")
        
        if isinstance(timestamp, (int, float)):
            # 假设是Unix时间戳
            return datetime.fromtimestamp(timestamp, timezone.utc)
        
        raise ValidationError("timestamp", "不支持的时间戳类型")
    
    @classmethod
    def validate_json_string(cls, data: str) -> str:
        """
        验证JSON字符串
        
        Args:
            data: JSON字符串
            
        Returns:
            验证后的JSON字符串
        """
        if not isinstance(data, str):
            raise ValidationError("json", "必须是字符串类型")
        
        if len(data) > cls.MAX_STRING_LENGTH * 10:  # JSON可以更大一些
            raise ValidationError("json", f"JSON数据过大")
        
        # 检查危险字符
        cls._check_dangerous_chars(data, "json")
        
        return data
    
    @classmethod
    def validate_ip_address(cls, ip: str) -> str:
        """
        验证IP地址
        
        Args:
            ip: IP地址字符串
            
        Returns:
            验证后的IP地址
        """
        import ipaddress
        
        try:
            ipaddress.ip_address(ip)
            return ip
        except ValueError:
            raise ValidationError("ip", "无效的IP地址格式")
    
    @classmethod
    def _check_dangerous_chars(cls, value: str, field_name: str):
        """检查危险字符"""
        value_lower = value.lower()
        for char in cls.DANGEROUS_CHARS:
            if char == ';':
                # Ignore semicolons that only exist inside escaped HTML entities
                # such as ``&lt;`` or ``&#x27;``.
                normalized = re.sub(r'&(?:[a-z]+|#x?[0-9a-f]+);', '', value_lower)
                if char not in normalized:
                    continue
            if char.lower() in value_lower:
                logger.warning(f"检测到潜在的危险字符 '{char}' 在字段 {field_name}")
                raise ValidationError(field_name, f"包含非法字符: {char}")
    
    @classmethod
    def sanitize_sql_identifier(cls, identifier: str) -> str:
        """
        清理SQL标识符
        
        只允许字母、数字、下划线
        """
        if not cls.MEASUREMENT_PATTERN.match(identifier):
            raise ValidationError("identifier", "SQL标识符包含非法字符")
        return identifier


class RateLimiter:
    """
    速率限制器
    
    防止资源耗尽攻击
    """
    
    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[datetime]] = {}
    
    def is_allowed(self, key: str) -> bool:
        """
        检查是否允许请求
        
        Args:
            key: 标识符（如IP地址或用户ID）
            
        Returns:
            是否允许
        """
        now = InputValidator._utc_now()
        window_start = now - timedelta(seconds=self.window_seconds)
        
        # 清理过期记录
        if key in self._requests:
            self._requests[key] = [
                req_time for req_time in self._requests[key]
                if req_time > window_start
            ]
        else:
            self._requests[key] = []
        
        # 检查是否超过限制
        if len(self._requests[key]) >= self.max_requests:
            return False
        
        # 记录本次请求
        self._requests[key].append(now)
        return True
    
    def get_remaining(self, key: str) -> int:
        """获取剩余请求次数"""
        now = InputValidator._utc_now()
        window_start = now - timedelta(seconds=self.window_seconds)
        
        if key not in self._requests:
            return self.max_requests
        
        # 清理过期记录
        valid_requests = [
            req_time for req_time in self._requests[key]
            if req_time > window_start
        ]
        
        return max(0, self.max_requests - len(valid_requests))


# 使用示例
if __name__ == "__main__":
    # 测试输入验证
    try:
        measurement = InputValidator.validate_measurement("temperature_sensor")
        print(f"✅ 验证通过: {measurement}")
        
        tags = InputValidator.validate_tags({
            "device": "PLC_001",
            "location": "车间A"
        })
        print(f"✅ 标签验证通过: {tags}")
        
        # 测试危险字符检测
        try:
            InputValidator.validate_measurement("test'; DROP TABLE users; --")
        except ValidationError as e:
            print(f"✅ 危险字符检测成功: {e}")
        
    except ValidationError as e:
        print(f"❌ 验证失败: {e}")
    
    # 测试速率限制
    limiter = RateLimiter(max_requests=5, window_seconds=10)
    
    for i in range(7):
        allowed = limiter.is_allowed("user_123")
        print(f"请求 {i+1}: {'允许' if allowed else '拒绝'}")
