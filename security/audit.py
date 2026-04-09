"""
安全审计模块

作者: Security Team
职责: 操作日志记录、审计追踪、合规报告
"""

import json
import hashlib
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from enum import Enum
from pathlib import Path
import sqlite3
from loguru import logger


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AuditLevel(Enum):
    """审计级别"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditAction(Enum):
    """审计动作类型"""
    # 认证相关
    LOGIN = "login"
    LOGOUT = "logout"
    LOGIN_FAILED = "login_failed"
    TOKEN_REFRESH = "token_refresh"
    
    # 数据操作
    DATA_READ = "data_read"
    DATA_WRITE = "data_write"
    DATA_DELETE = "data_delete"
    
    # 规则操作
    RULE_CREATE = "rule_create"
    RULE_UPDATE = "rule_update"
    RULE_DELETE = "rule_delete"
    RULE_TOGGLE = "rule_toggle"
    
    # 告警操作
    ALERT_ACK = "alert_ack"
    ALERT_RESOLVE = "alert_resolve"
    
    # 系统操作
    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop"
    CONFIG_CHANGE = "config_change"
    
    # 控制操作
    CONTROL_COMMAND = "control_command"


class AuditRecord:
    """审计记录"""
    
    def __init__(
        self,
        action: AuditAction,
        user_id: str,
        user_name: str,
        resource_type: str,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        level: AuditLevel = AuditLevel.INFO,
        result: str = "success"
    ):
        self.id = self._generate_id()
        self.timestamp = utc_now()
        self.action = action
        self.user_id = user_id
        self.user_name = user_name
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.details = details or {}
        self.ip_address = ip_address
        self.user_agent = user_agent
        self.level = level
        self.result = result
    
    def _generate_id(self) -> str:
        """生成唯一ID"""
        data = f"{utc_now().isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "action": self.action.value,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "details": self.details,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "level": self.level.value,
            "result": self.result
        }


class AuditLogger:
    """
    审计日志记录器
    
    功能:
    - 记录所有关键操作
    - 防篡改存储（哈希链）
    - 审计查询
    - 合规报告生成
    """
    
    def __init__(self, db_path: str = "data/audit.db", private_key: Optional[str] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.private_key = private_key
        self._lock = threading.Lock()  # 线程安全锁
        self._init_db()
    
    def _init_db(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT,
                details TEXT,
                ip_address TEXT,
                user_agent TEXT,
                level TEXT NOT NULL,
                result TEXT NOT NULL,
                hash TEXT NOT NULL
            )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON audit_logs(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user ON audit_logs(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_action ON audit_logs(action)")
        
        conn.commit()
        conn.close()
        
        logger.info(f"审计数据库初始化完成: {self.db_path}")
    
    def _calculate_hash(self, record: AuditRecord, previous_hash: str = "") -> str:
        """
        计算记录哈希 (防篡改)
        
        使用简单的链式哈希，每条记录的哈希包含上一条记录的哈希
        添加随机nonce防重放攻击
        """
        import time
        
        data = {
            "id": record.id,
            "timestamp": record.timestamp.isoformat(),
            "action": record.action.value,
            "user_id": record.user_id,
            "details": json.dumps(record.details, sort_keys=True),
            "previous_hash": previous_hash,
            "nonce": hashlib.sha256(str(time.time()).encode()).hexdigest()[:16]
        }
        
        hash_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(hash_str.encode()).hexdigest()
    
    def _sign_hash(self, hash_value: str) -> Optional[str]:
        """使用私钥签名哈希（增强防篡改）"""
        if not hasattr(self, 'private_key') or not self.private_key:
            return None
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding
            
            private_key = serialization.load_pem_private_key(
                self.private_key.encode(),
                password=None
            )
            signature = private_key.sign(
                hash_value.encode(),
                padding.PKCS1v15(),
                hashes.SHA256()
            )
            return signature.hex()
        except Exception as e:
            logger.error(f"签名失败：{e}")
            return None
    
    def log(self, record: AuditRecord):
        """
        记录审计日志（线程安全）
        
        Args:
            record: 审计记录
        """
        with self._lock:
            try:
                # 获取上一条记录的哈希
                previous_hash = self._get_last_hash()
                
                # 计算当前记录哈希
                record_hash = self._calculate_hash(record, previous_hash)
                
                # 计算签名（增强防篡改）
                signature = self._sign_hash(record_hash)
                
                # 写入数据库
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # 更新表结构（如果签名字段不存在）
                try:
                    cursor.execute("ALTER TABLE audit_logs ADD COLUMN signature TEXT")
                except:
                    pass  # 字段已存在
                
                cursor.execute("""
                    INSERT INTO audit_logs 
                    (id, timestamp, action, user_id, user_name, resource_type, 
                     resource_id, details, ip_address, user_agent, level, result, hash, signature)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.id,
                    record.timestamp.isoformat(),
                    record.action.value,
                    record.user_id,
                    record.user_name,
                    record.resource_type,
                    record.resource_id,
                    json.dumps(record.details),
                    record.ip_address,
                    record.user_agent,
                    record.level.value,
                    record.result,
                    record_hash,
                    signature
                ))
                
                conn.commit()
                conn.close()
                
                # 同时输出到日志
                logger.info(f"Audit: {record.action.value} by {record.user_name}")
                
            except Exception as e:
                logger.error(f"审计日志记录失败: {e}")
                # 审计日志失败不应影响主流程，但需要告警
    
    def _get_last_hash(self) -> str:
        """获取最后一条记录的哈希"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT hash FROM audit_logs ORDER BY timestamp DESC LIMIT 1"
        )
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else ""
    
    def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user_id: Optional[str] = None,
        action: Optional[AuditAction] = None,
        resource_type: Optional[str] = None,
        limit: int = 100
    ) -> list:
        """
        查询审计日志
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            user_id: 用户ID
            action: 动作类型
            resource_type: 资源类型
            limit: 返回数量限制
            
        Returns:
            审计记录列表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM audit_logs WHERE 1=1"
        params = []
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())
        
        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        
        if action:
            query += " AND action = ?"
            params.append(action.value)
        
        if resource_type:
            query += " AND resource_type = ?"
            params.append(resource_type)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        # 转换为字典
        records = []
        for row in rows:
            records.append({
                "id": row[0],
                "timestamp": row[1],
                "action": row[2],
                "user_id": row[3],
                "user_name": row[4],
                "resource_type": row[5],
                "resource_id": row[6],
                "details": json.loads(row[7]) if row[7] else {},
                "ip_address": row[8],
                "user_agent": row[9],
                "level": row[10],
                "result": row[11],
                "hash": row[12]
            })
        
        return records
    
    def verify_integrity(self) -> bool:
        """
        验证审计日志完整性
        
        Returns:
            是否通过验证
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id, timestamp, action, user_id, details, hash FROM audit_logs ORDER BY timestamp"
        )
        rows = cursor.fetchall()
        conn.close()
        
        previous_hash = ""
        
        for row in rows:
            data = {
                "id": row[0],
                "timestamp": row[1],
                "action": row[2],
                "user_id": row[3],
                "details": row[4],
                "previous_hash": previous_hash
            }
            
            expected_hash = hashlib.sha256(
                json.dumps(data, sort_keys=True).encode()
            ).hexdigest()
            
            if expected_hash != row[5]:
                logger.error(f"审计日志完整性验证失败: {row[0]}")
                return False
            
            previous_hash = row[5]
        
        logger.info(f"审计日志完整性验证通过，共 {len(rows)} 条记录")
        return True
    
    def generate_report(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        生成审计报告
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            审计报告
        """
        records = self.query(start_time=start_time, end_time=end_time, limit=10000)
        
        # 统计
        action_counts = {}
        user_counts = {}
        level_counts = {}
        
        for record in records:
            action = record["action"]
            user = record["user_name"]
            level = record["level"]
            
            action_counts[action] = action_counts.get(action, 0) + 1
            user_counts[user] = user_counts.get(user, 0) + 1
            level_counts[level] = level_counts.get(level, 0) + 1
        
        return {
            "period": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "summary": {
                "total_records": len(records),
                "unique_users": len(user_counts),
                "action_types": len(action_counts)
            },
            "action_distribution": action_counts,
            "user_activity": user_counts,
            "level_distribution": level_counts,
            "integrity_verified": self.verify_integrity()
        }


# 全局审计日志实例
audit_logger = AuditLogger()


def log_audit(
    action: AuditAction,
    user_id: str,
    user_name: str,
    resource_type: str,
    **kwargs
):
    """
    便捷函数：记录审计日志
    
    使用示例:
        log_audit(
            action=AuditAction.RULE_CREATE,
            user_id="user_001",
            user_name="admin",
            resource_type="rule",
            resource_id="RULE_001",
            details={"rule_name": "缺氧异常"}
        )
    """
    record = AuditRecord(
        action=action,
        user_id=user_id,
        user_name=user_name,
        resource_type=resource_type,
        **kwargs
    )
    audit_logger.log(record)
