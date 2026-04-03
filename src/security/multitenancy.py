"""
多租户隔离系统

支持租户级别的资源隔离
"""

from typing import Optional, Dict, List, Any, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import uuid

from src.utils.structured_logging import get_logger
from src.utils.thread_safe import ThreadSafeDict

logger = get_logger("multitenancy")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TenantStatus(Enum):
    """租户状态"""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    PENDING = "pending"
    EXPIRED = "expired"


@dataclass
class TenantQuota:
    """租户配额"""
    max_devices: int = 100
    max_users: int = 50
    max_storage_gb: float = 100.0
    max_api_calls_per_minute: int = 10000
    max_alerts_per_day: int = 1000
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_devices": self.max_devices,
            "max_users": self.max_users,
            "max_storage_gb": self.max_storage_gb,
            "max_api_calls_per_minute": self.max_api_calls_per_minute,
            "max_alerts_per_day": self.max_alerts_per_day
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TenantQuota":
        return cls(
            max_devices=data.get("max_devices", 100),
            max_users=data.get("max_users", 50),
            max_storage_gb=data.get("max_storage_gb", 100.0),
            max_api_calls_per_minute=data.get("max_api_calls_per_minute", 10000),
            max_alerts_per_day=data.get("max_alerts_per_day", 1000)
        )


@dataclass
class TenantUsage:
    """租户使用情况"""
    devices_count: int = 0
    users_count: int = 0
    storage_gb: float = 0.0
    api_calls_today: int = 0
    alerts_today: int = 0
    last_updated: datetime = field(default_factory=utc_now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "devices_count": self.devices_count,
            "users_count": self.users_count,
            "storage_gb": self.storage_gb,
            "api_calls_today": self.api_calls_today,
            "alerts_today": self.alerts_today,
            "last_updated": self.last_updated.isoformat()
        }


@dataclass
class Tenant:
    """租户定义"""
    id: str
    name: str
    domain: Optional[str] = None
    status: TenantStatus = TenantStatus.ACTIVE
    created_at: datetime = field(default_factory=utc_now)
    expires_at: Optional[datetime] = None
    quota: TenantQuota = field(default_factory=TenantQuota)
    usage: TenantUsage = field(default_factory=TenantUsage)
    settings: Dict[str, Any] = field(default_factory=dict)
    parent_tenant_id: Optional[str] = None
    
    def is_active(self) -> bool:
        """检查租户是否有效"""
        if self.status != TenantStatus.ACTIVE:
            return False
        if self.expires_at and utc_now() > self.expires_at:
            return False
        return True
    
    def check_quota(self, resource_type: str) -> bool:
        """检查配额"""
        if resource_type == "devices":
            return self.usage.devices_count < self.quota.max_devices
        elif resource_type == "users":
            return self.usage.users_count < self.quota.max_users
        elif resource_type == "storage":
            return self.usage.storage_gb < self.quota.max_storage_gb
        elif resource_type == "api_calls":
            return self.usage.api_calls_today < self.quota.max_api_calls_per_minute * 24 * 60
        elif resource_type == "alerts":
            return self.usage.alerts_today < self.quota.max_alerts_per_day
        return True
    
    def increment_usage(self, resource_type: str, amount: int = 1):
        """增加使用量"""
        if resource_type == "devices":
            self.usage.devices_count += amount
        elif resource_type == "users":
            self.usage.users_count += amount
        elif resource_type == "api_calls":
            self.usage.api_calls_today += amount
        elif resource_type == "alerts":
            self.usage.alerts_today += amount
        
        self.usage.last_updated = utc_now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "domain": self.domain,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "quota": self.quota.to_dict(),
            "usage": self.usage.to_dict(),
            "settings": self.settings,
            "parent_tenant_id": self.parent_tenant_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tenant":
        return cls(
            id=data["id"],
            name=data["name"],
            domain=data.get("domain"),
            status=TenantStatus(data.get("status", "active")),
            created_at=datetime.fromisoformat(data["created_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]) if data.get("expires_at") else None,
            quota=TenantQuota.from_dict(data.get("quota", {})),
            usage=TenantUsage(**data.get("usage", {})),
            settings=data.get("settings", {}),
            parent_tenant_id=data.get("parent_tenant_id")
        )


class TenantManager:
    """租户管理器"""
    
    def __init__(self):
        self._tenants: ThreadSafeDict = ThreadSafeDict()
        self._domain_map: Dict[str, str] = {}  # domain -> tenant_id
        self._init_default_tenant()
    
    def _init_default_tenant(self):
        """初始化默认租户"""
        default_tenant = Tenant(
            id="default",
            name="Default Tenant",
            status=TenantStatus.ACTIVE,
            quota=TenantQuota(
                max_devices=1000,
                max_users=100,
                max_storage_gb=500.0
            )
        )
        self._tenants.set("default", default_tenant)
        logger.info("默认租户已初始化")
    
    # 租户CRUD
    def create_tenant(self, name: str, domain: Optional[str] = None,
                     quota: Optional[TenantQuota] = None,
                     parent_tenant_id: Optional[str] = None) -> Tenant:
        """
        创建租户
        
        Args:
            name: 租户名称
            domain: 租户域名
            quota: 租户配额
            parent_tenant_id: 父租户ID（子租户）
        
        Returns:
            创建的租户
        """
        tenant_id = f"tenant_{uuid.uuid4().hex[:12]}"
        
        # 检查域名是否已被使用
        if domain and domain in self._domain_map:
            raise ValueError(f"域名 {domain} 已被使用")
        
        tenant = Tenant(
            id=tenant_id,
            name=name,
            domain=domain,
            quota=quota or TenantQuota(),
            parent_tenant_id=parent_tenant_id
        )
        
        self._tenants.set(tenant_id, tenant)
        
        if domain:
            self._domain_map[domain] = tenant_id
        
        logger.info(f"租户已创建: {tenant_id} - {name}")
        return tenant
    
    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """获取租户"""
        return self._tenants.get(tenant_id)
    
    def get_tenant_by_domain(self, domain: str) -> Optional[Tenant]:
        """通过域名获取租户"""
        tenant_id = self._domain_map.get(domain)
        if tenant_id:
            return self.get_tenant(tenant_id)
        return None
    
    def update_tenant(self, tenant: Tenant) -> Tenant:
        """更新租户"""
        existing = self._tenants.get(tenant.id)
        if not existing:
            raise ValueError(f"租户 {tenant.id} 不存在")
        
        # 更新域名映射
        if existing.domain != tenant.domain:
            if existing.domain:
                del self._domain_map[existing.domain]
            if tenant.domain:
                if tenant.domain in self._domain_map:
                    raise ValueError(f"域名 {tenant.domain} 已被使用")
                self._domain_map[tenant.domain] = tenant.id
        
        self._tenants.set(tenant.id, tenant)
        logger.info(f"租户已更新: {tenant.id}")
        return tenant
    
    def delete_tenant(self, tenant_id: str):
        """删除租户"""
        if tenant_id == "default":
            raise ValueError("不能删除默认租户")
        
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            raise ValueError(f"租户 {tenant_id} 不存在")
        
        # 清理域名映射
        if tenant.domain:
            del self._domain_map[tenant.domain]
        
        self._tenants.delete(tenant_id)
        logger.info(f"租户已删除: {tenant_id}")
    
    def list_tenants(self, status: Optional[TenantStatus] = None,
                    parent_id: Optional[str] = None) -> List[Tenant]:
        """列出租户"""
        tenants = []
        for tenant_id in self._tenants.keys():
            tenant = self._tenants.get(tenant_id)
            if tenant:
                # 状态过滤
                if status and tenant.status != status:
                    continue
                # 父租户过滤
                if parent_id is not None and tenant.parent_tenant_id != parent_id:
                    continue
                tenants.append(tenant)
        
        return tenants
    
    # 租户状态管理
    def suspend_tenant(self, tenant_id: str, reason: str = ""):
        """暂停租户"""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            raise ValueError(f"租户 {tenant_id} 不存在")
        
        tenant.status = TenantStatus.SUSPENDED
        tenant.settings["suspension_reason"] = reason
        tenant.settings["suspended_at"] = utc_now().isoformat()
        
        self._tenants.set(tenant_id, tenant)
        logger.warning(f"租户已暂停: {tenant_id} - 原因: {reason}")
    
    def activate_tenant(self, tenant_id: str):
        """激活租户"""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            raise ValueError(f"租户 {tenant_id} 不存在")
        
        tenant.status = TenantStatus.ACTIVE
        tenant.settings.pop("suspension_reason", None)
        tenant.settings.pop("suspended_at", None)
        
        self._tenants.set(tenant_id, tenant)
        logger.info(f"租户已激活: {tenant_id}")
    
    # 配额管理
    def update_quota(self, tenant_id: str, quota: TenantQuota):
        """更新租户配额"""
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            raise ValueError(f"租户 {tenant_id} 不存在")
        
        tenant.quota = quota
        self._tenants.set(tenant_id, tenant)
        logger.info(f"租户配额已更新: {tenant_id}")
    
    def check_and_increment(self, tenant_id: str, resource_type: str, 
                           amount: int = 1) -> bool:
        """
        检查配额并增加使用量
        
        Returns:
            是否允许操作
        """
        tenant = self._tenants.get(tenant_id)
        if not tenant:
            return False
        
        if not tenant.is_active():
            return False
        
        if not tenant.check_quota(resource_type):
            logger.warning(f"租户 {tenant_id} 超出 {resource_type} 配额")
            return False
        
        tenant.increment_usage(resource_type, amount)
        self._tenants.set(tenant_id, tenant)
        return True
    
    def get_usage(self, tenant_id: str) -> Optional[TenantUsage]:
        """获取租户使用情况"""
        tenant = self._tenants.get(tenant_id)
        if tenant:
            return tenant.usage
        return None
    
    # 租户隔离辅助方法
    def apply_tenant_filter(self, query: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """
        为查询添加租户过滤
        
        Args:
            query: 原始查询
            tenant_id: 租户ID
        
        Returns:
            添加了租户过滤的查询
        """
        query = query.copy()
        query["tenant_id"] = tenant_id
        return query
    
    def filter_by_tenant(self, items: List[Dict[str, Any]], 
                        tenant_id: str) -> List[Dict[str, Any]]:
        """
        过滤列表中的租户数据
        
        Args:
            items: 数据列表
            tenant_id: 租户ID
        
        Returns:
            过滤后的列表
        """
        return [item for item in items if item.get("tenant_id") == tenant_id]


class TenantContext:
    """租户上下文管理器"""
    
    def __init__(self, tenant_manager: TenantManager, tenant_id: str):
        self.tenant_manager = tenant_manager
        self.tenant_id = tenant_id
        self._tenant: Optional[Tenant] = None
    
    def __enter__(self):
        self._tenant = self.tenant_manager.get_tenant(self.tenant_id)
        if not self._tenant:
            raise ValueError(f"租户 {self.tenant_id} 不存在")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    @property
    def tenant(self) -> Tenant:
        return self._tenant
    
    def check_permission(self, resource_type: str, amount: int = 1) -> bool:
        """检查租户权限"""
        return self.tenant_manager.check_and_increment(
            self.tenant_id, resource_type, amount
        )


# 全局租户管理器实例
tenant_manager = TenantManager()


# 租户装饰器
def with_tenant_context(func):
    """自动注入租户上下文的装饰器"""
    def wrapper(*args, **kwargs):
        tenant_id = kwargs.get("tenant_id", "default")
        with TenantContext(tenant_manager, tenant_id):
            return func(*args, **kwargs)
    return wrapper
