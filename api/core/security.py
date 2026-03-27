"""
安全认证模块

作者: Security Team
职责: JWT认证、权限控制、API安全
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from api.core.config import settings


# 密码加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# HTTP Bearer 认证
security = HTTPBearer()


class TokenData(BaseModel):
    """Token 数据模型"""
    user_id: str
    username: str
    role: str
    permissions: list


class User(BaseModel):
    """用户模型"""
    user_id: str
    username: str
    email: str
    role: str
    is_active: bool = True


class JWTAuth:
    """JWT 认证依赖"""
    
    async def __call__(
        self,
        credentials: HTTPAuthorizationCredentials = Depends(security)
    ) -> TokenData:
        """
        验证 JWT Token
        
        Args:
            credentials: HTTP Authorization Header
            
        Returns:
            TokenData: 解码后的 Token 数据
            
        Raises:
            HTTPException: 认证失败
        """
        token = credentials.credentials
        
        try:
            # 解码 JWT
            payload = jwt.decode(
                token,
                settings.SECRET_KEY,
                algorithms=[settings.JWT_ALGORITHM]
            )
            
            user_id: str = payload.get("sub")
            if user_id is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="无效的认证凭证",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            
            token_data = TokenData(
                user_id=user_id,
                username=payload.get("username"),
                role=payload.get("role", "user"),
                permissions=payload.get("permissions", [])
            )
            
            return token_data
            
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="认证已过期或无效",
                headers={"WWW-Authenticate": "Bearer"},
            )


class AuthService:
    """认证服务"""
    
    # 模拟用户数据库 (实际应用应使用真实数据库)
    USERS_DB = {
        "admin": {
            "user_id": "user_001",
            "username": "admin",
            "email": "admin@jamin.local",
            "hashed_password": pwd_context.hash("admin123"),
            "role": "admin",
            "permissions": ["*"],
            "is_active": True
        },
        "operator": {
            "user_id": "user_002",
            "username": "operator",
            "email": "operator@jamin.local",
            "hashed_password": pwd_context.hash("operator123"),
            "role": "operator",
            "permissions": ["data:read", "alerts:read", "alerts:ack"],
            "is_active": True
        },
        "viewer": {
            "user_id": "user_003",
            "username": "viewer",
            "email": "viewer@jamin.local",
            "hashed_password": pwd_context.hash("viewer123"),
            "role": "viewer",
            "permissions": ["data:read", "dashboard:read"],
            "is_active": True
        }
    }
    
    @classmethod
    def verify_password(cls, plain_password: str, hashed_password: str) -> bool:
        """验证密码"""
        return pwd_context.verify(plain_password, hashed_password)
    
    @classmethod
    def get_password_hash(cls, password: str) -> str:
        """获取密码哈希"""
        return pwd_context.hash(password)
    
    @classmethod
    def authenticate_user(cls, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        验证用户
        
        Args:
            username: 用户名
            password: 密码
            
        Returns:
            用户数据或 None
        """
        user = cls.USERS_DB.get(username)
        if not user:
            return None
        
        if not cls.verify_password(password, user["hashed_password"]):
            return None
        
        if not user.get("is_active", True):
            return None
        
        return user
    
    @classmethod
    def create_access_token(
        cls,
        data: Dict[str, Any],
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        创建 JWT Token
        
        Args:
            data: Token 数据
            expires_delta: 过期时间增量
            
        Returns:
            JWT Token 字符串
        """
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(hours=settings.JWT_EXPIRE_HOURS)
        
        to_encode.update({"exp": expire})
        
        encoded_jwt = jwt.encode(
            to_encode,
            settings.SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM
        )
        
        return encoded_jwt
    
    @classmethod
    def check_permission(cls, token_data: TokenData, required_permission: str) -> bool:
        """
        检查权限
        
        Args:
            token_data: Token 数据
            required_permission: 需要的权限
            
        Returns:
            是否有权限
        """
        # 超级管理员拥有所有权限
        if "*" in token_data.permissions:
            return True
        
        # 检查具体权限
        return required_permission in token_data.permissions


def require_permission(permission: str):
    """
    权限检查依赖工厂
    
    使用示例:
        @app.delete("/api/v1/rules/{rule_id}")
        async def delete_rule(
            rule_id: str,
            token_data: TokenData = Depends(JWTAuth()),
            _: None = Depends(require_permission("rules:delete"))
        ):
            pass
    """
    async def checker(token_data: TokenData = Depends(JWTAuth())):
        if not AuthService.check_permission(token_data, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足"
            )
        return None
    
    return checker
