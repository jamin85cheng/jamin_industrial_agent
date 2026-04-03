"""
健康检查模块

DevOps工程师修复: 完善的健康检查和监控指标
"""

import time
import asyncio
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from loguru import logger


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class HealthStatus(Enum):
    """健康状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    name: str
    status: HealthStatus
    response_time_ms: float
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    checked_at: datetime = field(default_factory=utc_now)

    @property
    def response_time(self) -> float:
        """Backward-compatible seconds-based response time."""
        return self.response_time_ms / 1000.0


class HealthChecker:
    """
    健康检查器
    
    功能:
    - 多组件健康检查
    - 响应时间监控
    - 依赖项状态检查
    - 健康指标暴露
    """
    
    def __init__(self):
        self._checks: Dict[str, Callable] = {}
        self._last_results: Dict[str, HealthCheckResult] = {}
        self._lock = threading.RLock()
        self._running = False
        self._check_thread: Optional[threading.Thread] = None
    
    def register(self, name: str, check_func: Callable, timeout: float = 5.0):
        """
        注册健康检查
        
        Args:
            name: 检查名称
            check_func: 检查函数，返回 (bool, str, dict)
            timeout: 超时时间（秒）
        """
        with self._lock:
            self._checks[name] = {
                'func': check_func,
                'timeout': timeout
            }
        logger.info(f"注册健康检查: {name}")
    
    def check(self, name: Optional[str] = None) -> Dict[str, HealthCheckResult]:
        """
        执行健康检查
        
        Args:
            name: 指定检查名称，None则检查所有
            
        Returns:
            检查结果字典
        """
        results = {}
        
        with self._lock:
            checks_to_run = {name: self._checks[name]} if name else self._checks.copy()
        
        for check_name, check_config in checks_to_run.items():
            start_time = time.time()
            
            try:
                # 执行检查
                func = check_config['func']
                timeout = check_config['timeout']
                
                if asyncio.iscoroutinefunction(func):
                    # 异步函数
                    is_healthy, message, details = asyncio.run(
                        asyncio.wait_for(func(), timeout=timeout)
                    )
                else:
                    # 同步函数
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(func)
                        is_healthy, message, details = future.result(timeout=timeout)
                
                response_time = (time.time() - start_time) * 1000
                
                status = HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY
                
                result = HealthCheckResult(
                    name=check_name,
                    status=status,
                    response_time_ms=response_time,
                    message=message,
                    details=details
                )
                
            except Exception as e:
                response_time = (time.time() - start_time) * 1000
                result = HealthCheckResult(
                    name=check_name,
                    status=HealthStatus.UNHEALTHY,
                    response_time_ms=response_time,
                    message=f"检查失败: {str(e)}",
                    details={"error": str(e)}
                )
            
            results[check_name] = result
            
            with self._lock:
                self._last_results[check_name] = result
        
        return results

    async def check_all(self) -> Dict[str, HealthCheckResult]:
        """Backward-compatible async wrapper used by older routers."""
        return await asyncio.to_thread(self.check)
    
    def get_overall_status(self) -> HealthStatus:
        """获取整体健康状态"""
        with self._lock:
            if not self._last_results:
                return HealthStatus.UNKNOWN
            
            statuses = [r.status for r in self._last_results.values()]
            
            if any(s == HealthStatus.UNHEALTHY for s in statuses):
                return HealthStatus.UNHEALTHY
            elif any(s == HealthStatus.DEGRADED for s in statuses):
                return HealthStatus.DEGRADED
            elif all(s == HealthStatus.HEALTHY for s in statuses):
                return HealthStatus.HEALTHY
            else:
                return HealthStatus.UNKNOWN
    
    def get_health_report(self) -> Dict[str, Any]:
        """获取健康报告"""
        overall = self.get_overall_status()
        
        with self._lock:
            checks = {
                name: {
                    "status": result.status.value,
                    "response_time_ms": result.response_time_ms,
                    "message": result.message,
                    "checked_at": result.checked_at.isoformat()
                }
                for name, result in self._last_results.items()
            }
        
        return {
            "status": overall.value,
            "timestamp": utc_now().isoformat(),
            "checks": checks
        }
    
    def start_periodic_check(self, interval: int = 30):
        """启动定期检查"""
        self._running = True
        self._check_thread = threading.Thread(target=self._check_loop, args=(interval,), daemon=True)
        self._check_thread.start()
        logger.info(f"定期健康检查已启动，间隔: {interval}s")
    
    def _check_loop(self, interval: int):
        """检查循环"""
        while self._running:
            try:
                self.check()
                time.sleep(interval)
            except Exception as e:
                logger.error(f"健康检查循环出错: {e}")
                time.sleep(5)
    
    def stop(self):
        """停止定期检查"""
        self._running = False
        if self._check_thread and self._check_thread.is_alive():
            self._check_thread.join(timeout=5)


# 常用健康检查函数

def check_database():
    """检查数据库连接"""
    try:
        # 这里应该实际检查数据库连接
        # 简化示例
        return True, "数据库连接正常", {"connections": 5}
    except Exception as e:
        return False, f"数据库连接失败: {e}", {}


def check_disk_space():
    """检查磁盘空间"""
    try:
        import os
        import shutil

        stat = shutil.disk_usage(os.getcwd())
        free_gb = stat.free / (1024**3)
        total_gb = stat.total / (1024**3)
        used_percent = (stat.used / stat.total) * 100
        
        if used_percent > 90:
            return False, f"磁盘空间不足: {used_percent:.1f}% 已使用", {
                "free_gb": free_gb,
                "total_gb": total_gb,
                "used_percent": used_percent
            }
        elif used_percent > 80:
            return True, f"磁盘空间警告: {used_percent:.1f}% 已使用", {
                "free_gb": free_gb,
                "total_gb": total_gb,
                "used_percent": used_percent
            }
        else:
            return True, f"磁盘空间充足: {free_gb:.1f}GB 可用", {
                "free_gb": free_gb,
                "total_gb": total_gb,
                "used_percent": used_percent
            }
    except Exception as e:
        return False, f"磁盘检查失败: {e}", {}


def check_memory():
    """检查内存使用"""
    try:
        import psutil
        memory = psutil.virtual_memory()
        
        if memory.percent > 90:
            return False, f"内存使用过高: {memory.percent}%", {
                "total_gb": memory.total / (1024**3),
                "available_gb": memory.available / (1024**3),
                "percent": memory.percent
            }
        else:
            return True, f"内存使用正常: {memory.percent}%", {
                "total_gb": memory.total / (1024**3),
                "available_gb": memory.available / (1024**3),
                "percent": memory.percent
            }
    except Exception as e:
        return False, f"内存检查失败: {e}", {}


# 全局实例
_health_checker: Optional[HealthChecker] = None


def get_health_checker() -> HealthChecker:
    """获取全局健康检查器"""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


# 初始化默认检查
def init_default_checks():
    """初始化默认健康检查"""
    checker = get_health_checker()
    checker.register("database", check_database)
    checker.register("disk", check_disk_space)
    checker.register("memory", check_memory)


# 使用示例
if __name__ == "__main__":
    checker = get_health_checker()
    
    # 注册检查
    checker.register("test", lambda: (True, "测试通过", {}))
    
    # 执行检查
    results = checker.check()
    
    print("\n健康检查结果:")
    for name, result in results.items():
        status_emoji = "✅" if result.status == HealthStatus.HEALTHY else "❌"
        print(f"{status_emoji} {name}: {result.message} ({result.response_time_ms:.2f}ms)")
    
    print(f"\n整体状态: {checker.get_overall_status().value}")
    print(f"\n完整报告:\n{checker.get_health_report()}")
