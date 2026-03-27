"""
数据库连接池模块

数据库工程师修复: 连接池管理，提升性能和资源利用
"""

import sqlite3
import threading
import queue
import time
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from loguru import logger


@dataclass
class PoolConfig:
    """连接池配置"""
    max_connections: int = 10
    min_connections: int = 2
    max_idle_time: int = 300  # 5分钟
    connection_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0


class PooledConnection:
    """池化连接包装器"""
    
    def __init__(self, connection: sqlite3.Connection, pool: 'ConnectionPool'):
        self._connection = connection
        self._pool = pool
        self._created_at = time.time()
        self._last_used = time.time()
        self._in_use = False
    
    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection
    
    @property
    def idle_time(self) -> float:
        """空闲时间（秒）"""
        return time.time() - self._last_used
    
    @property
    def age(self) -> float:
        """连接年龄（秒）"""
        return time.time() - self._created_at
    
    def mark_used(self):
        """标记为使用中"""
        self._in_use = True
        self._last_used = time.time()
    
    def mark_idle(self):
        """标记为空闲"""
        self._in_use = False
        self._last_used = time.time()
    
    def is_valid(self) -> bool:
        """检查连接是否有效"""
        try:
            self._connection.execute("SELECT 1")
            return True
        except:
            return False
    
    def close(self):
        """关闭连接"""
        try:
            self._connection.close()
        except:
            pass


class ConnectionPool:
    """
    SQLite连接池
    
    特性:
    - 连接复用，减少创建开销
    - 自动清理空闲连接
    - 连接健康检查
    - 线程安全
    """
    
    def __init__(self, db_path: str, config: Optional[PoolConfig] = None):
        self.db_path = db_path
        self.config = config or PoolConfig()
        
        self._pool: queue.Queue[PooledConnection] = queue.Queue()
        self._in_use: Dict[int, PooledConnection] = {}
        self._lock = threading.RLock()
        self._shutdown = False
        
        self._stats = {
            'created': 0,
            'reused': 0,
            'closed': 0,
            'errors': 0
        }
        
        # 初始化最小连接数
        self._init_connections()
        
        # 启动清理线程
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()
        
        logger.info(f"连接池初始化完成: {db_path}, 大小: {self.config.min_connections}/{self.config.max_connections}")
    
    def _init_connections(self):
        """初始化最小连接数"""
        for _ in range(self.config.min_connections):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)
    
    def _create_connection(self) -> Optional[PooledConnection]:
        """创建新连接"""
        try:
            db_path = Path(self.db_path)
            if db_path.parent and str(db_path.parent) not in ("", "."):
                db_path.parent.mkdir(parents=True, exist_ok=True)
            raw_conn = sqlite3.connect(
                str(db_path),
                timeout=self.config.connection_timeout,
                check_same_thread=False  # 允许跨线程使用
            )
            raw_conn.row_factory = sqlite3.Row
            
            # 优化SQLite性能
            raw_conn.execute("PRAGMA journal_mode=WAL")
            raw_conn.execute("PRAGMA synchronous=NORMAL")
            raw_conn.execute("PRAGMA cache_size=10000")
            raw_conn.execute("PRAGMA temp_store=MEMORY")
            
            pooled = PooledConnection(raw_conn, self)
            with self._lock:
                self._stats['created'] += 1
            
            return pooled
            
        except Exception as e:
            logger.error(f"创建连接失败: {e}")
            with self._lock:
                self._stats['errors'] += 1
            return None
    
    @contextmanager
    def get_connection(self, timeout: Optional[float] = None):
        """
        获取连接（上下文管理器）
        
        Usage:
            with pool.get_connection() as conn:
                conn.execute("SELECT * FROM table")
        """
        timeout = timeout or self.config.connection_timeout
        pooled_conn = None
        
        try:
            # 从池中获取连接
            pooled_conn = self._acquire_connection(timeout)
            
            if pooled_conn is None:
                raise Exception("无法获取数据库连接")
            
            pooled_conn.mark_used()
            
            yield pooled_conn.connection
            
        finally:
            if pooled_conn:
                self._release_connection(pooled_conn)
    
    def _acquire_connection(self, timeout: float) -> Optional[PooledConnection]:
        """获取连接"""
        if self._shutdown:
            raise Exception("连接池已关闭")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # 尝试从池中获取
            try:
                pooled_conn = self._pool.get(block=False)
                
                # 检查连接是否有效
                if pooled_conn.is_valid():
                    with self._lock:
                        self._in_use[id(pooled_conn)] = pooled_conn
                        self._stats['reused'] += 1
                    return pooled_conn
                else:
                    # 连接无效，关闭并创建新连接
                    pooled_conn.close()
                    
            except queue.Empty:
                pass
            
            # 池为空，尝试创建新连接
            if len(self._in_use) < self.config.max_connections:
                new_conn = self._create_connection()
                if new_conn:
                    with self._lock:
                        self._in_use[id(new_conn)] = new_conn
                    return new_conn
            
            # 等待其他连接释放
            time.sleep(0.1)
        
        return None
    
    def _release_connection(self, pooled_conn: PooledConnection):
        """释放连接回池中"""
        with self._lock:
            self._in_use.pop(id(pooled_conn), None)
        
        pooled_conn.mark_idle()
        
        # 如果连接有效，放回池中
        if pooled_conn.is_valid():
            try:
                self._pool.put(pooled_conn, block=False)
            except queue.Full:
                # 池已满，关闭连接
                pooled_conn.close()
                with self._lock:
                    self._stats['closed'] += 1
        else:
            pooled_conn.close()
            with self._lock:
                self._stats['closed'] += 1
    
    def _cleanup_loop(self):
        """清理循环（后台线程）"""
        while not self._shutdown:
            try:
                self._cleanup_idle_connections()
                time.sleep(60)  # 每分钟清理一次
            except Exception as e:
                logger.error(f"连接池清理出错: {e}")
    
    def _cleanup_idle_connections(self):
        """清理空闲连接"""
        to_remove = []
        
        # 收集需要清理的连接
        while not self._pool.empty():
            try:
                conn = self._pool.get(block=False)
                
                # 关闭超时的连接
                if conn.idle_time > self.config.max_idle_time:
                    conn.close()
                    with self._lock:
                        self._stats['closed'] += 1
                else:
                    to_remove.append(conn)
                    
            except queue.Empty:
                break
        
        # 将保留的连接放回池中
        for conn in to_remove:
            try:
                self._pool.put(conn, block=False)
            except queue.Full:
                conn.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        with self._lock:
            return {
                **self._stats,
                'pool_size': self._pool.qsize(),
                'in_use': len(self._in_use),
                'max_size': self.config.max_connections
            }
    
    def shutdown(self):
        """关闭连接池"""
        self._shutdown = True
        
        # 等待清理线程结束
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)
        
        # 关闭所有连接
        while not self._pool.empty():
            try:
                conn = self._pool.get(block=False)
                conn.close()
            except:
                pass
        
        for conn in list(self._in_use.values()):
            conn.close()
        
        logger.info("连接池已关闭")


# 全局连接池管理器
class ConnectionPoolManager:
    """连接池管理器（单例）"""
    
    _instance = None
    _lock = threading.Lock()
    _pools: Dict[str, ConnectionPool] = {}
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_pool(self, db_path: str, config: Optional[PoolConfig] = None) -> ConnectionPool:
        """获取或创建连接池"""
        if db_path not in self._pools:
            with self._lock:
                if db_path not in self._pools:
                    self._pools[db_path] = ConnectionPool(db_path, config)
        
        return self._pools[db_path]
    
    def shutdown_all(self):
        """关闭所有连接池"""
        for pool in self._pools.values():
            pool.shutdown()
        self._pools.clear()


# 便捷函数
def get_pool(db_path: str, config: Optional[PoolConfig] = None) -> ConnectionPool:
    """获取连接池"""
    return ConnectionPoolManager().get_pool(db_path, config)


# 使用示例
if __name__ == "__main__":
    import tempfile
    
    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    # 获取连接池
    pool = get_pool(db_path, PoolConfig(max_connections=5))
    
    # 创建测试表
    with pool.get_connection() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()
    
    # 并发测试
    import concurrent.futures
    
    def worker(worker_id):
        with pool.get_connection() as conn:
            conn.execute("INSERT INTO test (name) VALUES (?)", (f"worker_{worker_id}",))
            conn.commit()
            
            cursor = conn.execute("SELECT COUNT(*) FROM test")
            count = cursor.fetchone()[0]
            return count
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker, i) for i in range(20)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    print(f"✅ 并发写入完成，最终计数: {results[-1]}")
    print(f"📊 连接池统计: {pool.get_stats()}")
    
    # 关闭连接池
    pool.shutdown()
