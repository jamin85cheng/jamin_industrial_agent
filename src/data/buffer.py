"""
数据缓存与断线补传模块

功能需求: D-04 数据缓存 - 本地缓存，网络恢复后补传
作者: Data Team
"""

import json
import sqlite3
import threading
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from pathlib import Path
from loguru import logger
from concurrent.futures import ThreadPoolExecutor

from src.utils.connection_pool import get_pool, PoolConfig
from security.input_validator import InputValidator, ValidationError


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BufferedPoint:
    """缓存的数据点"""
    id: Optional[int]
    measurement: str
    tags: Dict[str, str]
    fields: Dict[str, Any]
    timestamp: str
    quality: str
    retry_count: int = 0
    created_at: Optional[str] = None


class DataBuffer:
    """
    数据缓存管理器
    
    功能:
    1. 本地SQLite缓存数据
    2. 批量写入时序数据库
    3. 网络断开时自动缓存
    4. 网络恢复后自动补传
    5. 防溢出策略（旧数据清理）
    """
    
    def __init__(self, db_path: str = "data/buffer.db", max_size: int = 1000000):
        """
        初始化缓存管理器
        
        Args:
            db_path: 缓存数据库路径
            max_size: 最大缓存条数
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_size = max_size
        self._lock = threading.RLock()
        self._running = False
        self._flush_task = None
        
        self._init_db()
        
        # 使用连接池提升性能
        self._pool = get_pool(str(db_path), PoolConfig(
            max_connections=10,
            min_connections=2,
            max_idle_time=300
        ))
        
        logger.info(f"数据缓存管理器已初始化，缓存路径: {db_path}")
    
    def _init_db(self):
        """初始化数据库表"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_buffer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    measurement TEXT NOT NULL,
                    tags TEXT NOT NULL,
                    fields TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    quality TEXT DEFAULT 'good',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON data_buffer(timestamp)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_measurement 
                ON data_buffer(measurement)
            """)
            
            conn.commit()
    
    def write(self, measurement: str, tags: Dict[str, str],
              fields: Dict[str, Any], timestamp: datetime,
              quality: str = "good") -> bool:
        """
        写入单条数据到缓存
        
        Args:
            measurement: 测量名称
            tags: 标签
            fields: 字段值
            timestamp: 时间戳
            quality: 数据质量
            
        Returns:
            是否写入成功
        """
        try:
            # 输入验证
            measurement = InputValidator.validate_measurement(measurement)
            tags = InputValidator.validate_tags(tags)
            fields = InputValidator.validate_fields(fields)
            timestamp = InputValidator.validate_timestamp(timestamp)
            
            with self._pool.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO data_buffer 
                    (measurement, tags, fields, timestamp, quality)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        measurement,
                        json.dumps(tags),
                        json.dumps(fields),
                        timestamp.isoformat(),
                        quality
                    )
                )
                conn.commit()
                
                # 检查并清理旧数据
                self._cleanup_if_needed(conn)
                
                return True
                
        except ValidationError as e:
            logger.error(f"写入缓存失败 - 验证错误: {e}")
            return False
        except Exception as e:
            logger.error(f"写入缓存失败: {e}")
            return False
    
    def write_batch(self, points: List[Dict]) -> int:
        """
        批量写入缓存
        
        Args:
            points: 数据点列表
            
        Returns:
            成功写入数量
        """
        if not points:
            return 0
        
        try:
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                written = 0
                
                # 使用事务批量写入
                conn.execute("BEGIN TRANSACTION")
                
                try:
                    for point in points:
                        try:
                            # 验证每个点
                            measurement = InputValidator.validate_measurement(
                                point['measurement']
                            )
                            tags = InputValidator.validate_tags(
                                point.get('tags', {})
                            )
                            fields = InputValidator.validate_fields(
                                point['fields']
                            )
                            ts = InputValidator.validate_timestamp(
                                point.get('timestamp', utc_now())
                            )
                            
                            cursor.execute(
                                """
                                INSERT INTO data_buffer 
                                (measurement, tags, fields, timestamp, quality)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    measurement,
                                    json.dumps(tags),
                                    json.dumps(fields),
                                    ts.isoformat(),
                                    point.get('quality', 'good')
                                )
                            )
                            written += 1
                            
                        except ValidationError as e:
                            logger.warning(f"数据点验证失败，跳过: {e}")
                            continue
                    
                    conn.commit()
                    
                    # 清理旧数据
                    self._cleanup_if_needed(conn)
                    
                    return written
                    
                except Exception as e:
                    conn.rollback()
                    raise
                    
        except Exception as e:
            logger.error(f"批量写入缓存失败: {e}")
            return 0
    
    def read_batch(self, limit: int = 1000) -> List[BufferedPoint]:
        """
        读取一批缓存数据
        
        Args:
            limit: 读取数量
            
        Returns:
            数据点列表
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    cursor.execute(
                        """
                        SELECT * FROM data_buffer 
                        ORDER BY timestamp ASC 
                        LIMIT ?
                        """,
                        (limit,)
                    )
                    
                    rows = cursor.fetchall()
                    
                    points = []
                    for row in rows:
                        point = BufferedPoint(
                            id=row['id'],
                            measurement=row['measurement'],
                            tags=json.loads(row['tags']),
                            fields=json.loads(row['fields']),
                            timestamp=row['timestamp'],
                            quality=row['quality'],
                            retry_count=row['retry_count'],
                            created_at=row['created_at']
                        )
                        points.append(point)
                    
                    return points
                    
            except Exception as e:
                logger.error(f"读取缓存失败: {e}")
                return []
    
    def delete_batch(self, ids: List[int]) -> bool:
        """
        批量删除已上传的数据
        
        Args:
            ids: 数据点ID列表
            
        Returns:
            是否删除成功
        """
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    placeholders = ','.join('?' * len(ids))
                    conn.execute(
                        f"DELETE FROM data_buffer WHERE id IN ({placeholders})",
                        ids
                    )
                    conn.commit()
                    return True
                    
            except Exception as e:
                logger.error(f"删除缓存失败: {e}")
                return False
    
    def update_retry_count(self, id: int, increment: int = 1):
        """更新重试次数"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        UPDATE data_buffer 
                        SET retry_count = retry_count + ?
                        WHERE id = ?
                        """,
                        (increment, id)
                    )
                    conn.commit()
            except Exception as e:
                logger.error(f"更新重试次数失败: {e}")
    
    def get_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # 总数量
                    cursor.execute("SELECT COUNT(*) FROM data_buffer")
                    total = cursor.fetchone()[0]
                    
                    # 今天新增
                    today = datetime.now().strftime('%Y-%m-%d')
                    cursor.execute(
                        "SELECT COUNT(*) FROM data_buffer WHERE DATE(created_at) = ?",
                        (today,)
                    )
                    today_count = cursor.fetchone()[0]
                    
                    # 重试次数>0的
                    cursor.execute(
                        "SELECT COUNT(*) FROM data_buffer WHERE retry_count > 0"
                    )
                    retry_count = cursor.fetchone()[0]
                    
                    return {
                        'total': total,
                        'today': today_count,
                        'retrying': retry_count
                    }
                    
            except Exception as e:
                logger.error(f"获取缓存统计失败: {e}")
                return {'total': 0, 'today': 0, 'retrying': 0}
    
    def _cleanup_if_needed(self, conn: sqlite3.Connection):
        """如果需要，清理旧数据"""
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data_buffer")
        count = cursor.fetchone()[0]
        
        if count > self.max_size:
            # 删除最旧的数据，保留 max_size * 0.9
            to_delete = count - int(self.max_size * 0.9)
            cursor.execute(
                """
                DELETE FROM data_buffer 
                WHERE id IN (
                    SELECT id FROM data_buffer 
                    ORDER BY timestamp ASC 
                    LIMIT ?
                )
                """,
                (to_delete,)
            )
            conn.commit()
            logger.warning(f"缓存溢出，已清理 {to_delete} 条旧数据")
    
    async def start_flush_task(self, storage_backend, interval: int = 30):
        """
        启动自动刷新任务
        
        Args:
            storage_backend: 存储后端 (如InfluxDB)
            interval: 刷新间隔(秒)
        """
        self._running = True
        
        while self._running:
            try:
                await self._flush_buffer(storage_backend)
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"刷新缓存失败: {e}")
                await asyncio.sleep(5)
    
    async def _flush_buffer(self, storage_backend):
        """刷新缓存到存储后端"""
        points = self.read_batch(limit=1000)
        
        if not points:
            return
        
        # 转换为存储格式
        storage_points = []
        for p in points:
            storage_points.append({
                'measurement': p.measurement,
                'tags': p.tags,
                'fields': p.fields,
                'timestamp': datetime.fromisoformat(p.timestamp),
                'quality': p.quality
            })
        
        # 尝试写入
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                storage_backend.write_batch,
                storage_points
            )
            if isinstance(result, tuple):
                success, failed = result
            else:
                success = int(result or 0)
                failed = max(len(storage_points) - success, 0)
            
            if success > 0:
                # 删除已成功的
                successful_ids = [p.id for p in points[:success]]
                self.delete_batch(successful_ids)
                logger.info(f"缓存刷新成功: {success} 条")
            
            if failed > 0:
                # 增加重试次数
                for p in points[success:]:
                    self.update_retry_count(p.id)
                
        except Exception as e:
            logger.error(f"缓存刷新失败: {e}")
            # 增加所有重试次数
            for p in points:
                self.update_retry_count(p.id)
    
    def stop_flush_task(self):
        """停止刷新任务"""
        self._running = False


class NetworkMonitor:
    """
    网络连接监控器
    
    监控与存储后端的连接状态，自动切换缓存模式
    """
    
    def __init__(self, check_interval: int = 10):
        self.check_interval = check_interval
        self._is_online = True
        self._callbacks = []
        self._running = False
    
    def add_status_callback(self, callback):
        """添加状态变化回调"""
        self._callbacks.append(callback)
    
    async def start_monitoring(self, check_func):
        """
        开始监控
        
        Args:
            check_func: 检查连接的函数，返回bool
        """
        self._running = True
        
        while self._running:
            try:
                is_online = await asyncio.get_event_loop().run_in_executor(
                    None, check_func
                )
                
                if is_online != self._is_online:
                    self._is_online = is_online
                    
                    # 触发回调
                    for callback in self._callbacks:
                        try:
                            callback(is_online)
                        except Exception as e:
                            logger.error(f"网络状态回调错误: {e}")
                    
                    if is_online:
                        logger.info("🌐 网络连接恢复")
                    else:
                        logger.warning("⚠️ 网络连接断开，切换到缓存模式")
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"网络监控错误: {e}")
                await asyncio.sleep(5)
    
    def stop_monitoring(self):
        """停止监控"""
        self._running = False
    
    @property
    def is_online(self) -> bool:
        return self._is_online


# 使用示例
if __name__ == "__main__":
    # 创建缓存管理器
    buffer = DataBuffer()
    
    # 写入测试数据
    for i in range(100):
        buffer.write(
            measurement="test_metric",
            tags={"device": f"DEV_{i % 10}"},
            fields={"value": i * 1.5},
            timestamp=datetime.now() - timedelta(minutes=i),
            quality="good"
        )
    
    # 查看统计
    stats = buffer.get_stats()
    print(f"缓存统计: {stats}")
    
    # 读取数据
    points = buffer.read_batch(limit=10)
    print(f"读取 {len(points)} 条数据")
    for p in points[:3]:
        print(f"  {p.measurement}: {p.fields}")
