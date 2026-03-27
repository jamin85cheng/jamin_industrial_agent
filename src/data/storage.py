"""
时序数据存储模块

支持多种时序数据库：
- InfluxDB (推荐用于工业场景)
- IoTDB (Apache IoTDB，国产开源)
- SQLite (轻量级，开发测试用)

功能：
- 时间序列数据写入
- 高效查询与聚合
- 数据保留策略
- 批量导入导出
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from abc import ABC, abstractmethod
import json

from loguru import logger


class TimeSeriesStorage(ABC):
    """时序存储抽象基类"""
    
    @abstractmethod
    async def connect(self) -> bool:
        """连接数据库"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """断开连接"""
        pass
    
    @abstractmethod
    async def write(self, measurement: str, tags: Dict[str, str], 
                    fields: Dict[str, float], timestamp: Optional[datetime] = None) -> bool:
        """写入单条数据"""
        pass
    
    @abstractmethod
    async def write_batch(self, points: List[Dict]) -> int:
        """批量写入数据"""
        pass
    
    @abstractmethod
    async def query(self, measurement: str, start_time: datetime, 
                    end_time: datetime, tags: Optional[Dict[str, str]] = None,
                    aggregation: Optional[str] = None, 
                    interval: Optional[str] = None) -> List[Dict]:
        """查询数据"""
        pass
    
    @abstractmethod
    async def get_latest(self, measurement: str, tags: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """获取最新数据点"""
        pass


class InfluxDBStorage(TimeSeriesStorage):
    """InfluxDB 时序存储实现"""
    
    def __init__(self, host: str = "localhost", port: int = 8086,
                 username: str = "", password: str = "",
                 database: str = "jamin_industrial",
                 org: str = "jamin", bucket: str = "industrial_data"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.org = org
        self.bucket = bucket
        
        self.client = None
        self.write_api = None
        self.query_api = None
        
    async def connect(self) -> bool:
        """连接到 InfluxDB"""
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
            
            connection_string = f"http://{self.host}:{self.port}"
            
            self.client = InfluxDBClient(
                url=connection_string,
                token=f"{self.username}:{self.password}" if self.username else "my-token",
                org=self.org
            )
            
            # 检查连接
            health = self.client.health()
            if health.status != "pass":
                logger.warning(f"InfluxDB 健康检查未通过：{health.message}")
                return False
            
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            
            logger.info(f"成功连接到 InfluxDB: {connection_string}/{self.bucket}")
            return True
            
        except ImportError:
            logger.error("缺少 influxdb-client 依赖：pip install influxdb-client")
            return False
        except Exception as e:
            logger.error(f"连接 InfluxDB 失败：{e}")
            return False
    
    async def disconnect(self):
        """断开连接"""
        if self.client:
            self.client.close()
            logger.info("已断开 InfluxDB 连接")
    
    async def write(self, measurement: str, tags: Dict[str, str], 
                    fields: Dict[str, float], timestamp: Optional[datetime] = None) -> bool:
        """写入单条数据"""
        try:
            from influxdb_client import Point
            
            point = Point(measurement)
            
            # 添加标签
            for key, value in tags.items():
                point = point.tag(key, value)
            
            # 添加字段
            for key, value in fields.items():
                point = point.field(key, value)
            
            # 设置时间戳
            if timestamp:
                point = point.time(timestamp)
            
            self.write_api.write(bucket=self.bucket, record=point)
            logger.debug(f"写入数据点：{measurement}, tags={tags}, fields={fields}")
            return True
            
        except Exception as e:
            logger.error(f"写入数据失败：{e}")
            return False
    
    async def write_batch(self, points: List[Dict]) -> int:
        """批量写入数据"""
        try:
            from influxdb_client import Point
            
            write_points = []
            for p in points:
                point = Point(p.get("measurement", "data"))
                
                # 标签
                for key, value in p.get("tags", {}).items():
                    point = point.tag(key, value)
                
                # 字段
                for key, value in p.get("fields", {}).items():
                    point = point.field(key, value)
                
                # 时间戳
                if "time" in p:
                    point = point.time(p["time"])
                
                write_points.append(point)
            
            self.write_api.write(bucket=self.bucket, record=write_points)
            logger.info(f"批量写入 {len(points)} 个数据点")
            return len(points)
            
        except Exception as e:
            logger.error(f"批量写入失败：{e}")
            return 0
    
    async def query(self, measurement: str, start_time: datetime, 
                    end_time: datetime, tags: Optional[Dict[str, str]] = None,
                    aggregation: Optional[str] = None, 
                    interval: Optional[str] = None) -> List[Dict]:
        """查询数据"""
        try:
            # 构建 Flux 查询
            tag_filter = ""
            if tags:
                tag_conditions = [f'r["{k}"] == "{v}"' for k, v in tags.items()]
                tag_filter = f"|> filter(fn: (r) => {' and '.join(tag_conditions)})"
            
            flux = f'''
                from(bucket: "{self.bucket}")
                |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                {tag_filter}
            '''
            
            # 聚合函数
            if aggregation:
                agg_interval = interval or "5m"
                if aggregation == "mean":
                    flux += f'|> aggregateWindow(every: {agg_interval}, fn: mean, createEmpty: false)'
                elif aggregation == "max":
                    flux += f'|> aggregateWindow(every: {agg_interval}, fn: max, createEmpty: false)'
                elif aggregation == "min":
                    flux += f'|> aggregateWindow(every: {agg_interval}, fn: min, createEmpty: false)'
                elif aggregation == "sum":
                    flux += f'|> aggregateWindow(every: {agg_interval}, fn: sum, createEmpty: false)'
            
            flux += '|> yield(name: "result")'
            
            result = self.query_api.query(flux)
            
            # 解析结果
            data = []
            for table in result:
                for record in table.records:
                    data.append({
                        "time": record.get_time(),
                        "measurement": record.get_measurement(),
                        "field": record.get_field(),
                        "value": record.get_value(),
                        "tags": dict(record.values.get("_tags", {}))
                    })
            
            logger.debug(f"查询返回 {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"查询失败：{e}")
            return []
    
    async def get_latest(self, measurement: str, tags: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """获取最新数据点"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=1)
            
            results = await self.query(measurement, start_time, end_time, tags)
            
            if results:
                # 按时间排序取最新
                latest = sorted(results, key=lambda x: x["time"], reverse=True)[0]
                logger.debug(f"获取最新数据点：{latest['time']}")
                return latest
            
            return None
            
        except Exception as e:
            logger.error(f"获取最新数据失败：{e}")
            return None


class IoTDBStorage(TimeSeriesStorage):
    """Apache IoTDB 时序存储实现"""
    
    def __init__(self, host: str = "localhost", port: int = 6667,
                 username: str = "root", password: str = "root",
                 storage_group: str = "root.jamin"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.storage_group = storage_group
        
        self.connection = None
        self.cursor = None
    
    async def connect(self) -> bool:
        """连接到 IoTDB"""
        try:
            import apache.iotdb.dbapi as iotdb
            
            self.connection = iotdb.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            
            # 设置存储组
            self.cursor.execute(f"SET STORAGE GROUP TO {self.storage_group}")
            
            logger.info(f"成功连接到 IoTDB: {self.host}:{self.port}/{self.storage_group}")
            return True
            
        except ImportError:
            logger.error("缺少 apache-iotdb 依赖：pip install apache-iotdb")
            return False
        except Exception as e:
            logger.error(f"连接 IoTDB 失败：{e}")
            return False
    
    async def disconnect(self):
        """断开连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("已断开 IoTDB 连接")
    
    async def write(self, measurement: str, tags: Dict[str, str], 
                    fields: Dict[str, float], timestamp: Optional[datetime] = None) -> bool:
        """写入单条数据"""
        try:
            if timestamp is None:
                timestamp = datetime.now()
            
            # 构建 IoTDB SQL
            device_path = f"{self.storage_group}.{measurement}"
            if tags:
                device_path += "." + ".".join([f"{k}={v}" for k, v in tags.items()])
            
            columns = list(fields.keys())
            values = list(fields.values())
            time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            sql = f"INSERT INTO {device_path} (timestamp, {', '.join(columns)}) VALUES ({time_str}, {', '.join(['%s'] * len(values))})"
            
            self.cursor.execute(sql, values)
            self.connection.commit()
            
            logger.debug(f"写入 IoTDB: {device_path}")
            return True
            
        except Exception as e:
            logger.error(f"写入 IoTDB 失败：{e}")
            return False
    
    async def write_batch(self, points: List[Dict]) -> int:
        """批量写入数据"""
        try:
            count = 0
            for p in points:
                success = await self.write(
                    p.get("measurement", "data"),
                    p.get("tags", {}),
                    p.get("fields", {}),
                    p.get("time")
                )
                if success:
                    count += 1
            
            logger.info(f"批量写入 IoTDB {count}/{len(points)} 个数据点")
            return count
            
        except Exception as e:
            logger.error(f"批量写入 IoTDB 失败：{e}")
            return 0
    
    async def query(self, measurement: str, start_time: datetime, 
                    end_time: datetime, tags: Optional[Dict[str, str]] = None,
                    aggregation: Optional[str] = None, 
                    interval: Optional[str] = None) -> List[Dict]:
        """查询数据"""
        try:
            device_path = f"{self.storage_group}.{measurement}"
            if tags:
                device_path += "." + ".".join([f"{k}={v}" for k, v in tags.items()])
            
            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            end_str = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            # 构建查询
            select_clause = "SELECT *"
            if aggregation:
                if aggregation == "mean":
                    select_clause = "SELECT AVG(*)"
                elif aggregation == "max":
                    select_clause = "SELECT MAX(*)"
                elif aggregation == "min":
                    select_clause = "SELECT MIN(*)"
                elif aggregation == "sum":
                    select_clause = "SELECT SUM(*)"
            
            sql = f"{select_clause} FROM {device_path} WHERE time >= '{start_str}' AND time <= '{end_str}'"
            
            if interval:
                sql += f" GROUP BY ([{start_str}, {end_str}], {interval})"
            
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            
            # 解析结果
            data = []
            for row in rows:
                data.append({
                    "time": row[0] if isinstance(row[0], datetime) else datetime.strptime(str(row[0]), "%Y-%m-%d %H:%M:%S.%f"),
                    "values": row[1:] if len(row) > 1 else row[0]
                })
            
            logger.debug(f"IoTDB 查询返回 {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"IoTDB 查询失败：{e}")
            return []
    
    async def get_latest(self, measurement: str, tags: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """获取最新数据点"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=1)
            
            results = await self.query(measurement, start_time, end_time, tags)
            
            if results:
                return results[-1]
            
            return None
            
        except Exception as e:
            logger.error(f"获取 IoTDB 最新数据失败：{e}")
            return None


class SQLiteStorage(TimeSeriesStorage):
    """SQLite 轻量级时序存储 (开发测试用)"""
    
    def __init__(self, db_path: str = "data/timeseries.db"):
        self.db_path = db_path
        self.connection = None
        self._ensure_db_dir()
    
    def _ensure_db_dir(self):
        """确保数据库目录存在"""
        import os
        os.makedirs(os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else ".", exist_ok=True)
    
    async def connect(self) -> bool:
        """连接 SQLite"""
        try:
            import aiosqlite
            
            self.connection = await aiosqlite.connect(self.db_path)
            await self.connection.execute("""
                CREATE TABLE IF NOT EXISTS timeseries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    measurement TEXT NOT NULL,
                    tags TEXT,
                    fields TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await self.connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_measurement_time ON timeseries(measurement, timestamp)"
            )
            await self.connection.commit()
            
            logger.info(f"SQLite 数据库就绪：{self.db_path}")
            return True
            
        except ImportError:
            logger.error("缺少 aiosqlite 依赖：pip install aiosqlite")
            # 降级使用同步 sqlite3
            import sqlite3
            self.connection = sqlite3.connect(self.db_path)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS timeseries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    measurement TEXT NOT NULL,
                    tags TEXT,
                    fields TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_measurement_time ON timeseries(measurement, timestamp)"
            )
            self.connection.commit()
            logger.info(f"SQLite 数据库就绪 (同步模式): {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"初始化 SQLite 失败：{e}")
            return False
    
    async def disconnect(self):
        """断开连接"""
        if self.connection:
            await self.connection.close() if hasattr(self.connection, 'execute') else self.connection.close()
            logger.info("已断开 SQLite 连接")
    
    async def write(self, measurement: str, tags: Dict[str, str], 
                    fields: Dict[str, float], timestamp: Optional[datetime] = None) -> bool:
        """写入单条数据"""
        try:
            import json
            ts = timestamp or datetime.now()
            
            await self.connection.execute(
                "INSERT INTO timeseries (measurement, tags, fields, timestamp) VALUES (?, ?, ?, ?)",
                (measurement, json.dumps(tags), json.dumps(fields), ts.isoformat())
            )
            await self.connection.commit()
            
            logger.debug(f"SQLite 写入：{measurement} @ {ts}")
            return True
            
        except Exception as e:
            logger.error(f"SQLite 写入失败：{e}")
            return False
    
    async def write_batch(self, points: List[Dict]) -> int:
        """批量写入数据"""
        try:
            import json
            
            data = []
            for p in points:
                ts = p.get("time", datetime.now())
                data.append((
                    p.get("measurement", "data"),
                    json.dumps(p.get("tags", {})),
                    json.dumps(p.get("fields", {})),
                    ts.isoformat() if isinstance(ts, datetime) else ts
                ))
            
            await self.connection.executemany(
                "INSERT INTO timeseries (measurement, tags, fields, timestamp) VALUES (?, ?, ?, ?)",
                data
            )
            await self.connection.commit()
            
            logger.info(f"SQLite 批量写入 {len(points)} 条记录")
            return len(points)
            
        except Exception as e:
            logger.error(f"SQLite 批量写入失败：{e}")
            return 0
    
    async def query(self, measurement: str, start_time: datetime, 
                    end_time: datetime, tags: Optional[Dict[str, str]] = None,
                    aggregation: Optional[str] = None, 
                    interval: Optional[str] = None) -> List[Dict]:
        """查询数据"""
        try:
            import json
            
            sql = "SELECT tags, fields, timestamp FROM timeseries WHERE measurement = ? AND timestamp BETWEEN ? AND ?"
            params = [measurement, start_time.isoformat(), end_time.isoformat()]
            
            cursor = await self.connection.execute(sql, params) if hasattr(self.connection, 'execute') else self.connection.execute(sql, params)
            rows = await cursor.fetchall() if hasattr(cursor, 'fetchall') else cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    "time": datetime.fromisoformat(row[2]),
                    "tags": json.loads(row[0]) if row[0] else {},
                    "fields": json.loads(row[1]) if row[1] else {}
                })
            
            logger.debug(f"SQLite 查询返回 {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"SQLite 查询失败：{e}")
            return []
    
    async def get_latest(self, measurement: str, tags: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """获取最新数据点"""
        try:
            import json
            
            sql = "SELECT tags, fields, timestamp FROM timeseries WHERE measurement = ? ORDER BY timestamp DESC LIMIT 1"
            cursor = await self.connection.execute(sql, [measurement]) if hasattr(self.connection, 'execute') else self.connection.execute(sql, [measurement])
            row = await cursor.fetchone() if hasattr(cursor, 'fetchone') else cursor.fetchone()
            
            if row:
                return {
                    "time": datetime.fromisoformat(row[2]),
                    "tags": json.loads(row[0]) if row[0] else {},
                    "fields": json.loads(row[1]) if row[1] else {}
                }
            
            return None
            
        except Exception as e:
            logger.error(f"SQLite 获取最新数据失败：{e}")
            return None


class StorageManager:
    """存储管理器 - 统一接口"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage: Optional[TimeSeriesStorage] = None
    
    async def initialize(self) -> bool:
        """根据配置初始化存储"""
        storage_type = self.config.get("type", "sqlite")
        
        if storage_type == "influxdb":
            self.storage = InfluxDBStorage(**self.config.get("config", {}))
        elif storage_type == "iotdb":
            self.storage = IoTDBStorage(**self.config.get("config", {}))
        elif storage_type == "sqlite":
            self.storage = SQLiteStorage(self.config.get("config", {}).get("db_path", "data/timeseries.db"))
        else:
            logger.error(f"不支持的存储类型：{storage_type}")
            return False
        
        return await self.storage.connect()
    
    async def shutdown(self):
        """关闭存储连接"""
        if self.storage:
            await self.storage.disconnect()
    
    async def write(self, measurement: str, tags: Dict[str, str], 
                    fields: Dict[str, float], timestamp: Optional[datetime] = None) -> bool:
        """写入数据"""
        if not self.storage:
            logger.error("存储未初始化")
            return False
        return await self.storage.write(measurement, tags, fields, timestamp)
    
    async def write_batch(self, points: List[Dict]) -> int:
        """批量写入"""
        if not self.storage:
            logger.error("存储未初始化")
            return 0
        return await self.storage.write_batch(points)
    
    async def query(self, measurement: str, start_time: datetime, 
                    end_time: datetime, tags: Optional[Dict[str, str]] = None,
                    aggregation: Optional[str] = None, 
                    interval: Optional[str] = None) -> List[Dict]:
        """查询数据"""
        if not self.storage:
            logger.error("存储未初始化")
            return []
        return await self.storage.query(measurement, start_time, end_time, tags, aggregation, interval)
    
    async def get_latest(self, measurement: str, tags: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """获取最新数据"""
        if not self.storage:
            logger.error("存储未初始化")
            return None
        return await self.storage.get_latest(measurement, tags)


# 测试代码
if __name__ == "__main__":
    import asyncio
    
    async def test_sqlite():
        """测试 SQLite 存储"""
        storage = SQLiteStorage("data/test_timeseries.db")
        await storage.connect()
        
        # 写入测试
        now = datetime.now()
        await storage.write(
            measurement="temperature",
            tags={"device": "sensor_001", "location": "workshop_a"},
            fields={"value": 25.5, "unit": "celsius"},
            timestamp=now
        )
        
        # 批量写入
        points = [
            {"measurement": "pressure", "tags": {"device": "pump_01"}, "fields": {"value": 1.2}, "time": now},
            {"measurement": "pressure", "tags": {"device": "pump_01"}, "fields": {"value": 1.3}, "time": now + timedelta(seconds=1)},
            {"measurement": "pressure", "tags": {"device": "pump_01"}, "fields": {"value": 1.4}, "time": now + timedelta(seconds=2)},
        ]
        count = await storage.write_batch(points)
        print(f"批量写入：{count} 条")
        
        # 查询测试
        start = now - timedelta(minutes=5)
        end = now + timedelta(minutes=5)
        results = await storage.query("pressure", start, end)
        print(f"查询结果：{len(results)} 条")
        for r in results:
            print(f"  {r['time']}: {r['fields']}")
        
        # 获取最新
        latest = await storage.get_latest("pressure")
        if latest:
            print(f"最新数据：{latest['time']} - {latest['fields']}")
        
        await storage.disconnect()
    
    asyncio.run(test_sqlite())
