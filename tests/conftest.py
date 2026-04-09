"""
Pytest 配置和共享fixture

测试基础设施 - 提供统一的测试环境和工具
"""

import pytest
import tempfile
import shutil
import json
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Generator
import sqlite3

# 添加项目根目录到路径
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

WORKSPACE_TMP_ROOT = Path(tempfile.gettempdir()) / "jamin_pytest"
WORKSPACE_TMP_ROOT.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session")
def test_data_dir() -> Generator[Path, None, None]:
    """创建临时测试数据目录"""
    temp_dir = Path(tempfile.mkdtemp(prefix="jamin_test_"))
    yield temp_dir
    # 清理
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def temp_db_path(test_data_dir) -> Generator[str, None, None]:
    """创建临时数据库路径"""
    db_path = test_data_dir / f"test_{datetime.now().timestamp()}.db"
    yield str(db_path)
    # 清理
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def tmp_path() -> Generator[Path, None, None]:
    """Create a writable per-test temp directory inside the repo workspace."""
    temp_dir = WORKSPACE_TMP_ROOT / f"pytest_{uuid.uuid4().hex[:8]}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def sqlite_connection(temp_db_path) -> Generator[sqlite3.Connection, None, None]:
    """创建SQLite连接"""
    conn = sqlite3.connect(temp_db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def sample_plc_config() -> Dict[str, Any]:
    """示例PLC配置"""
    return {
        "type": "s7",
        "host": "192.168.1.100",
        "port": 102,
        "rack": 0,
        "slot": 1,
        "scan_interval": 5,
        "tags": [
            {"name": "temperature", "address": "DB1.DBD0", "type": "float"},
            {"name": "pressure", "address": "DB1.DBD4", "type": "float"}
        ]
    }


@pytest.fixture
def sample_rules_config() -> Dict[str, Any]:
    """示例规则配置"""
    return {
        "rules": [
            {
                "rule_id": "RULE_001",
                "name": "高温告警",
                "enabled": True,
                "condition": {
                    "type": "threshold",
                    "tag": "temperature",
                    "operator": ">",
                    "value": 100
                },
                "severity": "critical",
                "message": "温度超过100度"
            }
        ]
    }


@pytest.fixture
def sample_audit_record() -> Dict[str, Any]:
    """示例审计记录"""
    return {
        "id": "AUDIT_001",
        "timestamp": utc_now(),
        "action": "LOGIN",
        "user_id": "user_001",
        "user_name": "admin",
        "resource_type": "system",
        "resource_id": "login",
        "details": {"ip": "192.168.1.1"},
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0",
        "level": "INFO",
        "result": "success"
    }


@pytest.fixture
def mock_influxdb():
    """模拟InfluxDB客户端"""
    class MockInfluxDBClient:
        def __init__(self):
            self.points = []
        
        def write_points(self, points):
            self.points.extend(points)
            return True
        
        def query(self, query):
            return []
    
    return MockInfluxDBClient()


@pytest.fixture
def mock_plc_client():
    """模拟PLC客户端"""
    class MockPLCClient:
        def __init__(self):
            self.connected = False
            self.values = {
                "temperature": 25.0,
                "pressure": 1.0
            }
        
        def connect(self, host, rack, slot, port):
            self.connected = True
            return True
        
        def disconnect(self):
            self.connected = False
        
        def is_connected(self):
            return self.connected
        
        def read_area(self, area, db_number, start, size):
            # 模拟返回数据
            import struct
            return struct.pack('>f', self.values.get("temperature", 0.0))
    
    return MockPLCClient()


@pytest.fixture(scope="session")
def event_loop():
    """提供异步事件循环"""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# 自定义标记
def pytest_configure(config):
    """配置pytest"""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")


# 测试数据生成器
class TestDataGenerator:
    """测试数据生成器"""
    
    @staticmethod
    def generate_time_series(length: int = 100, 
                            start_value: float = 20.0,
                            trend: float = 0.0,
                            noise: float = 1.0) -> list:
        """生成时间序列数据"""
        import random
        import math
        
        data = []
        current = start_value
        
        for i in range(length):
            # 添加趋势
            current += trend
            # 添加噪声
            current += random.gauss(0, noise)
            # 添加周期性
            current += math.sin(i / 10) * 2
            
            data.append({
                "timestamp": utc_now() + timedelta(seconds=i),
                "value": current
            })
        
        return data
    
    @staticmethod
    def generate_anomalous_data(normal_data: list, 
                                anomaly_indices: list,
                                anomaly_factor: float = 3.0) -> list:
        """在数据中注入异常"""
        import random
        
        data = normal_data.copy()
        for idx in anomaly_indices:
            if 0 <= idx < len(data):
                # 随机正负异常
                factor = anomaly_factor if random.random() > 0.5 else -anomaly_factor
                data[idx]["value"] *= factor
        
        return data


@pytest.fixture
def data_generator():
    """提供测试数据生成器"""
    return TestDataGenerator()


# 性能测试fixture
@pytest.fixture(scope="function")
def benchmark():
    """性能测试fixture"""
    import time
    
    class Benchmark:
        def __init__(self):
            self.results = []
        
        def measure(self, func, *args, **kwargs):
            """测量函数执行时间"""
            start = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            
            self.results.append({
                "function": func.__name__,
                "elapsed_ms": elapsed * 1000
            })
            
            return result
    
    return Benchmark()
