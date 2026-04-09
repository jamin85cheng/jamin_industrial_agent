"""Legacy SQLite-oriented migration manager.

This module is retained for historical local SQLite workflows only.
The production runtime database path is now:

    scripts/manage_database.py
    src.utils.runtime_migrations
"""

import os
import re
import json
import sqlite3
import warnings
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
import hashlib

from src.utils.structured_logging import get_logger

logger = get_logger("migration")
LEGACY_MIGRATION_NOTICE = (
    "Legacy migration_manager.py is SQLite-oriented and no longer the production "
    "runtime migration entrypoint. Use `python scripts/manage_database.py "
    "<prepare|migrate|seed|init|status>` for the unified bootstrap flow."
)


class Migration:
    """迁移记录"""
    
    def __init__(self, version: str, name: str, up_sql: str, down_sql: str = ""):
        self.version = version
        self.name = name
        self.up_sql = up_sql
        self.down_sql = down_sql
        self.checksum = hashlib.md5(up_sql.encode()).hexdigest()
        self.applied_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "name": self.name,
            "checksum": self.checksum,
            "applied_at": self.applied_at.isoformat() if self.applied_at else None
        }


class MigrationManager:
    """迁移管理器"""
    
    MIGRATIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        execution_time_ms INTEGER
    )
    """
    
    def __init__(self, db_path: str, migrations_dir: str = "migrations"):
        warnings.warn(
            LEGACY_MIGRATION_NOTICE,
            DeprecationWarning,
            stacklevel=2,
        )
        self.db_path = db_path
        self.migrations_dir = Path(migrations_dir)
        self.migrations_dir.mkdir(exist_ok=True)
        self._ensure_migrations_table()
    
    def _ensure_migrations_table(self):
        """确保迁移表存在"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(self.MIGRATIONS_TABLE)
            conn.commit()
    
    def _get_connection(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_applied_migrations(self) -> List[Migration]:
        """获取已应用的迁移"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM schema_migrations ORDER BY version"
            )
            migrations = []
            for row in cursor.fetchall():
                migration = Migration(
                    version=row["version"],
                    name=row["name"],
                    up_sql="",
                    down_sql=""
                )
                migration.checksum = row["checksum"]
                migration.applied_at = datetime.fromisoformat(row["applied_at"])
                migrations.append(migration)
            return migrations
    
    def get_pending_migrations(self) -> List[Migration]:
        """获取待执行的迁移"""
        applied = {m.version for m in self.get_applied_migrations()}
        all_migrations = self._load_migrations_from_files()
        
        pending = [m for m in all_migrations if m.version not in applied]
        pending.sort(key=lambda m: m.version)
        return pending
    
    def _load_migrations_from_files(self) -> List[Migration]:
        """从文件加载迁移"""
        migrations = []
        
        if not self.migrations_dir.exists():
            return migrations
        
        # 查找所有.sql文件
        for sql_file in sorted(self.migrations_dir.glob("*.sql")):
            migration = self._parse_migration_file(sql_file)
            if migration:
                migrations.append(migration)
        
        return migrations
    
    def _parse_migration_file(self, file_path: Path) -> Optional[Migration]:
        """解析迁移文件"""
        # 文件名格式: V001__create_users_table.sql
        pattern = r"^V(\d+)__(.+)\.sql$"
        match = re.match(pattern, file_path.name)
        
        if not match:
            return None
        
        version = match.group(1)
        name = match.group(2).replace("_", " ")
        
        content = file_path.read_text(encoding="utf-8")
        
        # 解析UP和DOWN部分
        up_sql = ""
        down_sql = ""
        
        if "-- @UP" in content:
            parts = content.split("-- @DOWN")
            up_sql = parts[0].split("-- @UP")[1].strip() if "-- @UP" in parts[0] else parts[0].strip()
            down_sql = parts[1].strip() if len(parts) > 1 else ""
        else:
            up_sql = content
        
        return Migration(version, name, up_sql, down_sql)
    
    def migrate_up(self, target_version: Optional[str] = None) -> List[str]:
        """
        执行迁移升级
        
        Args:
            target_version: 目标版本，None则执行所有待处理迁移
        
        Returns:
            已应用的迁移版本列表
        """
        pending = self.get_pending_migrations()
        applied = []
        
        for migration in pending:
            if target_version and migration.version > target_version:
                break
            
            try:
                self._apply_migration(migration)
                applied.append(migration.version)
                logger.info(f"迁移已应用: {migration.version} - {migration.name}")
            except Exception as e:
                logger.error(f"迁移失败: {migration.version} - {e}")
                raise
        
        return applied
    
    def _apply_migration(self, migration: Migration):
        """应用单个迁移"""
        start_time = datetime.now()
        
        with self._get_connection() as conn:
            # 执行迁移SQL
            conn.executescript(migration.up_sql)
            
            # 记录迁移
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            conn.execute(
                """
                INSERT INTO schema_migrations (version, name, checksum, execution_time_ms)
                VALUES (?, ?, ?, ?)
                """,
                (migration.version, migration.name, migration.checksum, execution_time)
            )
            conn.commit()
        
        migration.applied_at = datetime.now()
    
    def migrate_down(self, steps: int = 1) -> List[str]:
        """
        回滚迁移
        
        Args:
            steps: 回滚步数
        
        Returns:
            已回滚的迁移版本列表
        """
        applied = self.get_applied_migrations()
        to_rollback = applied[-steps:] if steps < len(applied) else applied
        
        rolled_back = []
        
        for migration in reversed(to_rollback):
            try:
                self._rollback_migration(migration)
                rolled_back.append(migration.version)
                logger.info(f"迁移已回滚: {migration.version} - {migration.name}")
            except Exception as e:
                logger.error(f"回滚失败: {migration.version} - {e}")
                raise
        
        return rolled_back
    
    def _rollback_migration(self, migration: Migration):
        """回滚单个迁移"""
        # 加载完整的迁移信息
        all_migrations = self._load_migrations_from_files()
        full_migration = next((m for m in all_migrations if m.version == migration.version), None)
        
        if not full_migration or not full_migration.down_sql:
            raise ValueError(f"迁移 {migration.version} 没有回滚脚本")
        
        with self._get_connection() as conn:
            # 执行回滚SQL
            conn.executescript(full_migration.down_sql)
            
            # 删除迁移记录
            conn.execute(
                "DELETE FROM schema_migrations WHERE version = ?",
                (migration.version,)
            )
            conn.commit()
    
    def create_migration(self, name: str) -> str:
        """
        创建新的迁移文件
        
        Args:
            name: 迁移名称
        
        Returns:
            迁移文件路径
        """
        # 获取下一个版本号
        applied = self.get_applied_migrations()
        pending = self._load_migrations_from_files()
        
        all_versions = [m.version for m in applied + pending]
        if all_versions:
            next_version = str(int(max(all_versions)) + 1).zfill(3)
        else:
            next_version = "001"
        
        # 创建文件名
        file_name = f"V{next_version}__{name.replace(' ', '_')}.sql"
        file_path = self.migrations_dir / file_name
        
        # 写入模板
        template = f"""-- Migration: {name}
-- Created at: {datetime.now().isoformat()}

-- @UP
-- Write your up migration here

-- @DOWN
-- Write your down migration here (optional)
"""
        
        file_path.write_text(template, encoding="utf-8")
        logger.info(f"迁移文件已创建: {file_path}")
        
        return str(file_path)
    
    def status(self) -> Dict[str, Any]:
        """获取迁移状态"""
        applied = self.get_applied_migrations()
        pending = self.get_pending_migrations()
        
        return {
            "database": self.db_path,
            "applied_count": len(applied),
            "pending_count": len(pending),
            "current_version": applied[-1].version if applied else None,
            "latest_version": pending[-1].version if pending else (
                applied[-1].version if applied else None
            ),
            "applied": [m.to_dict() for m in applied],
            "pending": [{"version": m.version, "name": m.name} for m in pending]
        }
    
    def verify(self) -> List[Dict[str, Any]]:
        """验证迁移完整性"""
        applied = self.get_applied_migrations()
        issues = []
        
        for migration in applied:
            # 加载文件中的迁移
            all_migrations = self._load_migrations_from_files()
            file_migration = next((m for m in all_migrations if m.version == migration.version), None)
            
            if not file_migration:
                issues.append({
                    "version": migration.version,
                    "issue": "迁移文件丢失"
                })
            elif file_migration.checksum != migration.checksum:
                issues.append({
                    "version": migration.version,
                    "issue": "迁移文件已被修改",
                    "expected_checksum": migration.checksum,
                    "actual_checksum": file_migration.checksum
                })
        
        return issues


# 初始化迁移脚本
INITIAL_MIGRATIONS = {
    "V001__create_devices_table.sql": """
-- @UP
CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    rack INTEGER DEFAULT 0,
    slot INTEGER DEFAULT 1,
    scan_interval INTEGER DEFAULT 10,
    status TEXT DEFAULT 'offline',
    enabled BOOLEAN DEFAULT 1,
    last_seen TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tenant_id TEXT DEFAULT 'default',
    created_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_devices_tenant ON devices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status);

-- @DOWN
DROP TABLE IF EXISTS devices;
""",
    "V002__create_alerts_table.sql": """
-- @UP
CREATE TABLE IF NOT EXISTS alerts (
    id TEXT PRIMARY KEY,
    rule_id TEXT,
    rule_name TEXT,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    device_id TEXT,
    tag TEXT,
    value REAL,
    threshold REAL,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    tenant_id TEXT DEFAULT 'default',
    FOREIGN KEY (device_id) REFERENCES devices(id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_tenant ON alerts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at);

-- @DOWN
DROP TABLE IF EXISTS alerts;
""",
    "V003__create_audit_logs_table.sql": """
-- @UP
CREATE TABLE IF NOT EXISTS audit_logs (
    id TEXT PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    action TEXT NOT NULL,
    user_id TEXT,
    user_name TEXT,
    resource_type TEXT,
    resource_id TEXT,
    details TEXT,
    ip_address TEXT,
    user_agent TEXT,
    level TEXT DEFAULT 'INFO',
    result TEXT,
    tenant_id TEXT DEFAULT 'default'
);

CREATE INDEX IF NOT EXISTS idx_audit_tenant ON audit_logs(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_logs(user_id);

-- @DOWN
DROP TABLE IF EXISTS audit_logs;
""",
    "V004__create_users_table.sql": """
-- @UP
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    roles TEXT DEFAULT '[]',
    permissions TEXT DEFAULT '[]',
    tenant_id TEXT DEFAULT 'default',
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- @DOWN
DROP TABLE IF EXISTS users;
"""
}


def init_migrations(migrations_dir: str = "migrations"):
    """初始化迁移文件"""
    migrations_path = Path(migrations_dir)
    migrations_path.mkdir(exist_ok=True)
    
    for filename, content in INITIAL_MIGRATIONS.items():
        file_path = migrations_path / filename
        if not file_path.exists():
            file_path.write_text(content, encoding="utf-8")
            print(f"已创建: {file_path}")
    
    print(f"\n迁移文件已初始化到: {migrations_path.absolute()}")


if __name__ == "__main__":
    import sys

    print(LEGACY_MIGRATION_NOTICE)
    if len(sys.argv) < 2:
        print("Usage: python migration_manager.py <command> [options]")
        print("Recommended: python scripts/manage_database.py <command>")
        print("\nCommands:")
        print("  init                 初始化迁移文件")
        print("  status               查看迁移状态")
        print("  migrate              执行所有待处理迁移")
        print("  create <name>        创建新迁移")
        print("  rollback [steps]     回滚迁移")
        print("  verify               验证迁移完整性")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "init":
        init_migrations()
    elif command == "status":
        manager = MigrationManager("data/jamin.db")
        status = manager.status()
        print(json.dumps(status, indent=2))
    elif command == "migrate":
        manager = MigrationManager("data/jamin.db")
        applied = manager.migrate_up()
        print(f"已应用 {len(applied)} 个迁移")
    elif command == "create":
        if len(sys.argv) < 3:
            print("请提供迁移名称")
            sys.exit(1)
        manager = MigrationManager("data/jamin.db")
        path = manager.create_migration(sys.argv[2])
        print(f"已创建: {path}")
    elif command == "rollback":
        steps = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        manager = MigrationManager("data/jamin.db")
        rolled_back = manager.migrate_down(steps)
        print(f"已回滚 {len(rolled_back)} 个迁移")
    elif command == "verify":
        manager = MigrationManager("data/jamin.db")
        issues = manager.verify()
        if issues:
            print("发现问题:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("验证通过，没有问题")
    else:
        print(f"未知命令: {command}")
