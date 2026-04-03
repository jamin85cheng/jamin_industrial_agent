"""
配置管理工具
"""

import yaml
import os
import re
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger


ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_config_path(config_file: str) -> Path:
    path = Path(config_file)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def load_config(config_file: str = "config/settings.yaml") -> Dict[str, Any]:
    """
    加载配置文件
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        配置字典
    """
    config_path = _resolve_config_path(config_file)
    _load_env_file(config_path.parent / ".env")
    
    if not config_path.exists():
        logger.warning(f"配置文件不存在：{config_path}，使用默认配置")
        return get_default_config()
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        config = _expand_env_vars(config)
        logger.info(f"配置文件已加载：{config_path}")
        return config
        
    except Exception as e:
        logger.error(f"加载配置文件失败：{e}")
        return get_default_config()


def _load_env_file(env_file: Path):
    """从本地 .env 文件加载环境变量"""
    if not env_file.exists():
        return

    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
    except Exception as exc:
        logger.warning(f"读取环境变量文件失败：{exc}")


def _expand_env_vars(value: Any) -> Any:
    """递归展开配置中的环境变量"""
    if isinstance(value, dict):
        return {key: _expand_env_vars(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_expand_env_vars(item) for item in value]

    if not isinstance(value, str):
        return value

    def replace(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2)
        return os.environ.get(var_name, default or "")

    return ENV_VAR_PATTERN.sub(replace, value)


def save_config(config: Dict[str, Any], config_file: str = "config/settings.yaml"):
    """
    保存配置文件
    
    Args:
        config: 配置字典
        config_file: 配置文件路径
    """
    config_path = _resolve_config_path(config_file)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        
        logger.info(f"配置文件已保存：{config_path}")
        
    except Exception as e:
        logger.error(f"保存配置文件失败：{e}")


def get_default_config() -> Dict[str, Any]:
    """获取默认配置"""
    return {
        'project': {
            'name': 'Jamin Industrial Agent',
            'version': 'v1.0.0-beta2',
            'environment': 'development'
        },
        'plc': {
            'type': 's7',
            'host': '127.0.0.1',
            'port': 102,
            'scan_interval': 10
        },
        'database': {
            'sqlite': {
                'enabled': True,
                'path': 'data/metadata.db'
            },
            'postgres': {
                'enabled': False,
                'host': '127.0.0.1',
                'port': 5432,
                'database': 'jamin_industrial_agent',
                'user': 'postgres',
                'password': 'postgres',
                'schema': 'jamin_industrial_agent',
                'sslmode': 'prefer',
            },
            'task_tracking': {
                'backend': 'sqlite',
                'sqlite_path': 'data/runtime/tasks.sqlite',
            }
        },
        'rules': {
            'config_file': 'config/rules.json',
            'evaluation_interval': 10
        },
        'logging': {
            'level': 'INFO',
            'log_dir': 'logs'
        }
    }


# 使用示例
if __name__ == "__main__":
    config = load_config()
    
    print("\n当前配置:")
    print(f"  项目名称：{config['project']['name']}")
    print(f"  PLC 地址：{config['plc']['host']}:{config['plc']['port']}")
    print(f"  采集频率：{config['plc']['scan_interval']}秒")

