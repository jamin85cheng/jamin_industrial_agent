"""
配置管理工具
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger


def load_config(config_file: str = "config/settings.yaml") -> Dict[str, Any]:
    """
    加载配置文件
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        配置字典
    """
    config_path = Path(config_file)
    
    if not config_path.exists():
        logger.warning(f"配置文件不存在：{config_path}，使用默认配置")
        return get_default_config()
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"配置文件已加载：{config_path}")
        return config
        
    except Exception as e:
        logger.error(f"加载配置文件失败：{e}")
        return get_default_config()


def save_config(config: Dict[str, Any], config_file: str = "config/settings.yaml"):
    """
    保存配置文件
    
    Args:
        config: 配置字典
        config_file: 配置文件路径
    """
    config_path = Path(config_file)
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

