"""
规则 DSL 解析器
将 JSON 格式的规则定义转换为可执行的条件判断
"""

import json
import re
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path
from loguru import logger


class RuleParser:
    """规则解析器"""
    
    def __init__(self):
        self.operators = {
            '>': lambda a, b: a > b,
            '<': lambda a, b: a < b,
            '>=': lambda a, b: a >= b,
            '<=': lambda a, b: a <= b,
            '==': lambda a, b: a == b,
            '!=': lambda a, b: a != b,
        }
    
    def parse_rule_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        解析规则配置文件
        
        Args:
            file_path: JSON 规则文件路径
            
        Returns:
            规则列表
        """
        path = Path(file_path)
        if not path.exists():
            logger.error(f"规则文件不存在：{file_path}")
            return []
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            rules = data.get('rules', [])
            logger.info(f"成功加载 {len(rules)} 条规则")
            
            # 验证每条规则的结构
            valid_rules = []
            for rule in rules:
                if self._validate_rule(rule):
                    valid_rules.append(rule)
                else:
                    logger.warning(f"规则 {rule.get('rule_id', 'UNKNOWN')} 结构无效，已跳过")
            
            return valid_rules
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败：{e}")
            return []
        except Exception as e:
            logger.error(f"读取规则文件失败：{e}")
            return []
    
    def _validate_rule(self, rule: Dict[str, Any]) -> bool:
        """验证规则结构是否完整"""
        required_fields = ['rule_id', 'name', 'condition']
        for field in required_fields:
            if field not in rule:
                logger.warning(f"规则缺少必需字段：{field}")
                return False
        
        condition = rule['condition']
        if 'type' not in condition:
            logger.warning("规则条件缺少 type 字段")
            return False
        
        return True
    
    def compile_condition(self, condition: Dict[str, Any]) -> Callable[[Dict[str, Any]], bool]:
        """
        编译条件为可执行函数
        
        Args:
            condition: 条件定义
            
        Returns:
            判断函数，接收实时数据，返回布尔值
        """
        condition_type = condition.get('type')
        
        if condition_type == 'threshold':
            return self._compile_threshold(condition)
        elif condition_type == 'duration':
            return self._compile_duration(condition)
        elif condition_type == 'rate_of_change':
            return self._compile_rate_of_change(condition)
        elif condition_type == 'logic':
            return self._compile_logic(condition)
        elif condition_type == 'correlation_violation':
            return self._compile_correlation_violation(condition)
        else:
            logger.warning(f"不支持的条件类型：{condition_type}")
            return lambda data: False
    
    def _compile_threshold(self, condition: Dict[str, Any]) -> Callable:
        """编译阈值条件"""
        metric = condition.get('metric') or condition.get('tag')
        operator = condition['operator']
        threshold = condition.get('threshold', condition.get('value'))

        if metric is None or threshold is None:
            logger.error("threshold condition requires metric/tag and threshold/value")
            return lambda data: False
        
        op_func = self.operators.get(operator)
        if not op_func:
            logger.error(f"不支持的运算符：{operator}")
            return lambda data: False
        
        def check(data: Dict[str, Any]) -> bool:
            value = self._get_metric_value(data, metric)
            if value is None:
                return False
            return op_func(value, threshold)
        
        return check
    
    def _compile_duration(self, condition: Dict[str, Any]) -> Callable:
        """编译持续时间条件 (需要历史数据支持)"""
        metric = condition['metric']
        operator = condition['operator']
        threshold = condition['threshold']
        duration_minutes = condition.get('duration_minutes', 10)
        
        op_func = self.operators.get(operator)
        
        def check(data: Dict[str, Any]) -> bool:
            # 这里需要从历史数据中检查持续时间的条件
            # 简化实现：假设 data 中包含历史数据
            history = data.get('_history', {}).get(metric, [])
            
            if len(history) < duration_minutes:
                return False
            
            # 检查最近 N 分钟的数据是否都满足条件
            recent_values = history[-duration_minutes:]
            for value in recent_values:
                if not op_func(value, threshold):
                    return False
            
            return True
        
        return check
    
    def _compile_rate_of_change(self, condition: Dict[str, Any]) -> Callable:
        """编译变化率条件"""
        metric = condition['metric']
        change_threshold = condition.get('change_threshold', condition.get('min_change'))
        window_minutes = condition.get('window_minutes', 5)

        if change_threshold is None:
            logger.error("rate_of_change condition requires change_threshold or min_change")
            return lambda data: False
        
        def check(data: Dict[str, Any]) -> bool:
            history = data.get('_history', {}).get(metric, [])
            
            if len(history) < 1:
                return False
            
            current_value = self._get_metric_value(data, metric)
            if current_value is None:
                current_value = history[-1]
            past_value = history[-min(window_minutes, len(history))]
            
            change = abs(current_value - past_value)
            return change > change_threshold
        
        return check
    
    def _compile_logic(self, condition: Dict[str, Any]) -> Callable:
        """编译逻辑组合条件"""
        sub_conditions = condition.get('conditions', [])
        logic = str(condition.get('logic', condition.get('operator', 'AND'))).upper()
        
        compiled_checks = []
        for sub_cond in sub_conditions:
            check_func = self.compile_condition(sub_cond)
            compiled_checks.append(check_func)
        
        def check(data: Dict[str, Any]) -> bool:
            results = [check_func(data) for check_func in compiled_checks]
            
            if logic == 'AND':
                return all(results)
            elif logic == 'OR':
                return any(results)
            else:
                return False
        
        return check
    
    def _compile_correlation_violation(self, condition: Dict[str, Any]) -> Callable:
        """编译相关性违背条件"""
        metrics = condition.get('metrics', [])
        expected_correlation = condition.get('expected_correlation', 'positive')
        
        def check(data: Dict[str, Any]) -> bool:
            if len(metrics) < 2:
                return False
            
            # 获取两个指标的历史数据
            history_1 = data.get('_history', {}).get(metrics[0], [])
            history_2 = data.get('_history', {}).get(metrics[1], [])
            
            if len(history_1) < 10 or len(history_2) < 10:
                return False
            
            # 简化实现：检查最近趋势是否相反
            recent_1 = history_1[-5:]
            recent_2 = history_2[-5:]
            
            trend_1 = recent_1[-1] - recent_1[0]
            trend_2 = recent_2[-1] - recent_2[0]
            
            if expected_correlation == 'positive':
                # 正相关：应该同向变化
                return (trend_1 > 0 and trend_2 < 0) or (trend_1 < 0 and trend_2 > 0)
            elif expected_correlation == 'negative':
                # 负相关：应该反向变化
                return (trend_1 > 0 and trend_2 > 0) or (trend_1 < 0 and trend_2 < 0)
            
            return False
        
        return check
    
    def _get_metric_value(self, data: Dict[str, Any], metric: str) -> Optional[float]:
        """从数据字典中获取指标值"""
        # 支持多种数据格式
        if metric in data:
            value = data[metric]
            if isinstance(value, dict):
                return value.get('value')
            return value
        
        # 尝试从嵌套结构获取
        parts = metric.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        if isinstance(current, dict):
            return current.get('value')
        return current


# 使用示例
if __name__ == "__main__":
    parser = RuleParser()
    
    # 解析规则文件
    rules = parser.parse_rule_file('config/rules.json')
    
    print(f"加载了 {len(rules)} 条规则\n")
    
    # 编译第一条规则的条件
    if rules:
        rule = rules[0]
        print(f"规则：{rule['name']}")
        print(f"条件类型：{rule['condition']['type']}")
        
        # 编译条件
        check_func = parser.compile_condition(rule['condition'])
        
        # 测试数据
        test_data = {
            'TAG_DO_001': 1.5,
            '_history': {
                'TAG_DO_001': [1.8, 1.7, 1.6, 1.5, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8]
            }
        }
        
        result = check_func(test_data)
        print(f"\n测试结果：{result} (期望：True)")
