"""
LLM 诊断模块

功能：
- 基于大语言模型的故障诊断
- 自然语言问答
- 根因分析
- 维修建议生成

支持的模型：
- Qwen (通义千问)
- ChatGLM (智谱 AI)
- Baichuan (百川智能)
- OpenAI API 兼容接口
"""

import json
import urllib.request
import urllib.error
from urllib.parse import urljoin
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger


class LLMClient:
    """LLM 客户端基类"""
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """对话"""
        raise NotImplementedError
    
    def complete(self, prompt: str, **kwargs) -> str:
        """补全"""
        raise NotImplementedError


class QwenClient(LLMClient):
    """通义千问客户端"""
    
    def __init__(self, api_key: str, model: str = "qwen-max"):
        self.api_key = api_key
        self.model = model
        self.client = None
    
    def initialize(self) -> bool:
        """初始化客户端"""
        try:
            from dashscope import Generation
            
            self.client = Generation
            logger.info(f"Qwen 客户端初始化成功，模型：{self.model}")
            return True
            
        except ImportError:
            logger.error("缺少 dashscope 依赖：pip install dashscope")
            return False
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """对话"""
        if not self.client:
            raise ValueError("Qwen 客户端未初始化")
        
        try:
            response = self.client.call(
                model=self.model,
                messages=messages,
                api_key=self.api_key,
                **kwargs
            )
            
            if response.status_code == 200:
                return response.output.choices[0].message.content
            else:
                logger.error(f"Qwen API 错误：{response.code} - {response.message}")
                return f"API 错误：{response.message}"
                
        except Exception as e:
            logger.error(f"Qwen 调用失败：{e}")
            return f"调用失败：{e}"
    
    def complete(self, prompt: str, **kwargs) -> str:
        """补全"""
        messages = [{"role": "user", "content": prompt}]
        return self.chat(messages, **kwargs)


class ChatGLMClient(LLMClient):
    """智谱 ChatGLM 客户端"""
    
    def __init__(self, api_key: str, model: str = "glm-4"):
        self.api_key = api_key
        self.model = model
        self.client = None
    
    def initialize(self) -> bool:
        """初始化客户端"""
        try:
            from zhipuai import ZhipuAI
            
            self.client = ZhipuAI(api_key=self.api_key)
            logger.info(f"ChatGLM 客户端初始化成功，模型：{self.model}")
            return True
            
        except ImportError:
            logger.error("缺少 zhipuai 依赖：pip install zhipuai")
            return False
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """对话"""
        if not self.client:
            raise ValueError("ChatGLM 客户端未初始化")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                **kwargs
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"ChatGLM 调用失败：{e}")
            return f"调用失败：{e}"
    
    def complete(self, prompt: str, **kwargs) -> str:
        """补全"""
        messages = [{"role": "user", "content": prompt}]
        return self.chat(messages, **kwargs)


class OpenAICompatibleClient(LLMClient):
    """OpenAI API 兼容客户端"""
    
    def __init__(self, api_key: str, base_url: str, model: str = "gpt-3.5-turbo",
                 timeout_seconds: int = 20):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.initialized = False
    
    def initialize(self) -> bool:
        """初始化客户端"""
        try:
            if not self.base_url:
                raise ValueError("base_url 不能为空")

            self.initialized = True
            logger.info(
                f"OpenAI 兼容客户端初始化成功，模型：{self.model}, URL: {self.base_url}"
            )
            return True

        except Exception as e:
            logger.error(f"OpenAI 兼容客户端初始化失败：{e}")
            return False
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """对话"""
        if not self.initialized:
            raise ValueError("客户端未初始化")

        try:
            payload = {
                "model": kwargs.pop("model", self.model),
                "messages": messages,
                **kwargs,
            }
            request_body = json.dumps(payload).encode("utf-8")
            request = urllib.request.Request(
                urljoin(self.base_url.rstrip("/") + "/", "chat/completions"),
                data=request_body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
            )

            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                response_data = json.loads(response.read().decode("utf-8"))

            return response_data["choices"][0]["message"]["content"]

        except urllib.error.HTTPError as e:
            details = e.read().decode("utf-8", errors="ignore")
            logger.error(f"API 调用失败：HTTP {e.code} - {details}")
            return f"调用失败：HTTP {e.code}"
        except urllib.error.URLError as e:
            logger.error(f"API 调用失败：{e}")
            return f"调用失败：{e}"
        except Exception as e:
            logger.error(f"API 调用失败：{e}")
            return f"调用失败：{e}"
    
    def complete(self, prompt: str, **kwargs) -> str:
        """补全"""
        messages = [{"role": "user", "content": prompt}]
        return self.chat(messages, **kwargs)


class LLMDiagnoser:
    """LLM 诊断器"""
    
    def __init__(self, client: LLMClient, knowledge_base: Optional[List[Dict]] = None):
        self.client = client
        self.knowledge_base = knowledge_base or []
        self.diagnosis_history = []
    
    def diagnose(self, symptom: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        故障诊断
        
        Args:
            symptom: 故障现象描述
            context: 上下文信息 (设备状态、传感器数据等)
        
        Returns:
            诊断结果
        """
        # 构建提示词
        prompt = self._build_diagnosis_prompt(symptom, context)
        
        # 调用 LLM
        response = self.client.complete(prompt, temperature=0.3)
        
        # 解析响应
        diagnosis = self._parse_diagnosis(response)
        
        # 保存历史
        self.diagnosis_history.append({
            "timestamp": datetime.now().isoformat(),
            "symptom": symptom,
            "context": context,
            "diagnosis": diagnosis
        })
        
        logger.info(f"完成诊断：{symptom[:50]}... → {diagnosis.get('root_cause', '未知')}")
        
        return diagnosis
    
    def _build_diagnosis_prompt(self, symptom: str, context: Optional[Dict]) -> str:
        """构建诊断提示词"""
        prompt = f"""你是一位工业自动化领域的专家，擅长设备故障诊断。

## 故障现象
{symptom}

## 设备上下文
"""
        
        if context:
            for key, value in context.items():
                prompt += f"- {key}: {value}\n"
        else:
            prompt += "暂无额外信息\n"
        
        if self.knowledge_base:
            prompt += "\n## 相关知识库\n"
            for i, kb_item in enumerate(self.knowledge_base[:5], 1):
                prompt += f"{i}. {kb_item.get('title', '无标题')}: {kb_item.get('content', '')[:200]}...\n"
        
        prompt += """
## 任务
请根据以上信息，进行故障诊断，输出格式如下：

```json
{
  "root_cause": "最可能的根本原因",
  "confidence": 0.85,
  "possible_causes": [
    {"cause": "原因 1", "probability": 0.6},
    {"cause": "原因 2", "probability": 0.3},
    {"cause": "原因 3", "probability": 0.1}
  ],
  "suggested_actions": [
    {"action": "检查步骤 1", "priority": "high"},
    {"action": "检查步骤 2", "priority": "medium"}
  ],
  "maintenance_suggestions": [
    {"suggestion": "维修建议 1", "urgency": "immediate"},
    {"suggestion": "维修建议 2", "urgency": "scheduled"}
  ],
  "estimated_downtime": "预计停机时间",
  "spare_parts_needed": ["备件 1", "备件 2"]
}
```

请确保输出是合法的 JSON 格式。"""
        
        return prompt
    
    def _parse_diagnosis(self, response: str) -> Dict[str, Any]:
        """解析诊断结果"""
        try:
            # 尝试提取 JSON
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response[start_idx:end_idx]
                diagnosis = json.loads(json_str)
                return diagnosis
            else:
                logger.warning("无法从响应中提取 JSON")
                return {"error": "解析失败", "raw_response": response}
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败：{e}")
            return {"error": f"解析失败：{e}", "raw_response": response[:500]}
    
    def answer_question(self, question: str, context: Optional[str] = None) -> str:
        """回答技术问题"""
        prompt = f"""你是工业自动化领域的技术专家。

## 问题
{question}
"""
        
        if context:
            prompt += f"\n## 参考上下文\n{context}\n"
        
        prompt += "\n请给出专业、准确、简洁的回答。"
        
        response = self.client.complete(prompt, temperature=0.5)
        
        logger.info(f"回答问题：{question[:50]}...")
        return response
    
    def generate_report(self, incident_data: Dict[str, Any]) -> str:
        """生成事故报告"""
        prompt = f"""你是工业自动化领域的技术文档专家。

请根据以下事件数据，生成一份专业的事故分析报告：

## 事件数据
{json.dumps(incident_data, ensure_ascii=False, indent=2)}

## 报告要求
1. 事件概述
2. 时间线
3. 影响范围
4. 根本原因分析
5. 处理过程
6. 改进建议
7. 预防措施

请用 Markdown 格式输出。"""
        
        report = self.client.complete(prompt, temperature=0.3)
        
        logger.info(f"生成事故报告：{incident_data.get('id', 'unknown')}")
        return report


# 测试代码
if __name__ == "__main__":
    # 模拟 LLM 客户端 (用于测试)
    class MockClient(LLMClient):
        def chat(self, messages, **kwargs):
            return """```json
{
  "root_cause": "传感器故障导致读数异常",
  "confidence": 0.85,
  "possible_causes": [
    {"cause": "传感器老化", "probability": 0.5},
    {"cause": "接线松动", "probability": 0.3},
    {"cause": "电磁干扰", "probability": 0.2}
  ],
  "suggested_actions": [
    {"action": "检查传感器接线", "priority": "high"},
    {"action": "校准传感器", "priority": "medium"},
    {"action": "更换传感器", "priority": "low"}
  ],
  "maintenance_suggestions": [
    {"suggestion": "立即检查所有关键传感器", "urgency": "immediate"},
    {"suggestion": "建立定期校准计划", "urgency": "scheduled"}
  ],
  "estimated_downtime": "2-4 小时",
  "spare_parts_needed": ["温度传感器 PT100", "接线端子"]
}
```"""
        
        def complete(self, prompt, **kwargs):
            return self.chat([{"role": "user", "content": prompt}])
    
    # 测试诊断器
    diagnoser = LLMDiagnoser(MockClient())
    
    symptom = "3 号反应釜温度传感器读数突然下降到 0°C，但压力表显示正常"
    context = {
        "设备": "3 号反应釜",
        "位置": "车间 A",
        "运行时间": "连续运行 720 小时",
        "上次维护": "30 天前",
        "环境温度": "25°C",
        "其他传感器": "正常"
    }
    
    print("=== 测试 LLM 诊断 ===")
    print(f"症状：{symptom}")
    print(f"上下文：{context}")
    print()
    
    diagnosis = diagnoser.diagnose(symptom, context)
    
    print("诊断结果:")
    print(f"根本原因：{diagnosis.get('root_cause', 'N/A')}")
    print(f"置信度：{diagnosis.get('confidence', 0):.2f}")
    print()
    
    print("可能原因:")
    for cause in diagnosis.get('possible_causes', []):
        print(f"  - {cause['cause']} ({cause['probability']:.0%})")
    print()
    
    print("建议操作:")
    for action in diagnosis.get('suggested_actions', []):
        print(f"  [{action['priority']}] {action['action']}")
    print()
    
    print("维修建议:")
    for suggestion in diagnosis.get('maintenance_suggestions', []):
        print(f"  [{suggestion['urgency']}] {suggestion['suggestion']}")
