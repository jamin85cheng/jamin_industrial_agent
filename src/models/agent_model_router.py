"""
按智能体角色分配模型的路由器。
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from loguru import logger

from src.models.llm_diagnosis import OpenAICompatibleClient
from src.utils.config import load_config


@dataclass
class AgentModelProfile:
    """智能体模型配置"""
    endpoint: str
    model: str
    temperature: float
    max_tokens: int
    timeout_seconds: int


class AgentModelRouter:
    """为不同 agent 选择不同的模型和端点"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or load_config()
        llm_config = self.config.get("llm", {})
        routing_config = llm_config.get("agent_routing", {})

        self.enabled = routing_config.get("enabled", False)
        self.endpoints = llm_config.get("endpoints", {})
        self.default_generation = llm_config.get("generation", {})
        self.routes = routing_config.get("agents", {})
        self._client_cache: Dict[Tuple[str, str], OpenAICompatibleClient] = {}

    def get_profile(self, agent_key: str) -> Optional[AgentModelProfile]:
        """获取 agent 对应的模型配置"""
        if not self.enabled:
            return None

        route = self.routes.get(agent_key) or self.routes.get("default")
        if not route:
            return None

        endpoint_name = route.get("endpoint")
        endpoint = self.endpoints.get(endpoint_name, {})
        model = route.get("model")

        if not endpoint_name or not endpoint.get("base_url") or not model:
            return None

        return AgentModelProfile(
            endpoint=endpoint_name,
            model=model,
            temperature=float(route.get("temperature", self.default_generation.get("temperature", 0.2))),
            max_tokens=int(route.get("max_tokens", self.default_generation.get("max_tokens", 512))),
            timeout_seconds=int(route.get("timeout_seconds", endpoint.get("timeout_seconds", 20))),
        )

    def get_client(self, agent_key: str) -> Optional[OpenAICompatibleClient]:
        """获取 agent 对应的客户端"""
        profile = self.get_profile(agent_key)
        if not profile:
            return None

        endpoint = self.endpoints.get(profile.endpoint, {})
        cache_key = (profile.endpoint, profile.model)

        if cache_key in self._client_cache:
            return self._client_cache[cache_key]

        client = OpenAICompatibleClient(
            api_key=str(endpoint.get("api_key", "")),
            base_url=str(endpoint.get("base_url", "")),
            model=profile.model,
            timeout_seconds=profile.timeout_seconds
        )
        client.initialize()
        self._client_cache[cache_key] = client
        logger.info(f"模型路由已绑定: {agent_key} -> {profile.endpoint}/{profile.model}")
        return client
