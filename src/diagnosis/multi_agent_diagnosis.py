"""Multi-agent diagnosis engine with runtime visibility."""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from src.knowledge.graph_rag import graph_rag
from src.models.agent_model_router import AgentModelProfile, AgentModelRouter
from src.utils.structured_logging import get_logger
from src.utils.thread_safe import ThreadSafeDict

logger = get_logger("multi_agent_diagnosis")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ExpertType(Enum):
    MECHANICAL = "mechanical"
    ELECTRICAL = "electrical"
    PROCESS = "process"
    SENSOR = "sensor"
    HISTORICAL = "historical"
    COORDINATOR = "coordinator"


@dataclass
class ExpertOpinion:
    expert_type: ExpertType
    expert_name: str
    confidence: float
    root_cause: str
    evidence: List[str]
    suggestions: List[str]
    reasoning: str
    model_name: Optional[str] = None
    llm_attempted: bool = False
    llm_used: bool = False
    used_fallback: bool = False
    fallback_reason: Optional[str] = None
    response_excerpt: Optional[str] = None
    duration_ms: Optional[float] = None
    timestamp: datetime = field(default_factory=utc_now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expert_type": self.expert_type.value,
            "expert_name": self.expert_name,
            "confidence": self.confidence,
            "root_cause": self.root_cause,
            "evidence": self.evidence,
            "suggestions": self.suggestions,
            "reasoning": self.reasoning,
            "model_name": self.model_name,
            "llm_attempted": self.llm_attempted,
            "llm_used": self.llm_used,
            "used_fallback": self.used_fallback,
            "fallback_reason": self.fallback_reason,
            "response_excerpt": self.response_excerpt,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class MultiAgentDiagnosisResult:
    diagnosis_id: str
    symptoms: str
    final_conclusion: str
    confidence: float
    consensus_level: float
    expert_opinions: List[ExpertOpinion]
    dissenting_views: List[ExpertOpinion]
    recommended_actions: List[Dict[str, Any]]
    spare_parts: List[Dict[str, Any]]
    related_cases: List[str]
    simulation_scenarios: List[Dict[str, Any]]
    agent_model_map: Dict[str, Dict[str, Any]]
    fallback_summary: Dict[str, Any]
    coordinator_metadata: Dict[str, Any]
    debug_metadata: Dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=utc_now)

    def to_dict(self, include_debug: bool = False) -> Dict[str, Any]:
        payload = {
            "diagnosis_id": self.diagnosis_id,
            "symptoms": self.symptoms,
            "final_conclusion": self.final_conclusion,
            "confidence": self.confidence,
            "consensus_level": self.consensus_level,
            "expert_opinions": [item.to_dict() for item in self.expert_opinions],
            "dissenting_views": [item.to_dict() for item in self.dissenting_views],
            "recommended_actions": self.recommended_actions,
            "spare_parts": self.spare_parts,
            "related_cases": self.related_cases,
            "simulation_scenarios": self.simulation_scenarios,
            "agent_model_map": self.agent_model_map,
            "fallback_summary": self.fallback_summary,
            "coordinator_metadata": self.coordinator_metadata,
            "generated_at": self.generated_at.isoformat(),
        }
        if include_debug:
            payload["debug"] = self.debug_metadata
        return payload


class LLMExpertAgent:
    output_contract = {
        "type": "json",
        "required_fields": ["confidence", "root_cause", "evidence", "suggestions", "reasoning"],
    }

    def __init__(
        self,
        expert_type: ExpertType,
        name: str,
        description: str,
        capabilities: List[str],
        system_prompt: str,
        llm_client: Any = None,
        model_profile: Optional[AgentModelProfile] = None,
    ):
        self.expert_type = expert_type
        self.name = name
        self.description = description
        self.capabilities = capabilities
        self.system_prompt = system_prompt
        self.llm_client = llm_client
        self.model_profile = model_profile

    @property
    def model_name(self) -> Optional[str]:
        return self.model_profile.model if self.model_profile else None

    async def analyze(
        self,
        symptoms: str,
        sensor_data: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> ExpertOpinion:
        context = context or {}
        prompt = self._build_prompt(symptoms, sensor_data, context)
        if not self.llm_client:
            return self._heuristic_opinion(symptoms, sensor_data, context)

        try:
            response = await self._call_llm(prompt)
            payload = self._extract_json(response)
            opinion = ExpertOpinion(
                expert_type=self.expert_type,
                expert_name=self.name,
                confidence=float(payload.get("confidence", 0.65)),
                root_cause=str(payload.get("root_cause") or self._heuristic_payload(symptoms, sensor_data, context)["root_cause"]),
                evidence=[str(item) for item in payload.get("evidence", [])][:4],
                suggestions=[str(item) for item in payload.get("suggestions", [])][:4],
                reasoning=str(payload.get("reasoning", "")).strip()[:500] or "模型返回了简短结论。",
                model_name=self.model_name,
                llm_attempted=True,
                llm_used=True,
                used_fallback=False,
                response_excerpt=str(response).strip()[:240],
            )
            if not opinion.evidence or not opinion.suggestions:
                raise ValueError("missing evidence or suggestions")
            return opinion
        except Exception as exc:
            fallback = self._heuristic_opinion(symptoms, sensor_data, context)
            fallback.llm_attempted = True
            fallback.used_fallback = True
            fallback.fallback_reason = str(exc)
            return fallback

    def to_runtime_dict(self) -> Dict[str, Any]:
        profile = self.model_profile
        return {
            "expert_name": self.name,
            "description": self.description,
            "capabilities": self.capabilities,
            "route_key": self.expert_type.value,
            "model_name": self.model_name,
            "llm_enabled": bool(self.llm_client),
            "endpoint": profile.endpoint if profile else None,
            "temperature": profile.temperature if profile else None,
            "max_tokens": profile.max_tokens if profile else None,
            "timeout_seconds": profile.timeout_seconds if profile else None,
            "prompt_summary": self.system_prompt.splitlines()[0].strip(),
            "system_prompt": self.system_prompt,
            "output_contract": self.output_contract,
        }

    def _build_prompt(self, symptoms: str, sensor_data: Dict[str, Any], context: Dict[str, Any]) -> str:
        return "\n".join(
            [
                self.system_prompt,
                "只输出一个 JSON 对象，不要 markdown，不要解释。",
                f"故障现象: {symptoms}",
                f"传感器数据: {json.dumps(sensor_data, ensure_ascii=False)}",
                f"上下文: {json.dumps(self._slim_context(context), ensure_ascii=False)}",
                "JSON fields: confidence, root_cause, evidence, suggestions, reasoning",
            ]
        )

    async def _call_llm(self, prompt: str) -> str:
        if hasattr(self.llm_client, "complete"):
            return await asyncio.to_thread(
                self.llm_client.complete,
                prompt,
                temperature=self.model_profile.temperature if self.model_profile else 0.2,
                max_tokens=self.model_profile.max_tokens if self.model_profile else 512,
            )
        raise ValueError("unsupported llm client")

    def _extract_json(self, raw_text: str) -> Dict[str, Any]:
        text = str(raw_text).strip()
        text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise ValueError("no json object found")
        payload = json.loads(match.group(0))
        if not payload.get("root_cause"):
            raise ValueError("missing root_cause")
        return payload

    def _slim_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "graph_rag_query": context.get("graph_rag_query"),
            "graph_rag_sources": context.get("graph_rag_summary", {}).get("sources"),
            "device_id": context.get("device_id"),
        }

    def _heuristic_opinion(self, symptoms: str, sensor_data: Dict[str, Any], context: Dict[str, Any]) -> ExpertOpinion:
        payload = self._heuristic_payload(symptoms, sensor_data, context)
        return ExpertOpinion(
            expert_type=self.expert_type,
            expert_name=self.name,
            confidence=payload["confidence"],
            root_cause=payload["root_cause"],
            evidence=payload["evidence"],
            suggestions=payload["suggestions"],
            reasoning=payload["reasoning"],
            model_name=self.model_name,
            llm_attempted=False,
            llm_used=False,
            used_fallback=False,
            response_excerpt=payload["reasoning"][:160],
        )

    def _heuristic_payload(self, symptoms: str, sensor_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        do_value = float(sensor_data.get("do", 0) or 0)
        vibration = float(sensor_data.get("vibration", 0) or 0)
        current = float(sensor_data.get("current", 0) or 0)
        graph_summary = context.get("graph_rag_summary", {})

        if self.expert_type == ExpertType.MECHANICAL:
            return {
                "confidence": 0.82,
                "root_cause": "风机轴承磨损或曝气盘局部堵塞",
                "evidence": [f"振动值 {vibration} 偏高", "异常噪声与旋转设备劣化特征一致"],
                "suggestions": ["检查风机轴承与转子平衡", "清洗曝气盘并复核风量"],
                "reasoning": "机械侧优先怀疑旋转部件磨损和气路堵塞。",
            }
        if self.expert_type == ExpertType.ELECTRICAL:
            return {
                "confidence": 0.74,
                "root_cause": "电机负载偏高或供电不平衡",
                "evidence": [f"电流值 {current} 偏高", "异常噪声可能由负载波动放大"],
                "suggestions": ["检查三相电流与电压平衡", "复核变频器参数"],
                "reasoning": "电气侧从高电流和负载波动判断驱动链存在压力。",
            }
        if self.expert_type == ExpertType.PROCESS:
            return {
                "confidence": 0.86,
                "root_cause": "曝气效率下降导致溶解氧不足",
                "evidence": [f"DO 值 {do_value} 偏低", "症状描述直接指向曝气工艺不足"],
                "suggestions": ["提高排查优先级并确认实际供氧量", "检查污泥负荷与曝气分配"],
                "reasoning": "工艺侧认为供氧不足是当前最直接的生产风险。",
            }
        if self.expert_type == ExpertType.SENSOR:
            return {
                "confidence": 0.90,
                "root_cause": "DO 传感器可能存在污染或漂移",
                "evidence": ["低 DO 读数需要先校验仪表可信度", "在线仪表污染会放大误判风险"],
                "suggestions": ["清洗并校准 DO 传感器", "对比人工采样结果"],
                "reasoning": "仪表侧先确认数据是否可信，避免误调工艺。",
            }
        return {
            "confidence": 0.71,
            "root_cause": "历史案例显示曝气系统劣化与进氧链路异常高度相关",
            "evidence": [
                f"GraphRAG 相关源: {', '.join(graph_summary.get('sources', ['CASE_20230815_001']))}",
                "GraphRAG 匹配到曝气异常历史案例",
            ],
            "suggestions": ["参考历史案例先排查曝气盘与风机", "同步核查近期进水负荷变化"],
            "reasoning": "历史案例和图谱信息支持先从曝气链路着手。",
        }


class MultiAgentDiagnosisEngine:
    def __init__(
        self,
        llm_client: Any = None,
        model_router: Optional[AgentModelRouter] = None,
        enable_model_routing: Optional[bool] = None,
    ):
        self.model_router = model_router or AgentModelRouter()
        self.enable_model_routing = bool(self.model_router.enabled) if enable_model_routing is None else enable_model_routing
        self.llm_client = llm_client
        self.experts: Dict[ExpertType, LLMExpertAgent] = {}
        self.coordinator: Optional[LLMExpertAgent] = None
        self._diagnosis_history = ThreadSafeDict()
        self._init_experts()

    def _init_experts(self) -> None:
        expert_configs = [
            (
                ExpertType.MECHANICAL,
                "机械故障诊断专家",
                "聚焦旋转设备、轴承、曝气盘与气路堵塞。",
                ["振动分析", "轴承诊断", "动平衡"],
                "你是机械故障诊断专家，负责判断风机、轴承、曝气盘等机械部件问题。",
            ),
            (
                ExpertType.ELECTRICAL,
                "电气系统诊断专家",
                "聚焦电机、供电稳定性与驱动控制。",
                ["电机诊断", "绝缘测试", "变频控制"],
                "你是电气系统诊断专家，负责判断电机负载、供电和变频控制问题。",
            ),
            (
                ExpertType.PROCESS,
                "工艺分析专家",
                "聚焦供氧效率、污泥负荷和工艺参数。",
                ["工艺优化", "参数调整", "水质分析"],
                "你是污水处理工艺专家，负责判断曝气效率和运行参数问题。",
            ),
            (
                ExpertType.SENSOR,
                "传感器与仪表专家",
                "聚焦 DO 传感器、校准和测量可信度。",
                ["仪表校准", "漂移诊断", "信号分析"],
                "你是工业仪表专家，负责判断在线传感器污染、漂移和校准问题。",
            ),
            (
                ExpertType.HISTORICAL,
                "历史案例匹配专家",
                "聚焦历史案例、知识图谱和故障复盘。",
                ["案例匹配", "知识检索", "处置复盘"],
                "你是历史案例匹配专家，负责利用 GraphRAG 和历史工单寻找相似故障。",
            ),
        ]
        for expert_type, name, description, capabilities, prompt in expert_configs:
            self.experts[expert_type] = LLMExpertAgent(
                expert_type=expert_type,
                name=name,
                description=description,
                capabilities=capabilities,
                system_prompt=prompt,
                llm_client=self._get_client(expert_type.value),
                model_profile=self._get_profile(expert_type.value),
            )

        self.coordinator = LLMExpertAgent(
            expert_type=ExpertType.COORDINATOR,
            name="诊断协调专家",
            description="负责整合多专家意见，形成最终结论和处置优先级。",
            capabilities=["意见整合", "冲突化解", "报告生成"],
            system_prompt="你是诊断协调专家，需要整合多位专家意见并形成统一结论。",
            llm_client=self._get_client("coordinator"),
            model_profile=self._get_profile("coordinator"),
        )

    def _get_profile(self, route_key: str) -> Optional[AgentModelProfile]:
        return self.model_router.get_profile(route_key) if self.enable_model_routing and self.model_router else None

    def _get_client(self, route_key: str) -> Any:
        return self.model_router.get_client(route_key) if self.enable_model_routing and self.model_router else self.llm_client

    async def diagnose(
        self,
        symptoms: str,
        sensor_data: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        trace_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> MultiAgentDiagnosisResult:
        context = dict(context or {})
        diagnosis_id = f"MAD_{uuid.uuid4().hex[:12].upper()}"
        execution_trace: List[Dict[str, Any]] = []
        started_at = time.perf_counter()

        def emit(stage: str, message: str, **extra: Any) -> None:
            event = {"stage": stage, "message": message, "timestamp": datetime.now(timezone.utc).isoformat(), **extra}
            execution_trace.append(event)
            if trace_callback:
                trace_callback(event)

        emit("diagnosis_started", "开始多智能体诊断", progress=5)
        graph_payload = await self._prepare_graph_context(symptoms, sensor_data, context, emit)

        async def run_expert(expert: LLMExpertAgent) -> ExpertOpinion:
            expert_started_at = time.perf_counter()
            emit("expert_started", f"{expert.name} 开始分析", progress=20, agent_key=expert.expert_type.value, agent_name=expert.name)
            opinion = await expert.analyze(symptoms, sensor_data, context)
            opinion.duration_ms = round((time.perf_counter() - expert_started_at) * 1000, 2)
            emit(
                "expert_completed",
                f"{expert.name} 完成分析",
                progress=55,
                agent_key=expert.expert_type.value,
                agent_name=expert.name,
                used_fallback=opinion.used_fallback,
                duration_ms=opinion.duration_ms,
            )
            return opinion

        opinions = await asyncio.gather(*[run_expert(expert) for expert in self.experts.values()])
        emit(
            "coordinator_started",
            "协调者开始整合意见",
            progress=70,
            agent_key="coordinator",
            agent_name=self.coordinator.name if self.coordinator else "诊断协调专家",
        )
        coordinator_started_at = time.perf_counter()
        coordinated = await self._coordinate(opinions)
        coordinated["coordinator_metadata"]["duration_ms"] = round((time.perf_counter() - coordinator_started_at) * 1000, 2)
        emit(
            "coordinator_completed",
            "协调者完成整合",
            progress=85,
            agent_key="coordinator",
            used_fallback=coordinated["coordinator_metadata"]["used_fallback"],
            duration_ms=coordinated["coordinator_metadata"]["duration_ms"],
        )
        scenarios = self._build_scenarios()
        emit("scenarios_generated", "完成场景推演", progress=92)

        result = MultiAgentDiagnosisResult(
            diagnosis_id=diagnosis_id,
            symptoms=symptoms,
            final_conclusion=coordinated["conclusion"],
            confidence=coordinated["confidence"],
            consensus_level=coordinated["consensus_level"],
            expert_opinions=opinions,
            dissenting_views=coordinated["dissenting_views"],
            recommended_actions=coordinated["actions"],
            spare_parts=coordinated["spare_parts"],
            related_cases=graph_payload["related_cases"],
            simulation_scenarios=scenarios,
            agent_model_map=self.get_agent_runtime_profiles(),
            fallback_summary={
                "experts": {
                    item.expert_type.value: {
                        "agent_name": item.expert_name,
                        "used_fallback": item.used_fallback,
                        "fallback_reason": item.fallback_reason,
                        "llm_attempted": item.llm_attempted,
                        "llm_used": item.llm_used,
                        "model_name": item.model_name,
                    }
                    for item in opinions
                },
                "coordinator": coordinated["coordinator_metadata"],
            },
            coordinator_metadata=coordinated["coordinator_metadata"],
            debug_metadata={
                "graph_rag": graph_payload["debug"],
                "experts": {
                    item.expert_type.value: {
                        "expert_name": item.expert_name,
                        "model_name": item.model_name,
                        "llm_attempted": item.llm_attempted,
                        "llm_used": item.llm_used,
                        "used_fallback": item.used_fallback,
                        "fallback_reason": item.fallback_reason,
                        "response_excerpt": item.response_excerpt,
                        "duration_ms": item.duration_ms,
                    }
                    for item in opinions
                },
                "coordinator": coordinated["coordinator_metadata"],
                "execution_trace": execution_trace,
            },
        )
        self._diagnosis_history.set(diagnosis_id, result)
        emit(
            "diagnosis_completed",
            "多智能体诊断完成",
            progress=100,
            diagnosis_id=diagnosis_id,
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
        result.debug_metadata["execution_trace"] = execution_trace
        return result

    async def _prepare_graph_context(
        self,
        symptoms: str,
        sensor_data: Dict[str, Any],
        context: Dict[str, Any],
        emit: Callable[[str, str], None],
    ) -> Dict[str, Any]:
        if not context.get("use_graph_rag", False):
            summary = {"sources": ["CASE_20230815_001", "CASE_20231022_003"]}
            context["graph_rag_summary"] = summary
            return {"related_cases": summary["sources"], "debug": {"enabled": False, "query": None, "summary": summary}}

        query = f"{symptoms} | sensor={json.dumps(sensor_data, ensure_ascii=False)}"
        graph_started_at = time.perf_counter()
        emit("graph_rag_started", "GraphRAG 开始检索", progress=10)
        try:
            graph_result = await graph_rag.query(query)
            sources = graph_result.get("sources") or ["CASE_20230815_001"]
            summary = {
                "sources": sources[:4],
                "answer_excerpt": str(graph_result.get("answer", ""))[:180],
            }
            context["graph_rag_query"] = query
            context["graph_rag_summary"] = summary
            emit(
                "graph_rag_completed",
                "GraphRAG 检索完成",
                progress=18,
                source_count=len(summary["sources"]),
                duration_ms=round((time.perf_counter() - graph_started_at) * 1000, 2),
            )
            return {"related_cases": summary["sources"], "debug": {"enabled": True, "query": query, "summary": summary}}
        except Exception as exc:
            logger.warning(f"graph rag failed: {exc}")
            summary = {"sources": ["CASE_20230815_001", "CASE_20231022_003"], "error": str(exc)}
            context["graph_rag_query"] = query
            context["graph_rag_summary"] = summary
            emit(
                "graph_rag_failed",
                "GraphRAG 检索失败，使用默认案例",
                progress=18,
                duration_ms=round((time.perf_counter() - graph_started_at) * 1000, 2),
            )
            return {"related_cases": summary["sources"], "debug": {"enabled": True, "query": query, "summary": summary}}

    async def _coordinate(self, opinions: List[ExpertOpinion]) -> Dict[str, Any]:
        if self.coordinator and self.coordinator.llm_client:
            llm_result = await self._coordinate_with_llm(opinions)
            if llm_result is not None:
                return llm_result
        return self._coordinate_with_rules(opinions, used_fallback=False, fallback_reason=None)

    async def _coordinate_with_llm(self, opinions: List[ExpertOpinion]) -> Optional[Dict[str, Any]]:
        opinion_payload = [
            {
                "expert_type": item.expert_type.value,
                "expert_name": item.expert_name,
                "confidence": item.confidence,
                "root_cause": item.root_cause,
                "evidence": item.evidence,
                "suggestions": item.suggestions,
            }
            for item in opinions
        ]
        prompt = "\n".join(
            [
                self.coordinator.system_prompt,
                "请整合以下专家意见，只输出一个 JSON 对象，不要 markdown，不要解释。",
                json.dumps(opinion_payload, ensure_ascii=False),
                "JSON fields: conclusion, confidence, consensus_level, actions, spare_parts",
            ]
        )
        try:
            response = await self.coordinator._call_llm(prompt)
            payload = self.coordinator._extract_json(response)
            grouped = self._group_opinions(opinions)
            best_group = max(grouped.values(), key=lambda items: (len(items), sum(op.confidence for op in items)))
            return {
                "conclusion": str(payload.get("conclusion") or payload.get("root_cause") or best_group[0].root_cause),
                "confidence": float(payload.get("confidence", round(sum(item.confidence for item in best_group) / len(best_group), 2))),
                "consensus_level": float(payload.get("consensus_level", round(len(best_group) / max(len(opinions), 1), 2))),
                "dissenting_views": [item for item in opinions if item not in best_group][:2],
                "actions": self._normalize_actions(payload.get("actions")) or self._prioritize_actions(
                    [suggestion for item in opinions for suggestion in item.suggestions]
                ),
                "spare_parts": self._normalize_parts(payload.get("spare_parts")) or self._build_spare_parts(best_group[0].root_cause),
                "coordinator_metadata": {
                    "model_name": self.coordinator.model_name if self.coordinator else None,
                    "llm_attempted": True,
                    "llm_used": True,
                    "used_fallback": False,
                    "fallback_reason": None,
                    "response_excerpt": str(response).strip()[:240],
                },
            }
        except Exception as exc:
            return self._coordinate_with_rules(opinions, used_fallback=True, fallback_reason=str(exc))

    def _coordinate_with_rules(
        self,
        opinions: List[ExpertOpinion],
        used_fallback: bool,
        fallback_reason: Optional[str],
    ) -> Dict[str, Any]:
        grouped = self._group_opinions(opinions)
        best_group = max(grouped.values(), key=lambda items: (len(items), sum(op.confidence for op in items)))
        best_root_cause = best_group[0].root_cause
        suggestions = [suggestion for item in opinions for suggestion in item.suggestions]
        return {
            "conclusion": best_root_cause,
            "confidence": round(sum(item.confidence for item in best_group) / len(best_group), 2),
            "consensus_level": round(len(best_group) / max(len(opinions), 1), 2),
            "dissenting_views": [item for item in opinions if item not in best_group][:2],
            "actions": self._prioritize_actions(suggestions),
            "spare_parts": self._build_spare_parts(best_root_cause),
            "coordinator_metadata": {
                "model_name": self.coordinator.model_name if self.coordinator else None,
                "llm_attempted": bool(self.coordinator and self.coordinator.llm_client),
                "llm_used": False,
                "used_fallback": used_fallback,
                "fallback_reason": fallback_reason,
                "response_excerpt": f"协调者整合了 {len(opinions)} 位专家意见，主结论为 {best_root_cause}"[:240],
            },
        }

    def _group_opinions(self, opinions: List[ExpertOpinion]) -> Dict[str, List[ExpertOpinion]]:
        grouped: Dict[str, List[ExpertOpinion]] = {}
        for item in opinions:
            grouped.setdefault(self._normalize_cause(item.root_cause), []).append(item)
        return grouped

    def _normalize_cause(self, text: str) -> str:
        normalized = re.sub(r"[，。,. ]+", "", text.lower())
        if any(word in normalized for word in ["曝气", "供氧", "风机"]):
            return "aeration"
        if any(word in normalized for word in ["传感器", "校准", "do"]):
            return "sensor"
        if any(word in normalized for word in ["电机", "供电", "电流"]):
            return "electrical"
        return normalized[:24]

    def _normalize_actions(self, actions: Any) -> List[Dict[str, Any]]:
        if not isinstance(actions, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for item in actions:
            if isinstance(item, str):
                normalized.append(
                    {
                        "action": item,
                        "priority": "high" if any(keyword in item for keyword in ["检查", "清洗", "校准"]) else "medium",
                        "estimated_time": "30分钟",
                        "requires_shutdown": False,
                    }
                )
            elif isinstance(item, dict) and item.get("action"):
                normalized.append(
                    {
                        "action": str(item.get("action")),
                        "priority": str(item.get("priority", "medium")),
                        "estimated_time": str(item.get("estimated_time", "30分钟")),
                        "requires_shutdown": bool(item.get("requires_shutdown", False)),
                    }
                )
        return normalized[:5]

    def _normalize_parts(self, parts: Any) -> List[Dict[str, Any]]:
        if not isinstance(parts, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for item in parts:
            if isinstance(item, dict) and item.get("name"):
                normalized.append(
                    {
                        "name": str(item.get("name")),
                        "quantity": int(item.get("quantity", 1)),
                        "spec": str(item.get("spec", "待确认")),
                    }
                )
        return normalized[:4]

    def _prioritize_actions(self, suggestions: List[str]) -> List[Dict[str, Any]]:
        seen = set()
        actions: List[Dict[str, Any]] = []
        for suggestion in suggestions:
            suggestion = str(suggestion).strip()
            if not suggestion or suggestion in seen:
                continue
            seen.add(suggestion)
            actions.append(
                {
                    "action": suggestion,
                    "priority": "high" if any(keyword in suggestion for keyword in ["检查", "清洗", "校准"]) else "medium",
                    "estimated_time": "30分钟",
                    "requires_shutdown": False,
                }
            )
        return actions[:6]

    def _build_spare_parts(self, root_cause: str) -> List[Dict[str, Any]]:
        if "传感器" in root_cause or "sensor" in self._normalize_cause(root_cause):
            return [{"name": "DO 传感器探头", "quantity": 1, "spec": "与现场仪表型号匹配"}]
        return [
            {"name": "风机轴承组件", "quantity": 1, "spec": "按现场风机型号选配"},
            {"name": "曝气盘膜片", "quantity": 2, "spec": "耐腐蚀型"},
        ]

    def _build_scenarios(self) -> List[Dict[str, Any]]:
        return [
            {"scenario": "及时处理", "impact": "2小时内恢复供氧能力，工艺风险可控", "probability": 0.72},
            {"scenario": "延迟处理", "impact": "溶解氧持续偏低，出水稳定性下降并放大设备损耗", "probability": 0.28},
        ]

    def get_agent_runtime_profiles(self) -> Dict[str, Dict[str, Any]]:
        profiles = {expert_type.value: expert.to_runtime_dict() for expert_type, expert in self.experts.items()}
        if self.coordinator:
            profiles["coordinator"] = self.coordinator.to_runtime_dict()
        return profiles

    def get_agent_catalog(self) -> Dict[str, Any]:
        experts = [expert.to_runtime_dict() for expert in self.experts.values()]
        return {
            "experts": experts,
            "coordinator": self.coordinator.to_runtime_dict() if self.coordinator else None,
            "total_agents": len(experts) + (1 if self.coordinator else 0),
        }

    def get_diagnosis_history(self, limit: int = 10) -> List[MultiAgentDiagnosisResult]:
        items = list(self._diagnosis_history.values())
        items.sort(key=lambda item: item.generated_at, reverse=True)
        return items[:limit]
