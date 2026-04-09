"""CAMEL-style industrial diagnosis collaboration with true multi-round reasoning."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from src.knowledge.graph_rag import graph_rag
from src.models.agent_model_router import AgentModelRouter
from src.utils.structured_logging import get_logger

logger = get_logger("camel_integration")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MessageType(Enum):
    TASK_ASSIGNMENT = "task_assignment"
    OPINION = "opinion"
    QUESTION = "question"
    ANSWER = "answer"
    DEBATE = "debate"
    CONSENSUS = "consensus"
    SYSTEM = "system"


class AgentRole(Enum):
    EXPERT = "expert"
    CRITIC = "critic"
    COORDINATOR = "coordinator"


@dataclass
class AgentMessage:
    message_id: str
    sender_id: str
    receiver_id: Optional[str]
    message_type: MessageType
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utc_now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "message_type": self.message_type.value,
            "content": self.content,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class Task:
    task_id: str
    description: str
    task_type: str
    assigned_agents: List[str] = field(default_factory=list)
    status: str = "pending"
    result: Any = None
    created_at: datetime = field(default_factory=utc_now)
    completed_at: Optional[datetime] = None
    parent_task_id: Optional[str] = None
    sub_tasks: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "description": self.description,
            "task_type": self.task_type,
            "assigned_agents": self.assigned_agents,
            "status": self.status,
            "result": self.result,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "parent_task_id": self.parent_task_id,
            "sub_tasks": self.sub_tasks,
            "metadata": self.metadata,
        }


class CamelAgent:
    output_contract = {
        "type": "json",
        "required_fields": [
            "root_cause",
            "confidence",
            "evidence",
            "actions",
            "open_questions",
            "stance_changed",
            "summary",
        ],
    }

    def __init__(
        self,
        agent_id: str,
        name: str,
        role: AgentRole,
        route_key: str,
        system_message: str,
        capabilities: Optional[List[str]] = None,
        llm_client: Any = None,
        model_name: Optional[str] = None,
    ):
        self.agent_id = agent_id
        self.name = name
        self.role = role
        self.route_key = route_key
        self.system_message = system_message
        self.capabilities = capabilities or []
        self.llm_client = llm_client
        self.model_name = model_name
        self.memory: List[AgentMessage] = []
        self.task_history: List[str] = []
        self.is_busy = False
        self.last_execution_metadata = {
            "llm_attempted": False,
            "llm_used": False,
            "used_fallback": False,
            "fallback_reason": None,
            "response_excerpt": None,
        }

    def receive_message(self, message: AgentMessage) -> None:
        self.memory.append(message)

    async def send_message(
        self,
        content: str,
        message_type: MessageType,
        receiver_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentMessage:
        return AgentMessage(
            message_id=f"MSG_{uuid.uuid4().hex[:8].upper()}",
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            message_type=message_type,
            content=content,
            metadata=metadata or {},
        )

    async def execute_task(self, task: Task, round_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self.is_busy = True
        self.task_history.append(task.task_id)
        started_at = time.perf_counter()
        try:
            payload, metadata = await self._generate_payload(task, round_context or {})
            self.last_execution_metadata = metadata
            return {
                "agent_id": self.agent_id,
                "agent_name": self.name,
                "role": self.role.value,
                "route_key": self.route_key,
                "task_id": task.task_id,
                "status": "completed",
                "output": payload,
                "model_name": self.model_name,
                "llm_attempted": metadata["llm_attempted"],
                "llm_used": metadata["llm_used"],
                "used_fallback": metadata["used_fallback"],
                "fallback_reason": metadata["fallback_reason"],
                "response_excerpt": metadata["response_excerpt"],
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        finally:
            self.is_busy = False

    async def _generate_payload(self, task: Task, round_context: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        if not self.llm_client:
            payload = self._heuristic_payload(task, round_context)
            return payload, {
                "llm_attempted": False,
                "llm_used": False,
                "used_fallback": False,
                "fallback_reason": None,
                "response_excerpt": payload["summary"][:240],
            }

        try:
            response = self._call_llm(task, round_context)
            if inspect.isawaitable(response):
                response = await response
            payload = self._extract_json(response)
            normalized = self._normalize_payload(payload, task, round_context)
            return normalized, {
                "llm_attempted": True,
                "llm_used": True,
                "used_fallback": False,
                "fallback_reason": None,
                "response_excerpt": str(response).strip()[:240],
            }
        except Exception as exc:
            payload = self._heuristic_payload(task, round_context)
            return payload, {
                "llm_attempted": True,
                "llm_used": False,
                "used_fallback": True,
                "fallback_reason": str(exc),
                "response_excerpt": payload["summary"][:240],
            }

    def _call_llm(self, task: Task, round_context: Dict[str, Any]):
        prompt = self._build_prompt(task, round_context)
        if hasattr(self.llm_client, "complete"):
            return self.llm_client.complete(prompt, temperature=0.2, max_tokens=700)
        return self.llm_client.chat(
            [
                {"role": "system", "content": self.system_message},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=700,
        )

    def _build_prompt(self, task: Task, round_context: Dict[str, Any]) -> str:
        previous_summary = round_context.get("previous_summary", "")
        critic_feedback = round_context.get("critic_feedback", [])
        conflict_summary = round_context.get("conflict_summary", "")
        previous_opinions = round_context.get("previous_opinions", [])
        return "\n".join(
            [
                self.system_message,
                f"Role: {self.role.value}",
                f"Task: {task.description}",
                f"Symptoms: {task.metadata.get('symptoms', task.description)}",
                f"SensorData: {json.dumps(task.metadata.get('sensor_data', {}), ensure_ascii=False)}",
                f"GraphRAG: {json.dumps(task.metadata.get('graph_rag_summary', {}), ensure_ascii=False)}",
                f"Round: {round_context.get('round', 1)}",
                f"PreviousSummary: {previous_summary}",
                f"CriticFeedback: {json.dumps(critic_feedback, ensure_ascii=False)}",
                f"ConflictSummary: {conflict_summary}",
                f"PreviousOpinions: {json.dumps(previous_opinions, ensure_ascii=False)[:2500]}",
                "Return exactly one JSON object with fields: root_cause, confidence, evidence, actions, open_questions, stance_changed, summary, blocking, rebuttal_targets.",
            ]
        )

    def _extract_json(self, raw_text: str) -> Dict[str, Any]:
        text = str(raw_text).strip()
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("no json object found")
        payload = json.loads(text[start : end + 1])
        if self.role != AgentRole.COORDINATOR and not payload.get("root_cause"):
            raise ValueError("missing root_cause")
        return payload

    def _normalize_payload(self, payload: Dict[str, Any], task: Task, round_context: Dict[str, Any]) -> Dict[str, Any]:
        heuristic = self._heuristic_payload(task, round_context)
        actions = payload.get("actions", heuristic["actions"])
        evidence = payload.get("evidence", heuristic["evidence"])
        open_questions = payload.get("open_questions", heuristic["open_questions"])
        rebuttal_targets = payload.get("rebuttal_targets", [])
        return {
            "root_cause": str(payload.get("root_cause") or heuristic["root_cause"]),
            "confidence": float(payload.get("confidence", heuristic["confidence"])),
            "evidence": [str(item) for item in (evidence if isinstance(evidence, list) else [evidence])][:4],
            "actions": [str(item) for item in (actions if isinstance(actions, list) else [actions])][:4],
            "open_questions": [str(item) for item in (open_questions if isinstance(open_questions, list) else [open_questions])][:4],
            "stance_changed": bool(payload.get("stance_changed", heuristic["stance_changed"])),
            "summary": str(payload.get("summary") or heuristic["summary"]),
            "blocking": bool(payload.get("blocking", heuristic["blocking"])),
            "rebuttal_targets": [str(item) for item in rebuttal_targets][:4] if isinstance(rebuttal_targets, list) else [],
        }

    def _heuristic_payload(self, task: Task, round_context: Dict[str, Any]) -> Dict[str, Any]:
        symptoms = str(task.metadata.get("symptoms", task.description))
        sensor_data = task.metadata.get("sensor_data", {}) or {}
        graph_summary = task.metadata.get("graph_rag_summary", {}) or {}
        graph_sources = graph_summary.get("sources") or []
        previous_summary = str(round_context.get("previous_summary", ""))
        round_number = int(round_context.get("round", 1))

        if self.role == AgentRole.CRITIC:
            return {
                "root_cause": "current consensus may be over-indexing on aeration while under-checking instrumentation error",
                "confidence": 0.72,
                "evidence": [
                    "The first round still has competing hypotheses across experts.",
                    f"Prior summary: {previous_summary or 'none'}",
                ],
                "actions": [
                    "re-check sensor credibility before irreversible maintenance",
                    "verify whether electrical load swings explain the abnormal blower noise",
                ],
                "open_questions": [
                    "Could DO drift be amplifying the perceived process severity?",
                    "Do recent load changes explain the current draw increase?",
                ],
                "stance_changed": round_number > 1,
                "summary": "The critic recommends challenging the leading aeration hypothesis until sensor credibility and load stability are verified.",
                "blocking": round_number == 1,
                "rebuttal_targets": ["sensor", "electrical"],
            }

        if self.role == AgentRole.COORDINATOR:
            sources = ", ".join(str(item) for item in graph_sources[:2]) or "no similar case hit"
            return {
                "root_cause": "aeration chain degradation remains the most likely primary fault, with sensor verification required in parallel",
                "confidence": 0.79 if round_number == 1 else 0.84,
                "evidence": [
                    "Most experts converge on an aeration or blower-side issue.",
                    f"GraphRAG references: {sources}",
                ],
                "actions": [
                    "inspect blower, bearing, and aeration branch first",
                    "validate DO sensor against manual sample before tuning process settings",
                ],
                "open_questions": [
                    "Is shutdown inspection required for blower internals?",
                    "Is there simultaneous clogging and sensor drift?",
                ],
                "stance_changed": round_number > 1,
                "summary": "The coordinator selects aeration-side degradation as the working root cause and keeps sensor validation as a mandatory parallel check.",
                "blocking": False,
                "rebuttal_targets": [],
            }

        do_value = float(sensor_data.get("do", 0) or 0)
        vibration = float(sensor_data.get("vibration", 0) or 0)
        current = float(sensor_data.get("current", 0) or 0)

        if self.route_key == "mechanical":
            root_cause = "blower bearing wear or local aeration branch clogging"
            evidence = [f"vibration={vibration}", "abnormal blower noise matches rotating equipment degradation"]
            actions = ["inspect blower bearing and rotor balance", "clean aeration branch and diffuser"]
        elif self.route_key == "electrical":
            root_cause = "motor overloading or unstable power quality"
            evidence = [f"current={current}", "abnormal sound may be amplified by unstable drive load"]
            actions = ["verify three-phase current and voltage balance", "check VFD parameters and overload records"]
        elif self.route_key == "process":
            root_cause = "aeration efficiency drop causing insufficient dissolved oxygen"
            evidence = [f"do={do_value}", "symptoms point directly to oxygen transfer degradation"]
            actions = ["verify delivered air volume and basin distribution", "review sludge load and aeration allocation"]
        elif self.route_key == "sensor":
            root_cause = "DO sensor fouling or calibration drift"
            evidence = ["online DO reading must be validated before process correction", "sensor contamination can exaggerate low-DO alarms"]
            actions = ["clean and calibrate the DO probe", "compare online reading with manual sample"]
        else:
            sources = ", ".join(str(item) for item in graph_sources[:2]) or "CASE_20230815_001"
            root_cause = "historical cases suggest aeration-system degradation with intake-side anomalies"
            evidence = [f"matched cases: {sources}", "historical patterns are close to the current symptom bundle"]
            actions = ["replay the aeration-side case handling sequence", "compare recent influent-load changes with similar incidents"]

        return {
            "root_cause": root_cause,
            "confidence": 0.70 if round_number == 1 else 0.76,
            "evidence": evidence,
            "actions": actions,
            "open_questions": [f"symptom snippet: {symptoms[:60]}", "does this align with other experts after debate?"],
            "stance_changed": round_number > 1 and bool(previous_summary),
            "summary": f"{self.name} currently favors '{root_cause}' and recommends '{actions[0]}'.",
            "blocking": False,
            "rebuttal_targets": [],
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "role": self.role.value,
            "route_key": self.route_key,
            "capabilities": self.capabilities,
            "model_name": self.model_name,
            "last_execution_metadata": self.last_execution_metadata,
            "is_busy": self.is_busy,
            "task_count": len(self.task_history),
            "output_contract": self.output_contract,
        }


class CamelSociety:
    def __init__(self, society_id: str, name: str, description: str = ""):
        self.society_id = society_id
        self.name = name
        self.description = description
        self.agents: Dict[str, CamelAgent] = {}
        self.tasks: Dict[str, Task] = {}
        self.message_bus: List[AgentMessage] = []
        self.max_rounds = 3

    def register_agent(self, agent: CamelAgent) -> None:
        self.agents[agent.agent_id] = agent

    async def create_task(
        self,
        description: str,
        task_type: str,
        agent_ids: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Task:
        task = Task(
            task_id=f"TASK_{uuid.uuid4().hex[:8].upper()}",
            description=description,
            task_type=task_type,
            assigned_agents=agent_ids or list(self.agents.keys()),
            metadata=metadata or {},
        )
        self.tasks[task.task_id] = task
        return task

    def _emit_message(self, message: AgentMessage) -> None:
        self.message_bus.append(message)
        if message.receiver_id and message.receiver_id in self.agents:
            self.agents[message.receiver_id].receive_message(message)
            return
        for agent in self.agents.values():
            if agent.agent_id != message.sender_id:
                agent.receive_message(message)

    async def execute_collaborative_task(
        self,
        task: Task,
        mode: str = "parallel",
        trace_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        if mode == "parallel":
            return await self._parallel(task)
        if mode == "sequential":
            return await self._sequential(task)
        if mode == "debate":
            return await self._debate(task, trace_callback=trace_callback)
        raise ValueError(f"unknown mode: {mode}")

    async def _parallel(self, task: Task) -> Dict[str, Any]:
        results = await asyncio.gather(
            *[self.agents[agent_id].execute_task(task, {"round": 1}) for agent_id in task.assigned_agents if agent_id in self.agents]
        )
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.result = {"mode": "parallel", "results": results}
        return task.result

    async def _sequential(self, task: Task) -> Dict[str, Any]:
        results = []
        previous_summary = ""
        for agent_id in task.assigned_agents:
            agent = self.agents.get(agent_id)
            if not agent:
                continue
            result = await agent.execute_task(task, {"round": 1, "previous_summary": previous_summary})
            previous_summary = result["output"]["summary"]
            results.append(result)
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.result = {"mode": "sequential", "results": results}
        return task.result

    async def _debate(self, task: Task, trace_callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> Dict[str, Any]:
        def emit(stage: str, message: str, **extra: Any) -> None:
            if trace_callback:
                trace_callback({"stage": stage, "message": message, "timestamp": datetime.now(timezone.utc).isoformat(), **extra})

        expert_agents = [agent for agent in self.agents.values() if agent.role == AgentRole.EXPERT]
        critic = next((agent for agent in self.agents.values() if agent.role == AgentRole.CRITIC), None)
        coordinator = next((agent for agent in self.agents.values() if agent.role == AgentRole.COORDINATOR), None)
        round_outputs: List[Dict[str, Any]] = []
        message_count_start = len(self.message_bus)
        previous_summary = ""
        critic_feedback: List[str] = []
        conflict_summary = ""
        degraded_mode = False
        final_decision: Dict[str, Any] = {}

        emit("debate_started", "CAMEL debate started", progress=10, round=1)
        for round_number in range(1, self.max_rounds + 1):
            emit(
                "debate_round_started",
                f"Starting CAMEL reasoning round {round_number}",
                progress=15 + round_number * 15,
                round=round_number,
            )
            if round_number < 3:
                target_experts = expert_agents
            else:
                target_experts = self._select_conflicted_experts(expert_agents, round_outputs[-1] if round_outputs else None) or expert_agents

            prior_opinions = [item["output"] for item in round_outputs[-1]["opinions"]] if round_outputs else []
            expert_results: List[Dict[str, Any]] = []
            for agent in target_experts:
                emit(
                    "debate_agent_started",
                    f"{agent.name} started round {round_number}",
                    progress=20 + round_number * 15,
                    round=round_number,
                    agent_id=agent.agent_id,
                    agent_name=agent.name,
                    agent_key=agent.route_key,
                )
                result = await agent.execute_task(
                    task,
                    {
                        "round": round_number,
                        "previous_summary": previous_summary,
                        "critic_feedback": critic_feedback,
                        "conflict_summary": conflict_summary,
                        "previous_opinions": prior_opinions,
                    },
                )
                degraded_mode = degraded_mode or result.get("used_fallback", False)
                expert_results.append(result)
                self._emit_message(
                    await agent.send_message(
                        json.dumps(result["output"], ensure_ascii=False),
                        MessageType.OPINION,
                        metadata={"round": round_number, "task_id": task.task_id},
                    )
                )
                emit(
                    "debate_agent_completed",
                    f"{agent.name} finished round {round_number}",
                    progress=28 + round_number * 15,
                    round=round_number,
                    agent_id=agent.agent_id,
                    agent_name=agent.name,
                    agent_key=agent.route_key,
                    duration_ms=result["duration_ms"],
                    used_fallback=result.get("used_fallback", False),
                )

            consensus = self._build_consensus_summary(expert_results)
            conflict_matrix = self._build_conflict_matrix(expert_results)
            conflict_summary = self._build_conflict_summary(conflict_matrix)

            critic_result = None
            if critic:
                critic_result = await critic.execute_task(
                    task,
                    {
                        "round": round_number,
                        "previous_summary": previous_summary,
                        "previous_opinions": [item["output"] for item in expert_results],
                        "conflict_summary": conflict_summary,
                    },
                )
                degraded_mode = degraded_mode or critic_result.get("used_fallback", False)
                critic_feedback = critic_result["output"].get("open_questions", [])
                self._emit_message(
                    await critic.send_message(
                        json.dumps(critic_result["output"], ensure_ascii=False),
                        MessageType.DEBATE,
                        metadata={"round": round_number, "task_id": task.task_id},
                    )
                )

            coordinator_result = None
            if coordinator:
                coordinator_result = await coordinator.execute_task(
                    task,
                    {
                        "round": round_number,
                        "previous_summary": previous_summary,
                        "previous_opinions": [item["output"] for item in expert_results],
                        "critic_feedback": critic_feedback,
                        "conflict_summary": conflict_summary,
                    },
                )
                degraded_mode = degraded_mode or coordinator_result.get("used_fallback", False)
                previous_summary = coordinator_result["output"]["summary"]
                final_decision = coordinator_result["output"]
                self._emit_message(
                    await coordinator.send_message(
                        json.dumps(coordinator_result["output"], ensure_ascii=False),
                        MessageType.CONSENSUS,
                        metadata={"round": round_number, "task_id": task.task_id},
                    )
                )
            elif not previous_summary:
                previous_summary = consensus["summary"]

            round_outputs.append(
                {
                    "round": round_number,
                    "opinions": expert_results,
                    "critic": critic_result,
                    "coordinator": coordinator_result,
                    "summary": previous_summary or consensus["summary"],
                    "consensus": consensus,
                    "conflict_matrix": conflict_matrix,
                }
            )
            emit(
                "debate_round_completed",
                f"Completed CAMEL reasoning round {round_number}",
                progress=38 + round_number * 15,
                round=round_number,
                degraded_mode=degraded_mode,
            )

            if round_number >= 2 and consensus["consensus_level"] >= 0.8 and not (critic_result and critic_result["output"].get("blocking")):
                break

        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.result = {
            "mode": "debate",
            "rounds": len(round_outputs),
            "opinions": round_outputs[-1]["opinions"] if round_outputs else [],
            "round_outputs": round_outputs,
            "round_summaries": [
                {
                    "round": item["round"],
                    "summary": item["summary"],
                    "consensus": item["consensus"],
                    "conflict_count": len(item["conflict_matrix"]),
                }
                for item in round_outputs
            ],
            "conflict_matrix": round_outputs[-1]["conflict_matrix"] if round_outputs else [],
            "final_decision": final_decision,
            "consensus_summary": round_outputs[-1]["consensus"] if round_outputs else {"summary": "No consensus", "participants": 0, "consensus_level": 0},
            "message_count": len(self.message_bus) - message_count_start,
            "agent_model_map": {agent_id: self.agents[agent_id].to_dict() for agent_id in self.agents},
            "fallback_summary": self._build_fallback_summary(round_outputs),
            "latency_breakdown": self._build_latency_breakdown(round_outputs),
            "degraded_mode": degraded_mode,
        }
        if task.metadata.get("debug"):
            task.result["debug"] = self._build_debug_payload(task, round_outputs)
        emit("debate_completed", "CAMEL debate completed", progress=100, round=len(round_outputs), degraded_mode=degraded_mode)
        return task.result

    def _select_conflicted_experts(self, experts: List[CamelAgent], previous_round: Optional[Dict[str, Any]]) -> List[CamelAgent]:
        if not previous_round:
            return experts
        conflicted = {
            item["expert_key"]
            for item in previous_round.get("conflict_matrix", [])
            if item.get("severity") in {"high", "medium"}
        }
        return [agent for agent in experts if agent.route_key in conflicted]

    def _build_consensus_summary(self, expert_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in expert_results:
            grouped.setdefault(self._normalize_cause(item["output"]["root_cause"]), []).append(item)
        best_group = max(grouped.values(), key=len) if grouped else []
        best_cause = best_group[0]["output"]["root_cause"] if best_group else "No consensus"
        confidence = round(sum(result["output"]["confidence"] for result in best_group) / len(best_group), 2) if best_group else 0.0
        consensus_level = round(len(best_group) / max(len(expert_results), 1), 2)
        return {
            "summary": f"Current leading root cause: {best_cause}",
            "participants": len(expert_results),
            "confidence": confidence,
            "consensus_level": consensus_level,
            "leading_root_cause": best_cause,
        }

    def _build_conflict_matrix(self, expert_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in expert_results:
            grouped.setdefault(self._normalize_cause(item["output"]["root_cause"]), []).append(item)
        if len(grouped) <= 1:
            return []
        matrix: List[Dict[str, Any]] = []
        for items in grouped.values():
            matrix.append(
                {
                    "expert_key": items[0]["route_key"],
                    "root_cause": items[0]["output"]["root_cause"],
                    "supporters": [item["agent_name"] for item in items],
                    "severity": "high" if len(items) == 1 else "medium",
                }
            )
        return matrix

    def _build_conflict_summary(self, conflict_matrix: List[Dict[str, Any]]) -> str:
        if not conflict_matrix:
            return "No meaningful conflict remains."
        parts = [
            f"{item['root_cause']} (supporters: {', '.join(item['supporters'])})"
            for item in conflict_matrix[:3]
        ]
        return "; ".join(parts)

    def _build_fallback_summary(self, round_outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        for round_output in round_outputs:
            result_candidates = round_output["opinions"] + [item for item in [round_output.get("critic"), round_output.get("coordinator")] if item]
            for result in result_candidates:
                summary[result["agent_id"]] = {
                    "agent_name": result["agent_name"],
                    "used_fallback": result.get("used_fallback", False),
                    "fallback_reason": result.get("fallback_reason"),
                    "llm_attempted": result.get("llm_attempted", False),
                    "llm_used": result.get("llm_used", False),
                    "model_name": result.get("model_name"),
                }
        return summary

    def _build_latency_breakdown(self, round_outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        per_round = []
        for round_output in round_outputs:
            total = sum(item.get("duration_ms", 0) for item in round_output["opinions"])
            if round_output.get("critic"):
                total += round_output["critic"].get("duration_ms", 0)
            if round_output.get("coordinator"):
                total += round_output["coordinator"].get("duration_ms", 0)
            per_round.append({"round": round_output["round"], "duration_ms": round(total, 2)})
        return {
            "rounds": per_round,
            "total_duration_ms": round(sum(item["duration_ms"] for item in per_round), 2),
        }

    def _build_debug_payload(self, task: Task, round_outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "task_metadata": task.metadata,
            "rounds": round_outputs,
            "agents": {
                agent_id: {
                    "agent_name": agent.name,
                    "model_name": agent.model_name,
                    "last_execution_metadata": agent.last_execution_metadata,
                }
                for agent_id, agent in self.agents.items()
            },
        }

    def _normalize_cause(self, text: str) -> str:
        normalized = str(text).lower().replace(" ", "")
        if any(word in normalized for word in ["aeration", "oxygen", "blower", "diffuser"]):
            return "aeration"
        if any(word in normalized for word in ["sensor", "probe", "calibration", "drift", "do"]):
            return "sensor"
        if any(word in normalized for word in ["motor", "power", "electrical", "current"]):
            return "electrical"
        return normalized[:24]

    def get_society_status(self) -> Dict[str, Any]:
        return {
            "society_id": self.society_id,
            "name": self.name,
            "description": self.description,
            "agent_count": len(self.agents),
            "task_count": len(self.tasks),
            "active_tasks": sum(1 for task in self.tasks.values() if task.status == "running"),
            "agents": [agent.to_dict() for agent in self.agents.values()],
            "recent_messages": len(self.message_bus),
            "max_rounds": self.max_rounds,
        }


class IndustrialDiagnosisSociety(CamelSociety):
    def __init__(self, model_router: Optional[AgentModelRouter] = None, enable_model_routing: Optional[bool] = None):
        super().__init__(
            "industrial_diagnosis_001",
            "Industrial Diagnosis CAMEL Society",
            "Cross-domain experts collaborate to diagnose industrial faults.",
        )
        self.model_router = model_router or AgentModelRouter()
        self.enable_model_routing = enable_model_routing if enable_model_routing is not None else bool(self.model_router and self.model_router.enabled)
        self._init_agents()

    def _get_client(self, route_key: str):
        return self.model_router.get_client(route_key) if self.enable_model_routing and self.model_router else None

    def _get_model(self, route_key: str) -> Optional[str]:
        profile = self.model_router.get_profile(route_key) if self.enable_model_routing and self.model_router else None
        return profile.model if profile else None

    def _init_agents(self) -> None:
        agent_specs = [
            (
                "EXP_MECH_001",
                "Mechanical Expert",
                AgentRole.EXPERT,
                "mechanical",
                "You are a mechanical diagnostics expert focused on blowers, rotating equipment, bearings, and aeration hardware.",
                ["vibration analysis", "bearing inspection", "rotating equipment diagnostics"],
            ),
            (
                "EXP_ELEC_001",
                "Electrical Expert",
                AgentRole.EXPERT,
                "electrical",
                "You are an electrical diagnostics expert focused on motors, drives, power quality, and overload behavior.",
                ["motor diagnostics", "power quality review", "drive tuning"],
            ),
            (
                "EXP_PROC_001",
                "Process Expert",
                AgentRole.EXPERT,
                "process",
                "You are a wastewater-process expert focused on oxygen transfer, basin operation, sludge load, and process stability.",
                ["process optimization", "air allocation", "load analysis"],
            ),
            (
                "EXP_SENSOR_001",
                "Instrumentation Expert",
                AgentRole.EXPERT,
                "sensor",
                "You are an instrumentation expert focused on DO probes, calibration, drift, and measurement credibility.",
                ["instrument calibration", "sensor drift review", "signal validation"],
            ),
            (
                "EXP_HIST_001",
                "Historical Case Expert",
                AgentRole.EXPERT,
                "historical",
                "You are a historical-case expert focused on similar incidents, GraphRAG retrieval, and recovery playbooks.",
                ["case matching", "knowledge retrieval", "postmortem reasoning"],
            ),
            (
                "CRITIC_001",
                "Critic",
                AgentRole.CRITIC,
                "critic",
                "You are a critical reviewer. Challenge weak evidence, expose hidden risk, and block premature conclusions.",
                ["risk review", "counterfactual reasoning", "evidence challenge"],
            ),
            (
                "COORD_001",
                "Coordinator",
                AgentRole.COORDINATOR,
                "coordinator",
                "You are the coordinator. Integrate expert opinions, resolve conflicts, and produce the final operational decision.",
                ["synthesis", "conflict resolution", "final decision making"],
            ),
        ]
        for agent_id, name, role, route_key, system_message, capabilities in agent_specs:
            self.register_agent(
                CamelAgent(
                    agent_id=agent_id,
                    name=name,
                    role=role,
                    route_key=route_key,
                    system_message=system_message,
                    capabilities=capabilities,
                    llm_client=self._get_client(route_key),
                    model_name=self._get_model(route_key),
                )
            )

    async def diagnose(
        self,
        symptoms: str,
        sensor_data: Dict[str, Any],
        debug: bool = False,
        trace_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        graph_payload = await self._prepare_graph_context(symptoms, sensor_data, trace_callback)
        task = await self.create_task(
            f"Diagnose fault: {symptoms}",
            "fault_diagnosis",
            metadata={
                "symptoms": symptoms,
                "sensor_data": sensor_data,
                "debug": debug,
                "graph_rag_summary": graph_payload["summary"],
                "graph_rag_query": graph_payload["query"],
            },
        )
        result = await self.execute_collaborative_task(task, mode="debate", trace_callback=trace_callback)
        return {
            "diagnosis_id": task.task_id,
            "symptoms": symptoms,
            "collaboration_result": result,
            "expert_count": len([agent for agent in self.agents.values() if agent.role == AgentRole.EXPERT]),
            "society": self.name,
            "graph_rag": graph_payload,
        }

    async def _prepare_graph_context(
        self,
        symptoms: str,
        sensor_data: Dict[str, Any],
        trace_callback: Optional[Callable[[Dict[str, Any]], None]],
    ) -> Dict[str, Any]:
        if trace_callback:
            trace_callback(
                {
                    "stage": "graph_rag_started",
                    "message": "CAMEL GraphRAG retrieval started",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "progress": 8,
                }
            )
        query = f"{symptoms} | sensor={json.dumps(sensor_data, ensure_ascii=False)}"
        try:
            result = await graph_rag.query(query)
            summary = {
                "sources": (result.get("sources") or [])[:4],
                "answer_excerpt": str(result.get("answer", ""))[:180],
                "degraded": False,
            }
            if trace_callback:
                trace_callback(
                    {
                        "stage": "graph_rag_completed",
                        "message": "CAMEL GraphRAG retrieval completed",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "progress": 12,
                        "source_count": len(summary["sources"]),
                    }
                )
            return {"query": query, "summary": summary}
        except Exception as exc:
            logger.warning(f"camel graph rag failed: {exc}")
            summary = {
                "sources": ["CASE_20230815_001", "CASE_20231022_003"],
                "answer_excerpt": "",
                "error": str(exc),
                "degraded": True,
            }
            if trace_callback:
                trace_callback(
                    {
                        "stage": "graph_rag_failed",
                        "message": "CAMEL GraphRAG retrieval failed, using fallback references",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "progress": 12,
                    }
                )
            return {"query": query, "summary": summary}
