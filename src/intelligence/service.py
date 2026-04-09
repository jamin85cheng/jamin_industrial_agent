"""Industrial intelligence service for patrol, review, and learning loops."""

from __future__ import annotations

import asyncio
import json
import math
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

from src.intelligence.models import Area, AssetDefinition, Line, Plant, PointDefinition, serialize_value, utc_now
from src.intelligence.repository import IntelligenceRepository
from src.tasks.task_tracker import TaskPriority, TaskTracker, TrackedTask, task_tracker
from src.utils.config import load_config
from src.utils.structured_logging import get_logger

logger = get_logger("intelligence.service")

_RISK_ORDER = {"normal": 0, "attention": 1, "warning": 2, "high_risk": 3, "needs_review": 4}

_DEFAULT_INTELLIGENCE_CONFIG: Dict[str, Any] = {
    "default_scene": "dust",
    "snapshot_history_limit": 36,
    "stale_after_seconds": 600,
    "knowledge_top_k": 4,
    "patrol": {
        "interval_seconds": 180,
        "run_on_startup": False,
    },
    "learning": {
        "min_confirmed_labels_for_candidate": 3,
        "min_confirmed_labels_for_model_candidate": 12,
        "min_external_grounding_ratio": 0.30,
    },
    "llm": {
        "enabled": False,
        "temperature": 0.1,
        "max_tokens": 450,
    },
}


class PatrolLLMUnavailableError(RuntimeError):
    """Raised when optional LLM synthesis is not available."""


class OpenAICompatiblePatrolLLM:
    """Optional local/private LLM client for patrol summary synthesis."""

    def __init__(self, config: Dict[str, Any]):
        llm_config = config.get("llm", {})
        intelligence_config = config.get("intelligence", {}).get("llm", {})
        legacy_endpoint = llm_config.get("openai_api", {})

        self.enabled = bool(intelligence_config.get("enabled", False))
        self.base_url = str(intelligence_config.get("base_url") or legacy_endpoint.get("base_url") or "").rstrip("/")
        self.api_key = str(intelligence_config.get("api_key") or legacy_endpoint.get("api_key") or "")
        self.model = str(intelligence_config.get("model") or legacy_endpoint.get("model") or "Qwen3.5-9B")
        self.timeout_seconds = float(intelligence_config.get("timeout_seconds", 20))

    async def summarize(self, payload: Dict[str, Any], *, temperature: float, max_tokens: int) -> Dict[str, Any]:
        if not self.enabled or not self.base_url:
            raise PatrolLLMUnavailableError("Patrol LLM synthesis is disabled.")

        prompt = "\n".join(
            [
                "You are an industrial patrol copilot. Return strict JSON only.",
                "Required keys: summary, operator_actions, suspected_faults.",
                "operator_actions must be an array of strings. suspected_faults must be an array of strings.",
                json.dumps(payload, ensure_ascii=False),
            ]
        )

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json={
                        "model": self.model,
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                        "messages": [
                            {"role": "system", "content": "Return compact JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                    },
                )
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, httpx.TimeoutException, json.JSONDecodeError) as exc:
                raise PatrolLLMUnavailableError(str(exc)) from exc

        choices = data.get("choices") or []
        if not choices:
            raise PatrolLLMUnavailableError("No LLM choices returned.")

        message = choices[0].get("message", {}) or {}
        content = str(message.get("content") or choices[0].get("text") or "").strip()
        if not content:
            raise PatrolLLMUnavailableError("Empty LLM response.")

        cleaned = content.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        parsed = json.loads(cleaned)
        return {
            "summary": str(parsed.get("summary") or "").strip(),
            "operator_actions": [
                str(item).strip()
                for item in (parsed.get("operator_actions") or [])
                if str(item).strip()
            ],
            "suspected_faults": [
                str(item).strip()
                for item in (parsed.get("suspected_faults") or [])
                if str(item).strip()
            ],
        }


def _deep_merge(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class IndustrialIntelligenceService:
    """Main orchestration service for intelligent patrol and feedback learning."""

    def __init__(
        self,
        *,
        config: Optional[Dict[str, Any]] = None,
        repository: Optional[IntelligenceRepository] = None,
        tracker: Optional[TaskTracker] = None,
    ):
        self.config = config or load_config()
        self.runtime_config = _deep_merge(
            _DEFAULT_INTELLIGENCE_CONFIG,
            self.config.get("intelligence", {}),
        )
        self.repository = repository or IntelligenceRepository(self.config.get("database", {}))
        self.repository.ensure_ready()
        self.tracker = tracker or task_tracker
        self.llm_client = OpenAICompatiblePatrolLLM(self.config)
        self.plants, self.areas, self.lines, self.assets = self._build_catalog()
        self.assets_by_id = {asset.asset_id: asset for asset in self.assets}
        self._seed_default_knowledge_cases()

    def _build_catalog(self):
        plant = Plant("PLANT_ENV_001", "环保处理示范厂", "一期以单厂粉尘处理场景为主。")
        area = Area("AREA_DUST_001", plant.plant_id, "粉尘治理车间")
        line = Line("LINE_BAGHOUSE_001", area.area_id, "布袋除尘一线")

        points = [
            PointDefinition("pressure_diff_kpa", "压差", "kPa", "float", high_limit=1.8, low_limit=0.3),
            PointDefinition("fan_current_a", "风机电流", "A", "float", high_limit=42.0, low_limit=18.0),
            PointDefinition("airflow_m3h", "风量", "m3/h", "float", high_limit=16000.0, low_limit=7800.0),
            PointDefinition("dust_concentration_mg_m3", "出口粉尘浓度", "mg/m3", "float", high_limit=20.0, low_limit=0.0),
            PointDefinition("cleaning_frequency_hz", "清灰频率", "Hz", "float", high_limit=1.6, low_limit=0.1),
            PointDefinition("valve_state", "阀门状态", "state", "string", required=False),
            PointDefinition("temperature_c", "温度", "C", "float", high_limit=90.0, low_limit=0.0),
            PointDefinition("running_state", "运行状态", "bool", "boolean", required=False),
        ]

        asset = AssetDefinition(
            asset_id="ASSET_DUST_COLLECTOR_01",
            line_id=line.line_id,
            area_id=area.area_id,
            plant_id=plant.plant_id,
            scene_type="dust",
            name="1# 布袋除尘器",
            description="一期重点闭环设备。",
            points=points,
        )
        return [plant], [area], [line], [asset]

    def list_assets(self) -> Dict[str, Any]:
        return {
            "plants": [item.to_dict() for item in self.plants],
            "areas": [item.to_dict() for item in self.areas],
            "lines": [item.to_dict() for item in self.lines],
            "assets": [item.to_dict() for item in self.assets],
        }

    def get_runtime_summary(self) -> Dict[str, Any]:
        patrol_runs = self.repository.list_patrol_runs(limit=1)
        labels = self.repository.list_labels(limit=200)
        confirmed_labels = [item for item in labels if item["status"] == "confirmed"]
        pending_labels = [item for item in labels if item["status"] == "pending"]
        knowledge_cases = self.repository.list_knowledge_cases(limit=200)
        candidates = self.repository.list_learning_candidates(limit=200)

        return {
            "default_scene": self.runtime_config["default_scene"],
            "patrol_interval_seconds": int(self.runtime_config["patrol"]["interval_seconds"]),
            "assets": len(self.assets),
            "latest_run_id": patrol_runs[0]["run_id"] if patrol_runs else None,
            "pending_review_count": len(pending_labels),
            "confirmed_label_count": len(confirmed_labels),
            "knowledge_case_count": len(knowledge_cases),
            "candidate_count": len(candidates),
        }

    def ingest_snapshot(
        self,
        *,
        asset_id: str,
        points: Dict[str, Dict[str, Any]],
        source: str = "plc",
    ) -> Dict[str, Any]:
        asset = self._require_asset(asset_id)
        existing = self.repository.get_snapshot(asset_id) or {}
        existing_points = dict(existing.get("points") or {})
        history_limit = int(self.runtime_config["snapshot_history_limit"])
        now = utc_now()

        normalized_points: Dict[str, Dict[str, Any]] = {}
        for definition in asset.points:
            incoming = points.get(definition.point_id)
            previous = existing_points.get(definition.point_id)
            if incoming is None and previous is not None:
                normalized_points[definition.point_id] = previous
                continue

            if incoming is None:
                continue

            timestamp = incoming.get("timestamp") or now
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            value = incoming.get("value")
            history = list((previous or {}).get("history") or [])
            numeric_value = self._coerce_float(value)
            if numeric_value is not None:
                history.append(round(numeric_value, 4))
            history = history[-history_limit:]

            normalized_points[definition.point_id] = {
                "point_id": definition.point_id,
                "display_name": definition.display_name,
                "value": value,
                "unit": incoming.get("unit") or definition.unit,
                "quality": incoming.get("quality") or "good",
                "timestamp": timestamp.isoformat(),
                "history": history,
                "point_type": definition.point_type,
            }

        completeness = round(len(normalized_points) / max(len(asset.points), 1), 3)
        snapshot = {
            "snapshot_id": f"SNAP_{uuid.uuid4().hex[:12].upper()}",
            "asset_id": asset.asset_id,
            "scene_type": asset.scene_type,
            "collected_at": now.isoformat(),
            "source": source,
            "plant_id": asset.plant_id,
            "area_id": asset.area_id,
            "line_id": asset.line_id,
            "points": normalized_points,
            "completeness": completeness,
        }
        return self.repository.upsert_snapshot(snapshot)

    def get_latest_snapshots(self, asset_ids: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        snapshots = self.repository.list_snapshots(asset_ids)
        for snapshot in snapshots:
            snapshot["asset_name"] = self.assets_by_id.get(snapshot["asset_id"]).name if snapshot.get("asset_id") in self.assets_by_id else snapshot.get("asset_id")
        return snapshots

    async def seed_demo_snapshot(
        self,
        *,
        asset_id: str = "ASSET_DUST_COLLECTOR_01",
        profile: str = "warning",
        run_patrol: bool = False,
        triggered_by: str = "demo",
    ) -> Dict[str, Any]:
        asset = self._require_asset(asset_id)
        now = utc_now()
        phase = now.timestamp() / 240.0

        if profile == "normal":
            values = {
                "pressure_diff_kpa": 1.15 + math.sin(phase) * 0.08,
                "fan_current_a": 26.5 + math.cos(phase * 1.3) * 1.6,
                "airflow_m3h": 10400 + math.sin(phase * 0.9) * 320,
                "dust_concentration_mg_m3": 8.2 + abs(math.sin(phase * 1.1)) * 1.8,
                "cleaning_frequency_hz": 0.78 + math.cos(phase * 0.7) * 0.06,
                "valve_state": "open",
                "temperature_c": 63 + math.sin(phase * 0.8) * 3.2,
                "running_state": True,
            }
        elif profile == "critical":
            values = {
                "pressure_diff_kpa": 2.25 + abs(math.sin(phase)) * 0.12,
                "fan_current_a": 17.4 + math.cos(phase * 1.4) * 0.9,
                "airflow_m3h": 6900 + math.sin(phase * 0.8) * 220,
                "dust_concentration_mg_m3": 29.0 + abs(math.sin(phase * 1.1)) * 2.4,
                "cleaning_frequency_hz": 1.85 + abs(math.cos(phase * 0.9)) * 0.12,
                "valve_state": "closed",
                "temperature_c": 95 + abs(math.sin(phase * 0.6)) * 4.5,
                "running_state": True,
            }
        else:
            values = {
                "pressure_diff_kpa": 1.72 + abs(math.sin(phase)) * 0.1,
                "fan_current_a": 19.2 + math.cos(phase * 1.2) * 1.2,
                "airflow_m3h": 8050 + math.sin(phase * 0.8) * 240,
                "dust_concentration_mg_m3": 18.5 + abs(math.sin(phase * 1.05)) * 1.8,
                "cleaning_frequency_hz": 1.38 + abs(math.cos(phase * 0.75)) * 0.11,
                "valve_state": "partial",
                "temperature_c": 82 + abs(math.sin(phase * 0.55)) * 4.0,
                "running_state": True,
            }

        point_payload = {
            point.point_id: {
                "value": round(values[point.point_id], 3) if isinstance(values[point.point_id], (int, float)) else values[point.point_id],
                "unit": point.unit,
                "quality": "good",
                "timestamp": now.isoformat(),
            }
            for point in asset.points
        }

        snapshot = self.ingest_snapshot(
            asset_id=asset_id,
            points=point_payload,
            source=f"demo:{profile}",
        )
        patrol_result = None
        if run_patrol:
            patrol_result = await self.run_patrol(
                asset_ids=[asset_id],
                triggered_by=triggered_by,
                schedule_type="manual",
            )

        return {
            "profile": profile,
            "snapshot": snapshot,
            "patrol_run": patrol_result,
        }

    def list_patrol_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self.repository.list_patrol_runs(limit=limit)

    def get_patrol_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self.repository.get_patrol_run(run_id)

    def list_review_queue(self, status: str = "pending", limit: int = 50) -> List[Dict[str, Any]]:
        labels = self.repository.list_labels(status=status, limit=limit)
        patrol_runs = {item["run_id"]: item for item in self.repository.list_patrol_runs(limit=200)}
        for label in labels:
            run_payload = patrol_runs.get(label["run_id"], {})
            asset_result = self._find_asset_result(run_payload, label["asset_id"])
            label["asset_name"] = self.assets_by_id.get(label["asset_id"]).name if label.get("asset_id") in self.assets_by_id else label.get("asset_id")
            label["current_assessment"] = asset_result
        return labels

    def list_knowledge_cases(self, scene_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return self.repository.list_knowledge_cases(scene_type=scene_type, limit=limit)

    def get_knowledge_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        return self.repository.get_knowledge_case(case_id)

    def search_knowledge_cases(self, query: str, *, scene_type: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
        return self.repository.search_knowledge_cases(query, scene_type=scene_type, top_k=limit)

    def record_knowledge_activity(
        self,
        *,
        tenant_id: str,
        event_type: str,
        actor_id: Optional[str] = None,
        query: Optional[str] = None,
        scene_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.repository.record_knowledge_activity(
            tenant_id=tenant_id,
            event_type=event_type,
            actor_id=actor_id,
            query=query,
            scene_type=scene_type,
            resource_id=resource_id,
            metadata=metadata,
        )

    def record_knowledge_feedback(
        self,
        *,
        diagnosis_id: str,
        tenant_id: str,
        helpful: bool,
        created_by: Optional[str],
        comment: Optional[str] = None,
        reference_case_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return self.repository.create_knowledge_feedback(
            diagnosis_id=diagnosis_id,
            tenant_id=tenant_id,
            helpful=helpful,
            created_by=created_by,
            comment=comment,
            reference_case_ids=reference_case_ids,
        )

    def get_knowledge_statistics(self, *, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        return self.repository.get_knowledge_statistics(tenant_id=tenant_id)

    def list_learning_candidates(self, candidate_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return self.repository.list_learning_candidates(candidate_type=candidate_type, limit=limit)

    async def run_patrol(
        self,
        *,
        asset_ids: Optional[List[str]] = None,
        triggered_by: str = "manual",
        schedule_type: str = "manual",
    ) -> Dict[str, Any]:
        selected_assets = asset_ids or [asset.asset_id for asset in self.assets]
        task = self.tracker.create_task(
            task_type="intelligent_patrol",
            description=f"Industrial patrol for {len(selected_assets)} assets",
            priority=TaskPriority.HIGH if schedule_type == "scheduled" else TaskPriority.NORMAL,
            metadata={
                "asset_ids": selected_assets,
                "scene_type": self.runtime_config["default_scene"],
                "triggered_by": triggered_by,
                "schedule_type": schedule_type,
                "timeout_seconds": 600,
            },
        )
        result = await self.tracker.execute(
            task,
            self._execute_patrol_task,
            selected_assets,
            triggered_by,
            schedule_type,
        )
        result["task_id"] = task.task_id
        return result

    async def confirm_label(
        self,
        label_id: str,
        *,
        reviewer: str,
        anomaly_type: Optional[str] = None,
        root_cause: Optional[str] = None,
        review_notes: str = "",
        final_action: str = "",
        false_positive: bool = False,
    ) -> Dict[str, Any]:
        label = self.repository.get_label(label_id)
        if not label:
            raise KeyError(f"Unknown label: {label_id}")

        next_status = "rejected" if false_positive else "confirmed"
        updated_label = self.repository.update_label(
            label_id,
            {
                "status": next_status,
                "anomaly_type": anomaly_type or label.get("anomaly_type"),
                "root_cause": root_cause or label.get("root_cause"),
                "updated_at": utc_now(),
                "review": {
                    **dict(label.get("review") or {}),
                    "reviewer": reviewer,
                    "review_notes": review_notes,
                    "final_action": final_action,
                    "false_positive": false_positive,
                    "reviewed_at": utc_now().isoformat(),
                },
            },
        )
        if not updated_label:
            raise RuntimeError(f"Failed to update label {label_id}")

        created_case = None
        if next_status == "confirmed":
            created_case = self._promote_label_to_case(updated_label)

        return {
            "label": serialize_value(updated_label),
            "knowledge_case": created_case,
        }

    async def reject_label(
        self,
        label_id: str,
        *,
        reviewer: str,
        review_notes: str = "",
    ) -> Dict[str, Any]:
        return await self.confirm_label(
            label_id,
            reviewer=reviewer,
            review_notes=review_notes,
            false_positive=True,
        )

    def generate_learning_candidates(
        self,
        *,
        requested_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        requested = set(requested_types or ["workflow", "prompt", "model"])
        labels = self.repository.list_labels(limit=500)
        confirmed_labels = [item for item in labels if item["status"] == "confirmed"]
        abnormal_runs = [
            item
            for item in self.repository.list_patrol_runs(limit=200)
            if item.get("status") != "normal"
        ]
        knowledge_cases = self.repository.list_knowledge_cases(limit=500)

        grounding_values = []
        for run_payload in abnormal_runs:
            for asset_result in run_payload.get("asset_results", []):
                if asset_result.get("status") == "normal":
                    continue
                grounding_values.append(float(asset_result.get("knowledge_grounding_ratio", 0.0)))
        avg_grounding = sum(grounding_values) / len(grounding_values) if grounding_values else 0.0

        candidates: List[Dict[str, Any]] = []
        min_candidate = int(self.runtime_config["learning"]["min_confirmed_labels_for_candidate"])
        min_model = int(self.runtime_config["learning"]["min_confirmed_labels_for_model_candidate"])
        min_grounding = float(self.runtime_config["learning"]["min_external_grounding_ratio"])
        holdout_score = round(_clamp(0.52 + len(confirmed_labels) * 0.035, 0.52, 0.92), 3)

        if "workflow" in requested and len(confirmed_labels) >= min_candidate:
            candidates.append(
                self._store_candidate(
                    candidate_type="workflow",
                    name="Dust Patrol Multi-Agent Workflow",
                    score=round((avg_grounding + holdout_score) / 2, 3),
                    status="ready_for_shadow" if avg_grounding >= min_grounding and holdout_score >= 0.60 else "draft",
                    rationale="Confirmed labels now cover enough abnormal patterns to optimize the patrol workflow topology.",
                    payload={
                        "source_projects": [
                            "self-evolving-AI-Agents",
                            "Self-Improving-AI-Agents",
                            "jamin_evolve_agent",
                        ],
                        "agents": [
                            {"id": "data-parser", "role": "Normalize PLC telemetry and validate completeness"},
                            {"id": "anomaly-detector", "role": "Detect threshold, rate, and correlation anomalies"},
                            {"id": "root-cause-analyzer", "role": "Fuse rules, history, and case evidence"},
                            {"id": "risk-reviewer", "role": "Challenge weak evidence and enforce grounding"},
                            {"id": "alert-composer", "role": "Generate operator-ready action recommendations"},
                        ],
                        "workflow": [
                            "data-parser",
                            "anomaly-detector",
                            "root-cause-analyzer",
                            "risk-reviewer",
                            "alert-composer",
                        ],
                        "guardrails": {
                            "minimum_grounding_ratio": min_grounding,
                            "minimum_holdout_score": 0.60,
                            "shadow_release_required": True,
                        },
                    },
                )
            )

        if "prompt" in requested and len(confirmed_labels) >= min_candidate:
            recurring_faults = Counter(
                item.get("root_cause") or item.get("anomaly_type") or "unknown"
                for item in confirmed_labels
            )
            candidates.append(
                self._store_candidate(
                    candidate_type="prompt",
                    name="Grounded Patrol Prompt Pack",
                    score=round(_clamp(0.48 + len(knowledge_cases) * 0.02, 0.48, 0.90), 3),
                    status="ready_for_shadow" if avg_grounding >= min_grounding else "draft",
                    rationale="Knowledge cases are sufficient to tighten retrieval-heavy patrol prompts.",
                    payload={
                        "top_faults": recurring_faults.most_common(5),
                        "prompt_template": (
                            "Always ground conclusions in current PLC points, historical drift, and confirmed knowledge cases. "
                            "If grounding is weak, return needs_review instead of over-confident diagnosis."
                        ),
                        "anti_collapse": {
                            "minimum_external_grounding_ratio": min_grounding,
                            "blocked_without_rule_or_case_anchor": True,
                        },
                    },
                )
            )

        if "model" in requested:
            model_status = "collecting_samples"
            if len(confirmed_labels) >= min_model and avg_grounding >= min_grounding:
                model_status = "awaiting_review"
            candidates.append(
                self._store_candidate(
                    candidate_type="model",
                    name="Industrial Patrol Small Model Fine-Tune",
                    score=round(_clamp(0.40 + len(confirmed_labels) * 0.025, 0.40, 0.93), 3),
                    status=model_status,
                    rationale="Model fine-tuning is allowed only after enough confirmed labels and grounded patrol evidence accumulate.",
                    payload={
                        "source_projects": ["Self-Improving-AI-Agents", "jamin_evolve_agent"],
                        "confirmed_label_count": len(confirmed_labels),
                        "knowledge_case_count": len(knowledge_cases),
                        "minimum_required_labels": min_model,
                        "training_objectives": [
                            "fault_classification",
                            "root_cause_attribution",
                            "action_summarization",
                        ],
                        "release_guardrails": {
                            "offline_eval_required": True,
                            "shadow_release_required": True,
                            "minimum_grounding_ratio": min_grounding,
                        },
                    },
                )
            )

        return candidates

    def _seed_default_knowledge_cases(self) -> None:
        existing = {item["case_id"] for item in self.repository.list_knowledge_cases(limit=200)}
        now = utc_now()
        seeds = [
            {
                "case_id": "CASE_DUST_FILTER_CLOGGING",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "scene_type": "dust",
                "title": "布袋堵塞导致压差升高",
                "summary": "压差持续偏高且风量下降时，优先排查滤袋堵塞与清灰失效。",
                "content": "当压差 > 1.8kPa、风量 < 7800m3/h 且清灰频率升高时，通常说明滤袋阻力增加。",
                "tags": ["dust", "压差", "滤袋堵塞", "清灰失效"],
                "root_cause": "滤袋堵塞",
                "recommended_actions": ["检查滤袋阻力", "核对脉冲清灰阀动作", "安排停机检修滤袋"],
                "source_label_id": None,
                "source_type": "seed",
                "usage_count": 0,
                "created_at": now,
                "updated_at": now,
            },
            {
                "case_id": "CASE_DUST_FAN_DROP",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "scene_type": "dust",
                "title": "风机效率下降导致排放升高",
                "summary": "风机电流偏低与粉尘浓度升高组合出现时，优先排查风机效率或皮带打滑。",
                "content": "当出口粉尘浓度升高且风机电流低于常态区间时，应检查风机负载与传动状态。",
                "tags": ["dust", "风机异常", "电流偏低", "排放"],
                "root_cause": "风机效率下降",
                "recommended_actions": ["检查风机皮带", "核对电机电流", "确认风机叶轮积灰情况"],
                "source_label_id": None,
                "source_type": "seed",
                "usage_count": 0,
                "created_at": now,
                "updated_at": now,
            },
            {
                "case_id": "CASE_WATER_DOSING_SWING",
                "asset_id": None,
                "scene_type": "water",
                "title": "加药波动导致废水指标异常",
                "summary": "为后续废水场景预留的知识样例。",
                "content": "当药剂泵流量波动与 pH/COD 指标异常同步出现时，应检查药剂泵和加药策略。",
                "tags": ["water", "加药", "废水"],
                "root_cause": "加药系统波动",
                "recommended_actions": ["检查加药泵", "复核药剂浓度", "对比 pH 历史趋势"],
                "source_label_id": None,
                "source_type": "seed",
                "usage_count": 0,
                "created_at": now,
                "updated_at": now,
            },
            {
                "case_id": "CASE_GAS_BURNER_DRIFT",
                "asset_id": None,
                "scene_type": "gas",
                "title": "废气燃烧效率漂移",
                "summary": "为后续废气场景预留的知识样例。",
                "content": "当燃烧温度下降、风机负荷异常且排放浓度抬升时，应检查燃烧器效率与供风平衡。",
                "tags": ["gas", "燃烧器", "废气"],
                "root_cause": "燃烧效率漂移",
                "recommended_actions": ["检查燃烧器", "复核风机配风", "评估热回收系统状态"],
                "source_label_id": None,
                "source_type": "seed",
                "usage_count": 0,
                "created_at": now,
                "updated_at": now,
            },
        ]
        for case in seeds:
            if case["case_id"] in existing:
                continue
            self.repository.upsert_knowledge_case(case)

    async def _execute_patrol_task(
        self,
        task: TrackedTask,
        asset_ids: List[str],
        triggered_by: str,
        schedule_type: str,
    ) -> Dict[str, Any]:
        run_id = f"PATROL_{uuid.uuid4().hex[:12].upper()}"
        task.progress.total_steps = max(len(asset_ids), 1)
        task.progress.current_action = "Preparing patrol"

        asset_results: List[Dict[str, Any]] = []
        label_records: List[Dict[str, Any]] = []
        for index, asset_id in enumerate(asset_ids, start=1):
            self.tracker.update_progress(
                task.task_id,
                step=index,
                percentage=index / max(len(asset_ids), 1) * 100,
                action=f"Analyzing {asset_id}",
            )
            asset = self._require_asset(asset_id)
            snapshot = self.repository.get_snapshot(asset_id)
            assessment = await self._assess_asset(asset, snapshot)
            asset_results.append(assessment)

            if assessment["requires_review"]:
                label_record = self._maybe_queue_label(run_id, asset, assessment)
                assessment["review_label_id"] = label_record["label_id"]
                label_records.append(label_record)

        overall_risk_score = round(max((float(item["risk_score"]) for item in asset_results), default=0.0), 2)
        overall_risk_level = self._highest_risk_level(asset_results)
        overall_status = overall_risk_level

        run_payload = {
            "run_id": run_id,
            "scene_type": self.runtime_config["default_scene"],
            "status": overall_status,
            "risk_level": overall_risk_level,
            "risk_score": overall_risk_score,
            "created_at": utc_now().isoformat(),
            "triggered_by": triggered_by,
            "schedule_type": schedule_type,
            "asset_results": asset_results,
            "review_queue_size": len(label_records),
            "abnormal_asset_count": len([item for item in asset_results if item["status"] != "normal"]),
            "healthy_asset_count": len([item for item in asset_results if item["status"] == "normal"]),
        }
        stored = self.repository.create_patrol_run(run_payload)
        stored["labels_created"] = [serialize_value(item) for item in label_records]
        return stored

    async def _assess_asset(self, asset: AssetDefinition, snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        now = utc_now()
        findings: List[Dict[str, Any]] = []
        fault_scores: Counter[str] = Counter()
        affected_points: set[str] = set()

        if not snapshot:
            findings.append(
                self._finding(
                    "missing_snapshot",
                    "critical",
                    "缺少最新快照",
                    "当前资产还没有接收到最新 PLC 快照，无法进行可靠巡检。",
                    [],
                    {},
                )
            )
            return await self._finalize_assessment(
                asset,
                snapshot=None,
                findings=findings,
                fault_scores=fault_scores,
                affected_points=affected_points,
                risk_score=68.0,
                status_override="needs_review",
            )

        point_map = snapshot.get("points") or {}
        stale_after = int(self.runtime_config["stale_after_seconds"])
        risk_score = 0.0

        for definition in asset.points:
            point = point_map.get(definition.point_id)
            if not point and definition.required:
                findings.append(
                    self._finding(
                        "missing_point",
                        "high",
                        f"{definition.display_name}缺失",
                        f"{definition.display_name} 未进入最新快照，当前诊断需要人工复核。",
                        [definition.point_id],
                        {},
                    )
                )
                affected_points.add(definition.point_id)
                risk_score += 12
                continue

            if not point:
                continue

            timestamp = point.get("timestamp")
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            age_seconds = (now - timestamp).total_seconds() if isinstance(timestamp, datetime) else stale_after + 1
            if age_seconds > stale_after:
                findings.append(
                    self._finding(
                        "stale_point",
                        "high",
                        f"{definition.display_name}数据过期",
                        f"{definition.display_name} 数据已超过 {stale_after} 秒未更新。",
                        [definition.point_id],
                        {"age_seconds": round(age_seconds, 1)},
                    )
                )
                affected_points.add(definition.point_id)
                risk_score += 10

            quality = str(point.get("quality") or "good").lower()
            if quality not in {"good", "ok", "normal"}:
                findings.append(
                    self._finding(
                        "quality_issue",
                        "medium",
                        f"{definition.display_name}质量异常",
                        f"{definition.display_name} 当前质量标记为 {quality}。",
                        [definition.point_id],
                        {"quality": quality},
                    )
                )
                affected_points.add(definition.point_id)
                risk_score += 8

        pressure_diff = self._numeric(point_map.get("pressure_diff_kpa"))
        airflow = self._numeric(point_map.get("airflow_m3h"))
        dust = self._numeric(point_map.get("dust_concentration_mg_m3"))
        cleaning = self._numeric(point_map.get("cleaning_frequency_hz"))
        current = self._numeric(point_map.get("fan_current_a"))
        temperature = self._numeric(point_map.get("temperature_c"))
        running_state = self._truthy(point_map.get("running_state", {}).get("value"))
        valve_state = str(point_map.get("valve_state", {}).get("value") or "").lower()

        risk_score += self._apply_threshold_checks(
            pressure_diff=pressure_diff,
            airflow=airflow,
            dust=dust,
            cleaning=cleaning,
            current=current,
            temperature=temperature,
            findings=findings,
            fault_scores=fault_scores,
            affected_points=affected_points,
        )

        risk_score += self._apply_rate_checks(point_map, findings, fault_scores, affected_points)
        risk_score += self._apply_correlation_checks(
            pressure_diff=pressure_diff,
            airflow=airflow,
            dust=dust,
            cleaning=cleaning,
            current=current,
            temperature=temperature,
            running_state=running_state,
            valve_state=valve_state,
            findings=findings,
            fault_scores=fault_scores,
            affected_points=affected_points,
        )

        return await self._finalize_assessment(
            asset,
            snapshot=snapshot,
            findings=findings,
            fault_scores=fault_scores,
            affected_points=affected_points,
            risk_score=risk_score,
        )

    async def _finalize_assessment(
        self,
        asset: AssetDefinition,
        *,
        snapshot: Optional[Dict[str, Any]],
        findings: List[Dict[str, Any]],
        fault_scores: Counter[str],
        affected_points: set[str],
        risk_score: float,
        status_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        base_query = " ".join(
            [
                asset.scene_type,
                asset.name,
                " ".join(fault_scores.keys()),
                " ".join(item["title"] for item in findings),
            ]
        ).strip()
        knowledge_hits = self.repository.search_knowledge_cases(
            base_query,
            scene_type=asset.scene_type,
            top_k=int(self.runtime_config["knowledge_top_k"]),
        )
        for hit in knowledge_hits:
            root_cause = str(hit.get("root_cause") or "").strip()
            if root_cause:
                fault_scores[root_cause] += 1.5 if hit.get("source_type") == "confirmed_label" else 0.75
            self.repository.increment_knowledge_case_usage(hit["case_id"])

        suspected_faults = [item for item, _score in fault_scores.most_common(4)]
        knowledge_grounding_ratio = 0.0
        if findings or knowledge_hits:
            knowledge_grounding_ratio = round(
                _clamp((len(findings) + len(knowledge_hits)) / max(len(findings) + len(suspected_faults), 1), 0.0, 1.0),
                3,
            )

        risk_score = round(_clamp(risk_score + len(knowledge_hits) * 2.5, 0.0, 100.0), 2)
        risk_level = self._score_to_level(risk_score)
        status = status_override or ("normal" if risk_score < 20 and not findings else risk_level)
        requires_review = status != "normal"
        operator_actions = self._build_actions(suspected_faults, findings, knowledge_hits)
        prediction_window = self._build_predictions(snapshot, suspected_faults, risk_score)
        synthesis = await self._synthesize(
            asset=asset,
            findings=findings,
            suspected_faults=suspected_faults,
            operator_actions=operator_actions,
            knowledge_hits=knowledge_hits,
            risk_score=risk_score,
            risk_level=risk_level,
        )

        return {
            "asset_id": asset.asset_id,
            "asset_name": asset.name,
            "scene_type": asset.scene_type,
            "status": status,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "suspected_faults": synthesis["suspected_faults"] or suspected_faults,
            "affected_points": sorted(affected_points),
            "operator_actions": synthesis["operator_actions"] or operator_actions,
            "requires_review": requires_review,
            "summary": synthesis["summary"],
            "findings": findings,
            "knowledge_hits": knowledge_hits,
            "knowledge_grounding_ratio": knowledge_grounding_ratio,
            "prediction_window": prediction_window,
            "snapshot_id": snapshot.get("snapshot_id") if snapshot else None,
        }

    def _apply_threshold_checks(
        self,
        *,
        pressure_diff: Optional[float],
        airflow: Optional[float],
        dust: Optional[float],
        cleaning: Optional[float],
        current: Optional[float],
        temperature: Optional[float],
        findings: List[Dict[str, Any]],
        fault_scores: Counter[str],
        affected_points: set[str],
    ) -> float:
        risk = 0.0
        if pressure_diff is not None and pressure_diff > 1.8:
            findings.append(self._finding("pressure_high", "high", "压差过高", "压差超过 1.8kPa，存在滤袋堵塞或清灰效率下降风险。", ["pressure_diff_kpa"], {"value": pressure_diff}))
            fault_scores["滤袋堵塞"] += 2.5
            affected_points.add("pressure_diff_kpa")
            risk += 18
        if airflow is not None and airflow < 7800:
            findings.append(self._finding("airflow_low", "high", "风量偏低", "风量低于运行基线，除尘效率可能下降。", ["airflow_m3h"], {"value": airflow}))
            fault_scores["风量不足"] += 1.8
            affected_points.add("airflow_m3h")
            risk += 15
        if dust is not None and dust > 20:
            findings.append(self._finding("dust_high", "critical", "出口粉尘浓度升高", "出口粉尘浓度超过 20mg/m3，需要立即排查排放风险。", ["dust_concentration_mg_m3"], {"value": dust}))
            fault_scores["排放超标风险"] += 2.8
            affected_points.add("dust_concentration_mg_m3")
            risk += 22
        if cleaning is not None and cleaning > 1.6:
            findings.append(self._finding("cleaning_high", "medium", "清灰频率过高", "清灰频率高于常态，说明过滤阻力上升。", ["cleaning_frequency_hz"], {"value": cleaning}))
            fault_scores["清灰失效"] += 1.4
            affected_points.add("cleaning_frequency_hz")
            risk += 10
        if current is not None and (current < 18 or current > 42):
            findings.append(self._finding("fan_current_abnormal", "high", "风机电流异常", "风机电流偏离常态区间，存在负载异常或机械效率下降。", ["fan_current_a"], {"value": current}))
            fault_scores["风机效率下降"] += 2.0
            affected_points.add("fan_current_a")
            risk += 16
        if temperature is not None and temperature > 90:
            findings.append(self._finding("temperature_high", "high", "温度过高", "设备温度高于 90C，存在电机过载或粉尘高温风险。", ["temperature_c"], {"value": temperature}))
            fault_scores["电机过载"] += 1.9
            affected_points.add("temperature_c")
            risk += 17
        return risk

    def _apply_rate_checks(
        self,
        point_map: Dict[str, Dict[str, Any]],
        findings: List[Dict[str, Any]],
        fault_scores: Counter[str],
        affected_points: set[str],
    ) -> float:
        risk = 0.0
        for point_id, point in point_map.items():
            history = list(point.get("history") or [])
            if len(history) < 4:
                continue
            current = self._coerce_float(point.get("value"))
            if current is None:
                continue
            baseline = sum(history[:-1]) / max(len(history) - 1, 1)
            if math.isclose(baseline, 0.0, abs_tol=1e-6):
                continue
            change_ratio = abs((current - baseline) / baseline)
            if change_ratio < 0.35:
                continue
            findings.append(
                self._finding(
                    "rate_shift",
                    "medium",
                    f"{point.get('display_name') or point_id}波动异常",
                    "最近一次采样相对历史基线波动超过 35%。",
                    [point_id],
                    {"baseline": round(baseline, 3), "current": current, "change_ratio": round(change_ratio, 3)},
                )
            )
            affected_points.add(point_id)
            fault_scores["工况突变"] += 0.9
            risk += 7
        return risk

    def _apply_correlation_checks(
        self,
        *,
        pressure_diff: Optional[float],
        airflow: Optional[float],
        dust: Optional[float],
        cleaning: Optional[float],
        current: Optional[float],
        temperature: Optional[float],
        running_state: bool,
        valve_state: str,
        findings: List[Dict[str, Any]],
        fault_scores: Counter[str],
        affected_points: set[str],
    ) -> float:
        risk = 0.0
        if pressure_diff and airflow and pressure_diff > 1.6 and airflow < 8000:
            findings.append(self._finding("baghouse_clogging_pattern", "critical", "滤袋堵塞模式命中", "高压差与低风量组合命中滤袋堵塞模式。", ["pressure_diff_kpa", "airflow_m3h"], {"pressure_diff_kpa": pressure_diff, "airflow_m3h": airflow}))
            fault_scores["滤袋堵塞"] += 3.5
            affected_points.update({"pressure_diff_kpa", "airflow_m3h"})
            risk += 20
        if dust and current and dust > 18 and current < 20:
            findings.append(self._finding("fan_efficiency_pattern", "high", "风机效率下降模式命中", "高粉尘浓度与低风机电流组合命中风机效率下降模式。", ["dust_concentration_mg_m3", "fan_current_a"], {"dust": dust, "current": current}))
            fault_scores["风机效率下降"] += 3.0
            affected_points.update({"dust_concentration_mg_m3", "fan_current_a"})
            risk += 18
        if cleaning and pressure_diff and cleaning > 1.4 and pressure_diff > 1.5:
            findings.append(self._finding("cleaning_failure_pattern", "high", "清灰失效模式命中", "清灰频率升高但压差仍持续偏高，优先排查清灰阀或滤袋。", ["cleaning_frequency_hz", "pressure_diff_kpa"], {"cleaning_frequency_hz": cleaning, "pressure_diff_kpa": pressure_diff}))
            fault_scores["清灰失效"] += 2.7
            affected_points.update({"cleaning_frequency_hz", "pressure_diff_kpa"})
            risk += 17
        if temperature and current and temperature > 85 and current > 40:
            findings.append(self._finding("motor_overload_pattern", "high", "电机过载模式命中", "高温与高电流组合说明电机或风机机械负载过高。", ["temperature_c", "fan_current_a"], {"temperature_c": temperature, "fan_current_a": current}))
            fault_scores["电机过载"] += 2.6
            affected_points.update({"temperature_c", "fan_current_a"})
            risk += 18
        if not running_state and (pressure_diff or airflow or dust):
            findings.append(self._finding("state_mismatch", "medium", "运行状态与工艺数据不一致", "运行状态显示未运行，但仍存在工艺数据波动，建议核对状态点位。", ["running_state"], {"running_state": running_state}))
            fault_scores["状态点位异常"] += 1.1
            affected_points.add("running_state")
            risk += 8
        if valve_state and valve_state not in {"open", "opened", "on", "normal"}:
            findings.append(self._finding("valve_state_attention", "medium", "阀门状态异常", "阀门状态未处于正常开启工况，可能影响风路或清灰策略。", ["valve_state"], {"valve_state": valve_state}))
            fault_scores["阀门状态异常"] += 1.2
            affected_points.add("valve_state")
            risk += 8
        return risk

    async def _synthesize(
        self,
        *,
        asset: AssetDefinition,
        findings: List[Dict[str, Any]],
        suspected_faults: List[str],
        operator_actions: List[str],
        knowledge_hits: List[Dict[str, Any]],
        risk_score: float,
        risk_level: str,
    ) -> Dict[str, Any]:
        fallback = {
            "summary": self._build_summary(asset, findings, suspected_faults, risk_level),
            "operator_actions": operator_actions[:4],
            "suspected_faults": suspected_faults[:4],
        }

        if not self.runtime_config["llm"]["enabled"]:
            return fallback

        try:
            llm_result = await self.llm_client.summarize(
                {
                    "asset": asset.to_dict(),
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "findings": findings,
                    "suspected_faults": suspected_faults,
                    "operator_actions": operator_actions,
                    "knowledge_hits": knowledge_hits,
                },
                temperature=float(self.runtime_config["llm"]["temperature"]),
                max_tokens=int(self.runtime_config["llm"]["max_tokens"]),
            )
            if not llm_result.get("summary"):
                return fallback
            return {
                "summary": llm_result["summary"],
                "operator_actions": llm_result["operator_actions"] or fallback["operator_actions"],
                "suspected_faults": llm_result["suspected_faults"] or fallback["suspected_faults"],
            }
        except PatrolLLMUnavailableError as exc:
            logger.warning(f"Patrol summary fell back to deterministic synthesis: {exc}")
            return fallback

    def _build_summary(self, asset: AssetDefinition, findings: List[Dict[str, Any]], suspected_faults: List[str], risk_level: str) -> str:
        if not findings:
            return f"{asset.name} 当前未发现明显异常，已作为健康样本纳入基线。"
        top_fault = suspected_faults[0] if suspected_faults else findings[0]["title"]
        return f"{asset.name} 当前风险等级为 {risk_level}，重点怀疑 {top_fault}，建议结合现场复核与历史案例快速处理。"

    def _build_actions(self, suspected_faults: List[str], findings: List[Dict[str, Any]], knowledge_hits: List[Dict[str, Any]]) -> List[str]:
        actions: List[str] = []
        canned = {
            "滤袋堵塞": ["检查滤袋阻力与积灰情况", "核对脉冲清灰阀动作", "安排停机检修滤袋"],
            "清灰失效": ["检查清灰电磁阀", "核对喷吹压力", "确认清灰频率策略是否异常"],
            "风机效率下降": ["检查风机皮带与叶轮积灰", "复核电机电流与振动", "确认风门开度与阻力匹配"],
            "电机过载": ["复核电机温升", "检查轴承与机械卡滞", "确认供电与负载情况"],
            "排放超标风险": ["立即复核排口监测数据", "检查风量与压差匹配关系", "评估是否需要降负荷运行"],
        }
        for fault in suspected_faults:
            actions.extend(canned.get(fault, []))
        for hit in knowledge_hits:
            actions.extend(hit.get("recommended_actions") or [])
        if not actions and findings:
            actions.append("安排现场复核关键点位与设备状态。")
        seen = set()
        ordered = []
        for action in actions:
            text = str(action).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            ordered.append(text)
        return ordered[:4]

    def _build_predictions(
        self,
        snapshot: Optional[Dict[str, Any]],
        suspected_faults: List[str],
        risk_score: float,
    ) -> List[Dict[str, Any]]:
        point_map = (snapshot or {}).get("points") or {}
        pressure = self._numeric(point_map.get("pressure_diff_kpa"))
        airflow = self._numeric(point_map.get("airflow_m3h"))
        dust = self._numeric(point_map.get("dust_concentration_mg_m3"))
        drift = 0.0
        if pressure is not None and pressure > 1.5:
            drift += 4
        if airflow is not None and airflow < 8200:
            drift += 4
        if dust is not None and dust > 18:
            drift += 6

        faults = suspected_faults[:3] or ["工况稳定"]
        windows = []
        for horizon in (30, 60, 240):
            horizon_risk = round(_clamp(risk_score + drift * math.log10(horizon + 10), 0.0, 100.0), 2)
            windows.append(
                {
                    "horizon_minutes": horizon,
                    "risk_score": horizon_risk,
                    "summary": f"未来 {horizon} 分钟重点关注 {faults[0]} 风险变化。",
                    "fault_probabilities": {
                        fault: round(_clamp((risk_score / 100) + (index * 0.08) + (drift / 100), 0.05, 0.95), 3)
                        for index, fault in enumerate(faults)
                    },
                }
            )
        return windows

    def _promote_label_to_case(self, label: Dict[str, Any]) -> Dict[str, Any]:
        existing = [
            item
            for item in self.repository.list_knowledge_cases(limit=500)
            if item.get("source_label_id") == label["label_id"]
        ]
        if existing:
            return existing[0]

        run_payload = self.repository.get_patrol_run(label["run_id"]) or {}
        asset_result = self._find_asset_result(run_payload, label["asset_id"])
        title_root = label.get("root_cause") or label.get("anomaly_type") or asset_result.get("status") or "异常"
        case_payload = {
            "case_id": f"CASE_{uuid.uuid4().hex[:12].upper()}",
            "asset_id": label["asset_id"],
            "scene_type": label["scene_type"],
            "title": f"{self.assets_by_id[label['asset_id']].name} - {title_root}",
            "summary": asset_result.get("summary") or f"人工确认异常：{title_root}",
            "content": "\n".join(
                [
                    asset_result.get("summary") or "",
                    f"人工确认根因: {label.get('root_cause') or label.get('anomaly_type') or title_root}",
                    "建议动作:",
                    *[f"- {item}" for item in (asset_result.get("operator_actions") or [])],
                    f"审核备注: {label.get('review', {}).get('review_notes', '')}",
                ]
            ).strip(),
            "tags": list(
                dict.fromkeys(
                    [
                        label["scene_type"],
                        label.get("anomaly_type") or "",
                        label.get("root_cause") or "",
                        *list(asset_result.get("suspected_faults") or []),
                    ]
                )
            ),
            "root_cause": label.get("root_cause") or label.get("anomaly_type"),
            "recommended_actions": list(asset_result.get("operator_actions") or []),
            "source_label_id": label["label_id"],
            "source_type": "confirmed_label",
            "usage_count": 0,
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }
        return self.repository.upsert_knowledge_case(case_payload)

    def _maybe_queue_label(self, run_id: str, asset: AssetDefinition, assessment: Dict[str, Any]) -> Dict[str, Any]:
        anomaly_type = assessment["suspected_faults"][0] if assessment["suspected_faults"] else assessment["status"]
        pending_labels = self.repository.list_labels(status="pending", limit=200)
        for label in pending_labels:
            if label["asset_id"] != asset.asset_id:
                continue
            if (label.get("anomaly_type") or label.get("root_cause")) != anomaly_type:
                continue
            updated = self.repository.update_label(
                label["label_id"],
                {
                    "updated_at": utc_now(),
                    "review": {
                        **dict(label.get("review") or {}),
                        "latest_run_id": run_id,
                        "latest_risk_level": assessment["risk_level"],
                        "latest_risk_score": assessment["risk_score"],
                    },
                },
            )
            if updated:
                return updated

        label_payload = {
            "label_id": f"LABEL_{uuid.uuid4().hex[:12].upper()}",
            "run_id": run_id,
            "asset_id": asset.asset_id,
            "scene_type": asset.scene_type,
            "status": "pending",
            "anomaly_type": anomaly_type,
            "root_cause": anomaly_type,
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "review": {
                "queued_from_status": assessment["status"],
                "risk_level": assessment["risk_level"],
                "risk_score": assessment["risk_score"],
            },
        }
        return self.repository.create_label(label_payload)

    def _store_candidate(
        self,
        *,
        candidate_type: str,
        name: str,
        score: float,
        status: str,
        rationale: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        candidate_payload = {
            "candidate_id": f"CAND_{uuid.uuid4().hex[:12].upper()}",
            "candidate_type": candidate_type,
            "name": name,
            "status": status,
            "score": round(score, 3),
            "rationale": rationale,
            "payload": payload,
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }
        return self.repository.create_learning_candidate(candidate_payload)

    def _score_to_level(self, risk_score: float) -> str:
        if risk_score >= 70:
            return "high_risk"
        if risk_score >= 40:
            return "warning"
        if risk_score >= 20:
            return "attention"
        return "normal"

    def _highest_risk_level(self, asset_results: List[Dict[str, Any]]) -> str:
        if not asset_results:
            return "normal"
        return max(
            (item.get("risk_level", "normal") for item in asset_results),
            key=lambda value: _RISK_ORDER.get(value, 0),
        )

    def _find_asset_result(self, run_payload: Dict[str, Any], asset_id: str) -> Dict[str, Any]:
        for item in run_payload.get("asset_results", []):
            if item.get("asset_id") == asset_id:
                return item
        return {}

    def _require_asset(self, asset_id: str) -> AssetDefinition:
        asset = self.assets_by_id.get(asset_id)
        if not asset:
            raise KeyError(f"Unknown asset: {asset_id}")
        return asset

    def _coerce_float(self, value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _numeric(self, point: Optional[Dict[str, Any]]) -> Optional[float]:
        if not point:
            return None
        return self._coerce_float(point.get("value"))

    def _truthy(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "running", "on", "open", "yes"}

    def _finding(
        self,
        code: str,
        severity: str,
        title: str,
        description: str,
        affected_points: List[str],
        evidence: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "code": code,
            "severity": severity,
            "title": title,
            "description": description,
            "affected_points": affected_points,
            "evidence": evidence,
        }
