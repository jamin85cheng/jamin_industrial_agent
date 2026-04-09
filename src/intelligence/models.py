"""Domain models for the industrial intelligence runtime."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return {
            key: serialize_value(item)
            for key, item in asdict(value).items()
        }
    if isinstance(value, dict):
        return {key: serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    return value


@dataclass(frozen=True)
class Plant:
    plant_id: str
    name: str
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass(frozen=True)
class Area:
    area_id: str
    plant_id: str
    name: str
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass(frozen=True)
class Line:
    line_id: str
    area_id: str
    name: str
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass(frozen=True)
class PointDefinition:
    point_id: str
    display_name: str
    unit: str
    point_type: str
    required: bool = True
    low_limit: Optional[float] = None
    high_limit: Optional[float] = None
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass(frozen=True)
class AssetDefinition:
    asset_id: str
    line_id: str
    area_id: str
    plant_id: str
    scene_type: str
    name: str
    points: List[PointDefinition]
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass
class RealtimeSnapshot:
    snapshot_id: str
    asset_id: str
    scene_type: str
    collected_at: datetime
    source: str
    points: Dict[str, Dict[str, Any]]
    plant_id: Optional[str] = None
    area_id: Optional[str] = None
    line_id: Optional[str] = None
    completeness: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass
class PatrolFinding:
    code: str
    severity: str
    title: str
    description: str
    affected_points: List[str] = field(default_factory=list)
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass
class PredictionWindow:
    horizon_minutes: int
    risk_score: float
    summary: str
    fault_probabilities: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass
class LabelRecord:
    label_id: str
    run_id: str
    asset_id: str
    scene_type: str
    status: str
    anomaly_type: Optional[str]
    root_cause: Optional[str]
    created_at: datetime
    updated_at: datetime
    review: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass
class KnowledgeCase:
    case_id: str
    asset_id: Optional[str]
    scene_type: str
    title: str
    summary: str
    content: str
    tags: List[str]
    root_cause: Optional[str]
    recommended_actions: List[str]
    source_label_id: Optional[str]
    source_type: str
    usage_count: int
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)


@dataclass
class LearningCandidate:
    candidate_id: str
    candidate_type: str
    name: str
    status: str
    score: float
    rationale: str
    payload: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return serialize_value(self)
