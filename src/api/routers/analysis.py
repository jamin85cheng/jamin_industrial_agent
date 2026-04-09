"""Analysis APIs backed by persisted telemetry data."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from src.api.dependencies import TenantContext, UserContext, get_tenant_context, require_permissions
from src.api.repositories.telemetry_repository import TelemetryRepository

router = APIRouter(prefix="/analysis", tags=["analysis"])
telemetry_repository = TelemetryRepository()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AnomalyPoint(BaseModel):
    timestamp: datetime
    value: float
    expected: float
    score: float


class AnomalyAnalysisRequest(BaseModel):
    tag: str
    start_time: datetime
    end_time: datetime
    sensitivity: float = Field(default=0.95, ge=0.0, le=1.0)


class AnomalyAnalysisResponse(BaseModel):
    anomalies: List[AnomalyPoint]
    total_points: int
    anomaly_count: int
    anomaly_rate: float


class TrendPoint(BaseModel):
    timestamp: datetime
    value: float
    trend: str


class TrendResponse(BaseModel):
    tag: str
    start_time: datetime
    end_time: datetime
    trend: str
    change_percent: float
    data: List[TrendPoint]


class ForecastRequest(BaseModel):
    tag: str
    horizon: int = Field(default=24, ge=1, le=168)


class ForecastPoint(BaseModel):
    timestamp: datetime
    value: float
    lower_bound: float
    upper_bound: float
    confidence: float


class ForecastResponse(BaseModel):
    tag: str
    horizon: int
    forecast: List[ForecastPoint]
    model_info: Dict[str, Any]


class StatisticsResponse(BaseModel):
    tag: str
    start_time: datetime
    end_time: datetime
    count: int
    mean: float
    std: float
    min: float
    max: float
    median: float
    p95: float
    p99: float


def _default_window(*, hours: int = 24) -> tuple[datetime, datetime]:
    end_time = utc_now()
    start_time = end_time - timedelta(hours=hours)
    return start_time, end_time


def _load_series(
    *,
    tenant_id: str,
    tag: str,
    start_time: datetime,
    end_time: datetime,
    limit_per_tag: int = 5000,
) -> List[Dict[str, Any]]:
    series = telemetry_repository.query_points(
        tenant_id=tenant_id,
        tags=[tag],
        start_time=start_time,
        end_time=end_time,
        limit_per_tag=limit_per_tag,
    ).get(tag, [])
    if not series:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No telemetry data available for tag {tag}",
        )
    return series


def _compute_std(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _rolling_expected(values: Sequence[float], index: int, window_size: int = 6) -> tuple[float, float]:
    history = list(values[max(0, index - window_size):index])
    if len(history) < 2:
        return values[index], 0.0
    expected = sum(history) / len(history)
    deviation = _compute_std(history)
    return expected, deviation


def _infer_step_seconds(points: Sequence[Dict[str, Any]], default_seconds: int = 3600) -> int:
    if len(points) < 2:
        return default_seconds
    deltas: List[int] = []
    for current, nxt in zip(points[:-1], points[1:]):
        current_ts = current["timestamp"]
        next_ts = nxt["timestamp"]
        if not isinstance(current_ts, datetime) or not isinstance(next_ts, datetime):
            continue
        delta = int((next_ts - current_ts).total_seconds())
        if delta > 0:
            deltas.append(delta)
    if not deltas:
        return default_seconds
    deltas.sort()
    return deltas[len(deltas) // 2]


def _pearson(values_a: Sequence[float], values_b: Sequence[float]) -> float:
    if len(values_a) != len(values_b) or len(values_a) < 2:
        return 0.0
    mean_a = sum(values_a) / len(values_a)
    mean_b = sum(values_b) / len(values_b)
    numerator = sum((a - mean_a) * (b - mean_b) for a, b in zip(values_a, values_b))
    variance_a = sum((a - mean_a) ** 2 for a in values_a)
    variance_b = sum((b - mean_b) ** 2 for b in values_b)
    denominator = math.sqrt(variance_a * variance_b)
    if denominator == 0:
        return 0.0
    return max(-1.0, min(1.0, numerator / denominator))


def _align_series(points: Sequence[Dict[str, Any]]) -> Dict[datetime, float]:
    aligned: Dict[datetime, float] = {}
    for point in points:
        timestamp = point["timestamp"]
        if not isinstance(timestamp, datetime):
            continue
        bucket = timestamp.replace(second=0, microsecond=0)
        aligned[bucket] = float(point["value"])
    return aligned


@router.post("/anomaly", response_model=AnomalyAnalysisResponse)
async def analyze_anomalies(
    request: AnomalyAnalysisRequest,
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    if request.end_time <= request.start_time:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_time must be greater than start_time",
        )

    data_points = _load_series(
        tenant_id=tenant.tenant_id,
        tag=request.tag,
        start_time=request.start_time,
        end_time=request.end_time,
    )
    values = [float(point["value"]) for point in data_points]
    threshold = max(1.2, 3.5 - (request.sensitivity * 2.0))

    anomalies: List[AnomalyPoint] = []
    for index, point in enumerate(data_points):
        expected, local_std = _rolling_expected(values, index)
        if local_std <= 0:
            continue
        z_score = abs((float(point["value"]) - expected) / local_std)
        if z_score >= threshold:
            anomalies.append(
                AnomalyPoint(
                    timestamp=point["timestamp"],
                    value=float(point["value"]),
                    expected=round(expected, 4),
                    score=round(min(z_score / max(threshold, 1.0), 1.0), 4),
                )
            )

    total_points = len(data_points)
    anomaly_count = len(anomalies)
    return AnomalyAnalysisResponse(
        anomalies=anomalies,
        total_points=total_points,
        anomaly_count=anomaly_count,
        anomaly_rate=round(anomaly_count / total_points, 6) if total_points > 0 else 0.0,
    )


@router.get("/trend", response_model=TrendResponse)
async def analyze_trend(
    tag: str = Query(..., description="Tag name"),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    if end_time is None or start_time is None:
        default_start, default_end = _default_window(hours=24)
        start_time = start_time or default_start
        end_time = end_time or default_end
    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_time must be greater than start_time",
        )

    series = _load_series(
        tenant_id=tenant.tenant_id,
        tag=tag,
        start_time=start_time,
        end_time=end_time,
    )

    data_points: List[TrendPoint] = []
    previous_value: Optional[float] = None
    for point in series:
        current_value = round(float(point["value"]), 4)
        if previous_value is None:
            direction = "stable"
        else:
            delta = current_value - previous_value
            if delta > 0.2:
                direction = "up"
            elif delta < -0.2:
                direction = "down"
            else:
                direction = "stable"
        data_points.append(
            TrendPoint(
                timestamp=point["timestamp"],
                value=current_value,
                trend=direction,
            )
        )
        previous_value = current_value

    first_value = data_points[0].value
    last_value = data_points[-1].value
    if first_value == 0:
        change_percent = 0.0
    else:
        change_percent = ((last_value - first_value) / abs(first_value)) * 100

    if change_percent > 5:
        overall_trend = "rising"
    elif change_percent < -5:
        overall_trend = "falling"
    else:
        overall_trend = "stable"

    return TrendResponse(
        tag=tag,
        start_time=start_time,
        end_time=end_time,
        trend=overall_trend,
        change_percent=round(change_percent, 4),
        data=data_points,
    )


@router.post("/forecast", response_model=ForecastResponse)
async def forecast(
    request: ForecastRequest,
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    end_time = utc_now()
    start_time = end_time - timedelta(days=7)
    series = _load_series(
        tenant_id=tenant.tenant_id,
        tag=request.tag,
        start_time=start_time,
        end_time=end_time,
    )

    recent_values = [float(point["value"]) for point in series[-24:]]
    baseline_value = recent_values[-1]
    slope_candidates = [
        recent_values[index] - recent_values[index - 1]
        for index in range(1, len(recent_values))
    ]
    average_slope = sum(slope_candidates) / len(slope_candidates) if slope_candidates else 0.0
    uncertainty = max(_compute_std(recent_values), 0.5)
    step_seconds = _infer_step_seconds(series, default_seconds=3600)
    confidence_decay = max(0.0025, min(0.03, 1.0 / max(request.horizon * 8, 8)))

    forecast_points: List[ForecastPoint] = []
    last_timestamp = series[-1]["timestamp"]
    if not isinstance(last_timestamp, datetime):
        last_timestamp = end_time

    for index in range(request.horizon):
        forecast_time = last_timestamp + timedelta(seconds=step_seconds * (index + 1))
        projected_value = baseline_value + (average_slope * (index + 1))
        band = uncertainty * (1.0 + (index * 0.15))
        forecast_points.append(
            ForecastPoint(
                timestamp=forecast_time,
                value=round(projected_value, 4),
                lower_bound=round(projected_value - band, 4),
                upper_bound=round(projected_value + band, 4),
                confidence=round(max(0.5, 0.98 - ((index + 1) * confidence_decay)), 4),
            )
        )

    return ForecastResponse(
        tag=request.tag,
        horizon=request.horizon,
        forecast=forecast_points,
        model_info={
            "model_type": "linear_projection",
            "training_samples": len(series),
            "window_size": len(recent_values),
            "average_slope": round(average_slope, 6),
        },
    )


@router.get("/statistics", response_model=StatisticsResponse)
async def get_statistics(
    tag: str = Query(..., description="Tag name"),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    if end_time is None or start_time is None:
        default_start, default_end = _default_window(hours=24 * 7)
        start_time = start_time or default_start
        end_time = end_time or default_end
    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_time must be greater than start_time",
        )

    series = _load_series(
        tenant_id=tenant.tenant_id,
        tag=tag,
        start_time=start_time,
        end_time=end_time,
    )
    stats = telemetry_repository.compute_series_statistics(series)
    return StatisticsResponse(
        tag=tag,
        start_time=start_time,
        end_time=end_time,
        count=int(stats["count"]),
        mean=round(stats["mean"], 4),
        std=round(stats["std"], 4),
        min=round(stats["min"], 4),
        max=round(stats["max"], 4),
        median=round(stats["median"], 4),
        p95=round(stats["p95"], 4),
        p99=round(stats["p99"], 4),
    )


@router.post("/correlation")
async def analyze_correlation(
    tags: List[str],
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    if len(tags) < 2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least two tags are required",
        )

    if end_time is None or start_time is None:
        default_start, default_end = _default_window(hours=24)
        start_time = start_time or default_start
        end_time = end_time or default_end
    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_time must be greater than start_time",
        )

    tag_series = {
        tag: _align_series(
            _load_series(
                tenant_id=tenant.tenant_id,
                tag=tag,
                start_time=start_time,
                end_time=end_time,
            )
        )
        for tag in tags
    }

    correlation_matrix: Dict[str, Dict[str, float]] = {}
    strongest_pair: Optional[List[str]] = None
    strongest_value = 0.0

    for tag_a in tags:
        correlation_matrix[tag_a] = {}
        for tag_b in tags:
            if tag_a == tag_b:
                correlation_matrix[tag_a][tag_b] = 1.0
                continue
            common_timestamps = sorted(set(tag_series[tag_a]).intersection(tag_series[tag_b]))
            series_a = [tag_series[tag_a][timestamp] for timestamp in common_timestamps]
            series_b = [tag_series[tag_b][timestamp] for timestamp in common_timestamps]
            correlation = round(_pearson(series_a, series_b), 4)
            correlation_matrix[tag_a][tag_b] = correlation
            if abs(correlation) > abs(strongest_value):
                strongest_value = correlation
                strongest_pair = [tag_a, tag_b]

    return {
        "tags": tags,
        "start_time": start_time,
        "end_time": end_time,
        "correlation_matrix": correlation_matrix,
        "strongest_correlation": {
            "tags": strongest_pair or tags[:2],
            "value": strongest_value,
        },
    }
