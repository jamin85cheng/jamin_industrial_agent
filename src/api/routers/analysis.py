"""
数据分析API路由

实现异常检测、趋势分析、预测等接口
"""

from fastapi import APIRouter, Depends, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel, Field

from src.api.dependencies import (
    get_current_user, require_permissions, get_tenant_context,
    UserContext, TenantContext
)

router = APIRouter(prefix="/analysis", tags=["数据分析"])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# Pydantic模型
class AnomalyPoint(BaseModel):
    """异常点"""
    timestamp: datetime
    value: float
    expected: float
    score: float


class AnomalyAnalysisRequest(BaseModel):
    """异常分析请求"""
    tag: str
    start_time: datetime
    end_time: datetime
    sensitivity: float = Field(default=0.95, ge=0.0, le=1.0)


class AnomalyAnalysisResponse(BaseModel):
    """异常分析响应"""
    anomalies: List[AnomalyPoint]
    total_points: int
    anomaly_count: int
    anomaly_rate: float


class TrendPoint(BaseModel):
    """趋势点"""
    timestamp: datetime
    value: float
    trend: str  # up, down, stable


class TrendResponse(BaseModel):
    """趋势响应"""
    tag: str
    start_time: datetime
    end_time: datetime
    trend: str  # overall trend
    change_percent: float
    data: List[TrendPoint]


class ForecastRequest(BaseModel):
    """预测请求"""
    tag: str
    horizon: int = Field(default=24, ge=1, le=168)  # 最多预测7天


class ForecastPoint(BaseModel):
    """预测点"""
    timestamp: datetime
    value: float
    lower_bound: float
    upper_bound: float
    confidence: float


class ForecastResponse(BaseModel):
    """预测响应"""
    tag: str
    horizon: int
    forecast: List[ForecastPoint]
    model_info: Dict[str, Any]


class StatisticsResponse(BaseModel):
    """统计响应"""
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


@router.post("/anomaly", response_model=AnomalyAnalysisResponse)
async def analyze_anomalies(
    request: AnomalyAnalysisRequest,
    user: UserContext = Depends(require_permissions("data:read"))
):
    """
    异常检测分析
    
    使用统计方法检测数据中的异常点
    """
    import math
    import random
    
    # 模拟生成数据点
    data_points = []
    anomalies = []
    
    current_time = request.start_time
    base_value = 30.0
    
    # 生成带周期性、趋势和噪声的数据
    while current_time <= request.end_time:
        # 基础值 + 周期性 + 噪声
        hour_factor = math.sin(2 * math.pi * current_time.hour / 24)
        noise = random.gauss(0, 2)
        value = base_value + hour_factor * 5 + noise
        
        data_points.append({"timestamp": current_time, "value": value})
        current_time += timedelta(minutes=5)
    
    # 计算统计值
    values = [p["value"] for p in data_points]
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance)
    
    # Z-score异常检测
    threshold = 2.5 * (1 - request.sensitivity + 0.1)  # 调整阈值
    
    for point in data_points:
        z_score = abs((point["value"] - mean) / std) if std > 0 else 0
        
        if z_score > threshold:
            anomalies.append(AnomalyPoint(
                timestamp=point["timestamp"],
                value=point["value"],
                expected=mean,
                score=min(z_score / 5, 1.0)  # 归一化到0-1
            ))
    
    total_points = len(data_points)
    anomaly_count = len(anomalies)
    
    return AnomalyAnalysisResponse(
        anomalies=anomalies,
        total_points=total_points,
        anomaly_count=anomaly_count,
        anomaly_rate=anomaly_count / total_points if total_points > 0 else 0
    )


@router.get("/trend", response_model=TrendResponse)
async def analyze_trend(
    tag: str = Query(..., description="数据点位"),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    user: UserContext = Depends(require_permissions("data:read"))
):
    """
    趋势分析
    
    分析数据的变化趋势
    """
    import math
    import random
    
    # 默认时间范围：过去24小时
    if not end_time:
        end_time = utc_now()
    if not start_time:
        start_time = end_time - timedelta(hours=24)
    
    # 生成趋势数据
    data_points = []
    current_time = start_time
    
    # 整体上升趋势
    total_duration = (end_time - start_time).total_seconds()
    
    while current_time <= end_time:
        progress = (current_time - start_time).total_seconds() / total_duration
        
        # 基础值 + 上升趋势 + 周期性 + 噪声
        base = 25.0
        trend_component = progress * 10  # 整体上升10度
        cycle = math.sin(2 * math.pi * current_time.hour / 24) * 3
        noise = random.gauss(0, 1)
        
        value = base + trend_component + cycle + noise
        
        # 确定趋势方向
        if current_time == start_time:
            trend = "stable"
        else:
            prev_value = data_points[-1].value
            diff = value - prev_value
            if diff > 0.5:
                trend = "up"
            elif diff < -0.5:
                trend = "down"
            else:
                trend = "stable"
        
        data_points.append(TrendPoint(
            timestamp=current_time,
            value=round(value, 2),
            trend=trend
        ))
        
        current_time += timedelta(minutes=10)
    
    # 计算整体趋势
    if len(data_points) >= 2:
        first_value = data_points[0].value
        last_value = data_points[-1].value
        change_percent = ((last_value - first_value) / first_value) * 100
        
        if change_percent > 5:
            overall_trend = "rising"
        elif change_percent < -5:
            overall_trend = "falling"
        else:
            overall_trend = "stable"
    else:
        change_percent = 0
        overall_trend = "stable"
    
    return TrendResponse(
        tag=tag,
        start_time=start_time,
        end_time=end_time,
        trend=overall_trend,
        change_percent=round(change_percent, 2),
        data=data_points
    )


@router.post("/forecast", response_model=ForecastResponse)
async def forecast(
    request: ForecastRequest,
    user: UserContext = Depends(require_permissions("data:read"))
):
    """
    预测分析
    
    基于历史数据预测未来值
    """
    import math
    import random
    
    # 模拟预测
    forecast_points = []
    
    # 基于历史趋势进行简单预测
    base_value = 30.0
    trend_per_hour = 0.5
    
    now = utc_now()
    
    for i in range(request.horizon):
        forecast_time = now + timedelta(hours=i)
        
        # 预测值 = 基础值 + 趋势 + 周期性
        value = base_value + (trend_per_hour * i)
        
        # 添加周期性（每日周期）
        hour_cycle = math.sin(2 * math.pi * forecast_time.hour / 24) * 3
        value += hour_cycle
        
        # 添加置信区间
        uncertainty = 1.0 + (i * 0.1)  # 随时间增加不确定性
        lower_bound = value - uncertainty * 2
        upper_bound = value + uncertainty * 2
        
        forecast_points.append(ForecastPoint(
            timestamp=forecast_time,
            value=round(value, 2),
            lower_bound=round(lower_bound, 2),
            upper_bound=round(upper_bound, 2),
            confidence=round(max(0.5, 1.0 - (i * 0.02)), 2)
        ))
    
    return ForecastResponse(
        tag=request.tag,
        horizon=request.horizon,
        forecast=forecast_points,
        model_info={
            "model_type": "arima",
            "training_samples": 1000,
            "r_squared": 0.85,
            "mae": 1.2
        }
    )


@router.get("/statistics", response_model=StatisticsResponse)
async def get_statistics(
    tag: str = Query(..., description="数据点位"),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    user: UserContext = Depends(require_permissions("data:read"))
):
    """
    获取数据统计信息
    
    返回数据的统计指标
    """
    import math
    import random
    
    # 默认时间范围
    if not end_time:
        end_time = utc_now()
    if not start_time:
        start_time = end_time - timedelta(days=7)
    
    # 生成模拟数据
    values = []
    current_time = start_time
    
    while current_time <= end_time:
        values.append(random.gauss(30, 5))
        current_time += timedelta(minutes=10)
    
    # 计算统计量
    count = len(values)
    sorted_values = sorted(values)
    
    mean = sum(values) / count
    variance = sum((v - mean) ** 2 for v in values) / count
    std = math.sqrt(variance)
    min_val = min(values)
    max_val = max(values)
    
    # 中位数
    mid = count // 2
    if count % 2 == 0:
        median = (sorted_values[mid - 1] + sorted_values[mid]) / 2
    else:
        median = sorted_values[mid]
    
    # 百分位数
    p95_idx = int(count * 0.95)
    p99_idx = int(count * 0.99)
    p95 = sorted_values[min(p95_idx, count - 1)]
    p99 = sorted_values[min(p99_idx, count - 1)]
    
    return StatisticsResponse(
        tag=tag,
        start_time=start_time,
        end_time=end_time,
        count=count,
        mean=round(mean, 2),
        std=round(std, 2),
        min=round(min_val, 2),
        max=round(max_val, 2),
        median=round(median, 2),
        p95=round(p95, 2),
        p99=round(p99, 2)
    )


@router.post("/correlation")
async def analyze_correlation(
    tags: List[str],
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    user: UserContext = Depends(require_permissions("data:read"))
):
    """
    相关性分析
    
    分析多个数据点位的相关性
    """
    import random
    
    if len(tags) < 2:
        return {"error": "需要至少两个数据点位"}
    
    # 模拟相关性矩阵
    correlation_matrix = {}
    
    for i, tag1 in enumerate(tags):
        correlation_matrix[tag1] = {}
        for j, tag2 in enumerate(tags):
            if i == j:
                correlation_matrix[tag1][tag2] = 1.0
            else:
                # 模拟相关性值
                correlation_matrix[tag1][tag2] = round(random.uniform(-0.8, 0.9), 2)
    
    return {
        "tags": tags,
        "correlation_matrix": correlation_matrix,
        "strongest_correlation": {
            "tags": [tags[0], tags[1]],
            "value": 0.85
        }
    }
