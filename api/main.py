"""
Jamin Industrial Agent - FastAPI 主应用
后端服务入口

作者: Backend Team
职责: API服务、业务逻辑、WebSocket实时通信
"""

# LEGACY ENTRYPOINT:
# Use `python -m src.api.main` or `uvicorn src.api.main:app ...` instead.
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from api.core.config import settings
from api.core.events import startup_event, shutdown_event
from api.core.security import JWTAuth
from api.routers import data, rules, alerts, diagnosis, dashboard, auth
from api.websocket.data_stream import DataStreamManager


# WebSocket 连接管理器
data_stream_manager = DataStreamManager()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """
    应用生命周期管理
    
    启动时:
    - 初始化数据库连接
    - 启动数据采集服务
    - 加载规则引擎
    - 初始化AI模型
    
    关闭时:
    - 停止数据采集
    - 关闭数据库连接
    - 释放资源
    """
    logger.info("=" * 60)
    logger.info("🚀 Jamin Industrial Agent API 启动中...")
    logger.info("=" * 60)
    
    try:
        # 启动事件
        await startup_event()
        
        # 启动数据流管理器
        await data_stream_manager.start()
        
        logger.info("✅ 所有服务已启动")
        yield
        
    except Exception as e:
        logger.error(f"❌ 启动失败: {e}")
        raise
    finally:
        # 关闭事件
        logger.info("🛑 正在关闭服务...")
        
        # 停止数据流
        await data_stream_manager.stop()
        
        # 清理资源
        await shutdown_event()
        
        logger.info("✅ 服务已安全关闭")


# 创建 FastAPI 应用
app = FastAPI(
    title="Jamin Industrial Agent API",
    description="工业智能监控与诊断系统 API",
    version="v1.0.0-beta2",
    docs_url="/api/docs" if settings.DEBUG else None,
    redoc_url="/api/redoc" if settings.DEBUG else None,
    openapi_url="/api/openapi.json" if settings.DEBUG else None,
    lifespan=lifespan
)

# 中间件配置
# 1. CORS 跨域
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. GZip 压缩
app.add_middleware(GZipMiddleware, minimum_size=1000)

# 3. 请求日志 (自定义中间件)
@app.middleware("http")
async def log_requests(request, call_next):
    """记录所有请求日志"""
    start_time = asyncio.get_event_loop().time()
    
    response = await call_next(request)
    
    process_time = asyncio.get_event_loop().time() - start_time
    
    logger.info(
        f"{request.method} {request.url.path} "
        f"- {response.status_code} "
        f"- {process_time:.3f}s"
    )
    
    return response


# 注册路由
# 1. 健康检查 (无需认证)
@app.get("/health", tags=["健康检查"])
async def health_check():
    """服务健康检查端点"""
    return {
        "status": "healthy",
        "version": "v1.0.0-beta2",
        "timestamp": asyncio.get_event_loop().time()
    }

# 2. 认证路由
app.include_router(
    auth.router,
    prefix="/api/v1/auth",
    tags=["认证"]
)

# 3. 数据路由 (需要认证)
app.include_router(
    data.router,
    prefix="/api/v1/data",
    tags=["数据采集"],
    dependencies=[Depends(JWTAuth())]
)

# 4. 规则路由
app.include_router(
    rules.router,
    prefix="/api/v1/rules",
    tags=["规则引擎"],
    dependencies=[Depends(JWTAuth())]
)

# 5. 告警路由
app.include_router(
    alerts.router,
    prefix="/api/v1/alerts",
    tags=["告警管理"],
    dependencies=[Depends(JWTAuth())]
)

# 6. 诊断路由
app.include_router(
    diagnosis.router,
    prefix="/api/v1/diagnosis",
    tags=["智能诊断"],
    dependencies=[Depends(JWTAuth())]
)

# 7. 大屏路由
app.include_router(
    dashboard.router,
    prefix="/api/v1/dashboard",
    tags=["监控大屏"],
    dependencies=[Depends(JWTAuth())]
)


# WebSocket 端点
@app.websocket("/ws/realtime")
async def realtime_websocket(websocket: WebSocket):
    """
    实时数据 WebSocket
    
    客户端连接后，服务端会主动推送:
    - 实时采集数据
    - 告警通知
    - 系统状态
    
    消息格式:
    {
        "type": "data|alert|status",
        "payload": {...},
        "timestamp": "2024-01-01T00:00:00Z"
    }
    """
    await data_stream_manager.connect(websocket)
    
    try:
        while True:
            # 接收客户端消息 (订阅控制、心跳等)
            message = await websocket.receive_json()
            
            # 处理客户端命令
            await data_stream_manager.handle_message(websocket, message)
            
    except WebSocketDisconnect:
        await data_stream_manager.disconnect(websocket)
        logger.info("客户端断开连接")
    except Exception as e:
        logger.error(f"WebSocket 错误: {e}")
        await data_stream_manager.disconnect(websocket)


# 全局异常处理
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """HTTP 异常处理"""
    logger.warning(f"HTTP {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "path": request.url.path
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """通用异常处理"""
    logger.exception(f"未处理的异常: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "内部服务器错误",
            "message": str(exc) if settings.DEBUG else "请联系管理员",
            "path": request.url.path
        }
    )


# 启动入口
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        workers=1 if settings.DEBUG else 4,
        log_level="info"
    )

