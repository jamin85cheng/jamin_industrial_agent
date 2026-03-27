# Dockerfile - 多阶段构建
# 
# 构建生产镜像:
#   docker build -f docker/Dockerfile -t miaota/industrial-agent:v1.0.0-beta2 .

# =================== 阶段1: 基础依赖 ===================
FROM python:3.11-slim as base

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# =================== 阶段2: 依赖安装 ===================
FROM base as dependencies

# 复制依赖文件
COPY requirements.txt requirements-dev.txt ./

# 安装生产依赖
RUN pip install --user --no-cache-dir -r requirements.txt

# 安装额外依赖（可选）
RUN pip install --user --no-cache-dir \
    cryptography \
    redis \
    prometheus-client

# =================== 阶段3: 开发环境 ===================
FROM base as development

# 复制依赖
COPY --from=dependencies /root/.local /root/.local

# 设置PATH
ENV PATH=/root/.local/bin:$PATH

# 安装开发工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    vim \
    htop \
    && rm -rf /var/lib/apt/lists/*

# 复制代码（挂载卷优先）
COPY . .

# 暴露端口
EXPOSE 8000

# 开发模式启动
CMD ["python", "src/main.py", "--reload", "--host", "0.0.0.0", "--port", "8000"]

# =================== 阶段4: 生产环境 ===================
FROM base as production

# 创建非root用户
RUN groupadd -r miaota && useradd -r -g miaota miaota

# 复制依赖
COPY --from=dependencies /root/.local /root/.local

# 设置PATH
ENV PATH=/root/.local/bin:$PATH

# 创建必要目录
RUN mkdir -p /app/logs /app/data /app/config && \
    chown -R miaota:miaota /app

# 复制代码
COPY --chown=miaota:miaota src/ ./src/
COPY --chown=miaota:miaota security/ ./security/
COPY --chown=miaota:miaota start.py ./
COPY --chown=miaota:miaota config/ ./config/

# 复制启动脚本
COPY --chown=miaota:miaota docker/entrypoint.sh ./
RUN chmod +x entrypoint.sh

# 切换到非root用户
USER miaota

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 暴露端口
EXPOSE 8000

# 入口点
ENTRYPOINT ["./entrypoint.sh"]
CMD ["production"]

# =================== 阶段5: 测试环境 ===================
FROM dependencies as test

# 安装测试依赖
RUN pip install --user --no-cache-dir -r requirements-dev.txt

# 设置PATH
ENV PATH=/root/.local/bin:$PATH

# 复制代码
COPY . .

# 默认执行测试
CMD ["pytest", "-v", "--cov=src", "--cov-report=html"]

# =================== 元数据 ===================
LABEL maintainer="Jamin Cheng <jamin85cheng@users.noreply.github.com>" \
      version="v1.0.0-beta2" \
      description="Jamin Industrial Agent - 工业智能监控与诊断系统"

