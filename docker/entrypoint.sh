#!/bin/bash
# 容器入口脚本

set -e

echo "======================================"
echo " Jamin Industrial Agent"
echo " Version: v1.0.0-beta2"
echo "======================================"

# 环境检查
echo "检查环境..."

# 检查必需的环境变量
if [ -z "$INFLUXDB_TOKEN" ]; then
    echo "警告: INFLUXDB_TOKEN 未设置"
fi

# 等待依赖服务
wait_for_service() {
    local host=$1
    local port=$2
    local service=$3
    local timeout=${4:-60}
    
    echo "等待 $service ($host:$port)..."
    
    for i in $(seq 1 $timeout); do
        if nc -z $host $port 2>/dev/null; then
            echo "✅ $service 已就绪"
            return 0
        fi
        sleep 1
    done
    
    echo "❌ $service 连接超时"
    return 1
}

# 根据环境类型执行不同操作
case "${1:-production}" in
    production)
        echo "启动生产环境..."
        
        # 等待数据库
        wait_for_service influxdb 8086 "InfluxDB"
        wait_for_service redis 6379 "Redis"
        
        # 创建日志目录
        mkdir -p /app/logs
        
        # 启动应用
        echo "启动应用..."
        exec gunicorn \
            -w ${WORKERS:-4} \
            -k uvicorn.workers.UvicornWorker \
            --bind 0.0.0.0:8000 \
            --access-logfile /app/logs/access.log \
            --error-logfile /app/logs/error.log \
            --log-level ${LOG_LEVEL:-info} \
            --timeout 60 \
            --keep-alive 5 \
            --max-requests 1000 \
            --max-requests-jitter 100 \
            src.main:app
        ;;
    
    development)
        echo "启动开发环境..."
        
        # 等待数据库（开发环境可选）
        wait_for_service influxdb 8086 "InfluxDB" 10 || true
        
        # 启动开发服务器
        exec python -m uvicorn \
            src.main:app \
            --host 0.0.0.0 \
            --port 8000 \
            --reload \
            --log-level debug
        ;;
    
    migrate)
        echo "执行数据库迁移..."
        python start.py --init-config
        echo "✅ 迁移完成"
        ;;
    
    test)
        echo "运行测试..."
        exec pytest tests/ -v --tb=short
        ;;
    
    shell)
        echo "进入Shell..."
        exec /bin/bash
        ;;
    
    *)
        echo "未知命令: $1"
        echo "可用命令: production, development, migrate, test, shell"
        exit 1
        ;;
esac

