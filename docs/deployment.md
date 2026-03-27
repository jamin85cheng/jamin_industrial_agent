# 部署指南

**版本**: v1.0.0-beta2 (MiroFish)

**目标读者**: 运维工程师、DevOps工程师

---

## 📋 目录

1. [系统要求](#系统要求)
2. [快速部署](#快速部署)
3. [Docker部署](#docker部署)
4. [生产环境配置](#生产环境配置)
5. [V2新特性部署](#v2新特性部署)
6. [监控与运维](#监控与运维)

---

## 系统要求

### 最低配置

| 组件 | 配置 | 说明 |
|------|------|------|
| CPU | 2核 | 基础功能运行 |
| 内存 | 4GB | 基础功能运行 |
| 磁盘 | 20GB | 数据存储 |
| Python | 3.11+ | 运行环境 |

### 推荐配置 (V2完整功能)

| 组件 | 配置 | 说明 |
|------|------|------|
| CPU | 4核+ | 多智能体诊断并行计算 |
| 内存 | 8GB+ | 知识图谱、CAMEL社会 |
| 磁盘 | 50GB+ SSD | 时序数据、知识库存储 |
| Python | 3.11+ | 运行环境 |
| Neo4j | 可选 | 知识图谱持久化 |

---

## 快速部署

### 1. 环境准备

```bash
# 安装系统依赖 (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y python3.11 python3.11-venv python3-pip git

# 创建应用目录
sudo mkdir -p /opt/miaota
sudo chown $USER:$USER /opt/miaota
cd /opt/miaota
```

### 2. 代码部署

```bash
# 克隆代码
git clone https://github.com/jamin85cheng/miaota_industrial_agent.git .

# 创建虚拟环境
python3.11 -m venv venv
source venv/bin/activate

# 安装依赖
pip install --upgrade pip
pip install -r requirements.txt

# 可选: 安装CAMEL框架（用于高级多智能体功能）
pip install camel-ai
```

### 3. 初始化配置

```bash
# 创建数据目录
mkdir -p data logs config

# 初始化数据库
python migrations/migration_manager.py init
python migrations/migration_manager.py migrate

# 生成配置文件
python -c "
import yaml
config = {
    'app': {
        'name': 'Jamin Industrial Agent',
        'version': 'v1.0.0-beta2',
        'debug': False
    },
    'database': {
        'path': 'data/miaota.db'
    },
    'influxdb': {
        'url': 'http://localhost:8086',
        'token': 'your-token',
        'org': 'miaota',
        'bucket': 'industrial_data'
    },
    'diagnosis': {
        'v2': {
            'enabled': True,
            'default_mode': 'multi_agent'
        }
    },
    'security': {
        'jwt_secret': 'your-secret-key-change-in-production',
        'token_expire_minutes': 30
    }
}
with open('config/settings.yaml', 'w') as f:
    yaml.dump(config, f)
"
```

### 4. 启动服务

```bash
# 开发模式
python -m src.api.main

# 生产模式 (使用Gunicorn)
pip install gunicorn
gunicorn src.api.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --access-logfile logs/access.log \
  --error-logfile logs/error.log
```

---

## Docker部署

### 使用 Docker Compose (推荐)

#### 1. 创建 docker-compose.yml

```yaml
version: '3.8'

services:
  # Jamin API服务
  api:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    environment:
      - APP_ENV=production
      - DATABASE_URL=sqlite:///app/data/miaota.db
      - INFLUXDB_URL=http://influxdb:8086
    depends_on:
      - influxdb
      - redis
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G

  # 前端静态文件 (Nginx)
  web:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./src/web/static:/usr/share/nginx/html:ro
      - ./deploy/nginx.conf:/etc/nginx/conf.d/default.conf:ro
    depends_on:
      - api
    restart: unless-stopped

  # 时序数据库
  influxdb:
    image: influxdb:2.7
    ports:
      - "8086:8086"
    volumes:
      - influxdb-data:/var/lib/influxdb2
    environment:
      - DOCKER_INFLUXDB_INIT_MODE=setup
      - DOCKER_INFLUXDB_INIT_USERNAME=admin
      - DOCKER_INFLUXDB_INIT_PASSWORD=admin123
      - DOCKER_INFLUXDB_INIT_ORG=miaota
      - DOCKER_INFLUXDB_INIT_BUCKET=industrial_data
    restart: unless-stopped

  # 缓存与消息队列
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    restart: unless-stopped

  # 可选: 知识图谱数据库
  neo4j:
    image: neo4j:5-community
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - neo4j-data:/data
    environment:
      - NEO4J_AUTH=neo4j/password
      - NEO4J_PLUGINS=["apoc"]
    restart: unless-stopped

volumes:
  influxdb-data:
  redis-data:
  neo4j-data:
```

#### 2. 创建 Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# 安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 可选: 安装CAMEL
RUN pip install --no-cache-dir camel-ai

# 复制代码
COPY . .

# 创建数据目录
RUN mkdir -p data logs

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### 3. 部署

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f api

# 执行数据库迁移
docker-compose exec api python migrations/migration_manager.py migrate

# 停止
docker-compose down
```

---

## 生产环境配置

### 1. Nginx 反向代理

```nginx
# /etc/nginx/sites-available/miaota
server {
    listen 80;
    server_name your-domain.com;
    
    # 重定向到HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;

    # SSL证书
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    # 静态文件
    location /static/ {
        alias /opt/miaota/src/web/static/;
        expires 30d;
    }

    # API代理
    location / {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        
        # 超时设置
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
```

### 2. 系统服务配置

```bash
# /etc/systemd/system/miaota.service
sudo tee /etc/systemd/system/miaota.service > /dev/null <<EOF
[Unit]
Description=Jamin Industrial Agent
After=network.target

[Service]
Type=simple
User=miaota
Group=miaota
WorkingDirectory=/opt/miaota
Environment=PATH=/opt/miaota/venv/bin
Environment=PYTHONPATH=/opt/miaota
Environment=APP_ENV=production
ExecStart=/opt/miaota/venv/bin/gunicorn src.api.main:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000 \
    --access-logfile /opt/miaota/logs/access.log \
    --error-logfile /opt/miaota/logs/error.log
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# 启用服务
sudo systemctl daemon-reload
sudo systemctl enable miaota
sudo systemctl start miaota
sudo systemctl status miaota
```

### 3. 安全配置

```bash
# 设置文件权限
sudo chown -R miaota:miaota /opt/miaota
sudo chmod 600 /opt/miaota/config/settings.yaml
sudo chmod 750 /opt/miaota/data

# 配置防火墙
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable

# 配置Fail2Ban防止暴力破解
sudo apt-get install fail2ban
sudo tee /etc/fail2ban/jail.local > /dev/null <<EOF
[miaota-api]
enabled = true
port = http,https
filter = miaota-api
logpath = /opt/miaota/logs/access.log
maxretry = 5
bantime = 3600
EOF
```

---

## V2新特性部署

### 知识图谱 (Neo4j)

#### 1. 安装Neo4j

```bash
# Docker方式
docker run -d \
  --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -v /opt/neo4j/data:/data \
  -e NEO4J_AUTH=neo4j/your-password \
  neo4j:5-community

# 或 apt安装
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
echo 'deb https://debian.neo4j.com stable 5' | sudo tee /etc/apt/sources.list.d/neo4j.list
sudo apt-get update
sudo apt-get install neo4j
sudo systemctl enable neo4j
sudo systemctl start neo4j
```

#### 2. 配置连接

```yaml
# config/settings.yaml
knowledge_graph:
  enabled: true
  backend: "neo4j"  # memory 或 neo4j
  neo4j:
    uri: "bolt://localhost:7687"
    user: "neo4j"
    password: "your-password"
```

### CAMEL框架配置

```bash
# 安装CAMEL
pip install camel-ai

# 配置LLM (以OpenAI为例)
export OPENAI_API_KEY="your-api-key"

# 或配置其他LLM (Qwen/ChatGLM等)
export LLM_BASE_URL="https://api.your-llm.com/v1"
export LLM_API_KEY="your-api-key"
export LLM_MODEL="qwen-turbo"
```

### 任务追踪配置

```yaml
# config/settings.yaml
task_tracker:
  max_concurrent: 10        # 最大并发任务数
  default_timeout: 3600     # 默认超时(秒)
  cleanup_interval: 86400   # 清理间隔(秒)
  retention_days: 30        # 任务保留天数
```

---

## 监控与运维

### 1. 日志管理

```bash
# 使用logrotate管理日志
sudo tee /etc/logrotate.d/miaota > /dev/null <<EOF
/opt/miaota/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0644 miaota miaota
    sharedscripts
    postrotate
        systemctl reload miaota
    endscript
}
EOF
```

### 2. 健康检查

```bash
# 添加到crontab每分钟检查
* * * * * /opt/miaota/scripts/health_check.sh

# health_check.sh内容:
#!/bin/bash
HEALTH=$(curl -sf http://localhost:8000/health || echo "FAIL")
if [ "$HEALTH" = "FAIL" ]; then
    systemctl restart miaota
    echo "$(date): Service restarted" >> /opt/miaota/logs/health.log
fi
```

### 3. 备份策略

```bash
# 数据库备份脚本
#!/bin/bash
BACKUP_DIR="/backup/miaota/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# 备份SQLite
cp /opt/miaota/data/miaota.db $BACKUP_DIR/

# 备份配置
cp -r /opt/miaota/config $BACKUP_DIR/

# 备份InfluxDB (如果使用)
docker exec influxdb influx backup /tmp/backup
docker cp influxdb:/tmp/backup $BACKUP_DIR/influxdb

# 压缩并上传到远程
 tar czf $BACKUP_DIR.tar.gz $BACKUP_DIR
# scp or rsync to remote server
```

### 4. 性能监控

```bash
# 安装Prometheus + Grafana监控
# prometheus.yml添加:
scrape_configs:
  - job_name: 'miaota'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: /metrics
    scrape_interval: 15s
```

### 5. 常用运维命令

```bash
# 查看服务状态
sudo systemctl status miaota

# 重启服务
sudo systemctl restart miaota

# 查看日志
sudo tail -f /opt/miaota/logs/error.log

# 查看API访问日志
sudo tail -f /opt/miaota/logs/access.log

# 检查端口
netstat -tlnp | grep 8000

# 查看资源使用
htop

# 数据库迁移
python migrations/migration_manager.py status
python migrations/migration_manager.py migrate
```

---

## 升级指南

### 从 v1.0.0-beta1 升级到 v1.0.0-beta2

```bash
# 1. 备份现有数据
cp -r /opt/miaota/data /opt/miaota/data-backup-$(date +%Y%m%d)

# 2. 拉取新版本代码
cd /opt/miaota
git pull origin main

# 3. 更新依赖
source venv/bin/activate
pip install -r requirements.txt
pip install camel-ai  # V2新依赖

# 4. 执行数据库迁移
python migrations/migration_manager.py migrate

# 5. 更新配置
cp config/settings.yaml config/settings.yaml.bak
# 手动合并新配置项 (参考CHANGELOG.md)

# 6. 重启服务
sudo systemctl restart miaota

# 7. 验证
curl http://localhost:8000/version
# 应返回: {"version": "v1.0.0-beta2", ...}
```

---

## 故障排查

### 问题1: 服务启动失败

```bash
# 检查Python版本
python3 --version  # 需要3.11+

# 检查依赖
pip list | grep -E "fastapi|uvicorn"

# 检查端口占用
sudo lsof -i :8000

# 查看详细错误
python -m src.api.main 2>&1 | tee startup.log
```

### 问题2: 数据库连接失败

```bash
# 检查数据库文件权限
ls -la data/miaota.db

# 检查数据库版本
python migrations/migration_manager.py status

# 重新初始化（数据会丢失！）
rm data/miaota.db
python migrations/migration_manager.py init
python migrations/migration_manager.py migrate
```

### 问题3: V2诊断功能异常

```bash
# 检查知识图谱状态
curl http://localhost:8000/v2/diagnosis/knowledge/graph

# 检查CAMEL依赖
pip list | grep camel

# 检查任务追踪器
python -c "from src.tasks import task_tracker; print(task_tracker.get_stats())"
```

---

## 附录

### 环境变量参考

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `APP_ENV` | 运行环境 | `development` |
| `DATABASE_URL` | 数据库连接 | `sqlite:///data/miaota.db` |
| `INFLUXDB_URL` | InfluxDB地址 | `http://localhost:8086` |
| `REDIS_URL` | Redis地址 | `redis://localhost:6379` |
| `JWT_SECRET` | JWT密钥 | `change-in-production` |
| `OPENAI_API_KEY` | OpenAI API Key | - |
| `NEO4J_URI` | Neo4j连接URI | - |

### 端口说明

| 端口 | 服务 | 说明 |
|------|------|------|
| 8000 | API服务 | 主应用端口 |
| 80 | Nginx | Web服务器 |
| 443 | Nginx HTTPS | SSL加密 |
| 8086 | InfluxDB | 时序数据库 |
| 6379 | Redis | 缓存/队列 |
| 7474 | Neo4j Browser | 图数据库UI |
| 7687 | Neo4j Bolt | 图数据库协议 |

---

## 🙏 致谢

感谢以下开源项目为部署和运维提供支持：

- [Docker](https://www.docker.com/) - 容器化平台
- [Nginx](https://nginx.org/) - Web服务器与反向代理
- [Gunicorn](https://gunicorn.org/) - WSGI HTTP服务器
- [Neo4j](https://neo4j.com/) - 图数据库 (知识图谱)
- [InfluxDB](https://www.influxdata.com/) - 时序数据库
- [Redis](https://redis.io/) - 缓存与消息队列
- [Prometheus](https://prometheus.io/) - 监控系统
- [Grafana](https://grafana.com/) - 可视化监控

---

**版本**: v1.0.0-beta2 (MiroFish) | **最后更新**: 2026-03-27

