# Makefile - Jamin Industrial Agent
# 
# 常用命令快捷方式

.PHONY: help install test lint format build deploy clean

# 默认目标
.DEFAULT_GOAL := help

# 变量
PYTHON := python3
PIP := pip3
DOCKER := docker
COMPOSE := docker-compose
PROJECT_NAME := miaota-agent
VERSION := v1.0.0-beta2

# 颜色定义
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

## help: 显示帮助信息
help:
	@echo "$(BLUE)Jamin Industrial Agent - 可用命令$(NC)"
	@echo ""
	@echo "$(GREEN)开发命令:$(NC)"
	@echo "  make install          安装依赖"
	@echo "  make install-dev      安装开发依赖"
	@echo "  make test             运行测试"
	@echo "  make test-cov         运行测试并生成覆盖率报告"
	@echo "  make lint             代码检查"
	@echo "  make format           格式化代码"
	@echo ""
	@echo "$(GREEN)构建命令:$(NC)"
	@echo "  make build            构建Docker镜像"
	@echo "  make build-dev        构建开发镜像"
	@echo ""
	@echo "$(GREEN)运行命令:$(NC)"
	@echo "  make run              本地运行开发服务器"
	@echo "  make run-demo         运行演示模式"
	@echo "  make deploy           部署生产环境"
	@echo "  make deploy-dev       部署开发环境"
	@echo ""
	@echo "$(GREEN)维护命令:$(NC)"
	@echo "  make logs             查看日志"
	@echo "  make shell            进入容器Shell"
	@echo "  make migrate          执行数据库迁移"
	@echo "  make backup           备份数据"
	@echo "  make restore          恢复数据"
	@echo "  make clean            清理临时文件"
	@echo "  make clean-all        清理所有数据"
	@echo ""
	@echo "$(GREEN)监控命令:$(NC)"
	@echo "  make monitor-up       启动监控栈"
	@echo "  make monitor-down     停止监控栈"
	@echo "  make status           查看服务状态"

## install: 安装生产依赖
install:
	@echo "$(BLUE)安装生产依赖...$(NC)"
	$(PIP) install -r requirements.txt

## install-dev: 安装开发依赖
install-dev: install
	@echo "$(BLUE)安装开发依赖...$(NC)"
	$(PIP) install -r requirements-dev.txt

## test: 运行测试
test:
	@echo "$(BLUE)运行测试...$(NC)"
	pytest tests/ -v --tb=short

## test-cov: 运行测试并生成覆盖率报告
test-cov:
	@echo "$(BLUE)运行测试并生成覆盖率报告...$(NC)"
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term
	@echo "$(GREEN)覆盖率报告已生成: htmlcov/index.html$(NC)"

## lint: 代码检查
lint:
	@echo "$(BLUE)运行代码检查...$(NC)"
	@echo "$(YELLOW)Flake8 检查...$(NC)"
	-flake8 src/ --max-line-length=100 --ignore=E501,W503
	@echo "$(YELLOW)Pylint 检查...$(NC)"
	-pylint src/ --disable=C0103,C0111,R0903 --max-line-length=100
	@echo "$(YELLOW)Mypy 类型检查...$(NC)"
	-mypy src/ --ignore-missing-imports

## format: 格式化代码
format:
	@echo "$(BLUE)格式化代码...$(NC)"
	@echo "$(YELLOW)使用 Black 格式化...$(NC)"
	black src/ tests/ --line-length=100
	@echo "$(YELLOW)使用 isort 排序导入...$(NC)"
	isort src/ tests/ --profile black

## build: 构建Docker生产镜像
build:
	@echo "$(BLUE)构建 Docker 生产镜像...$(NC)"
	$(DOCKER) build -f docker/Dockerfile -t $(PROJECT_NAME):$(VERSION) --target production .
	$(DOCKER) tag $(PROJECT_NAME):$(VERSION) $(PROJECT_NAME):latest

## build-dev: 构建Docker开发镜像
build-dev:
	@echo "$(BLUE)构建 Docker 开发镜像...$(NC)"
	$(DOCKER) build -f docker/Dockerfile -t $(PROJECT_NAME):dev --target development .

## run: 本地运行开发服务器
run:
	@echo "$(BLUE)启动开发服务器...$(NC)"
	$(PYTHON) start.py --demo

## run-demo: 运行演示模式
run-demo:
	@echo "$(BLUE)启动演示模式...$(NC)"
	$(PYTHON) start.py --demo

## deploy: 部署生产环境
deploy:
	@echo "$(BLUE)部署生产环境...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml up -d
	@echo "$(GREEN)生产环境已启动$(NC)"
	@echo "访问地址:"
	@echo "  - 应用: http://localhost"
	@echo "  - Grafana: http://localhost:3000"
	@echo "  - Prometheus: http://localhost:9090"

## deploy-dev: 部署开发环境
deploy-dev:
	@echo "$(BLUE)部署开发环境...$(NC)"
	$(COMPOSE) -f docker/docker-compose.yml up -d
	@echo "$(GREEN)开发环境已启动$(NC)"

## stop: 停止服务
stop:
	@echo "$(BLUE)停止服务...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml down

## logs: 查看日志
logs:
	@echo "$(BLUE)查看日志...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml logs -f --tail=100

## shell: 进入容器Shell
shell:
	@echo "$(BLUE)进入容器 Shell...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml exec app /bin/bash

## migrate: 执行数据库迁移
migrate:
	@echo "$(BLUE)执行数据库迁移...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml exec app python start.py --init-config

## backup: 备份数据
backup:
	@echo "$(BLUE)备份数据...$(NC)"
	@mkdir -p backups/$(shell date +%Y%m%d)
	$(COMPOSE) -f docker/docker-compose.prod.yml exec influxdb influx backup /tmp/backup
	$(DOCKER) cp miaota-influxdb:/tmp/backup backups/$(shell date +%Y%m%d)/influxdb
	@echo "$(GREEN)备份完成: backups/$(shell date +%Y%m%d)/$(NC)"

## restore: 恢复数据 (BACKUP_DATE=20240115 make restore)
restore:
	@if [ -z "$(BACKUP_DATE)" ]; then \
		echo "$(RED)错误: 请指定 BACKUP_DATE 参数$(NC)"; \
		echo "用法: BACKUP_DATE=20240115 make restore"; \
		exit 1; \
	fi
	@echo "$(BLUE)恢复数据 $(BACKUP_DATE)...$(NC)"
	$(DOCKER) cp backups/$(BACKUP_DATE)/influxdb miaota-influxdb:/tmp/backup
	$(COMPOSE) -f docker/docker-compose.prod.yml exec influxdb influx restore /tmp/backup
	@echo "$(GREEN)数据恢复完成$(NC)"

## monitor-up: 启动监控栈
monitor-up:
	@echo "$(BLUE)启动监控栈...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml up -d prometheus grafana alertmanager
	@echo "$(GREEN)监控栈已启动$(NC)"

## monitor-down: 停止监控栈
monitor-down:
	@echo "$(BLUE)停止监控栈...$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml stop prometheus grafana alertmanager

## status: 查看服务状态
status:
	@echo "$(BLUE)服务状态:$(NC)"
	$(COMPOSE) -f docker/docker-compose.prod.yml ps

## health: 健康检查
health:
	@echo "$(BLUE)执行健康检查...$(NC)"
	@curl -s http://localhost/health | $(PYTHON) -m json.tool || echo "$(RED)健康检查失败$(NC)"

## clean: 清理临时文件
clean:
	@echo "$(BLUE)清理临时文件...$(NC)"
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".coverage" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ .tox/ dist/ build/
	@echo "$(GREEN)清理完成$(NC)"

## clean-all: 清理所有数据（包括卷）
clean-all: clean
	@echo "$(RED)警告: 这将删除所有数据！$(NC)"
	@read -p "确定继续? [y/N] " confirm && [ $$confirm = y ] || exit 1
	$(COMPOSE) -f docker/docker-compose.prod.yml down -v
	$(DOCKER) system prune -f
	@echo "$(GREEN)所有数据已清理$(NC)"

## update: 更新依赖
update:
	@echo "$(BLUE)更新依赖...$(NC)"
	$(PIP) install --upgrade -r requirements.txt
	$(PIP) freeze > requirements.lock

## security-check: 安全检查
security-check:
	@echo "$(BLUE)运行安全检查...$(NC)"
	@echo "$(YELLOW)检查依赖漏洞...$(NC)"
	-safety check
	@echo "$(YELLOW)检查代码安全问题...$(NC)"
	-bandit -r src/ -f json -o security-report.json || true
	@echo "$(GREEN)安全检查完成，报告: security-report.json$(NC)"

## load-test: 压力测试
load-test:
	@echo "$(BLUE)运行压力测试...$(NC)"
	@echo "$(YELLOW)启动 Locust...$(NC)"
	locust -f tests/load/locustfile.py --host=http://localhost

## docs: 生成文档
docs:
	@echo "$(BLUE)生成文档...$(NC)"
	cd docs && make html
	@echo "$(GREEN)文档已生成: docs/_build/html/index.html$(NC)"

## ci: CI流水线（在CI环境中运行）
ci: lint test security-check
	@echo "$(GREEN)CI检查完成$(NC)"

## release: 发布新版本
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "$(RED)错误: 请指定 VERSION 参数$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)发布版本 $(VERSION)...$(NC)"
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)
	$(DOCKER) build -f docker/Dockerfile -t $(PROJECT_NAME):$(VERSION) --target production .
	@echo "$(GREEN)版本 $(VERSION) 已发布$(NC)"

