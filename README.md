# Jamin Industrial Agent

Current active backend entrypoint: `src.api.main`

Enterprise readiness roadmap: [`docs/enterprise_readiness_roadmap_20260407.md`](docs/enterprise_readiness_roadmap_20260407.md)

面向工业场景的智能监控、告警分析、多智能体诊断与知识增强平台。

项目当前已经形成一条可试点落地的诊断闭环：
- 设备与告警数据接入
- GraphRAG 增强诊断
- 多专家并行分析
- CAMEL 多轮协作推理
- 长任务持久化追踪
- 前端中文控制台与报告导出

## 当前定位

项目分两阶段推进：

1. 智能诊断平台商用化
   目标是把多智能体诊断、GraphRAG、长任务、报告和可观测性做成可试点、可演示、可追溯的产品。
2. 全工业平台商用化
   在诊断平台基础上，继续完善设备采集、规则监控、告警闭环、多租户、安全审计与运维治理。

## 已落地的核心能力

### 1. 多智能体诊断
- 标准诊断链：`GraphRAG -> 5 位专家并行分析 -> 协调者整合 -> 建议/备件/场景`
- CAMEL 协作链：支持真实多轮重推理，而不是单轮意见复用
- 支持按智能体绑定不同模型与不同端点
- 支持回退策略、共识度、置信度和执行轨迹

### 2. GraphRAG 知识增强
- 工业知识图谱检索与相似案例摘要
- 诊断结果中稳定暴露 `related_cases`、查询摘要和调试信息
- 失败时显式标记降级，而不是静默回退

### 3. 长任务工作流
- 诊断任务支持异步提交、持久化保存、恢复查看、实时追踪
- 支持 SSE 实时事件流
- 支持任务列表、任务详情、报告导出

### 4. 数据与运行时底座
- 任务持久化支持 SQLite / Postgres
- 元数据数据库支持 SQLite / Postgres
- 设备、告警、规则已支持数据库仓储
- 首页和诊断页会显示真实运行时后端

### 5. 前端产品化
- 首页 Dashboard：任务概览、数据库运行态、设备快照、最近告警
- 诊断页：任务追踪、智能体配置、执行时间线、协作拓扑、专家意见、轮次回放、结果摘要
- 告警页支持一键发起多智能体诊断

## 系统架构

```text
应用层
  Dashboard / Alerts / Diagnosis / Reports

智能层
  Multi-Agent Diagnosis / CAMEL / GraphRAG / Forecast / Anomaly Detection

数据层
  Collectors / Time-Series Storage / Vector Store / Metadata DB / Task Store

设备层
  PLC / Sensors / Actuators
```

## 主要接口

当前对外重点接口如下：

- `POST /v2/diagnosis/analyze`
- `GET /v2/diagnosis/task/{task_id}`
- `GET /v2/diagnosis/task/{task_id}/events`
- `GET /v2/diagnosis/tasks`
- `GET /v2/diagnosis/history`
- `GET /v2/diagnosis/experts`
- `POST /v2/diagnosis/model-probe`
- `POST /v2/diagnosis/alerts/{alert_id}/analyze`

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/jamin85cheng/jamin_industrial_agent.git
cd jamin_industrial_agent
```

### 2. 安装依赖

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

前端：

```bash
cd web
npm install
```

### 3. 配置环境变量

```bash
copy .env.example .env
```

至少需要关注：
- `JWT_SECRET`
- `DEMO_API_KEY`（仅在启用 API Key 演示访问时需要）
- `DIAGNOSIS_EXECUTION_BACKEND`（可选 `background_tasks` 或 `asyncio_queue`）
- `DIAGNOSIS_EXECUTION_ASYNCIO_WORKERS`（仅 `asyncio_queue` 后端生效）
- `DIAGNOSIS_EXECUTION_AUTO_RESUME_RECOVERED`（仅 `asyncio_queue` 后端下生效，用于自动接管恢复任务）

### 4. 配置数据库

默认可使用 SQLite。

如需切换到 Postgres，可在 `config/settings.yaml` 中配置：

```yaml
database:
  postgres:
    enabled: true
    host: 127.0.0.1
    port: 5432
    user: postgres
    password: postgres
    database: jamin_industrial_agent
    schema: jamin_industrial_agent
```

### 5. 启动后端

```bash
python -m src.api.main
```

### 6. 启动前端

```bash
cd web
npm run dev
```

## 推荐运行地址

- 后端 OpenAPI: [http://localhost:8600/docs](http://localhost:8600/docs)
- 前端开发页: [http://localhost:3228](http://localhost:3228)

## 诊断调用示例

```bash
curl -X POST http://localhost:8600/v2/diagnosis/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "symptoms": "曝气池溶解氧持续偏低，风机噪声异常",
    "sensor_data": {
      "do": 1.5,
      "vibration": 8.5,
      "current": 25.3
    },
    "use_multi_agent": true,
    "use_graph_rag": true,
    "use_camel": false,
    "debug": true
  }'
```

## 目录说明

```text
src/
  api/            FastAPI 路由与依赖
  diagnosis/      标准多智能体诊断引擎
  agents/         CAMEL 协作与智能体运行时
  knowledge/      GraphRAG 与知识增强
  tasks/          任务追踪与持久化后端
  utils/          配置、日志、数据库适配

web/
  src/pages/      Dashboard / Alerts / Diagnosis 等页面
  src/lib/        API 请求与前端类型

tests/unit/
  多智能体诊断、任务持久化、数据库适配、设备与告警仓储测试
```

## 当前最适合继续完善的方向

- 统一剩余历史模块文案和日志编码
- 继续压第三方 warning 与测试噪音
- 增强诊断报告下载体验和失败后恢复操作
- 引入更强的任务后端适配器，如 Redis / Celery / Temporal
- 继续收口设备采集、告警中心与诊断闭环

## 说明

- 项目中“受 MiroFish 启发”的部分指多智能体协作诊断思路，不是外部依赖。
- `src/api/data/` 属于历史运行目录，不建议继续作为运行时数据根目录。
- 推荐统一使用仓库根目录下的 `data/` 作为运行期数据目录。

## Runtime Database Init

PostgreSQL is now the production-standard runtime database. Initialization commands and the new `scripts/manage_database.py` workflow are documented in [`docs/runtime_database_initialization_20260408.md`](docs/runtime_database_initialization_20260408.md).
The old `migrations/migration_manager.py` path is now legacy and should only be kept for historical SQLite-only workflows.

Quick start:

```powershell
.venv\Scripts\python.exe scripts\manage_database.py init
```

Use this variant if you also want demo devices and demo alerts:

```powershell
.venv\Scripts\python.exe scripts\manage_database.py init --include-demo-data
```

For development-only manual demo/bootstrap actions outside the main runtime init flow:

```powershell
.venv\Scripts\python.exe scripts\dev_runtime_bootstrap.py all
```

## License

MIT
