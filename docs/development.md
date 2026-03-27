# 开发指南

**版本**: v1.0.0-beta2 (MiroFish)

**目标读者**: 开发者、贡献者

---

## 📋 目录

1. [开发环境搭建](#开发环境搭建)
2. [代码规范](#代码规范)
3. [项目结构](#项目结构)
4. [V2新模块开发](#v2新模块开发)
5. [测试指南](#测试指南)
6. [贡献指南](#贡献指南)

---

## 开发环境搭建

### 1. 克隆项目

```bash
git clone https://github.com/jamin85cheng/jamin_industrial_agent.git
cd jamin_industrial_agent
```

### 2. 创建虚拟环境

```bash
# 使用Python 3.11+
python3.11 -m venv venv
source venv/bin/activate

# Windows
# python -m venv venv
# venv\Scripts\activate
```

### 3. 安装开发依赖

```bash
# 基础依赖
pip install -r requirements.txt

# 开发工具
pip install pytest pytest-asyncio pytest-cov black isort flake8 mypy

# V2新特性依赖 (可选)
pip install camel-ai neo4j
```

### 4. 配置开发环境

```bash
# 创建开发配置
cp config/settings.example.yaml config/settings.yaml
# 编辑配置文件，修改数据库路径等

# 初始化数据库
python migrations/migration_manager.py init
python migrations/migration_manager.py migrate
```

### 5. 启动开发服务器

```bash
# 热重载模式
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

# 或使用启动脚本
python start.py --dev
```

---

## 代码规范

### Python代码风格

使用 **Black** 进行代码格式化：

```bash
# 格式化代码
black src/ tests/

# 检查格式
black --check src/ tests/
```

### 导入排序

使用 **isort**：

```bash
# 排序导入
isort src/ tests/

# 检查导入顺序
isort --check-only src/ tests/
```

### 代码检查

使用 **Flake8**：

```bash
# 代码检查
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203
```

### 类型检查

使用 **MyPy**：

```bash
# 类型检查
mypy src/ --ignore-missing-imports
```

### 预提交钩子

```bash
# 安装pre-commit
pip install pre-commit
pre-commit install

# .pre-commit-config.yaml示例
repos:
  - repo: https://github.com/psf/black
    rev: 23.1.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort

  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
```

---

## 项目结构

```
jamin_industrial_agent/
├── src/
│   ├── api/                      # 🔷 FastAPI后端
│   │   ├── main.py               # 应用入口
│   │   ├── dependencies.py       # 依赖注入
│   │   ├── middleware/           # 中间件
│   │   └── routers/              # API路由
│   │       ├── health.py
│   │       ├── devices.py
│   │       ├── collection.py
│   │       ├── alerts.py
│   │       ├── analysis.py
│   │       ├── knowledge.py
│   │       └── diagnosis_v2.py   # 🆕 V2智能诊断
│   │
│   ├── diagnosis/                # 🆕 多智能体诊断 [v1.0.0-beta2]
│   │   ├── __init__.py
│   │   └── multi_agent_diagnosis.py   # 651行
│   │       # - MultiAgentDiagnosisEngine
│   │       # - LLMExpertAgent
│   │       # - ExpertOpinion
│   │
│   ├── knowledge/                # 🆕 GraphRAG [v1.0.0-beta2]
│   │   ├── __init__.py
│   │   └── graph_rag.py          # 579行
│   │       # - KnowledgeGraph
│   │       # - GraphRAG
│   │       # - Entity/Relation
│   │
│   ├── agents/                   # 🆕 CAMEL框架 [v1.0.0-beta2]
│   │   ├── __init__.py
│   │   └── camel_integration.py  # 526行
│   │       # - CamelAgent
│   │       # - CamelSociety
│   │       # - IndustrialDiagnosisSociety
│   │
│   ├── tasks/                    # 🆕 任务追踪 [v1.0.0-beta2]
│   │   ├── __init__.py
│   │   └── task_tracker.py       # 501行
│   │       # - TaskTracker
│   │       # - TrackedTask
│   │       # - TaskProgress
│   │
│   ├── security/                 # 🔐 安全模块
│   │   ├── rbac.py               # 权限控制
│   │   ├── auth.py               # 认证
│   │   └── multitenancy.py       # 多租户
│   │
│   ├── core/                     # 核心功能
│   ├── data/                     # 数据采集/存储
│   ├── rules/                    # 规则引擎
│   ├── models/                   # AI模型
│   └── utils/                    # 工具函数
│       ├── structured_logging.py
│       ├── thread_safe.py
│       └── graceful_shutdown.py
│
├── tests/                        # 🧪 测试
│   ├── unit/                     # 单元测试
│   │   ├── test_multi_agent.py   # 🆕
│   │   ├── test_graph_rag.py     # 🆕
│   │   └── ...
│   ├── integration/              # 集成测试
│   ├── load/                     # 压力测试 (Locust)
│   └── conftest.py               # pytest配置
│
├── docs/                         # 📚 文档
│   ├── api_reference.md
│   ├── user_manual.md
│   ├── deployment.md
│   └── development.md
│
├── migrations/                   # 数据库迁移
├── scripts/                      # 工具脚本
├── config/                       # 配置文件
├── CHANGELOG.md                  # 🆕 版本日志
└── README.md
```

---

## V2新模块开发

### 1. 多智能体诊断模块

#### 添加新专家Agent

```python
# src/diagnosis/multi_agent_diagnosis.py

class MultiAgentDiagnosisEngine:
    def _init_experts(self):
        # 添加新专家配置
        new_expert_config = {
            "type": ExpertType.NEW_DOMAIN,
            "name": "新领域专家",
            "prompt": """你是新领域诊断专家...
            
你的专长包括：
- 专长1
- 专长2

分析时请重点关注：
1. 要点1
2. 要点2
"""
        }
        
        # 注册到专家列表
        expert = LLMExpertAgent(
            expert_type=new_expert_config["type"],
            name=new_expert_config["name"],
            system_prompt=new_expert_config["prompt"],
            llm_client=self.llm_client
        )
        self.experts[new_expert_config["type"]] = expert
```

#### 自定义诊断逻辑

```python
# 在MultiAgentDiagnosisEngine中添加自定义协调器

async def custom_coordinate(self, opinions: List[ExpertOpinion]) -> Dict[str, Any]:
    """自定义意见整合逻辑"""
    # 实现特定的共识算法
    pass
```

### 2. GraphRAG知识图谱

#### 扩展实体类型

```python
# src/knowledge/graph_rag.py

class KnowledgeGraph:
    # 添加新实体类型
    ENTITY_TYPES = {
        "device": "设备",
        "component": "部件",
        "fault": "故障",
        # ... 新增
        "maintenance_record": "维护记录",
        "supplier": "供应商"
    }
    
    RELATION_TYPES = {
        "causes": "导致",
        "belongs_to": "属于",
        # ... 新增
        "maintained_by": "由...维护",
        "supplied_by": "由...供应"
    }
```

#### 添加知识到图谱

```python
from src.knowledge import graph_rag, Entity, Relation

# 创建实体
entity = Entity(
    id="DEV_NEW_001",
    name="新设备",
    entity_type="device",
    attributes={"type": "新型号", "power": "30kW"},
    description="新添加的设备"
)

# 添加到图谱
graph_rag.kg.add_entity(entity)

# 添加关系
relation = Relation("DEV_NEW_001", "FAULT_001", "manifests_as")
graph_rag.kg.add_relation(relation)
```

### 3. CAMEL智能体开发

#### 创建新社会类型

```python
# src/agents/camel_integration.py

class MaintenanceSociety(CamelSociety):
    """维护计划制定社会"""
    
    def __init__(self):
        super().__init__(
            society_id="maintenance_planning_001",
            name="维护计划制定委员会",
            description="制定设备维护计划"
        )
        self._init_agents()
    
    def _init_agents(self):
        # 添加专业Agent
        planner = CamelAgent(
            agent_id="PLANNER_001",
            name="维护计划员",
            role=AgentRole.EXPERT,
            system_message="你是维护计划专家...",
            capabilities=["计划制定", "资源调度"]
        )
        self.register_agent(planner)
```

### 4. 任务追踪扩展

#### 自定义任务处理器

```python
from src.tasks import task_tracker, TrackedTask

async def custom_task_handler(task: TrackedTask, data: dict):
    """自定义任务处理"""
    # 分步骤执行
    steps = [
        {"name": "数据预处理", "weight": 20},
        {"name": "模型推理", "weight": 60},
        {"name": "结果整理", "weight": 20}
    ]
    
    async def process_step(task: TrackedTask, step: dict):
        # 执行步骤
        await do_something()
        return {"step": step["name"], "status": "ok"}
    
    return await task_tracker.execute_with_progress(
        task, steps, process_step
    )

# 使用
task = task_tracker.create_task(
    task_type="custom_analysis",
    description="自定义分析任务"
)
result = await task_tracker.execute(task, custom_task_handler, data)
```

---

## 测试指南

### 1. 单元测试

```bash
# 运行所有单元测试
pytest tests/unit/ -v

# 运行特定测试
pytest tests/unit/test_multi_agent.py -v

# 带覆盖率
pytest tests/unit/ --cov=src --cov-report=html
```

#### 多智能体诊断测试示例

```python
# tests/unit/test_multi_agent.py

import pytest
from src.diagnosis import MultiAgentDiagnosisEngine

@pytest.fixture
def diagnosis_engine():
    return MultiAgentDiagnosisEngine()

@pytest.mark.asyncio
async def test_multi_agent_diagnose(diagnosis_engine):
    result = await diagnosis_engine.diagnose(
        symptoms="测试症状",
        sensor_data={"temp": 50.0}
    )
    
    assert result.confidence > 0
    assert len(result.expert_opinions) == 5
    assert result.final_conclusion is not None

@pytest.mark.asyncio
async def test_expert_opinion_format(diagnosis_engine):
    opinion = await diagnosis_engine.experts[ExpertType.MECHANICAL].analyze(
        symptoms="轴承过热",
        sensor_data={"vibration": 10.0}
    )
    
    assert 0 <= opinion.confidence <= 1
    assert len(opinion.evidence) > 0
    assert len(opinion.suggestions) > 0
```

### 2. 集成测试

```bash
# 启动测试服务器
uvicorn src.api.main:app --port 8001 &

# 运行集成测试
pytest tests/integration/ -v
```

#### API集成测试示例

```python
# tests/integration/test_diagnosis_v2.py

import pytest
from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_diagnosis_v2_endpoint():
    response = client.post(
        "/v2/diagnosis/analyze",
        headers={"Authorization": "Bearer test-token"},
        json={
            "symptoms": "曝气池DO偏低",
            "sensor_data": {"do": 1.5},
            "use_multi_agent": True
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "diagnosis_id" in data
    assert data["status"] == "completed"

def test_knowledge_graph_query():
    response = client.post(
        "/v2/diagnosis/knowledge/query",
        json={"query": "轴承过热"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert len(data["sources"]) > 0
```

### 3. 性能测试

```bash
# 安装Locust
pip install locust

# 运行压力测试
cd tests/load
locust -f locustfile.py --host=http://localhost:8000
```

#### Locust测试脚本

```python
# tests/load/locustfile.py

from locust import HttpUser, task, between

class DiagnosisUser(HttpUser):
    wait_time = between(1, 5)
    
    @task(3)
    def test_diagnosis_v2(self):
        self.client.post(
            "/v2/diagnosis/analyze",
            json={
                "symptoms": "测试症状",
                "sensor_data": {"temp": 45.0},
                "use_multi_agent": True
            },
            headers={"Authorization": "Bearer test-token"}
        )
    
    @task(1)
    def test_knowledge_query(self):
        self.client.post(
            "/v2/diagnosis/knowledge/query",
            json={"query": "轴承过热"}
        )
```

---

## 贡献指南

### 1. 开发流程

```
1. Fork仓库
2. 创建特性分支: git checkout -b feature/xxx
3. 开发并测试
4. 提交代码: git commit -m "feat: xxx"
5. 推送到远程: git push origin feature/xxx
6. 创建Pull Request
```

### 2. 提交信息规范

使用 **Conventional Commits**：

```
类型(范围): 简短描述

详细描述

相关Issue: #123
```

类型：
- `feat`: 新功能
- `fix`: Bug修复
- `docs`: 文档更新
- `style`: 代码格式
- `refactor`: 重构
- `test`: 测试
- `chore`: 构建/工具

示例：
```
feat(diagnosis): 添加传感器专家Agent

新增传感器专家Agent，用于诊断仪表和传感器故障

- 实现传感器漂移诊断
- 添加校准建议生成
- 更新测试用例

Closes #45
```

### 3. 代码审查清单

- [ ] 代码符合Black格式
- [ ] 类型注解完整
- [ ] 单元测试通过
- [ ] 文档已更新
- [ ] CHANGELOG已更新
- [ ] 无安全漏洞

### 4. 版本发布流程

```bash
# 1. 更新版本号
# 修改 src/api/__init__.py
__version__ = "v1.0.0-beta3"

# 2. 更新CHANGELOG.md

# 3. 创建Git标签
git tag -a v1.0.0-beta3 -m "Release v1.0.0-beta3"
git push origin v1.0.0-beta3

# 4. 构建Docker镜像
docker build -t jamin:v1.0.0-beta3 .
docker push jamin:v1.0.0-beta3
```

---

## 调试技巧

### 1. 日志调试

```python
from src.utils.structured_logging import get_logger

logger = get_logger("my_module")

# 不同级别
logger.debug("调试信息")
logger.info("一般信息")
logger.warning("警告")
logger.error("错误")

# 带额外字段
logger.info(
    "处理完成",
    extra={"task_id": "123", "duration_ms": 150}
)
```

### 2. 异步调试

```python
import asyncio
import pdb

async def debug_function():
    # 设置断点
    pdb.set_trace()
    result = await some_async_operation()
    return result

# 运行
asyncio.run(debug_function())
```

### 3. API调试

```bash
# 使用httpie
http :8000/v2/diagnosis/analyze \
  Authorization:"Bearer token" \
  symptoms="测试" \
  sensor_data:='{"temp": 50}'

# 或使用curl配合jq
curl -s http://localhost:8000/v2/diagnosis/experts | jq
```

---

## 常见问题

### Q1: 如何添加新的API端点？

```python
# 1. 在routers/创建或修改文件
# src/api/routers/my_feature.py

from fastapi import APIRouter

router = APIRouter(prefix="/my-feature", tags=["我的功能"])

@router.get("/")
async def get_items():
    return {"items": []}

# 2. 在main.py注册
from src.api.routers import my_feature
app.include_router(my_feature.router)
```

### Q2: 如何扩展专家Agent能力？

参考 [V2新模块开发](#v2新模块开发) 中的"添加新专家Agent"部分。

### Q3: 如何调试多智能体诊断？

```python
# 开启详细日志
import logging
logging.getLogger("multi_agent_diagnosis").setLevel(logging.DEBUG)

# 查看专家意见详情
engine = MultiAgentDiagnosisEngine()
result = await engine.diagnose(...)
for op in result.expert_opinions:
    print(f"{op.expert_name}: {op.root_cause} ({op.confidence})")
    print(f"  推理: {op.reasoning}")
```

---

## 🙏 致谢

感谢以下开源项目为开发提供支持：

### 开发工具
- [pytest](https://pytest.org/) - Python测试框架
- [Black](https://black.readthedocs.io/) - 代码格式化
- [isort](https://pycqa.github.io/isort/) - 导入排序
- [Flake8](https://flake8.pycqa.org/) - 代码检查
- [MyPy](https://mypy.readthedocs.io/) - 类型检查
- [pre-commit](https://pre-commit.com/) - Git钩子

### 测试与CI
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) - 异步测试
- [pytest-cov](https://pytest-cov.readthedocs.io/) - 覆盖率
- [Locust](https://locust.io/) - 压力测试

### 代码贡献
感谢所有为开源社区做出贡献的开发者！

---

**版本**: v1.0.0-beta2 (MiroFish) | **最后更新**: 2026-03-27
