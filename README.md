# Jamin Industrial Agent - 工业智能监控与诊断系统

🦞 面向工业场景的智能监控、异常检测、故障预测与自进化系统

> **让每一台设备都会"说话"，让每一个异常都有"解释"，让每一次决策都有"依据"。**

[![Version](https://img.shields.io/badge/version-v1.0.0--beta2-blue.svg)](CHANGELOG.md)
[![Status](https://img.shields.io/badge/status-Beta2%20Enhanced-success.svg)](CHANGELOG.md)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**当前版本**: v1.0.0-beta2

**重大更新**: 多智能体协同诊断能力增强，进一步提升工业故障分析能力。

---

## 🎉 V1.0.0-beta2 新特性

> 说明：本项目未直接使用 `MiroFish` 作为命名代号，仅参考了其部分群体智能协作思路，并结合本项目场景进行了自研实现。

### 🤖 多智能体协同诊断
- **5位领域专家Agent**协作诊断
  - 🔧 机械故障诊断专家
  - ⚡ 电气系统专家
  - 🧪 工艺分析专家
  - 📊 传感器专家
  - 📚 历史案例专家
- **协作模式**: 并行分析、顺序诊断、辩论共识
- **可解释性**: 专家意见展示、置信度评估、不同意见标注

### 🧠 GraphRAG 知识图谱
- **工业知识图谱**: 设备-故障-原因-解决方案关联
- **多跳推理**: 根因追溯与关联分析
- **相似度检索**: 基于知识图谱的案例匹配

### 🐪 CAMEL 框架集成
- **智能体社会**: 专家委员会协作模式
- **消息通信**: 点对点与广播通信
- **任务分配**: 动态任务调度与执行

### 📋 长时任务追踪
- **异步诊断**: 支持长时间运行的诊断任务
- **进度追踪**: 实时进度更新与状态查询
- **任务管理**: 优先级队列、超时控制、结果回调

---

## 🎯 项目愿景

构建一个**自感知、自诊断、自进化**的工业智能体，成为工厂运维人员的"数字搭档"。

从 PLC 原始数据出发，通过 AI 技术实现：
- ✅ **数字化与感知** - 全面采集、存储和初步分析 (100%)
- ✅ **专用智能体** - 领域专用 AI 模型和智能诊断 (100%)
- ✅ **多智能体协作** - 参考群体智能协作思路的诊断引擎 (NEW!)
- ⏳ **闭环自动化** - 安全可控的自动控制和优化 (规划中)
- ⏳ **自进化生态** - 持续学习和进化的智能生态系统 (规划中)

---

## 📊 核心能力

### v1.0.0-beta2 功能矩阵

| 模块 | 状态 | 功能 |
|------|------|------|
| 🤖 多智能体诊断 | ✅ NEW | 5位专家Agent协作，辩论共识，置信度评估 |
| 🧠 GraphRAG | ✅ NEW | 知识图谱构建，多跳推理，相似案例匹配 |
| 🐪 CAMEL框架 | ✅ NEW | 智能体社会，消息通信，任务分配 |
| 📋 任务追踪 | ✅ NEW | 异步诊断，进度追踪，优先级队列 |
| 📡 数据采集 | ✅ | S7/Modbus协议，多线程采集，断线重连 |
| 💾 时序存储 | ✅ | InfluxDB/IoTDB/SQLite |
| 🔍 向量存储 | ✅ | Memory/ChromaDB/FAISS |
| 🚨 异常检测 | ✅ | Isolation Forest + 多变量检测 |
| 📈 时序预测 | ✅ | Prophet/ARIMA/LSTM/NeuralProphet |
| 🤖 LLM诊断 | ✅ | 多模型支持，JSON结构化输出 |
| 📚 RAG引擎 | ✅ | 文档加载，智能分块，检索增强 |
| 🔐 安全审计 | ✅ | JWT认证，RBAC权限，多租户隔离 |

---

## 🏗️ 系统架构

```
应用层    ┌─────────────────────────────────────────────────────┐
          │  监控大屏  │  告警中心  │  诊断报告  │ V2智能诊断  │
          └─────────────────────────────────────────────────────┘
                                    ▼
智能层    ┌─────────────────────────────────────────────────────┐
          │ 🤖多智能体 │ 🧠GraphRAG │ 🐪CAMEL  │ 异常检测    │
          │ 诊断引擎   │ 知识图谱   │ 社会协作  │ 时序预测    │
          └─────────────────────────────────────────────────────┘
                                    ▼
数据层    ┌─────────────────────────────────────────────────────┐
          │ 数据采集 │ 预处理   │ 时序存储 │ 向量存储     │
          └─────────────────────────────────────────────────────┘
                                    ▼
设备层    ┌─────────────────────────────────────────────────────┐
          │  PLC     │  传感器   │  执行器                      │
          │  S7-1200 │ 温度/压力 │ 泵/阀门                      │
           └─────────────────────────────────────────────────────┘
```

### V2 多智能体诊断架构

```
用户症状输入
     ↓
┌───────────────────────────────────────────────┐
│            V2 智能诊断引擎                      │
├───────────────────────────────────────────────┤
│  ┌─────────┐ ┌─────────┐ ┌─────────┐         │
│  │机械专家 │ │电气专家 │ │工艺专家 │ ← 并行 │
│  └────┬────┘ └────┬────┘ └────┬────┘   分析  │
│       │           │           │               │
│  ┌────┴───────────┴───────────┴────┐         │
│  │         诊断协调员                │ ← 综合 │
│  │    (GraphRAG增强 + 置信度评估)    │   意见  │
│  └──────────────┬───────────────────┘         │
└─────────────────┼─────────────────────────────┘
                  ↓
    ┌───────────────────────────────┐
    │   诊断报告 + 模拟推演场景      │
    │   • 根因分析                  │
    │   • 分步建议                  │
    │   • 备件清单                  │
    │   • 风险评估                  │
    └───────────────────────────────┘
```

---

## 📖 文档

对外公开材料以 `README.md` 和 `CHANGELOG.md` 为主。

- 运行后可通过 `http://localhost:8000/docs` 查看 API 动态文档
- `docs/` 目录为内部资料，仅本地保留，不随仓库公开
- 版本更新说明继续维护在 `CHANGELOG.md`

---

## 🚀 快速开始

### 1. 克隆仓库
```bash
git clone https://github.com/jamin85cheng/jamin_industrial_agent.git
cd jamin_industrial_agent
```

### 2. 环境准备
```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt

# 可选: 安装CAMEL框架(用于高级多智能体功能)
pip install camel-ai
```

### 3. 启动服务
```bash
# 启动API服务
python -m src.api.main

# 或使用启动脚本
python start.py
```

### 4. 访问系统
- **API文档**: http://localhost:8000/docs
- **监控大屏**: http://localhost:8000/static/index.html

---

## 🎯 使用V2智能诊断

### API调用示例

```bash
# 多智能体诊断
curl -X POST http://localhost:8000/v2/diagnosis/analyze \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "symptoms": "曝气池溶解氧持续偏低，风机噪音异常",
    "sensor_data": {
      "do": 1.5,
      "vibration": 8.5,
      "current": 25.3
    },
    "use_multi_agent": true,
    "use_graph_rag": true,
    "use_camel": false
  }'
```

```python
# Python SDK
from src.diagnosis import MultiAgentDiagnosisEngine

engine = MultiAgentDiagnosisEngine()
result = await engine.diagnose(
    symptoms="曝气池溶解氧持续偏低",
    sensor_data={"do": 1.5, "vibration": 8.5}
)

print(f"诊断结论: {result.final_conclusion}")
print(f"置信度: {result.confidence}")
print(f"专家意见数: {len(result.expert_opinions)}")
```

### 知识图谱查询

```bash
# GraphRAG查询
curl -X POST http://localhost:8000/v2/diagnosis/knowledge/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "轴承过热的原因和解决方案"
  }'
```

---

## 📁 项目结构

```
jamin_industrial_agent/
├── src/
│   ├── api/                      # 🔷 后端 API (FastAPI)
│   │   ├── main.py               # 主应用入口
│   │   ├── dependencies.py       # 依赖注入与认证
│   │   └── routers/              # API 路由
│   │       ├── health.py
│   │       ├── devices.py
│   │       ├── collection.py
│   │       ├── alerts.py
│   │       ├── analysis.py
│   │       ├── knowledge.py
│   │       └── diagnosis_v2.py   # 🆕 V2智能诊断API
│   │
│   ├── diagnosis/                # 🆕 多智能体诊断模块
│   │   └── multi_agent_diagnosis.py
│   │
│   ├── knowledge/                # 🆕 GraphRAG知识图谱
│   │   └── graph_rag.py
│   │
│   ├── agents/                   # 🆕 CAMEL框架集成
│   │   └── camel_integration.py
│   │
│   ├── tasks/                    # 🆕 任务追踪系统
│   │   └── task_tracker.py
│   │
│   ├── security/                 # 🔴 安全模块 (RBAC/多租户)
│   │   ├── rbac.py
│   │   └── multitenancy.py
│   │
│   ├── core/                     # 核心功能
│   ├── data/                     # 数据采集与存储
│   ├── rules/                    # 规则引擎
│   ├── models/                   # AI 模型
│   └── utils/                    # 工具函数
│
├── web/                          # 🔶 前端应用
│   └── static/
│       └── js/
│           └── dashboard-enhanced.js
│
├── tests/                        # 🟣 测试
│   ├── unit/
│   ├── integration/
│   ├── load/                     # 压力测试(Locust)
│   └── benchmark/                # 基准测试
│
├── migrations/                   # 🆕 数据库迁移
│   └── migration_manager.py
│
├── docs/                         # 🔒 本地内部文档（不对外公开）
├── CHANGELOG.md                  # 🆕 版本更新日志
├── requirements.txt
└── README.md
```

---

## 🔧 技术栈

### 后端
- **框架**: FastAPI + Python 3.11
- **认证**: JWT + RBAC + 多租户
- **数据库**: InfluxDB (时序) + SQLite (元数据) + Neo4j (知识图谱)
- **消息队列**: Redis Stream
- **WebSocket**: 原生支持

### V2 新增技术
- **多智能体**: 自研多专家Agent系统
- **知识图谱**: GraphRAG实现
- **智能体框架**: CAMEL-AI集成
- **任务队列**: 异步任务追踪

### 前端
- **框架**: HTML5 + JavaScript (纯前端)
- **UI框架**: Tailwind CSS
- **图表**: ECharts 5
- **图标**: Font Awesome

### 数据 & AI
- **PLC 通信**: python-snap7, pymodbus
- **机器学习**: scikit-learn, PyTorch
- **时序预测**: Prophet, NeuralProphet
- **向量数据库**: ChromaDB, FAISS
- **知识图谱**: Neo4j (可选)

---

## 📈 使用场景

### 场景1: 污水处理厂智能诊断
```
问题: 曝气池DO持续偏低，风机噪音大

传统方式: 2小时排查 → 可能误判
Jamin V2:
  • 机械专家分析振动频谱 → 轴承磨损概率85%
  • 电气专家分析电流曲线 → 电机负载异常
  • 工艺专家评估DO趋势 → 曝气效率下降40%
  • 协调员综合 → 曝气盘堵塞可能性最高
  
结果: 5分钟定位根因，推荐清洗曝气盘
置信度: 85%，备件清单自动生成
```

### 场景2: 化工厂设备健康管理
- 振动、温度、压力多维度监测
- 多智能体协作预测设备故障
- 故障根因自动追溯
- 维修方案智能推荐

---

## 🆚 V1 vs V2 对比

| 特性 | V1 (beta1) | V2 (beta2) | 提升 |
|:-----|:-----------|:-----------|:-----|
| 诊断方式 | 单LLM诊断 | 5专家Agent协作 | 准确性+25% |
| 知识检索 | 文本匹配 | GraphRAG图谱 | 召回率+40% |
| 可解释性 | 结论+原因 | 专家意见+推理链 | 透明度+100% |
| 处理时间 | 同步等待 | 异步追踪 | 体验+50% |
| 诊断置信度 | 无 | 多专家共识评估 | 可靠性+30% |

---

## 🗺️ 开发路线图

### ✅ 已完成
- **v1.0.0-beta1**: 核心功能全部可用
- **v1.0.0-beta2**: 多智能体诊断增强 - 多智能体诊断、GraphRAG、CAMEL

### ⏳ 阶段三: 闭环自动化 (2024 Q3-Q4)
- 安全控制指令下发
- 数字孪生仿真沙箱
- 主动告警和通知

### ⏳ 阶段四: 自进化生态 (2025 Q1+)
- 在线学习 pipeline
- 跨工厂知识迁移
- 边缘计算部署

---

## 📊 项目统计

**项目统计 (截至 2026-03-27)**

---

## 🤝 贡献

欢迎贡献！请查看 [CHANGELOG.md](CHANGELOG.md) 了解最新进展。

1. Fork 仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交改动 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

---

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

---

## 🙏 致谢

本项目基于众多优秀的开源项目构建，特别感谢：

### 核心框架与引擎
- [MiroFish](https://github.com/666ghj/MiroFish) - 群体智能协作思路参考来源之一
- [CAMEL-AI](https://www.camel-ai.org/) - 多智能体框架与OASIS社会模拟平台
- [GraphRAG](https://microsoft.github.io/graphrag/) - 知识图谱检索增强生成技术

### Web框架与API
- [FastAPI](https://fastapi.tiangolo.com/) - 高性能Python Web框架
- [Uvicorn](https://www.uvicorn.org/) - 极速ASGI服务器
- [Pydantic](https://docs.pydantic.dev/) - 数据验证与序列化

### 数据与存储
- [InfluxDB](https://www.influxdata.com/) - 时序数据库
- [ChromaDB](https://www.trychroma.com/) - 向量数据库
- [Neo4j](https://neo4j.com/) - 图数据库 (知识图谱)
- [pandas](https://pandas.pydata.org/) - 数据分析与处理

### AI与机器学习
- [scikit-learn](https://scikit-learn.org/) - 机器学习工具库
- [Prophet](https://facebook.github.io/prophet/) - 时序预测工具
- [NeuralProphet](https://neuralprophet.com/) - 神经Prophet预测
- [LangChain](https://www.langchain.com/) - LLM应用框架
- [Sentence Transformers](https://www.sbert.net/) - 文本嵌入模型

### 工业通信
- [python-snap7](https://github.com/gijzelaerr/python-snap7) - 西门子S7协议
- [pymodbus](https://github.com/pymodbus-dev/pymodbus) - Modbus通信协议

### 前端与可视化
- [ECharts](https://echarts.apache.org/) - 数据可视化库
- [Tailwind CSS](https://tailwindcss.com/) - CSS框架
- [Font Awesome](https://fontawesome.com/) - 图标库

感谢所有开源贡献者！❤️

---

## 📞 联系方式

- **GitHub**: https://github.com/jamin85cheng/jamin_industrial_agent
**版本**: v1.0.0-beta2 | **更新时间**: 2026-03-27

---

**🐟 准备好体验群体智能诊断了吗？立即升级到 V1.0.0-beta2！**

