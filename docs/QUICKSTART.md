# 快速启动指南

**版本**: v1.0.0-beta2 (MiroFish)

---

## 🚀 5分钟快速启动

### 1. 克隆与安装 (2分钟)

```bash
# 克隆代码
git clone https://github.com/jamin85cheng/jamin_industrial_agent.git
cd jamin_industrial_agent

# 创建虚拟环境
python3.11 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 初始化配置 (1分钟)

```bash
# 初始化数据库
python migrations/migration_manager.py init
python migrations/migration_manager.py migrate

# 配置文件已自动生成
# 查看配置: cat config/settings.yaml
```

### 3. 启动服务 (1分钟)

```bash
# 启动API服务
python -m src.api.main

# 服务已启动!
# API文档: http://localhost:8000/docs
```

### 4. 首次体验V2智能诊断 (1分钟)

```bash
# 登录获取Token
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'

# 保存返回的access_token
TOKEN="your_access_token"

# 体验V2多智能体诊断
curl -X POST http://localhost:8000/v2/diagnosis/analyze \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "symptoms": "曝气池溶解氧持续偏低，风机噪音异常",
    "sensor_data": {"do": 1.5, "vibration": 8.5, "current": 25.3},
    "use_multi_agent": true,
    "use_graph_rag": true
  }'
```

**预期输出**:
```json
{
  "diagnosis_id": "MAD_ABC123DEF456",
  "status": "completed",
  "message": "多智能体诊断完成",
  "result": {
    "final_conclusion": "曝气盘部分堵塞，导致曝气效率下降40%",
    "confidence": 0.85,
    "consensus_level": 0.8,
    "expert_opinions": [...],
    "recommended_actions": [...],
    "spare_parts": [...]
  }
}
```

---

## 🐳 Docker快速启动

```bash
# 一键启动全部服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f api

# 访问API
curl http://localhost:8000/health
```

---

## 🎯 快速体验V2特性

### 1. 查看专家列表

```bash
curl http://localhost:8000/v2/diagnosis/experts \
  -H "Authorization: Bearer $TOKEN" | jq
```

### 2. 知识图谱查询

```bash
curl -X POST http://localhost:8000/v2/diagnosis/knowledge/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "轴承过热的原因"}' | jq
```

### 3. 查看CAMEL社会状态

```bash
curl http://localhost:8000/v2/diagnosis/society/status \
  -H "Authorization: Bearer $TOKEN" | jq
```

### 4. 浏览知识图谱

```bash
# 获取知识图谱
curl http://localhost:8000/v2/diagnosis/knowledge/graph \
  -H "Authorization: Bearer $TOKEN" | jq

# 获取特定设备子图
curl "http://localhost:8000/v2/diagnosis/knowledge/graph?entity_id=DEV_001&depth=2" \
  -H "Authorization: Bearer $TOKEN" | jq
```

### 5. 异步CAMEL诊断

```bash
# 提交异步任务
curl -X POST http://localhost:8000/v2/diagnosis/analyze \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "symptoms": "复杂故障分析...",
    "use_camel": true,
    "priority": "high"
  }'

# 返回: {"task_id": "TASK_XXX", "status": "processing"}

# 查询任务状态
curl http://localhost:8000/v2/diagnosis/task/TASK_XXX \
  -H "Authorization: Bearer $TOKEN" | jq
```

---

## 📊 监控大屏

访问监控大屏查看实时数据：

```
http://localhost:8000/static/index.html
```

大屏包含：
- 实时趋势图
- 设备状态分布
- 告警统计
- V2诊断历史（新增）

---

## 🔧 下一步

- [阅读完整文档](user_manual.md)
- [查看API参考](api_reference.md)
- [了解部署选项](deployment.md)
- [参与开发](development.md)

---

## 🙏 致谢

快速上手基于以下开源项目：

- [FastAPI](https://fastapi.tiangolo.com/) - 高性能API
- [Uvicorn](https://www.uvicorn.org/) - ASGI服务器
- [MiroFish](https://github.com/666ghj/MiroFish) - 群体智能引擎
- [CAMEL-AI](https://www.camel-ai.org/) - 多智能体框架

---

**版本**: v1.0.0-beta2 (MiroFish) | **更新时间**: 2026-03-27 | **快速上手，立即体验群体智能诊断！**
