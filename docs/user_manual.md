# Jamin Industrial Agent 用户手册

**版本**: v1.0.0-beta2 (MiroFish)

**适用对象**: 运维工程师、设备管理人员、系统管理员

---

## 📖 目录

1. [快速入门](#快速入门)
2. [V2智能诊断指南](#v2智能诊断指南)
3. [系统配置](#系统配置)
4. [日常操作](#日常操作)
5. [故障排查](#故障排查)

---

## 快速入门

### 1. 系统启动

```bash
# 1. 进入项目目录
cd jamin_industrial_agent

# 2. 激活虚拟环境
source venv/bin/activate

# 3. 启动服务
python -m src.api.main

# 4. 访问系统
# API文档: http://localhost:8000/docs
# 监控大屏: http://localhost:8000/static/index.html
```

### 2. 首次配置

#### 2.1 创建管理员账户

```bash
# 默认账户
用户名: admin
密码: admin123

# 登录后获取Token
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'
```

#### 2.2 添加设备

```bash
curl -X POST http://localhost:8000/devices \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "PLC-001",
    "type": "s7",
    "host": "192.168.1.10",
    "port": 102,
    "tags": [
      {"name": "temperature", "address": "DB1.DBD0", "data_type": "float"},
      {"name": "pressure", "address": "DB1.DBD4", "data_type": "float"}
    ]
  }'
```

---

## V2智能诊断指南

### 什么是V2智能诊断？

V2智能诊断是 **v1.0.0-beta2** 版本的核心升级，集成了 **MiroFish** 群体智能引擎：

- 🤖 **多智能体协作**: 5位领域专家Agent同时分析
- 🧠 **知识图谱增强**: 基于图结构的根因追溯
- 📊 **置信度评估**: 多专家共识与分歧展示
- 📋 **任务追踪**: 支持长时间异步诊断

### 使用场景

#### 场景1: 设备异常诊断

**问题**: 曝气池溶解氧(DO)持续偏低

**操作步骤**:

1. **准备症状描述**
   ```
   症状: 曝气池DO从正常的4-6mg/L下降到1.5mg/L，
         伴随风机噪音增大，电流波动
   ```

2. **收集传感器数据**
   ```json
   {
     "do": 1.5,
     "vibration": 8.5,
     "current": 25.3,
     "temperature": 45.2
   }
   ```

3. **调用V2诊断API**
   ```bash
   curl -X POST http://localhost:8000/v2/diagnosis/analyze \
     -H "Authorization: Bearer TOKEN" \
     -H "Content-Type: application/json" \
     -d '{
       "symptoms": "曝气池DO持续偏低，风机噪音异常",
       "sensor_data": {
         "do": 1.5,
         "vibration": 8.5,
         "current": 25.3
       },
       "use_multi_agent": true,
       "use_graph_rag": true,
       "priority": "high"
     }'
   ```

4. **解读诊断结果**

   **诊断结论**:
   ```
   根因: 曝气盘部分堵塞，导致曝气效率下降40%
   置信度: 85%
   共识度: 80% (4/5位专家认同)
   ```

   **专家意见分布**:
   | 专家 | 根因判断 | 置信度 |
   |------|----------|--------|
   | 机械专家 | 曝气盘堵塞 | 82% |
   | 电气专家 | 电机绝缘老化 | 75% |
   | 工艺专家 | 污泥龄过长 | 88% |
   | 传感器专家 | DO探头污染 | 90% |

   **处理建议** (按优先级排序):
   1. 【紧急】清洗曝气盘 (预计2-4小时，需停机)
   2. 【高】检查风机叶轮平衡 (预计1小时)
   3. 【中】校准DO传感器 (预计30分钟)

   **推荐备件**:
   - 微孔曝气盘 Φ215mm × 5个
   - 风机滤网 × 2个

### V2 vs V1 诊断对比

| 特性 | V1 诊断 | V2 诊断 | 优势 |
|------|---------|---------|------|
| 诊断方式 | 单LLM | 5专家Agent | 多角度分析 |
| 准确性 | 70% | 85%+ | +15% |
| 可解释性 | 结论+原因 | 专家意见+推理 | 完全透明 |
| 处理时间 | 2-5秒 | 5-15秒 | 可接受 |
| 知识来源 | 文档库 | 知识图谱 | 结构化关联 |
| 备件推荐 | 无 | 自动清单 | 一站式 |

### 异步诊断 (长时间任务)

对于复杂诊断，使用CAMEL社会协作模式：

```bash
# 1. 提交异步诊断任务
curl -X POST http://localhost:8000/v2/diagnosis/analyze \
  -H "Authorization: Bearer TOKEN" \
  -d '{
    "symptoms": "复杂故障描述...",
    "use_camel": true,
    "priority": "critical"
  }'

# 返回: {"task_id": "TASK_ABC123", "status": "processing"}

# 2. 轮询任务状态
curl http://localhost:8000/v2/diagnosis/task/TASK_ABC123 \
  -H "Authorization: Bearer TOKEN"

# 3. 获取最终结果
# 当 status = "completed" 时，result字段包含诊断报告
```

---

## 知识图谱使用

### 查询工业知识

```bash
# GraphRAG查询
curl -X POST http://localhost:8000/v2/diagnosis/knowledge/query \
  -H "Authorization: Bearer TOKEN" \
  -d '{"query": "轴承过热的原因"}'
```

**返回示例**:
```json
{
  "query": "轴承过热的原因",
  "answer": "根据知识图谱分析，轴承过热的主要原因包括：\n\n1. 润滑不良 (关联度: 0.95)\n   - 油脂老化\n   - 润滑不足\n   - 解决方案: 更换润滑脂\n\n2. 负载过大 (关联度: 0.82)\n   - 工艺参数异常\n   - 解决方案: 调整工艺参数\n\n3. 安装不当 (关联度: 0.75)\n   - 对中不良\n   - 解决方案: 重新对中校准",
  "sources": ["FAULT_002", "CAUSE_002", "SOL_002"]
}
```

### 浏览知识图谱

```bash
# 获取完整知识图谱
curl http://localhost:8000/v2/diagnosis/knowledge/graph \
  -H "Authorization: Bearer TOKEN"

# 获取特定实体子图
curl "http://localhost:8000/v2/diagnosis/knowledge/graph?entity_id=DEV_001&depth=2" \
  -H "Authorization: Bearer TOKEN"
```

---

## 系统配置

### 配置文件位置

```
config/
├── settings.yaml          # 主配置
├── rules.json            # 告警规则
└── devices.yaml          # 设备配置
```

### 常用配置项

#### 1. 诊断引擎配置

```yaml
# config/settings.yaml
diagnosis:
  v2:
    enabled: true
    default_mode: "multi_agent"  # multi_agent | single | camel
    experts:
      - mechanical
      - electrical
      - process
      - sensor
      - historical
    confidence_threshold: 0.7
    timeout_seconds: 30
```

#### 2. 知识图谱配置

```yaml
knowledge_graph:
  enabled: true
  backend: "memory"  # memory | neo4j
  neo4j:
    uri: "bolt://localhost:7687"
    user: "neo4j"
    password: "password"
```

#### 3. 任务追踪配置

```yaml
task_tracker:
  max_concurrent: 10
  default_timeout: 3600
  cleanup_interval: 86400  # 清理已完成任务
```

---

## 日常操作

### 1. 查看设备状态

```bash
# 获取所有设备
curl http://localhost:8000/devices \
  -H "Authorization: Bearer TOKEN"

# 按状态过滤
curl "http://localhost:8000/devices?status=online" \
  -H "Authorization: Bearer TOKEN"
```

### 2. 查询历史数据

```bash
curl -X POST http://localhost:8000/collection/data/query \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tags": ["temperature", "pressure"],
    "start_time": "2024-01-15T00:00:00Z",
    "end_time": "2024-01-15T23:59:59Z",
    "aggregation": "mean",
    "interval": "1h"
  }'
```

### 3. 查看告警

```bash
# 活跃告警
curl "http://localhost:8000/alerts?status=active" \
  -H "Authorization: Bearer TOKEN"

# 确认告警
curl -X POST http://localhost:8000/alerts/ALT_001/acknowledge \
  -H "Authorization: Bearer TOKEN" \
  -d '{"comment": "已处理，更换传感器"}'
```

### 4. 查看诊断历史

```bash
curl http://localhost:8000/v2/diagnosis/history?limit=10 \
  -H "Authorization: Bearer TOKEN"
```

---

## 故障排查

### 问题1: V2诊断返回空结果

**可能原因**:
- 症状描述过短
- 传感器数据格式错误

**解决方案**:
```bash
# 检查症状描述长度（至少5个字符）
# 检查传感器数据格式
{
  "symptoms": "曝气池DO持续偏低，风机噪音异常",  // 必须>=5字符
  "sensor_data": {
    "do": 1.5,           // 必须是数字
    "vibration": 8.5
  }
}
```

### 问题2: 知识图谱查询无结果

**可能原因**:
- 查询词不在知识库中
- GraphRAG未启用

**解决方案**:
```bash
# 检查知识图谱状态
curl http://localhost:8000/v2/diagnosis/knowledge/graph \
  -H "Authorization: Bearer TOKEN"

# 使用更通用的查询词
# 错误: "3号风机轴承温度高"
# 正确: "轴承过热"
```

### 问题3: 异步诊断任务卡住

**可能原因**:
- CAMEL社会初始化失败
- 任务超时

**解决方案**:
```bash
# 查询任务状态
curl http://localhost:8000/v2/diagnosis/task/TASK_ID \
  -H "Authorization: Bearer TOKEN"

# 如果卡住，取消任务
curl -X DELETE http://localhost:8000/v2/diagnosis/task/TASK_ID \
  -H "Authorization: Bearer TOKEN"

# 改用同步模式
curl -X POST http://localhost:8000/v2/diagnosis/analyze \
  -H "Authorization: Bearer TOKEN" \
  -d '{
    "symptoms": "...",
    "use_camel": false,  // 禁用CAMEL
    "use_multi_agent": true
  }'
```

### 问题4: 认证失败

**错误信息**: `401 Unauthorized`

**解决方案**:
```bash
# 1. 检查Token是否过期，重新登录
curl -X POST http://localhost:8000/auth/login \
  -d '{"username": "admin", "password": "admin123"}'

# 2. 检查Token格式
# 正确: "Authorization: Bearer eyJhbG..."
# 错误: "Authorization: eyJhbG..." (缺少Bearer)
```

---

## 最佳实践

### 1. 症状描述技巧

**好的症状描述**:
```
曝气池DO从正常的4-6mg/L下降到1.5mg/L，持续2小时，
伴随风机噪音明显增大，电流从20A波动到25A
```

**差的 symptom描述**:
```
DO低
```

### 2. 传感器数据收集

**建议收集的关键参数**:
- 温度类: temperature, bearing_temp, motor_temp
- 振动类: vibration, acceleration
- 电气类: current, voltage, power_factor
- 工艺类: do, ph, cod, flow, pressure

### 3. 诊断结果使用

**高置信度(>80%)**:
- 可直接按建议执行

**中置信度(50-80%)**:
- 结合现场检查确认

**低置信度(<50%)**:
- 提供更多数据重新诊断
- 或联系人工专家

---

## 附录

### A. 默认账户

| 角色 | 用户名 | 密码 | 权限 |
|------|--------|------|------|
| 管理员 | admin | admin123 | 全部权限 |
| 操作员 | operator | operator123 | 设备读写、告警确认 |
| 观察员 | viewer | viewer123 | 只读访问 |

### B. API速率限制

| 端点 | 限制 |
|------|------|
| /health | 无限制 |
| /v2/diagnosis/analyze | 10次/分钟 |
| /v2/diagnosis/knowledge/query | 30次/分钟 |
| 其他 | 100次/分钟 |

### C. 相关资源

- [CHANGELOG.md](../CHANGELOG.md) - 版本更新日志
- [API文档](api_reference.md) - 完整API参考
- [GitHub](https://github.com/jamin85cheng/jamin_industrial_agent) - 项目源码

---

## 🙏 致谢

感谢以下开源项目为工业智能化做出的贡献：

- [MiroFish](https://github.com/666ghj/MiroFish) - 群体智能诊断引擎
- [CAMEL-AI](https://www.camel-ai.org/) - 多智能体协作框架
- [GraphRAG](https://microsoft.github.io/graphrag/) - 知识图谱技术
- [FastAPI](https://fastapi.tiangolo.com/) - API框架
- [InfluxDB](https://www.influxdata.com/) - 时序数据存储

---

**版本**: v1.0.0-beta2 (MiroFish) | **最后更新**: 2026-03-27

