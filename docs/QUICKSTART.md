# 快速启动指南

## 🚀 5 分钟快速开始

### 1. 安装依赖

```bash
cd miaota_industrial_agent

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Linux/Mac:
source venv/bin/activate
# Windows:
.\venv\Scripts\activate

# 安装核心依赖 (最小化安装)
pip install pandas numpy loguru pyyaml openpyxl

# 完整安装 (包含所有功能)
pip install -r requirements.txt
```

### 2. 配置系统

编辑 `config/settings.yaml`，修改以下关键配置：

```yaml
# PLC 连接配置
plc:
  s7:
    host: "192.168.1.100"  # 改为你的 PLC IP
    port: 102
    rack: 0
    slot: 1
  
  scan_interval: 10  # 采集频率 (秒)

# 数据库配置 (使用默认 SQLite 即可)
database:
  sqlite:
    enabled: true
    path: "data/metadata.db"
```

### 3. 准备点位映射表

首次运行时会自动生成模板 `config/tag_mapping.xlsx`，打开并填写你的 PLC 点位信息：

| 点位 ID | PLC 地址 | 设备名称 | 业务含义 | 数据类型 | 单位 | 正常阈值 |
|--------|---------|---------|---------|---------|------|---------|
| TAG_DO_001 | MD100 | 1#曝气池 | 溶解氧浓度 | FLOAT | mg/L | 2.0-8.0 |
| TAG_PH_001 | MD104 | 1#曝气池 | pH 值 | FLOAT | | 6.5-8.5 |

### 4. 运行测试

#### 测试 1: 点位映射

```bash
python src/core/tag_mapping.py
```

预期输出：
```
TagMapper 初始化完成，加载 3 个点位
语义化数据:
  1#曝气池_溶解氧浓度：3.5 mg/L
  1#曝气池_pH 值：7.2
```

#### 测试 2: 规则引擎

```bash
python src/rules/rule_engine.py
```

预期输出：
```
RuleEngine 初始化完成，加载 10 条规则
触发了 2 条规则:
⚠️  [HIGH] 曝气池缺氧异常
   标签：缺氧异常
   建议动作:
     - 检查鼓风机运行状态
     - 增加曝气量
```

#### 测试 3: 数据采集 (模拟模式)

```bash
python src/data/collector.py
```

预期输出（模拟数据）：
```
PLCCollector 初始化完成：s7@192.168.1.100:102
模拟模式：连接到虚拟 PLC
收到数据 @ 2026-03-24T10:30:00:
  TAG_DO_001: 3.52
  TAG_PH_001: 7.21
  TAG_COD_001: 85.3
```

### 5. 启动完整系统

创建一个简单的启动脚本 `start_system.py`：

```python
from src.core import TagMapper, RuleEngine
from src.data import PLCCollector
import time

# 1. 初始化点位映射
mapper = TagMapper('config/tag_mapping.xlsx')

# 2. 初始化规则引擎
engine = RuleEngine('config/rules.json')

# 3. 初始化数据采集
plc_config = {
    'type': 's7',
    'host': '192.168.1.100',
    'port': 102,
    'scan_interval': 10
}
collector = PLCCollector(plc_config)

# 4. 定义数据处理回调
def on_data_received(data):
    print(f"\n=== {data['timestamp']} ===")
    
    # 转换为 DataFrame
    import pandas as pd
    df = pd.DataFrame([data['values']])
    
    # 执行规则评估
    alerts = engine.evaluate(df)
    
    if alerts:
        print(f"⚠️  触发 {len(alerts)} 条告警:")
        for alert in alerts:
            print(f"  [{alert['severity'].upper()}] {alert['name']}")
            print(f"     建议：{', '.join(alert['suggested_actions'][:2])}")
    else:
        print("✓ 系统运行正常")

# 5. 注册回调并启动
collector.register_callback(on_data_received)
collector.start_collection(mapper.tag_dict)

print("系统已启动，按 Ctrl+C 停止...")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n系统停止中...")
    collector.stop_collection()
    collector.disconnect()
```

运行：
```bash
python start_system.py
```

---

## 📦 Docker 部署 (生产环境)

### 1. 构建镜像

```bash
docker-compose build
```

### 2. 启动服务

```bash
docker-compose up -d
```

### 3. 查看日志

```bash
docker-compose logs -f
```

### 4. 访问监控大屏

浏览器打开：`http://localhost:8501`

---

## 🔧 常见问题

### Q1: 无法连接 PLC
**A:** 检查以下几点：
1. PLC IP 地址是否正确
2. 网络是否连通 (`ping <PLC_IP>`)
3. 防火墙是否开放端口 (S7: 102, Modbus: 502)
4. 如果是 S7-1200/1500，确认已启用"允许来自远程对象的 PUT/GET 通信"

### Q2: 依赖安装失败
**A:** 尝试分步安装：
```bash
# 先安装基础科学计算库
pip install numpy pandas scipy

# 再安装机器学习库
pip install scikit-learn

# 最后安装其他依赖
pip install loguru pyyaml openpyxl
```

### Q3: 规则不触发
**A:** 检查：
1. 规则是否启用 (`enabled: true`)
2. 点位 ID 是否与数据中的列名匹配
3. 阈值设置是否合理
4. 查看日志文件 `logs/app.log` 获取详细信息

### Q4: 内存占用过高
**A:** 调整配置：
```yaml
# 减少历史数据缓存
rule_engine:
  buffer_size_minutes: 30  # 从 60 改为 30

# 减少采集频率
plc:
  scan_interval: 30  # 从 10 改为 30 秒
```

---

## 📖 下一步

1. **完善点位映射**: 填写所有需要监控的 PLC 点位
2. **定制规则库**: 根据你的工艺特点修改 `config/rules.json`
3. **接入真实 PLC**: 修改配置，连接到实际设备
4. **构建知识库**: 在 `data/knowledge_base/` 添加故障案例和操作规程
5. **部署大屏**: 使用 Streamlit 或 ECharts 开发监控界面

---

## 🆘 获取帮助

- 查看完整文档：`docs/` 目录
- 示例代码：`notebooks/` 目录
- 问题反馈：提交 GitHub Issue
- 技术支持：联系项目维护者

---

**祝你使用愉快！** 🦞
