# 后端修复更新说明（2026-03-27）

## 1. 本次目标

本次更新聚焦于后端主链路的稳定性修复，优先处理以下问题：

- `src/api` 无法正常导入或启动的问题
- 依赖注入、认证、健康检查等基础设施接口漂移
- 启动脚本与采集器、规则引擎之间的历史兼容性问题
- 数据缓冲区与存储后端之间的批量写入契约不一致
- Pydantic v2 环境下的旧参数写法导致的运行时异常

本次修改遵循“兼容优先、最小破坏”的原则，先恢复运行能力，再为后续继续重构留出空间。

## 2. 主要修复内容

### 2.1 API 入口与依赖层

已修复文件：

- [src/api/main.py](/E:/jamin_industrial_agent/src/api/main.py)
- [src/api/dependencies.py](/E:/jamin_industrial_agent/src/api/dependencies.py)
- [src/api/routers/health.py](/E:/jamin_industrial_agent/src/api/routers/health.py)
- [src/api/routers/knowledge.py](/E:/jamin_industrial_agent/src/api/routers/knowledge.py)
- [api/routers/auth.py](/E:/jamin_industrial_agent/api/routers/auth.py)

修复内容：

- 重建 `src/api/main.py`，消除原有语法错误与导入错误。
- 将主应用改为按路由模块逐个安全加载，避免单个子模块异常导致整个 API 启动失败。
- 修复 `Depends`、`HTTPException` 等缺失导入。
- 重写 `src/api/dependencies.py`，去除对不存在配置模块和不存在连接池对象的依赖。
- 基于现有 `src/utils/config.py` 与 `src/utils/connection_pool.py` 实现新的数据库连接依赖。
- 在 `jwt` 第三方包缺失时，提供基于 `HMAC-SHA256` 的轻量 token 编解码回退实现，保证当前环境可运行。
- 修复健康检查路由与健康检查底层实现之间的接口不匹配问题。
- 对 `psutil` 缺失场景做降级处理，使健康检查仍然可以返回结果。

### 2.2 采集器与启动链路兼容

已修复文件：

- [src/data/collector.py](/E:/jamin_industrial_agent/src/data/collector.py)
- [start.py](/E:/jamin_industrial_agent/start.py)
- [src/core/__init__.py](/E:/jamin_industrial_agent/src/core/__init__.py)

修复内容：

- 重构 `PLCCollector` 的连接状态管理，统一为 `ConnectionGuard` 驱动。
- 增加历史兼容接口：
  - `register_callback()`
  - `unregister_callback()`
  - `start_collection()`
  - `stop_collection()`
- 支持 `simulated` 模式作为当前环境下的稳定回退采集源。
- 统一 `read_all_tags()` 返回结构，使其与启动脚本消费方式一致。
- 调整 `start.py`，使其与当前 `TagMapper`、`RuleEngine`、`PLCCollector` 的真实接口一致。
- 修复 `src/core/__init__.py` 导出错误，补充 `RuleEngine`、`LabelFactory` 的兼容导出，避免入口脚本导入失败。

### 2.3 规则引擎兼容与增强

已修复文件：

- [src/rules/rule_engine.py](/E:/jamin_industrial_agent/src/rules/rule_engine.py)

修复内容：

- 为 `RuleEngine` 增加 `config` 兼容参数，兼容旧入口调用。
- 保留现有规则编译逻辑，同时补上 `register_alert_callback()` 兼容方法。
- 增加 `add_rule()` 能力，便于旧测试和后续扩展调用。
- 补全 alert 数据结构中的旧字段兼容，如 `name`、`label`。

### 2.4 数据缓冲与存储契约修复

已修复文件：

- [src/data/buffer.py](/E:/jamin_industrial_agent/src/data/buffer.py)

修复内容：

- 修复 `_flush_buffer()` 对 `write_batch()` 返回值的假设错误。
- 兼容两种写法：
  - 返回成功条数 `int`
  - 返回 `(success, failed)` 元组

这样可以避免缓冲刷新任务在不同存储后端实现下直接抛异常。

### 2.5 Pydantic v2 兼容

已修复文件：

- [src/api/routers/devices.py](/E:/jamin_industrial_agent/src/api/routers/devices.py)
- [src/api/routers/collection.py](/E:/jamin_industrial_agent/src/api/routers/collection.py)
- [src/api/routers/diagnosis_v2.py](/E:/jamin_industrial_agent/src/api/routers/diagnosis_v2.py)
- [api/routers/rules.py](/E:/jamin_industrial_agent/api/routers/rules.py)

修复内容：

- 将 `regex=` 替换为 Pydantic v2 所需的 `pattern=`。
- 将可变默认值如 `[]` 替换为 `Field(default_factory=list)`，降低共享默认值风险。
- 去除不兼容的 `enum=` 参数使用方式。

### 2.6 健康检查与日志稳定性

已修复文件：

- [src/utils/health_check.py](/E:/jamin_industrial_agent/src/utils/health_check.py)
- [src/utils/structured_logging.py](/E:/jamin_industrial_agent/src/utils/structured_logging.py)
- [src/data/__init__.py](/E:/jamin_industrial_agent/src/data/__init__.py)

修复内容：

- 为 `HealthCheckResult` 增加旧字段兼容属性 `response_time`。
- 为 `HealthChecker` 增加异步兼容方法 `check_all()`。
- 修复 Windows 环境下磁盘检查路径问题。
- 修复 `loguru` 压缩格式配置错误。
- 将结构化日志默认输出降级为稳定文本格式，避免格式化器配置导致运行时日志报错。
- 将 `src.data` 包的导入改为轻量模式，避免 `scipy` 等可选依赖缺失时整个数据包无法导入。

## 3. 本次验证结果

### 3.1 编译验证

已执行：

```powershell
& 'E:\jamin_industrial_agent\.venv\Scripts\python.exe' -m compileall src api security migrations start.py
```

结果：

- 编译通过
- 说明本次修复涉及的后端主模块已无语法级错误

### 3.2 Smoke Check

已执行以下运行时冒烟验证：

- 导入 [src/api/main.py](/E:/jamin_industrial_agent/src/api/main.py) 中的 `app`
- 初始化默认用户并创建/校验访问 token
- 初始化 `simulated` 模式的 `PLCCollector` 并读取数据
- 初始化 `RuleEngine` 并执行规则评估
- 执行健康检查组件

结果：

- Smoke check 通过
- 关键主链路可在当前环境中导入并执行

## 4. 当前仍然存在的限制

以下问题本次没有继续扩大改动范围：

- 项目虚拟环境中当前未安装 `pytest`，因此没有完成正式测试套件执行。
- `snap7`、`pymodbus`、`psutil`、`scipy` 等部分依赖在当前环境缺失，本次通过兼容降级保证主链路不崩，但并不代表相关高级能力已完整验证。
- `src/api` 与 `api` 两套后端栈仍然同时存在，本次修复的是“可运行性”和“兼容性”，不是完整架构收敛。
- 规则文件中仍存在部分条件配置不规范的情况，因此运行时会出现“不支持的条件类型”警告，但不会阻塞主流程启动。

## 5. 建议的下一步

建议按以下顺序继续推进：

1. 补齐项目测试依赖，恢复 `pytest` 执行能力。
2. 给本次修复覆盖的模块补一组最小单测与集成测试。
3. 收敛 `src/api` 与 `api` 双后端入口，明确唯一主服务入口。
4. 将当前 mock/内存数据逐步替换为真实持久化实现。

## 6. 版本说明

本次更新属于“后端稳定性修复与兼容性增强”类型更新，适合作为当前版本的一次修补发布内容说明。
