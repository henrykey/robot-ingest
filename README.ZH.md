# Robot Ingest Pipeline v1.0.0 (IBC-AI CO.)

![Version](https://img.shields.io/badge/version-v1.0.0-blue.svg)
![Status](https://img.shields.io/badge/status-production--ready-green.svg)
![Architecture](https://img.shields.io/badge/architecture-sidecar--tap-orange.svg)

**完整的MQTT摄取管线** - 分步实现的高性能、可扩展的机器人数据摄取系统

## 🚀 核心特性

### 架构设计

- **🔄 旁路/Tap架构**: 不改变现有MQTT接收逻辑的非侵入式设计
- **🚪 特性门控**: 可开关的渐进式特性部署 (`INGEST_FEATURE_ENABLED`)
- **📊 分层处理**: GlobalQueue → Dispatcher → TopicWorker
- **✂️ 双重输出**: 传统Redis队列 + 批量ingest队列
- **⚡ 并行Writer**: 传统Writer + BatchWriter同时运行

### 核心功能栈

- **Step 1**: GlobalQueue Tap - 旁路队列架构
- **Step 2**: Dispatcher/TopicWorker - 按topic+deviceId分组处理  
- **Step 3**: 智能去重 - 基于内容哈希的去重机制
- **Step 4**: LastonePublisher - 实时MQTT lastone（retain）
- **Step 5**: RedisWriter - Redis立即入列（供批落库）
- **Step 6**: BatchWriter - 批量MongoDB落库

### 性能指标

- **📈 高吞吐**: 5000+消息/分钟处理能力
- **🎯 低延迟**: 75-899ms批量写入延迟
- **🔍 智能去重**: 减少重复数据处理
- **📊 实时监控**: 完整的统计和监控体系

## 🏗️ 系统架构

```text
MQTT Broker → ingest-service → Redis → writer-service → MongoDB
                    ↓               ↓
               GlobalQueue     Traditional     BatchWriter
                   ↓          Queue (q:*)    (ingest:*)
              Dispatcher          ↓              ↓
                   ↓         Legacy Writer   Batch Insert
              TopicWorker         ↓              ↓
                   ↓          MongoDB      MongoDB Collections
            [去重+LastOne]                 (state_events, etc.)
                   ↓
              RedisWriter
```

## 🛠️ 服务组件

### ingest-service

- **MQTT订阅**: 接收机器人遥测数据
- **GlobalQueue**: 旁路消息队列（不影响原有链路）
- **Dispatcher**: 按topic+deviceId分组分发
- **TopicWorker**: 去重处理 + 双重输出
- **LastonePublisher**: 实时retain消息发布
- **RedisWriter**: 批量队列入列

### writer-service

- **Legacy Writer**: 传统Redis队列处理
- **BatchWriter**: 批量ingest队列处理
- **MongoDB**: 多collection写入支持

## 📋 配置示例

```yaml
# 特性开关
ingest:
  featureEnabled: true
  
# 批量写入配置
writer:
  batch:
    enabled: true
    batchSize: 100
    batchIntervalMs: 120000
    maxPerFlush: 500
    topics: [state, connection, networkIp, error, cargo]
    topicMapping:
      state: state_events
      connection: connection_events
      networkIp: network_events
      error: error_events
      cargo: cargo_events
```

## 🚀 快速开始

### 构建

```bash
./gradlew clean build
```

### 本地运行

```bash
cp config/config.example.yml ./config.yml
java -jar ingest-service/build/libs/ingest-service-0.1.0.jar --config=./config.yml
java -jar writer-service/build/libs/writer-service-0.1.0.jar --config=./config.yml
```

### Docker Compose

```bash
docker compose up -d --build
docker compose logs -f ingest
docker compose logs -f writer
```

## ⚙️ 环境变量覆盖

使用环境变量覆盖配置，如:

```bash
CFG__mqtt__brokerUrl=tcp://192.168.123.61:1883
```

## 📊 监控与统计

### ingest-service监控

```bash
# 实时日志
docker compose logs -f ingest

# 关键指标
- 全局统计: 总消息数、丢弃数、有效数、吞吐量
- TopicWorker: 处理数、唯一数、重复数、Redis成功/失败
- Dispatcher: 分发数、批次数、工作线程数
```

### writer-service监控

```bash
# 实时日志  
docker compose logs -f writer

# 关键指标
- BatchWriter: 处理数、批次数、错误数
- 批量写入: 各topic写入数量和延迟
- MongoDB: 写入性能和错误统计
```

## 🔧 故障排查

### 常见问题

1. **特性未启用**: 检查 `INGEST_FEATURE_ENABLED=true`
2. **Redis连接**: 确认Redis服务运行在 `192.168.123.20:6379`
3. **MongoDB连接**: 确认MongoDB集群连接配置
4. **MQTT连接**: 检查broker地址和端口

### 调试命令

```bash
# 检查Redis队列
docker run --rm redis:latest redis-cli -h 192.168.123.20 -p 6379 keys "ingest:*"
docker run --rm redis:latest redis-cli -h 192.168.123.20 -p 6379 llen "q:state"

# 检查服务状态
docker compose ps
docker compose logs --tail 50 ingest
docker compose logs --tail 50 writer
```

## 📈 性能调优

### 批量写入优化

```yaml
writer:
  batch:
    batchSize: 100          # 批次大小 (推荐: 50-200)
    batchIntervalMs: 120000 # 时间间隔 (推荐: 60-300秒)
    maxPerFlush: 500        # 单次最大处理数
```

### 去重优化

```yaml
dedupe:
  enabled: true
  globalWindowMin: 5      # 全局去重窗口 (分钟)
  coreFields: [battery, taskStatus, position]  # 核心字段
  positionDecimals: 6     # 位置精度
```

## 🏷️ 版本历史

- **v1.0.0** (2025-08-11): 完整的MQTT摄取管线
  - ✅ Step 0-6: 特性开关到批量落库的完整实现
  - ✅ 旁路架构: 零侵入的Tap模式
  - ✅ 双写机制: 传统+批量并行处理
  - ✅ 生产验收: 119条消息，11批次，0错误

## 📄 许可证

IBC-AI CO. 内部项目
