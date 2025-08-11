# Robot Ingest Pipeline v1.0.0 (IBC-AI CO.)

![Version](https://img.shields.io/badge/version-v1.0.0-blue.svg)
![Status](https://img.shields.io/badge/status-production--ready-green.svg)
![Architecture](https://img.shields.io/badge/architecture-sidecar--tap-orange.svg)

**Complete MQTT Ingestion Pipeline** - A high-performance, scalable robot data ingestion system with step-by-step implementation

🌏 **Language**: [English](README.md) | [中文](README.ZH.md)

## 🚀 Key Features

### Architecture Design

- **🔄 Sidecar/Tap Architecture**: Non-intrusive design that doesn't change existing MQTT reception logic
- **🚪 Feature Gating**: Toggleable progressive feature deployment (`INGEST_FEATURE_ENABLED`)
- **📊 Layered Processing**: GlobalQueue → Dispatcher → TopicWorker
- **✂️ Dual Output**: Traditional Redis queues + Batch ingest queues
- **⚡ Parallel Writers**: Legacy Writer + BatchWriter running concurrently

### Core Feature Stack

- **Step 1**: GlobalQueue Tap - Sidecar queue architecture
- **Step 2**: Dispatcher/TopicWorker - Grouped processing by topic+deviceId
- **Step 3**: Smart Deduplication - Content hash-based deduplication mechanism
- **Step 4**: LastonePublisher - Real-time MQTT lastone (retain)
- **Step 5**: RedisWriter - Immediate Redis enqueue (for batch storage)
- **Step 6**: BatchWriter - Batch MongoDB storage

### Performance Metrics

- **📈 High Throughput**: 5000+ messages/minute processing capacity
- **🎯 Low Latency**: 75-899ms batch write latency
- **🔍 Smart Deduplication**: Reduces redundant data processing
- **📊 Real-time Monitoring**: Complete statistics and monitoring system

## 🏗️ System Architecture

```text
MQTT Broker → ingest-service → Redis → writer-service → MongoDB
                    ↓               ↓
               GlobalQueue     Traditional     BatchWriter
                   ↓          Queue (q:*)    (ingest:*)
              Dispatcher          ↓              ↓
                   ↓         Legacy Writer   Batch Insert
              TopicWorker         ↓              ↓
                   ↓          MongoDB      MongoDB Collections
            [Dedupe+LastOne]                (state_events, etc.)
                   ↓
              RedisWriter
```

## 🛠️ Service Components

### ingest-service

- **MQTT Subscription**: Receives robot telemetry data
- **GlobalQueue**: Sidecar message queue (doesn't affect original pipeline)
- **Dispatcher**: Groups and distributes by topic+deviceId
- **TopicWorker**: Deduplication processing + dual output
- **LastonePublisher**: Real-time retain message publishing
- **RedisWriter**: Batch queue enqueuing

### writer-service

- **Legacy Writer**: Traditional Redis queue processing
- **BatchWriter**: Batch ingest queue processing
- **MongoDB**: Multi-collection write support

## 📋 Configuration Example

```yaml
# Feature toggle
ingest:
  featureEnabled: true
  
# Batch writing configuration
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

## 🚀 Quick Start

### Build

```bash
./gradlew clean build
```

### Run Locally

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

## ⚙️ Environment Variable Override

Use environment variables to override configuration:

```bash
CFG__mqtt__brokerUrl=tcp://192.168.123.61:1883
```

## 📊 Monitoring & Statistics

### ingest-service Monitoring

```bash
# Real-time logs
docker compose logs -f ingest

# Key metrics
- Global stats: Total messages, dropped count, effective count, throughput
- TopicWorker: Processed, unique, duplicate, Redis success/failure
- Dispatcher: Dispatched count, batch count, worker threads
```

### writer-service Monitoring

```bash
# Real-time logs
docker compose logs -f writer

# Key metrics
- BatchWriter: Processed count, batch count, error count
- Batch writes: Per-topic write count and latency
- MongoDB: Write performance and error statistics
```

## 🔧 Troubleshooting

### Common Issues

1. **Feature not enabled**: Check `INGEST_FEATURE_ENABLED=true`
2. **Redis connection**: Ensure Redis service runs on `192.168.123.20:6379`
3. **MongoDB connection**: Verify MongoDB cluster connection configuration
4. **MQTT connection**: Check broker address and port

### Debug Commands

```bash
# Check Redis queues
docker run --rm redis:latest redis-cli -h 192.168.123.20 -p 6379 keys "ingest:*"
docker run --rm redis:latest redis-cli -h 192.168.123.20 -p 6379 llen "q:state"

# Check service status
docker compose ps
docker compose logs --tail 50 ingest
docker compose logs --tail 50 writer
```

## 📈 Performance Tuning

### Batch Write Optimization

```yaml
writer:
  batch:
    batchSize: 100          # Batch size (recommended: 50-200)
    batchIntervalMs: 120000 # Time interval (recommended: 60-300 seconds)
    maxPerFlush: 500        # Maximum per flush
```

### Deduplication Optimization

```yaml
dedupe:
  enabled: true
  globalWindowMin: 5      # Global dedup window (minutes)
  coreFields: [battery, taskStatus, position]  # Core fields
  positionDecimals: 6     # Position precision
```

## 🏷️ Version History

- **v1.0.0** (2025-08-11): Complete MQTT ingestion pipeline
  - ✅ Step 0-6: Complete implementation from feature toggle to batch storage
  - ✅ Sidecar architecture: Zero-intrusion tap mode
  - ✅ Dual-write mechanism: Traditional + batch parallel processing
  - ✅ Production validation: 119 messages, 11 batches, 0 errors

## 📄 License

IBC-AI CO. Internal Project

## 🤝 Contributing

This is an internal project. For questions or support, please contact the development team.

## 📞 Support

- **Documentation**: See [Chinese README](README.ZH.md) for detailed documentation
- **Issues**: Contact the internal development team
- **Architecture**: Designed with sidecar/tap pattern for zero-downtime deployment
