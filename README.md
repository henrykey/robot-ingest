# Robot Ingest Pipeline v1.1.3 (IBC AI CO.)

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
- **MongoDB**: Unified collection storage with optimized indexing

## 🗄️ Database Design

### MongoDB Architecture

```yaml
Database: MQTTLog
Collection: robots
Document Schema:
  _id: ObjectId          # MongoDB auto-generated
  time: Date            # Message ingestion time (indexed, TTL 30 days)
  deviceid: String      # Device ID extracted from topic (indexed)
  topic: String         # Message type: state/connection/cargo/error/networkIp (indexed)
  raw: String           # Original MQTT message content
```

### Index Strategy

```javascript
// Automatic index creation on startup
db.robots.createIndex({"time": 1}, {expireAfterSeconds: 2592000})  // TTL 30 days
db.robots.createIndex({"deviceid": 1, "time": 1})                 // Device timeline
db.robots.createIndex({"topic": 1, "time": 1})                    // Topic filtering
db.robots.createIndex({"deviceid": 1, "topic": 1})                // Device + topic
```

### Query Examples

```javascript
// Get device latest states
db.robots.find({"deviceid": "D00001", "topic": "state"}).sort({"time": -1}).limit(10)

// Get all connection events
db.robots.find({"topic": "connection"}).sort({"time": -1})

// Device time-range analysis
db.robots.find({
  "deviceid": "D00001", 
  "time": {"$gte": ISODate("2025-08-12"), "$lt": ISODate("2025-08-13")}
})

// Cross-topic device analysis
db.robots.find({"deviceid": "D00001"}).sort({"time": -1})
```

### Benefits

- **Unified Storage**: Single collection for all MQTT message types
- **Device-Centric**: Optimized for device timeline and cross-topic analysis
- **Performance**: Strategic indexing for fast queries
- **Auto Cleanup**: 30-day TTL prevents unlimited data growth
- **Scalability**: Ready for sharding on deviceid + time

## 📋 Configuration Example

```yaml
# Feature toggle
ingest:
  featureEnabled: true
  
# MongoDB configuration
mongodb:
  uri: "mongodb://192.168.123.46:27017"
  database: "MQTTLog"
  collection: "robots"
  ttlSeconds: 2592000  # 30 days auto cleanup
  
# Batch writing configuration
writer:
  batch:
    enabled: true
    batchSize: 100
    batchIntervalMs: 120000
    maxPerFlush: 500
    topics: [state, connection, networkIp, error, cargo]
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

## ⚙️ Configuration Management

### MQTT Broker Configuration

#### With Source Code (Recommended)

##### Method 1: Edit config.yml

```bash
# Edit configuration file
vim config.yml

# Modify brokerUrl
mqtt:
  brokerUrl: "tcp://NEW_IP:NEW_PORT"

# Restart container
docker-compose restart ingest-service
```

##### Method 2: Environment Variable Override

```bash
# Use environment variable
export CFG__MQTT__BROKER_URL="tcp://NEW_IP:NEW_PORT"

# Or add to docker-compose.yml
# ingest-service:
#   environment:
#     - CFG__MQTT__BROKER_URL=tcp://NEW_IP:NEW_PORT

# Restart container
docker-compose restart ingest-service
```

#### With Image/Container Only (Production Deployment)

##### Method 1: Environment Variables (Recommended)

```bash
# Stop current container
docker stop <container_name>

# Start with environment variable
docker run -d \
  --name <container_name> \
  -e CFG__MQTT__BROKER_URL="tcp://NEW_IP:NEW_PORT" \
  <image_name>
```

##### Method 2: Mount Configuration File

```bash
# Copy config from container
docker cp <container_name>:/app/config.yml ./config.yml

# Edit configuration
vim config.yml

# Restart with mounted config
docker stop <container_name>
docker run -d \
  --name <container_name> \
  -v $(pwd)/config.yml:/app/config.yml \
  <image_name>
```

##### Method 3: Docker Compose with .env File

```bash
# Create .env file
echo "MQTT_BROKER_URL=tcp://NEW_IP:NEW_PORT" > .env

# Use in docker-compose.yml
# services:
#   ingest-service:
#     environment:
#       - CFG__MQTT__BROKER_URL=${MQTT_BROKER_URL}

# Restart service
docker-compose up -d
```

### Configuration Priority

```text
Environment Variables > config.yml > Default Values
```

### Environment Variable Mapping

All configuration can be overridden using `CFG__` prefix:

```bash
CFG__MQTT__BROKER_URL=tcp://192.168.123.61:1883
CFG__MONGODB__URI=mongodb://192.168.123.46:27017
CFG__REDIS__HOST=192.168.123.45
CFG__INGEST__FEATURE_ENABLED=true
```

**Note**: Use double underscores `__` to separate configuration levels.

### MongoDB Configuration

#### MongoDB - With Source Code (Recommended)

##### MongoDB Method 1: Edit config.yml

```bash
# Edit configuration file
vim config.yml

# Modify MongoDB settings
mongodb:
  uri: "mongodb://NEW_IP:NEW_PORT"
  database: "MQTTLog"
  collection: "robots"
  ttlSeconds: 2592000

# Restart writer service
docker-compose restart writer-service
```

##### MongoDB Method 2: Environment Variable Override

```bash
# Use environment variables
export CFG__MONGODB__URI="mongodb://NEW_IP:NEW_PORT"
export CFG__MONGODB__DATABASE="NewDatabaseName"

# Or add to docker-compose.yml
# writer-service:
#   environment:
#     - CFG__MONGODB__URI=mongodb://NEW_IP:NEW_PORT
#     - CFG__MONGODB__DATABASE=NewDatabaseName

# Restart writer service
docker-compose restart writer-service
```

#### MongoDB - With Image/Container Only (Production Deployment)

##### MongoDB Method 1: Environment Variables (Recommended)

```bash
# Stop current container
docker stop <writer_container_name>

# Start with environment variables
docker run -d \
  --name <writer_container_name> \
  -e CFG__MONGODB__URI="mongodb://NEW_IP:NEW_PORT" \
  -e CFG__MONGODB__DATABASE="NewDatabaseName" \
  -e CFG__MONGODB__COLLECTION="robots" \
  <writer_image_name>
```

##### MongoDB Method 2: Mount Configuration File

```bash
# Copy config from container
docker cp <writer_container_name>:/app/config.yml ./config.yml

# Edit MongoDB configuration
vim config.yml

# Restart with mounted config
docker stop <writer_container_name>
docker run -d \
  --name <writer_container_name> \
  -v $(pwd)/config.yml:/app/config.yml \
  <writer_image_name>
```

##### MongoDB Method 3: Docker Compose with .env File

```bash
# Create .env file
cat > .env << EOF
MONGODB_URI=mongodb://NEW_IP:NEW_PORT
MONGODB_DATABASE=NewDatabaseName
MONGODB_COLLECTION=robots
MONGODB_TTL_SECONDS=2592000
EOF

# Use in docker-compose.yml
# services:
#   writer-service:
#     environment:
#       - CFG__MONGODB__URI=${MONGODB_URI}
#       - CFG__MONGODB__DATABASE=${MONGODB_DATABASE}
#       - CFG__MONGODB__COLLECTION=${MONGODB_COLLECTION}
#       - CFG__MONGODB__TTL_SECONDS=${MONGODB_TTL_SECONDS}

# Restart service
docker-compose up -d
```

### Common MongoDB Environment Variables

```bash
# Connection settings
CFG__MONGODB__URI=mongodb://192.168.123.46:27017
CFG__MONGODB__DATABASE=MQTTLog
CFG__MONGODB__COLLECTION=robots

# Performance settings
CFG__MONGODB__TTL_SECONDS=2592000
CFG__WRITER__BATCH__BATCH_SIZE=100
CFG__WRITER__BATCH__BATCH_INTERVAL_MS=120000
CFG__WRITER__BATCH__MAX_PER_FLUSH=500

# Authentication (if required)
CFG__MONGODB__URI=mongodb://username:password@192.168.123.46:27017/MQTTLog
```

### Verify Configuration

```bash
# Check container logs for MQTT connection
docker logs <container_name> | grep -i mqtt

# Check container logs for MongoDB connection
docker logs <writer_container_name> | grep -i mongo

# Verify environment variables in container
docker exec <container_name> printenv | grep CFG__
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

IBC AI CO. Internal Project

## 🤝 Contributing

This is an internal project. For questions or support, please contact the development team.

## 📞 Support

- **Documentation**: See [Chinese README](README.ZH.md) for detailed documentation
- **Issues**: Contact the internal development team
- **Architecture**: Designed with sidecar/tap pattern for zero-downtime deployment
