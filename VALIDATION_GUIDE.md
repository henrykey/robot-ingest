# Robot Ingest Validation Guide

This guide provides CLI commands to validate the MQTT-to-Redis/MongoDB ingestion pipeline with deduplication and time window throttling.

## Redis Validation Commands

### 1. Check Message Queues

```bash
# Check queue lengths for all topics
redis-cli LLEN q:state
redis-cli LLEN q:connection
redis-cli LLEN q:network_ip
redis-cli LLEN q:error
redis-cli LLEN q:cargo

# Peek at latest messages in queues
redis-cli LRANGE q:state 0 4
redis-cli LRANGE q:connection 0 4

# Check raw message queues (if logAll=true)
redis-cli LLEN q:raw:state
redis-cli LRANGE q:raw:state 0 2
```

### 2. Check Deduplication State

```bash
# Check core hashes for specific device
redis-cli GET "dedupe:state:corehash:robot001"
redis-cli GET "dedupe:connection:corehash:robot001"

# Check last accept timestamps
redis-cli GET "dedupe:state:lasttimestamp:robot001"
redis-cli GET "dedupe:connection:lasttimestamp:robot001"

# List all devices with deduplication state
redis-cli KEYS "dedupe:state:corehash:*"
redis-cli KEYS "dedupe:connection:corehash:*"
```

### 3. Check Duplicate Counters

```bash
# Check duplicate counts for specific device and topic
redis-cli HGETALL "dupcount:state:robot001"
redis-cli HGETALL "dupcount:connection:robot001"
redis-cli HGETALL "dupcount:error:robot001"

# Expected fields in duplicate counter:
# - total_dropped: number of duplicates dropped
# - total_accepted: number of unique messages accepted
# - last_drop_timestamp: timestamp of last dropped message
# - last_accept_timestamp: timestamp of last accepted message

# List all devices with duplicate counters
redis-cli KEYS "dupcount:state:*"
redis-cli KEYS "dupcount:*:robot001"

# Get duplicate stats for all devices on a topic
redis-cli --scan --pattern "dupcount:state:*" | xargs -I {} redis-cli HGETALL {}
```

### 4. Check Latest State

```bash
# Check latest state for devices (state topic only)
redis-cli GET "latest:state:robot001"
redis-cli GET "latest:state:robot002"

# List all devices with latest state
redis-cli KEYS "latest:state:*"
```

### 5. Monitor Real-time Activity

```bash
# Monitor all Redis commands in real-time
redis-cli MONITOR

# Watch queue growth
watch -n 1 'redis-cli LLEN q:state'

# Watch duplicate counters change
watch -n 2 'redis-cli HGET dupcount:state:robot001 total_dropped'
```

## MongoDB Validation Commands

### 1. Connect to MongoDB

```bash
mongosh "mongodb://192.168.123.46:27017/robotdb"
```

### 2. Check Collections and Counts

```javascript
// List all collections
show collections

// Check document counts
db.state_events.countDocuments()
db.connection_events.countDocuments()
db.error_events.countDocuments()
db.robot_latest_state.countDocuments()

// Check recent documents
db.state_events.find().sort({timestamp: -1}).limit(5)
db.connection_events.find().sort({timestamp: -1}).limit(5)
```

### 3. Validate Data Integrity

```javascript
// Check for specific device events
db.state_events.find({deviceId: "robot001"}).sort({timestamp: -1}).limit(10)

// Check timestamp ranges
db.state_events.find({
  timestamp: {
    $gte: new Date(Date.now() - 3600000) // Last hour
  }
}).count()

// Check latest state collection
db.robot_latest_state.find({deviceId: "robot001"})

// Validate no duplicate events in time windows (should be empty or minimal)
db.state_events.aggregate([
  {
    $group: {
      _id: {
        deviceId: "$deviceId",
        minute: {
          $dateToString: {
            format: "%Y-%m-%d %H:%M",
            date: "$timestamp"
          }
        }
      },
      count: {$sum: 1}
    }
  },
  {
    $match: {count: {$gt: 1}}
  },
  {
    $sort: {count: -1}
  }
])
```

### 4. Performance Queries

```javascript
// Check insertion rates by hour
db.state_events.aggregate([
  {
    $group: {
      _id: {
        $dateToString: {
          format: "%Y-%m-%d %H",
          date: "$timestamp"
        }
      },
      count: {$sum: 1}
    }
  },
  {
    $sort: {_id: -1}
  }
])

// Find devices with highest event rates
db.state_events.aggregate([
  {
    $group: {
      _id: "$deviceId",
      count: {$sum: 1},
      latest: {$max: "$timestamp"}
    }
  },
  {
    $sort: {count: -1}
  },
  {
    $limit: 10
  }
])
```

## Testing Deduplication

### 1. Send Duplicate Messages

Use an MQTT client to send identical messages within the time window:

```bash
# Send identical state messages rapidly
mosquitto_pub -h 192.168.123.61 -t "robots/robot001/state" -m '{"battery":85,"position":{"lat":39.123456,"lon":116.123456},"taskStatus":"idle"}'
mosquitto_pub -h 192.168.123.61 -t "robots/robot001/state" -m '{"battery":85,"position":{"lat":39.123456,"lon":116.123456},"taskStatus":"idle"}'
mosquitto_pub -h 192.168.123.61 -t "robots/robot001/state" -m '{"battery":85,"position":{"lat":39.123456,"lon":116.123456},"taskStatus":"idle"}'
```

### 2. Verify Deduplication

```bash
# Should see duplicate counter increase
redis-cli HGET "dupcount:state:robot001" total_dropped

# Queue should only have one message
redis-cli LLEN q:state

# Check logs for [DROP] messages
docker logs robot-ingest-ingest | grep "\[DROP\]"
```

### 3. Send Different Messages

```bash
# Change battery level - should pass through
mosquitto_pub -h 192.168.123.61 -t "robots/robot001/state" -m '{"battery":84,"position":{"lat":39.123456,"lon":116.123456},"taskStatus":"idle"}'
```

## Expected Log Output

When running with `LOG_LEVEL_INGEST=DEBUG`, you should see logs like:

```text
[RX] topic=robots/robot001/state qos=0 deviceId=robot001 size=123 preview="{\"battery\":85,\"position\"..."
[ENQ] topic=robots/robot001/state q=q:state deviceId=robot001 hash=a1b2c3d4 queued_ok
[DROP] topic=robots/robot001/state deviceId=robot001 reason=duplicate same_hash=true within_window=30000ms
[STATS] topic=state rx=100 enq=45 drop=55 drop_rate=55.00%
```

## Key Performance Indicators

1. **Deduplication Rate**: `drop_rate` should be > 0% when duplicate messages are sent
2. **Queue Growth**: Queues should only grow with unique messages
3. **Redis Keys**: All `dedupe:*`, `dupcount:*`, and `latest:*` keys should be populated
4. **MongoDB Insertion**: Only unique messages should appear in MongoDB collections
5. **Time Window**: Duplicates within the configured time window should be dropped

## Troubleshooting

### High Duplicate Rate

- Check if coreFields configuration matches your message structure
- Verify position quantization decimals setting
- Check time window configuration

### No Deduplication

- Ensure `dedupe.enable=true` in config.yml
- Check Redis connectivity
- Verify log level is DEBUG to see deduplication decisions

### Missing Messages

- Check MQTT subscription status in logs
- Verify Redis queue operations
- Check MongoDB writer service logs
