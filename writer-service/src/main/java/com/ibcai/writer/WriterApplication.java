package com.ibcai.writer;

import com.ibcai.common.Cfg;
import com.ibcai.common.ConfigLoader;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@SpringBootApplication
public class WriterApplication {

    private static final Logger log = LoggerFactory.getLogger(WriterApplication.class);

    public static void main(String[] args) throws Exception {
        SpringApplication.run(WriterApplication.class, args);

        Map<String,Object> cfg = ConfigLoader.load(args);

        String redisHost = Cfg.get(cfg, "redis.host", "127.0.0.1");
        int redisPort = Cfg.get(cfg, "redis.port", 6379);

        String mongoUri = Cfg.get(cfg, "mongodb.uri", "mongodb://127.0.0.1:27017");
        String mongoDb = Cfg.get(cfg, "mongodb.database", "MQTTLog");
        String robotsCollection = Cfg.get(cfg, "mongodb.collection", "robots");
        int ttlSeconds = Cfg.get(cfg, "mongodb.ttlSeconds", 2592000); // 30天默认TTL

        // 旧的batch配置（保持兼容性）
        int sizeTrigger = Cfg.get(cfg, "batch.sizeTrigger", 1000);
        int timeTrigger = Cfg.get(cfg, "batch.timeTriggerSec", 60);
        int maxPerFlush = Cfg.get(cfg, "batch.maxPerFlush", 5000);

        RedisClient rclient = RedisClient.create("redis://" + redisHost + ":" + redisPort);
        var rconn = rclient.connect();
        RedisCommands<String,String> R = rconn.sync();

        MongoTemplate mongo = new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoUri + "/" + mongoDb));

        // 创建索引和TTL
        createIndexes(mongo, robotsCollection, ttlSeconds);

        // Step 6: 启动新的批量Writer（如果启用）
        Map<String, Object> writerConfig = (Map<String, Object>) cfg.getOrDefault("writer", new HashMap<>());
        boolean newWriterEnabled = Cfg.get(writerConfig, "enabled", false);
        
        BatchWriter batchWriter = null;
        if (newWriterEnabled) {
            batchWriter = new BatchWriter(R, mongo, cfg);
            batchWriter.start();
            log.info("🚀 Step 6: BatchWriter started for ingest:* queues");
        }

        log.info("🚀 Writer service started - Legacy writer for q:state queue");
        
        long lastFlush = System.currentTimeMillis();
        long lastStats = System.currentTimeMillis();

        while (true) {
            // 旧的Writer逻辑（处理 q:state 队列，保持兼容性）
            long qlen = R.llen("q:state");
            if (qlen >= sizeTrigger || (System.currentTimeMillis() - lastFlush) / 1000 >= timeTrigger) {
                int toPop = (int)Math.min(qlen, maxPerFlush);
                List<String> batch = new ArrayList<>(toPop);
                for (int i = 0; i < toPop; i++) {
                    String v = R.rpop("q:state"); // LPUSH in ingest -> RPOP here
                    if (v == null) break;
                    batch.add(v);
                }
                if (!batch.isEmpty()) {
                    Instant t0 = Instant.now();
                    BulkOperations ops = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, robotsCollection);
                    for (String json : batch) {
                        Map<String,Object> doc = createRobotDocument("state", json, "legacy-writer");
                        ops.insert(doc);
                    }
                    ops.execute();
                    long ms = Duration.between(t0, Instant.now()).toMillis();
                    log.info("📊 Legacy Writer: Flushed {} state events in {} ms", batch.size(), ms);
                }
                lastFlush = System.currentTimeMillis();
            }
            
            // 每5分钟输出统计信息（减少日志频率）
            if (batchWriter != null && (System.currentTimeMillis() - lastStats) >= 300000) {
                log.info("📊 Writer Stats: {}", batchWriter.getStats());
                lastStats = System.currentTimeMillis();
            }
            
            Thread.sleep(50);
        }
    }
    
    /**
     * 创建索引和TTL策略
     */
    private static void createIndexes(MongoTemplate mongo, String collection, int ttlSeconds) {
        try {
            // 时间索引 + TTL自动清理
            mongo.getCollection(collection).createIndex(
                new org.bson.Document("time", 1),
                new com.mongodb.client.model.IndexOptions().expireAfter((long)ttlSeconds, java.util.concurrent.TimeUnit.SECONDS)
            );
            
            // 设备+时间复合索引
            mongo.getCollection(collection).createIndex(
                new org.bson.Document("deviceid", 1).append("time", 1)
            );
            
            // 主题+时间复合索引
            mongo.getCollection(collection).createIndex(
                new org.bson.Document("topic", 1).append("time", 1)
            );
            
            // 设备+主题复合索引
            mongo.getCollection(collection).createIndex(
                new org.bson.Document("deviceid", 1).append("topic", 1)
            );
            
            log.info("🔧 MongoDB indexes created successfully for collection: {}", collection);
        } catch (Exception e) {
            log.warn("⚠️ Failed to create indexes (may already exist): {}", e.getMessage());
        }
    }
    
    /**
     * 创建标准的机器人文档格式
     */
    private static Map<String, Object> createRobotDocument(String topic, String rawJson, String source) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("time", new Date());
        doc.put("deviceid", extractDeviceIdFromRaw(rawJson, topic));
        doc.put("topic", topic);
        doc.put("raw", rawJson);
        return doc;
    }
    
    /**
     * 从原始JSON和主题中提取设备ID
     */
    private static String extractDeviceIdFromRaw(String rawJson, String topic) {
        try {
            // 尝试从JSON中解析deviceId字段
            if (rawJson.contains("\"deviceId\"")) {
                int start = rawJson.indexOf("\"deviceId\"") + 11;
                start = rawJson.indexOf("\"", start) + 1;
                int end = rawJson.indexOf("\"", start);
                if (end > start) {
                    return rawJson.substring(start, end);
                }
            }
            
            // 备用方案：返回默认值
            return "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}
