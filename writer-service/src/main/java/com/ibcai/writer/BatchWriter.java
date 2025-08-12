package com.ibcai.writer;

import com.ibcai.common.Cfg;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量Writer服务 - 步骤6：从Redis批读并批写MongoDB
 * 
 * 要求：
 * - 触发条件：WRITER_BATCH_SIZE=100（数量）或 WRITER_BATCH_INTERVAL_MS=120000（2分钟）
 * - 从Redis批读 → 批写MongoDB
 * - 不修改现有数据模型/对外接口
 * - 幂等/重复避免策略与现有逻辑一致
 */
public class BatchWriter {
    
    private static final Logger log = LoggerFactory.getLogger(BatchWriter.class);
    
    private final RedisCommands<String, String> redis;
    private final MongoTemplate mongo;
    private final Map<String, Object> config;
    
    // 配置参数
    private final boolean enabled;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxPerFlush;
    private final List<String> supportedTopics;
    private final Map<String, String> topicCollectionMap;
    
    // 运行状态
    private volatile boolean running = false;
    private Thread writerThread;
    private final Map<String, Long> lastFlushMap = new HashMap<>();
    
    // 统计计数器
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    
    public BatchWriter(RedisCommands<String, String> redis, MongoTemplate mongo, Map<String, Object> config) {
        this.redis = redis;
        this.mongo = mongo;
        this.config = config;
        
        // 读取配置
        Map<String, Object> writerConfig = (Map<String, Object>) config.getOrDefault("writer", new HashMap<>());
        
        this.enabled = Cfg.get(writerConfig, "enabled", true);
        this.batchSize = Cfg.get(writerConfig, "batchSize", 100);
        
        // 安全处理 Long 类型转换
        Number batchIntervalNum = Cfg.get(writerConfig, "batchIntervalMs", 120000);
        this.batchIntervalMs = batchIntervalNum.longValue();
        
        this.maxPerFlush = Cfg.get(writerConfig, "maxPerFlush", 500);
        
        this.supportedTopics = (List<String>) writerConfig.getOrDefault("topics", 
                Arrays.asList("state", "connection", "networkIp", "error", "cargo"));
        
        // 新设计：统一使用robots集合
        String robotsCollection = Cfg.get(config, "mongodb.collection", "robots");
        this.topicCollectionMap = new HashMap<>();
        for (String topic : supportedTopics) {
            this.topicCollectionMap.put(topic, robotsCollection);
        }
        
        // 初始化最后刷新时间
        long currentTime = System.currentTimeMillis();
        for (String topic : supportedTopics) {
            lastFlushMap.put(topic, currentTime);
        }
        
        log.info("🔧 BatchWriter initialized: enabled={}, batchSize={}, batchIntervalMs={}, topics={}", 
                enabled, batchSize, batchIntervalMs, supportedTopics);
    }
    
    /**
     * 启动批量Writer
     */
    public void start() {
        if (!enabled) {
            log.info("📋 BatchWriter: disabled, not starting");
            return;
        }
        
        if (running) {
            return;
        }
        
        running = true;
        writerThread = new Thread(this::writerLoop, "batch-writer");
        writerThread.setDaemon(true);
        writerThread.start();
        
        log.info("🚀 BatchWriter started");
    }
    
    /**
     * 停止批量Writer
     */
    public void stop() {
        running = false;
        if (writerThread != null) {
            writerThread.interrupt();
        }
        log.info("🛑 BatchWriter stopped");
    }
    
    /**
     * Writer主循环
     */
    private void writerLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                processAllTopics();
                Thread.sleep(1000); // 1秒检查间隔
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("❌ Error in BatchWriter loop: {}", e.getMessage(), e);
                totalErrors.incrementAndGet();
                
                try {
                    Thread.sleep(5000); // 错误后等待5秒
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    /**
     * 处理所有支持的主题
     */
    private void processAllTopics() {
        for (String topic : supportedTopics) {
            try {
                processTopicBatches(topic);
            } catch (Exception e) {
                log.error("❌ Error processing topic {}: {}", topic, e.getMessage());
                totalErrors.incrementAndGet();
            }
        }
    }
    
    /**
     * 处理特定主题的批量数据
     */
    private void processTopicBatches(String topic) {
        // 查找所有匹配的Redis键
        String pattern = "ingest:" + topic + ":*";
        List<String> keyList = redis.keys(pattern);
        Set<String> keys = new HashSet<>(keyList);
        
        if (keys.isEmpty()) {
            return;
        }
        
        long currentTime = System.currentTimeMillis();
        long lastFlush = lastFlushMap.get(topic);
        boolean timeTriggered = (currentTime - lastFlush) >= batchIntervalMs;
        
        // 计算总的待处理消息数
        long totalMessages = 0;
        for (String key : keys) {
            totalMessages += redis.llen(key);
        }
        
        boolean sizeTriggered = totalMessages >= batchSize;
        
        if (sizeTriggered || timeTriggered) {
            processBatch(topic, keys, timeTriggered ? "TIME" : "SIZE");
            lastFlushMap.put(topic, currentTime);
        }
    }
    
    /**
     * 处理批量数据
     */
    private void processBatch(String topic, Set<String> keys, String triggerReason) {
        List<String> batch = new ArrayList<>();
        
        // 从所有相关键中收集消息
        for (String key : keys) {
            long keyLen = redis.llen(key);
            int toTake = (int) Math.min(keyLen, maxPerFlush - batch.size());
            
            for (int i = 0; i < toTake; i++) {
                String message = redis.rpop(key);
                if (message != null) {
                    batch.add(message);
                }
            }
            
            if (batch.size() >= maxPerFlush) {
                break;
            }
        }
        
        if (!batch.isEmpty()) {
            writeToMongoDB(topic, batch, triggerReason);
        }
    }
    
    /**
     * 写入MongoDB
     */
    private void writeToMongoDB(String topic, List<String> batch, String triggerReason) {
        String collectionName = topicCollectionMap.getOrDefault(topic, "robots");
        
        try {
            Instant startTime = Instant.now();
            
            BulkOperations ops = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionName);
            
            for (String json : batch) {
                Map<String, Object> doc = createRobotDocument(topic, json, "step6-batch-writer");
                ops.insert(doc);
            }
            
            ops.execute();
            
            long durationMs = Duration.between(startTime, Instant.now()).toMillis();
            
            totalProcessed.addAndGet(batch.size());
            totalBatches.incrementAndGet();
            
            log.info("✅ BatchWriter[{}]: Flushed {} messages to {} in {}ms (trigger={})", 
                    topic, batch.size(), collectionName, durationMs, triggerReason);
            
        } catch (Exception e) {
            log.error("❌ Failed to write batch to MongoDB for topic {}: {}", topic, e.getMessage(), e);
            totalErrors.incrementAndGet();
            
            // 失败时将消息重新放回Redis（简单重试策略）
            for (String message : batch) {
                String key = "ingest:" + topic + ":retry";
                redis.lpush(key, message);
            }
        }
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("BatchWriter[processed=%d, batches=%d, errors=%d, running=%s]", 
                           totalProcessed.get(), totalBatches.get(), totalErrors.get(), running);
    }
    
    /**
     * 获取处理的消息总数
     */
    public long getProcessedCount() {
        return totalProcessed.get();
    }
    
    /**
     * 获取批次总数
     */
    public long getBatchCount() {
        return totalBatches.get();
    }
    
    /**
     * 获取错误总数
     */
    public long getErrorCount() {
        return totalErrors.get();
    }
    
    /**
     * 检查是否正在运行
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * 创建标准的机器人文档格式
     */
    private Map<String, Object> createRobotDocument(String topic, String rawJson, String source) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("time", new Date());
        doc.put("deviceid", extractDeviceIdFromTopic(rawJson, topic));
        doc.put("topic", topic);
        doc.put("raw", rawJson);
        return doc;
    }
    
    /**
     * 从原始JSON中提取设备ID
     * 尝试多种方式解析设备ID
     */
    private String extractDeviceIdFromTopic(String rawJson, String topic) {
        try {
            // 方式1: 尝试从JSON中解析deviceId字段
            if (rawJson.contains("\"deviceId\"")) {
                int start = rawJson.indexOf("\"deviceId\"") + 11;
                start = rawJson.indexOf("\"", start) + 1;
                int end = rawJson.indexOf("\"", start);
                if (end > start) {
                    return rawJson.substring(start, end);
                }
            }
            
            // 方式2: 尝试解析robotId字段
            if (rawJson.contains("\"robotId\"")) {
                int start = rawJson.indexOf("\"robotId\"") + 10;
                start = rawJson.indexOf("\"", start) + 1;
                int end = rawJson.indexOf("\"", start);
                if (end > start) {
                    return rawJson.substring(start, end);
                }
            }
            
            // 方式3: 尝试解析id字段
            if (rawJson.contains("\"id\"")) {
                int start = rawJson.indexOf("\"id\"") + 5;
                start = rawJson.indexOf("\"", start) + 1;
                int end = rawJson.indexOf("\"", start);
                if (end > start) {
                    String id = rawJson.substring(start, end);
                    // 只有当id看起来像设备ID时才使用
                    if (id.matches("^[A-Z0-9]+$")) {
                        return id;
                    }
                }
            }
            
            // 备用方案：返回默认值
            return "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}
