package com.ibcai.ingest.queue;

import com.ibcai.common.Cfg;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 步骤3配置管理器 - 为侧挂架构提供Redis连接和配置共享
 */
public class Step3ConfigManager {
    
    private static final Logger log = LoggerFactory.getLogger(Step3ConfigManager.class);
    
    private static volatile RedisCommands<String, String> redisCommands;
    private static volatile Map<String, Object> dedupeConfig;
    private static volatile Map<String, String> queueMap;
    private static volatile int globalWindowMin;
    private static volatile boolean initialized = false;
    
    // Step 4: Lastone 发布服务
    private static volatile LastonePublisher lastonePublisher;
    
    // Step 5: RedisWriter 服务
    private static volatile RedisWriter redisWriter;
    
    /**
     * 初始化配置（从IngestApplication调用）
     */
    public static synchronized void initialize(Map<String, Object> cfg) {
        if (initialized) {
            return;
        }
        
        try {
            // 初始化Redis连接
            String redisHost = Cfg.get(cfg, "redis.host", "127.0.0.1");
            int redisPort = Cfg.get(cfg, "redis.port", 6379);
            
            RedisClient rclient = RedisClient.create("redis://" + redisHost + ":" + redisPort);
            var rconn = rclient.connect();
            redisCommands = rconn.sync();
            
            // 初始化去重配置
            dedupeConfig = (Map<String, Object>) cfg.get("dedupe");
            if (dedupeConfig == null) {
                dedupeConfig = new HashMap<>();
                dedupeConfig.put("enable", false);
                log.warn("⚠️ No dedupe config found, disabling deduplication");
            }
            
            // 初始化队列映射
            queueMap = Map.of(
                "state",      Cfg.get(cfg, "redis.queues.state", "q:state"),
                "connection", Cfg.get(cfg, "redis.queues.connection", "q:connection"),
                "network_ip", Cfg.get(cfg, "redis.queues.networkIp", "q:network_ip"),
                "error",      Cfg.get(cfg, "redis.queues.error", "q:error"),
                "cargo",      Cfg.get(cfg, "redis.queues.cargo", "q:cargo")
            );
            
            // 初始化全局时间窗口
            globalWindowMin = Cfg.get(cfg, "dedupe.timeWindowMinutes", 5);
            
            initialized = true;
            
            log.info("🔧 Step3ConfigManager initialized: dedupeEnabled={}, globalWindowMin={}, queues={}", 
                    dedupeConfig.get("enable"), globalWindowMin, queueMap.size());
                    
        } catch (Exception e) {
            log.error("❌ Failed to initialize Step3ConfigManager: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Step3ConfigManager", e);
        }
    }
    
    /**
     * 获取Redis命令接口
     */
    public static RedisCommands<String, String> getRedisCommands() {
        if (!initialized) {
            throw new IllegalStateException("Step3ConfigManager not initialized");
        }
        return redisCommands;
    }
    
    /**
     * 获取去重配置
     */
    public static Map<String, Object> getDedupeConfig() {
        if (!initialized) {
            throw new IllegalStateException("Step3ConfigManager not initialized");
        }
        return dedupeConfig;
    }
    
    /**
     * 获取队列映射
     */
    public static Map<String, String> getQueueMap() {
        if (!initialized) {
            throw new IllegalStateException("Step3ConfigManager not initialized");
        }
        return queueMap;
    }
    
    /**
     * 获取全局时间窗口（分钟）
     */
    public static int getGlobalWindowMin() {
        if (!initialized) {
            throw new IllegalStateException("Step3ConfigManager not initialized");
        }
        return globalWindowMin;
    }
    
    /**
     * 根据topicKey获取目标队列名
     */
    public static String getTargetQueue(String topicKey) {
        if (!initialized) {
            throw new IllegalStateException("Step3ConfigManager not initialized");
        }
        return queueMap.getOrDefault(topicKey, "q:unknown");
    }
    
    /**
     * 检查是否已初始化
     */
    public static boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Step 4: 设置 LastonePublisher
     */
    public static void setLastonePublisher(LastonePublisher publisher) {
        lastonePublisher = publisher;
        log.info("🔧 Step3ConfigManager: LastonePublisher set, enabled={}", 
                publisher != null ? "true" : "false");
    }
    
    /**
     * Step 4: 获取 LastonePublisher
     */
    public static LastonePublisher getLastonePublisher() {
        return lastonePublisher;
    }
    
    /**
     * Step 5: 设置 RedisWriter
     */
    public static void setRedisWriter(RedisWriter writer) {
        redisWriter = writer;
        log.info("🔧 Step3ConfigManager: RedisWriter set, enabled={}", 
                writer != null ? "true" : "false");
    }
    
    /**
     * Step 5: 获取 RedisWriter
     */
    public static RedisWriter getRedisWriter() {
        return redisWriter;
    }
}
