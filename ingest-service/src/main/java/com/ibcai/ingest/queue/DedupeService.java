package com.ibcai.ingest.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibcai.common.JsonCoreHasher;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 去重服务 - 步骤3：侧挂架构的去重与最新状态管理
 * 严格遵循红线：仅处理TopicWorker中的消息，不影响原MQTT链路
 */
public class DedupeService {
    
    private static final Logger log = LoggerFactory.getLogger(DedupeService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // 配置参数
    private final boolean enabled;
    private final List<String> coreFields;
    private final int positionDecimals;
    private final String hashAlgo;
    private final String dropCounterField;
    private final int globalWindowMin;
    
    // Redis接口
    private final RedisCommands<String, String> redis;
    
    // 统计计数器
    private static final AtomicLong totalProcessed = new AtomicLong(0);
    private static final AtomicLong totalDuplicated = new AtomicLong(0);
    private static final AtomicLong totalUnique = new AtomicLong(0);
    
    public DedupeService(RedisCommands<String, String> redis, Map<String, Object> dedupeConfig, int globalWindowMin) {
        this.redis = redis;
        this.globalWindowMin = globalWindowMin;
        
        if (dedupeConfig == null) {
            this.enabled = false;
            this.coreFields = Collections.emptyList();
            this.positionDecimals = 6;
            this.hashAlgo = "murmur3";
            this.dropCounterField = "state_dropped";
        } else {
            this.enabled = (Boolean) dedupeConfig.getOrDefault("enable", false);
            this.coreFields = (List<String>) dedupeConfig.getOrDefault("coreFields", Collections.emptyList());
            
            Map<String, Object> positionQuantize = (Map<String, Object>) dedupeConfig.get("positionQuantize");
            this.positionDecimals = positionQuantize != null ? 
                    ((Number) positionQuantize.getOrDefault("decimals", 6)).intValue() : 6;
                    
            this.hashAlgo = (String) dedupeConfig.getOrDefault("hashAlgo", "murmur3");
            this.dropCounterField = (String) dedupeConfig.getOrDefault("dropCounterField", "state_dropped");
        }
        
        log.info("🔧 DedupeService initialized: enabled={}, coreFields={}, positionDecimals={}, hashAlgo={}, globalWindowMin={}", 
                enabled, coreFields, positionDecimals, hashAlgo, globalWindowMin);
    }
    
    /**
     * 处理消息去重 - 返回是否应该保留此消息
     * @param message 原始消息
     * @return DedupeResult 包含处理结果和统计信息
     */
    public DedupeResult processMessage(Message message) {
        totalProcessed.incrementAndGet();
        
        if (!enabled) {
            totalUnique.incrementAndGet();
            return new DedupeResult(true, "DISABLED", null, null);
        }
        
        try {
            // 解析消息payload
            JsonNode payload = objectMapper.readTree(message.getPayloadString());
            
            String deviceId = message.getDeviceId();
            if (deviceId == null || deviceId.isEmpty()) {
                totalUnique.incrementAndGet();
                return new DedupeResult(true, "NO_DEVICE_ID", null, null);
            }
            
            String topicKey = message.getTopicKey();
            
            // 生成核心哈希
            String coreHash = generateCoreHash(payload);
            
            // Redis键
            String coreHashKey = String.format("robot:corehash:%s", deviceId);
            String lastTimestampKey = String.format("robot:lasttimestamp:%s:%s", topicKey, deviceId);
            
            long currentTime = System.currentTimeMillis();
            
            // 检查是否重复
            String lastHash = redis.get(coreHashKey);
            String lastTimestampStr = redis.get(lastTimestampKey);
            
            boolean isDuplicate = false;
            String reason = "NEW";
            
            if (lastHash != null && lastHash.equals(coreHash)) {
                // 核心内容相同，检查时间窗口
                if (lastTimestampStr != null) {
                    long lastTimestamp = Long.parseLong(lastTimestampStr);
                    long timeDiffMs = currentTime - lastTimestamp;
                    
                    if (timeDiffMs < globalWindowMin * 60_000) {
                        isDuplicate = true;
                        reason = String.format("DUPLICATE_WITHIN_WINDOW[%dms]", timeDiffMs);
                        totalDuplicated.incrementAndGet();
                    } else {
                        reason = String.format("SAME_CONTENT_OUTSIDE_WINDOW[%dms]", timeDiffMs);
                        totalUnique.incrementAndGet();
                    }
                } else {
                    reason = "SAME_CONTENT_NO_TIMESTAMP";
                    totalUnique.incrementAndGet();
                }
            } else {
                reason = "DIFFERENT_CONTENT";
                totalUnique.incrementAndGet();
            }
            
            // 如果不是重复，更新Redis状态
            if (!isDuplicate) {
                // 批量更新Redis
                Map<String, String> updates = new HashMap<>();
                updates.put(coreHashKey, coreHash);
                updates.put(lastTimestampKey, String.valueOf(currentTime));
                redis.mset(updates);
                
                // 更新latest状态
                String latestKey = String.format("robot:latest:%s", deviceId);
                redis.set(latestKey, message.getPayloadString());
            }
            
            return new DedupeResult(!isDuplicate, reason, coreHash, lastHash);
            
        } catch (Exception e) {
            log.error("❌ DedupeService error processing message: {}", e.getMessage(), e);
            totalUnique.incrementAndGet();
            return new DedupeResult(true, "ERROR", null, null);
        }
    }
    
    /**
     * 生成核心哈希
     */
    private String generateCoreHash(JsonNode payload) {
        if (coreFields.isEmpty()) {
            // 如果没有配置核心字段，使用整个payload
            return JsonCoreHasher.coreHash(payload.toString(), coreFields, positionDecimals);
        }
        
        // 使用配置的核心字段
        return JsonCoreHasher.coreHash(payload.toString(), coreFields, positionDecimals);
    }
    
    /**
     * 获取统计信息
     */
    public static String getStats() {
        long processed = totalProcessed.get();
        long duplicated = totalDuplicated.get();
        long unique = totalUnique.get();
        double dupeRate = processed > 0 ? (duplicated * 100.0 / processed) : 0.0;
        
        return String.format("DedupeService[processed=%d, unique=%d, duplicated=%d, dupeRate=%.1f%%]", 
                           processed, unique, duplicated, dupeRate);
    }
    
    /**
     * 重置计数器（测试用）
     */
    public static void resetCounters() {
        totalProcessed.set(0);
        totalDuplicated.set(0);
        totalUnique.set(0);
    }
    
    /**
     * 去重处理结果
     */
    public static class DedupeResult {
        public final boolean shouldKeep;
        public final String reason;
        public final String currentHash;
        public final String previousHash;
        
        public DedupeResult(boolean shouldKeep, String reason, String currentHash, String previousHash) {
            this.shouldKeep = shouldKeep;
            this.reason = reason;
            this.currentHash = currentHash;
            this.previousHash = previousHash;
        }
        
        @Override
        public String toString() {
            return String.format("DedupeResult[keep=%s, reason=%s]", shouldKeep, reason);
        }
    }
}
