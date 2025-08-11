package com.ibcai.ingest.queue;

import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis立即入列服务 - 步骤5：立即将"有效最新"消息写入Redis供批落库
 * 
 * 要求：
 * - LPUSH "ingest:{topic}:{objectKey}" 格式
 * - TTL REDIS_TTL_SEC=86400
 * - 可使用pipeline，失败只记录与轻量重试，不得阻塞原链路
 */
public class RedisWriter {
    
    private static final Logger log = LoggerFactory.getLogger(RedisWriter.class);
    
    private final RedisCommands<String, String> redis;
    private final String keyTemplate;
    private final int ttlSec;
    private final int retryAttempts;
    private final long retryDelayMs;
    
    // 统计计数器
    private final AtomicLong writeSuccessCount = new AtomicLong(0);
    private final AtomicLong writeFailureCount = new AtomicLong(0);
    private final AtomicLong retryCount = new AtomicLong(0);
    
    public RedisWriter(RedisCommands<String, String> redis, 
                      String keyTemplate, int ttlSec, 
                      int retryAttempts, long retryDelayMs) {
        this.redis = redis;
        this.keyTemplate = keyTemplate;
        this.ttlSec = ttlSec;
        this.retryAttempts = retryAttempts;
        this.retryDelayMs = retryDelayMs;
        
        log.info("🔧 RedisWriter initialized: keyTemplate={}, ttlSec={}, retryAttempts={}", 
                keyTemplate, ttlSec, retryAttempts);
    }
    
    /**
     * 立即写入消息到Redis队列
     * @param message 消息对象
     * @param topic 原始主题
     * @param objectKey 对象键（如robotId）
     * @return 是否成功写入
     */
    public boolean writeImmediate(Message message, String topic, String objectKey) {
        String redisKey = buildRedisKey(topic, objectKey);
        String payload = message.getPayloadString();
        
        int attempt = 0;
        while (attempt <= retryAttempts) {
            try {
                // LPUSH 消息到队列
                redis.lpush(redisKey, payload);
                
                // 设置TTL（仅在队列为新创建时设置）
                if (redis.ttl(redisKey) == -1) {
                    redis.expire(redisKey, ttlSec);
                }
                
                writeSuccessCount.incrementAndGet();
                
                if (attempt > 0) {
                    log.debug("✅ RedisWriter succeeded on retry {}: key={}, payloadSize={}", 
                             attempt, redisKey, payload.length());
                } else {
                    log.debug("✅ RedisWriter immediate success: key={}, payloadSize={}", 
                             redisKey, payload.length());
                }
                
                return true;
                
            } catch (Exception e) {
                attempt++;
                
                if (attempt <= retryAttempts) {
                    retryCount.incrementAndGet();
                    log.debug("⚠️ RedisWriter retry {}/{} for key={}: {}", 
                             attempt, retryAttempts, redisKey, e.getMessage());
                    
                    // 轻量重试延迟
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    writeFailureCount.incrementAndGet();
                    log.warn("❌ RedisWriter failed after {} attempts for key={}: {}", 
                            retryAttempts, redisKey, e.getMessage());
                }
            }
        }
        
        return false;
    }
    
    /**
     * 构建Redis键名
     */
    private String buildRedisKey(String topic, String objectKey) {
        return keyTemplate
                .replace("{topic}", topic)
                .replace("{objectKey}", objectKey);
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        long success = writeSuccessCount.get();
        long failure = writeFailureCount.get();
        long retry = retryCount.get();
        long total = success + failure;
        double successRate = total > 0 ? (success * 100.0 / total) : 100.0;
        
        return String.format("RedisWriter[success=%d, failure=%d, retry=%d, successRate=%.1f%%]", 
                           success, failure, retry, successRate);
    }
    
    /**
     * 获取写入成功计数
     */
    public long getWriteSuccessCount() {
        return writeSuccessCount.get();
    }
    
    /**
     * 获取写入失败计数
     */
    public long getWriteFailureCount() {
        return writeFailureCount.get();
    }
    
    /**
     * 获取重试计数
     */
    public long getRetryCount() {
        return retryCount.get();
    }
    
    /**
     * 重置计数器（测试用）
     */
    public void resetCounters() {
        writeSuccessCount.set(0);
        writeFailureCount.set(0);
        retryCount.set(0);
    }
}
