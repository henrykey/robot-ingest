package com.ibcai.ingest.queue;

import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis输出服务 - 步骤3：侧挂架构的Redis队列输出
 * 严格遵循红线：仅在TopicWorker中使用，不影响原MQTT链路
 */
public class RedisOutputService {
    
    private static final Logger log = LoggerFactory.getLogger(RedisOutputService.class);
    
    // Redis接口
    private final RedisCommands<String, String> redis;
    
    // 统计计数器
    private static final AtomicLong totalQueued = new AtomicLong(0);
    private static final AtomicLong totalDropped = new AtomicLong(0);
    
    public RedisOutputService(RedisCommands<String, String> redis) {
        this.redis = redis;
        log.info("🔧 RedisOutputService initialized");
    }
    
    /**
     * 输出消息到Redis队列
     * @param message 消息对象
     * @param queueName Redis队列名称
     * @return 是否成功输出
     */
    public boolean outputToQueue(Message message, String queueName) {
        try {
            // 输出到主队列
            redis.lpush(queueName, message.getPayloadString());
            totalQueued.incrementAndGet();
            
            log.debug("📤 Message queued to {}: deviceId={}, payloadSize={}", 
                     queueName, message.getDeviceId(), message.getPayloadString().length());
            
            return true;
            
        } catch (Exception e) {
            log.error("❌ Error outputting message to Redis queue {}: {}", queueName, e.getMessage());
            totalDropped.incrementAndGet();
            return false;
        }
    }
    
    /**
     * 批量输出消息到Redis队列
     * @param messages 消息列表
     * @param queueName Redis队列名称
     * @return 成功输出的数量
     */
    public int batchOutputToQueue(List<Message> messages, String queueName) {
        if (messages.isEmpty()) {
            return 0;
        }
        
        try {
            // 提取所有payload
            String[] payloads = messages.stream()
                    .map(Message::getPayloadString)
                    .toArray(String[]::new);
            
            // 批量输出到队列
            redis.lpush(queueName, payloads);
            
            int count = messages.size();
            totalQueued.addAndGet(count);
            
            log.debug("📤 Batch queued {} messages to {}", count, queueName);
            
            return count;
            
        } catch (Exception e) {
            log.error("❌ Error batch outputting {} messages to Redis queue {}: {}", 
                     messages.size(), queueName, e.getMessage());
            totalDropped.addAndGet(messages.size());
            return 0;
        }
    }
    
    /**
     * 获取统计信息
     */
    public static String getStats() {
        long queued = totalQueued.get();
        long dropped = totalDropped.get();
        long total = queued + dropped;
        double successRate = total > 0 ? (queued * 100.0 / total) : 100.0;
        
        return String.format("RedisOutputService[queued=%d, dropped=%d, successRate=%.1f%%]", 
                           queued, dropped, successRate);
    }
    
    /**
     * 获取Redis输出丢弃计数
     */
    public static long getDroppedCount() {
        return totalDropped.get();
    }
    
    /**
     * 重置计数器（测试用）
     */
    public static void resetCounters() {
        totalQueued.set(0);
        totalDropped.set(0);
    }
}
