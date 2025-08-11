package com.ibcai.ingest.queue;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Step 4: 立即产出 MQTT lastone（retain）
 * 当消息被判定为"有效最新"时，立即发布到 lastone 主题并设置 retain=true
 */
public class LastonePublisher {
    
    private static final Logger log = LoggerFactory.getLogger(LastonePublisher.class);
    
    private final MqttClient mqttClient;
    private final String lastoneTopicPrefix;
    private final boolean enabled;
    
    // 统计计数器
    private final AtomicLong publishedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    
    public LastonePublisher(MqttClient mqttClient, String lastoneTopicPrefix, boolean enabled) {
        this.mqttClient = mqttClient;
        this.lastoneTopicPrefix = lastoneTopicPrefix;
        this.enabled = enabled;
        
        if (enabled) {
            log.info("🚀 LastonePublisher initialized: prefix={}, enabled={}", lastoneTopicPrefix, enabled);
        } else {
            log.info("⏸️ LastonePublisher disabled");
        }
    }
    
    /**
     * 发布有效最新消息到 lastone 主题
     * 主题格式：将原始主题中的 "robots" 替换为 "lastone"
     * 例如：robots/test001/state -> lastone/test001/state
     * 
     * @param message 原始消息
     * @param originalTopic 原始主题名称（如：robots/test001/state）
     */
    public void publishLastone(Message message, String originalTopic) {
        if (!enabled) {
            return;
        }
        
        try {
            // 构建 lastone 主题：将 "robots" 替换为 "lastone"
            // 例如：robots/test001/state -> lastone/test001/state
            String lastoneTopic = originalTopic.replace("robots", lastoneTopicPrefix);
            
            // 创建 MQTT 消息，设置 retain=true
            MqttMessage mqttMessage = new MqttMessage(message.getPayload());
            mqttMessage.setQos(1); // 使用 QoS 1 确保送达
            mqttMessage.setRetained(true); // 关键：设置 retain=true
            
            // 发布消息
            mqttClient.publish(lastoneTopic, mqttMessage);
            
            long count = publishedCount.incrementAndGet();
            
            // 记录前几条发布的消息
            if (count <= 5) {
                log.info("📡 LastonePublisher: published retained message #{} to topic: {}, deviceId={}, payloadSize={}", 
                    count, lastoneTopic, message.getDeviceId(), message.getPayload().length);
            } else if (count % 100 == 0) {
                // 每100条记录一次
                log.info("📡 LastonePublisher: published {} retained messages (latest topic: {})", 
                    count, lastoneTopic);
            }
            
        } catch (Exception e) {
            long failCount = failedCount.incrementAndGet();
            log.error("❌ LastonePublisher failed to publish message #{}: deviceId={}, originalTopic={}, error={}", 
                failCount, message.getDeviceId(), originalTopic, e.getMessage());
        }
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("LastonePublisher[published=%d, failed=%d, enabled=%s]", 
            publishedCount.get(), failedCount.get(), enabled);
    }
    
    /**
     * 获取发布成功计数
     */
    public long getPublishedCount() {
        return publishedCount.get();
    }
    
    /**
     * 获取发布失败计数
     */
    public long getFailedCount() {
        return failedCount.get();
    }
}
