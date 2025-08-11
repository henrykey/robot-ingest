package com.ibcai.ingest.queue;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 消息包装类 - 用于全局队列传递（步骤3扩展：支持deviceId和topicKey提取）
 */
public class Message {
    private final String topic;
    private final byte[] payload;
    private final long timestamp;
    
    // 缓存计算结果以提高性能
    private String deviceId;
    private String topicKey;
    private String payloadString;
    
    // robotId提取正则表达式
    private static final Pattern ROBOT_ID_PATTERN = Pattern.compile("robots/([^/]+)/");
    
    public Message(String topic, byte[] payload, long timestamp) {
        this.topic = topic;
        this.payload = payload;
        this.timestamp = timestamp;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public byte[] getPayload() {
        return payload;
    }
    
    /**
     * 获取payload的字符串形式（步骤3新增）
     */
    public String getPayloadString() {
        if (payloadString == null && payload != null) {
            payloadString = new String(payload, StandardCharsets.UTF_8);
        }
        return payloadString;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * 提取设备ID（步骤3新增）
     */
    public String getDeviceId() {
        if (deviceId == null) {
            Matcher matcher = ROBOT_ID_PATTERN.matcher(topic);
            if (matcher.find()) {
                deviceId = matcher.group(1);
            } else {
                deviceId = ""; // 避免重复计算
            }
        }
        return deviceId.isEmpty() ? null : deviceId;
    }
    
    /**
     * 提取topic键（最后一段）（步骤3新增）
     */
    public String getTopicKey() {
        if (topicKey == null) {
            String[] parts = topic.split("/");
            topicKey = parts.length > 0 ? parts[parts.length - 1] : "unknown";
        }
        return topicKey;
    }
    
    @Override
    public String toString() {
        return "Message{topic='" + topic + "', payloadSize=" + 
               (payload != null ? payload.length : 0) + ", timestamp=" + timestamp + 
               ", deviceId='" + getDeviceId() + "', topicKey='" + getTopicKey() + "'}";
    }
}
