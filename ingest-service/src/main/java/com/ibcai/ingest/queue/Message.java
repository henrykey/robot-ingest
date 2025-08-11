package com.ibcai.ingest.queue;

/**
 * 消息包装类 - 用于全局队列传递
 */
public class Message {
    private final String topic;
    private final byte[] payload;
    private final long timestamp;
    
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
    
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String toString() {
        return "Message{topic='" + topic + "', payloadSize=" + 
               (payload != null ? payload.length : 0) + ", timestamp=" + timestamp + '}';
    }
}
