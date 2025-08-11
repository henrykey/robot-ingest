package com.ibcai.ingest.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * GlobalQueue 单元测试
 */
public class GlobalQueueTest {
    
    @BeforeEach
    void setUp() {
        // 重置计数器
        GlobalQueue.resetCounters();
        // 设置环境变量启用特性
        System.setProperty("INGEST_FEATURE_ENABLED", "true");
    }
    
    @Test
    void testOfferAndDrain() {
        // 创建测试消息
        Message msg1 = new Message("test/topic1", "payload1".getBytes(), System.currentTimeMillis());
        Message msg2 = new Message("test/topic2", "payload2".getBytes(), System.currentTimeMillis());
        
        // 入队
        assertTrue(GlobalQueue.offer(msg1));
        assertTrue(GlobalQueue.offer(msg2));
        assertEquals(2, GlobalQueue.size());
        
        // 取出
        var messages = GlobalQueue.drainTo(10);
        assertEquals(2, messages.size());
        assertEquals(0, GlobalQueue.size());
        
        // 验证消息内容
        assertEquals("test/topic1", messages.get(0).getTopic());
        assertEquals("test/topic2", messages.get(1).getTopic());
    }
    
    @Test
    void testFeatureDisabled() {
        // 关闭特性
        System.setProperty("INGEST_FEATURE_ENABLED", "false");
        
        Message msg = new Message("test/topic", "payload".getBytes(), System.currentTimeMillis());
        
        // 特性关闭时应该返回false但不报错
        assertFalse(GlobalQueue.offer(msg));
        assertEquals(0, GlobalQueue.size());
    }
    
    @Test
    void testStats() {
        Message msg = new Message("test/topic", "payload".getBytes(), System.currentTimeMillis());
        GlobalQueue.offer(msg);
        
        String stats = GlobalQueue.getStats();
        assertTrue(stats.contains("offered=1"));
        assertTrue(stats.contains("size=1"));
    }
}
