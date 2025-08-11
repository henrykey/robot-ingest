package com.ibcai.ingest.queue;

import com.ibcai.ingest.config.IngestFeatureConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 全局入队组件 - 零处理镜像式侧挂 Tap
 * 严格遵循红线：不阻塞原链路，失败仅计数
 */
public class GlobalQueue {
    
    private static final Logger log = LoggerFactory.getLogger(GlobalQueue.class);
    
    // 全局队列实例
    private static final LinkedBlockingQueue<Message> queue;
    
    // 计数器
    private static final AtomicLong totalOffered = new AtomicLong(0);
    private static final AtomicLong totalDropped = new AtomicLong(0);
    private static final AtomicLong totalDrained = new AtomicLong(0);
    
    // 配置
    private static final boolean dropWhenFull;
    
    static {
        int capacity = IngestFeatureConfig.getQueueCapacity();
        dropWhenFull = IngestFeatureConfig.isDropWhenFull();
        queue = new LinkedBlockingQueue<>(capacity);
        
        log.info("🚀 GlobalQueue initialized: capacity={}, dropWhenFull={}", capacity, dropWhenFull);
    }
    
    /**
     * 非阻塞入队 - Tap点调用
     * @param message 消息
     * @return true=成功入队, false=队满丢弃
     */
    public static boolean offer(Message message) {
        if (!IngestFeatureConfig.isFeatureEnabled()) {
            return false; // 特性关闭时直接返回
        }
        
        totalOffered.incrementAndGet();
        
        boolean success;
        if (dropWhenFull) {
            // 丢弃策略：非阻塞offer
            success = queue.offer(message);
        } else {
            // 带超时策略：暂不实现，当前版本使用丢弃策略
            success = queue.offer(message);
        }
        
        if (!success) {
            totalDropped.incrementAndGet();
            // 仅计数，不打印日志避免影响性能
        }
        
        return success;
    }
    
    /**
     * 批量取出消息 - Dispatcher调用
     * @param maxCount 最大取出数量
     * @return 消息列表
     */
    public static java.util.List<Message> drainTo(int maxCount) {
        java.util.List<Message> messages = new java.util.ArrayList<>(maxCount);
        int drained = queue.drainTo(messages, maxCount);
        totalDrained.addAndGet(drained);
        return messages;
    }
    
    /**
     * 获取当前队列大小
     */
    public static int size() {
        return queue.size();
    }
    
    /**
     * 获取统计信息
     */
    public static String getStats() {
        return String.format("GlobalQueue[size=%d, offered=%d, dropped=%d, drained=%d]", 
                           size(), totalOffered.get(), totalDropped.get(), totalDrained.get());
    }
    
    /**
     * 获取入队总数
     */
    public static long getOfferedCount() {
        return totalOffered.get();
    }
    
    /**
     * 获取丢弃总数
     */
    public static long getDroppedCount() {
        return totalDropped.get();
    }
    
    /**
     * 重置计数器（测试用）
     */
    public static void resetCounters() {
        totalOffered.set(0);
        totalDropped.set(0);
        totalDrained.set(0);
    }
}
