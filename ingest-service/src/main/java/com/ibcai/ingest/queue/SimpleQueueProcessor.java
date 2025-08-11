package com.ibcai.ingest.queue;

import com.ibcai.ingest.config.IngestFeatureConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简单的队列处理器 - 步骤1用，只做入队和丢弃验证
 */
public class SimpleQueueProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(SimpleQueueProcessor.class);
    
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "simple-queue-processor");
        t.setDaemon(true);
        return t;
    });
    
    private static final AtomicLong totalProcessed = new AtomicLong(0);
    private static final AtomicLong totalDropped = new AtomicLong(0);
    
    private static volatile boolean started = false;
    
    /**
     * 启动简单处理器
     */
    public static synchronized void start() {
        if (!IngestFeatureConfig.isFeatureEnabled()) {
            log.info("📋 SimpleQueueProcessor: Feature disabled, not starting");
            return;
        }
        
        if (started) {
            return;
        }
        
        started = true;
        
        // 每100ms检查一次队列
        scheduler.scheduleAtFixedRate(() -> {
            try {
                processQueue();
            } catch (Exception e) {
                log.error("❌ Error in queue processing: {}", e.getMessage());
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
        
        // 每10秒输出统计
        scheduler.scheduleAtFixedRate(() -> {
            if (totalProcessed.get() > 0 || totalDropped.get() > 0) {
                log.info("📊 SimpleQueueProcessor stats: processed={}, dropped={}, queueSize={}, globalStats={}", 
                        totalProcessed.get(), totalDropped.get(), GlobalQueue.size(), GlobalQueue.getStats());
            }
        }, 10, 10, TimeUnit.SECONDS);
        
        log.info("🚀 SimpleQueueProcessor started");
    }
    
    private static void processQueue() {
        int batchSize = IngestFeatureConfig.getBatchSize();
        List<Message> messages = GlobalQueue.drainTo(batchSize);
        
        if (!messages.isEmpty()) {
            // 简单处理：只计数然后丢弃
            totalProcessed.addAndGet(messages.size());
            
            // 为了验证，记录第一条消息的信息
            if (totalProcessed.get() <= 5) {
                Message first = messages.get(0);
                log.info("🔍 Sample message: topic={}, payloadSize={}, timestamp={}", 
                        first.getTopic(), first.getPayload().length, first.getTimestamp());
            }
        }
    }
    
    /**
     * 获取处理统计
     */
    public static String getStats() {
        return String.format("SimpleQueueProcessor[processed=%d, dropped=%d]", 
                           totalProcessed.get(), totalDropped.get());
    }
}
