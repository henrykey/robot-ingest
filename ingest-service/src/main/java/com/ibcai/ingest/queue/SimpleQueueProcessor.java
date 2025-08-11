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
 * 简单的队列处理器 - 步骤2：启动Dispatcher进行分发处理
 */
public class SimpleQueueProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(SimpleQueueProcessor.class);
    
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "simple-queue-processor");
        t.setDaemon(true);
        return t;
    });
    
    private static volatile boolean started = false;
    private static volatile long lastOffered = 0;  // 用于判断GlobalQueue是否有新活动
    
    /**
     * 启动处理器 - 步骤2：启动Dispatcher
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
        
        // 步骤2：启动Dispatcher代替原来的简单处理
        Dispatcher.start();
        
        // 每10秒输出统计（仅当有新活动时）
        scheduler.scheduleAtFixedRate(() -> {
            String globalStats = GlobalQueue.getStats();
            String dispatcherStats = Dispatcher.getStats();
            
            // 检查GlobalQueue是否有新的offered消息
            long currentOffered = GlobalQueue.getOfferedCount(); // 需要添加这个方法
            if (currentOffered > lastOffered) {
                log.info("📊 Step2 stats: {} | {}", globalStats, dispatcherStats);
                lastOffered = currentOffered;
            }
        }, 10, 10, TimeUnit.SECONDS);
        
        log.info("🚀 SimpleQueueProcessor started (Step 2: with Dispatcher)");
    }
    
    /**
     * 获取处理统计 - 步骤2：返回Dispatcher统计
     */
    public static String getStats() {
        return Dispatcher.getStats();
    }
}
