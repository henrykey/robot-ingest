package com.ibcai.ingest.queue;

import com.ibcai.ingest.config.IngestFeatureConfig;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 分发器 - 步骤3：从GlobalQueue取出消息，按topic+objectKey分组分发到TopicWorker（集成去重与Redis输出）
 */
public class Dispatcher {
    
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "dispatcher");
        t.setDaemon(true);
        return t;
    });
    
    // TopicWorker管理
    private static final Map<String, TopicWorker> topicWorkers = new HashMap<>();
    
    // 统计
    private static final AtomicLong totalDispatched = new AtomicLong(0);
    private static final AtomicLong batchCount = new AtomicLong(0);
    
    private static volatile boolean started = false;
    
    // robotId提取正则表达式
    private static final Pattern ROBOT_ID_PATTERN = Pattern.compile("robots/([^/]+)/");
    
    /**
     * 启动分发器
     */
    public static synchronized void start() {
        if (!IngestFeatureConfig.isFeatureEnabled()) {
            log.info("📋 Dispatcher: Feature disabled, not starting");
            return;
        }
        
        if (started) {
            return;
        }
        
        started = true;
        
        int batchSize = IngestFeatureConfig.getBatchSize();
        int intervalMs = IngestFeatureConfig.getBatchIntervalMs();
        
        // 定时分发任务
        scheduler.scheduleAtFixedRate(() -> {
            try {
                dispatchBatch(batchSize);
            } catch (Exception e) {
                log.error("❌ Error in dispatcher: {}", e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        
        // 统计输出
        scheduler.scheduleAtFixedRate(() -> {
            long dispatched = totalDispatched.get();
            long batches = batchCount.get();
            if (dispatched > 0) {
                log.info("📊 Dispatcher stats: dispatched={}, batches={}, workers={}", 
                        dispatched, batches, topicWorkers.size());
            }
        }, 10, 10, TimeUnit.SECONDS);
        
        log.info("🚀 Dispatcher started: batchSize={}, intervalMs={}", batchSize, intervalMs);
    }
    
    /**
     * 批量分发处理
     */
    private static void dispatchBatch(int batchSize) {
        List<Message> messages = GlobalQueue.drainTo(batchSize);
        
        if (messages.isEmpty()) {
            return;
        }
        
        batchCount.incrementAndGet();
        
        // 按分组键分发消息
        Map<String, Integer> groupCounts = new HashMap<>();
        
        for (Message message : messages) {
            String groupKey = generateGroupKey(message);
            if (groupKey != null) {
                // 获取或创建TopicWorker
                TopicWorker worker = getOrCreateTopicWorker(groupKey);
                worker.offer(message);
                
                groupCounts.put(groupKey, groupCounts.getOrDefault(groupKey, 0) + 1);
                totalDispatched.incrementAndGet();
            }
        }
        
        // 日志输出（前几批显示详细信息）
        if (batchCount.get() <= 5) {
            log.info("🔄 Dispatched batch #{}: {} messages to {} groups: {}", 
                    batchCount.get(), messages.size(), groupCounts.size(), groupCounts);
        }
    }
    
    /**
     * 生成分组键：topic + objectKey(robotId)
     */
    private static String generateGroupKey(Message message) {
        String topic = message.getTopic();
        
        // 提取robotId
        String robotId = extractRobotId(topic);
        if (robotId == null) {
            return null;
        }
        
        // 提取topic类型（最后一段）
        String[] parts = topic.split("/");
        String topicType = parts.length > 0 ? parts[parts.length - 1] : "unknown";
        
        return topicType + ":" + robotId;
    }
    
    /**
     * 从topic中提取robotId
     */
    private static String extractRobotId(String topic) {
        Matcher matcher = ROBOT_ID_PATTERN.matcher(topic);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
    
    /**
     * 获取或创建TopicWorker - 步骤3：传递去重和Redis配置
     */
    private static synchronized TopicWorker getOrCreateTopicWorker(String groupKey) {
        TopicWorker worker = topicWorkers.get(groupKey);
        if (worker == null) {
            // 步骤3：检查配置管理器是否已初始化
            if (!Step3ConfigManager.isInitialized()) {
                log.warn("⚠️ Step3ConfigManager not initialized, creating worker without Redis/Dedupe");
                worker = new TopicWorker(groupKey, null, null, 5, "q:unknown");
            } else {
                // 从groupKey提取topicKey
                String topicKey = groupKey.split(":")[0];
                
                RedisCommands<String, String> redis = Step3ConfigManager.getRedisCommands();
                Map<String, Object> dedupeConfig = Step3ConfigManager.getDedupeConfig();
                int globalWindowMin = Step3ConfigManager.getGlobalWindowMin();
                String targetQueue = Step3ConfigManager.getTargetQueue(topicKey);
                
                worker = new TopicWorker(groupKey, redis, dedupeConfig, globalWindowMin, targetQueue);
            }
            
            worker.start();
            topicWorkers.put(groupKey, worker);
            log.info("🔧 Created new TopicWorker for group: {}", groupKey);
        }
        return worker;
    }
    
    /**
     * 获取分发统计
     */
    public static String getStats() {
        return String.format("Dispatcher[dispatched=%d, batches=%d, workers=%d]", 
                           totalDispatched.get(), batchCount.get(), topicWorkers.size());
    }
    
    /**
     * 停止分发器（用于测试）
     */
    public static synchronized void stop() {
        if (!started) {
            return;
        }
        
        scheduler.shutdown();
        
        // 停止所有TopicWorker
        for (TopicWorker worker : topicWorkers.values()) {
            worker.stop();
        }
        topicWorkers.clear();
        
        started = false;
        log.info("🛑 Dispatcher stopped");
    }
}
