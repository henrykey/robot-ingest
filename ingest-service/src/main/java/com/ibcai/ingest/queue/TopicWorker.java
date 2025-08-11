package com.ibcai.ingest.queue;

import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 主题工作线程 - 步骤3：处理特定topic+objectKey的消息队列，集成去重与Redis输出
 */
public class TopicWorker {
    
    private static final Logger log = LoggerFactory.getLogger(TopicWorker.class);
    
    private final String groupKey;
    private final BlockingQueue<Message> inputQueue;
    private final Thread workerThread;
    private volatile boolean running = false;
    
    // 步骤3：去重与Redis输出服务
    private final DedupeService dedupeService;
    private final RedisOutputService redisOutputService;
    private final String targetQueueName;
    
    // Step 4: Lastone 发布服务
    private final LastonePublisher lastonePublisher;
    private final String originalTopic; // 用于构建 lastone 主题
    
    // 统计
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicLong uniqueCount = new AtomicLong(0);
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final AtomicLong redisSuccessCount = new AtomicLong(0);
    private final AtomicLong redisFailureCount = new AtomicLong(0);
    
    // 队列容量限制
    private static final int QUEUE_CAPACITY = 1000;
    
    public TopicWorker(String groupKey, RedisCommands<String, String> redis, 
                      Map<String, Object> dedupeConfig, int globalWindowMin, String targetQueueName,
                      LastonePublisher lastonePublisher, String originalTopic) {
        this.groupKey = groupKey;
        this.inputQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.workerThread = new Thread(this::processMessages, "ingest-topic-" + groupKey);
        this.workerThread.setDaemon(true);
        
        // 步骤3：初始化去重与Redis输出服务
        this.dedupeService = new DedupeService(redis, dedupeConfig, globalWindowMin);
        this.redisOutputService = new RedisOutputService(redis);
        this.targetQueueName = targetQueueName;
        
        // Step 4: 初始化 Lastone 发布服务
        this.lastonePublisher = lastonePublisher;
        this.originalTopic = originalTopic;
    }
    
    /**
     * 启动工作线程
     */
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        workerThread.start();
        log.info("🚀 TopicWorker started for group: {}", groupKey);
    }
    
    /**
     * 停止工作线程
     */
    public void stop() {
        running = false;
        workerThread.interrupt();
        log.info("🛑 TopicWorker stopped for group: {}", groupKey);
    }
    
    /**
     * 非阻塞入队消息
     */
    public boolean offer(Message message) {
        boolean success = inputQueue.offer(message);
        if (!success) {
            droppedCount.incrementAndGet();
            // 队满时丢弃，仅计数不打印日志（避免影响性能）
        }
        return success;
    }
    
    /**
     * 消息处理主循环
     */
    private void processMessages() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                // 阻塞等待消息（1秒超时）
                Message message = inputQueue.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                
                if (message != null) {
                    processMessage(message);
                    processedCount.incrementAndGet();
                    
                    // 显示前几条消息的处理信息
                    if (processedCount.get() <= 3) {
                        log.info("🔍 TopicWorker[{}] processed message: topic={}, payloadSize={}", 
                                groupKey, message.getTopic(), message.getPayload().length);
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("❌ Error processing message in TopicWorker[{}]: {}", groupKey, e.getMessage());
            }
        }
    }
    
    /**
     * 处理单条消息 - 步骤3：集成去重与Redis输出
     */
    private void processMessage(Message message) {
        try {
            // 步骤3：去重处理
            DedupeService.DedupeResult dedupeResult = dedupeService.processMessage(message);
            
            if (dedupeResult.shouldKeep) {
                // Step 4: 先发布到 lastone 主题（retain=true）
                if (lastonePublisher != null) {
                    lastonePublisher.publishLastone(message, originalTopic);
                }
                
                // 然后输出到Redis队列
                boolean redisSuccess = redisOutputService.outputToQueue(message, targetQueueName);
                
                if (redisSuccess) {
                    uniqueCount.incrementAndGet();
                    redisSuccessCount.incrementAndGet();
                    
                    // 显示前几条唯一消息的详细信息
                    if (uniqueCount.get() <= 3) {
                        log.info("✅ TopicWorker[{}] processed unique message: deviceId={}, reason={}, queuedTo={}", 
                                groupKey, message.getDeviceId(), dedupeResult.reason, targetQueueName);
                    }
                } else {
                    redisFailureCount.incrementAndGet();
                    log.warn("❌ TopicWorker[{}] failed to queue message to Redis: deviceId={}", 
                            groupKey, message.getDeviceId());
                }
            } else {
                // 重复消息，仅计数
                duplicateCount.incrementAndGet();
                
                // 显示前几条重复消息的信息
                if (duplicateCount.get() <= 3) {
                    log.info("🔄 TopicWorker[{}] dropped duplicate message: deviceId={}, reason={}", 
                            groupKey, message.getDeviceId(), dedupeResult.reason);
                }
            }
            
        } catch (Exception e) {
            log.error("❌ Error processing message in TopicWorker[{}]: {}", groupKey, e.getMessage());
        }
    }
    
    /**
     * 获取统计信息 - 步骤3：包含去重与Redis输出统计
     */
    public String getStats() {
        return String.format("TopicWorker[%s: processed=%d, unique=%d, duplicate=%d, redisOK=%d, redisFail=%d, queueSize=%d]", 
                           groupKey, processedCount.get(), uniqueCount.get(), duplicateCount.get(),
                           redisSuccessCount.get(), redisFailureCount.get(), inputQueue.size());
    }
    
    /**
     * 获取队列大小
     */
    public int getQueueSize() {
        return inputQueue.size();
    }
    
    /**
     * 获取处理数量
     */
    public long getProcessedCount() {
        return processedCount.get();
    }
    
    /**
     * 获取丢弃数量
     */
    public long getDroppedCount() {
        return droppedCount.get();
    }
}
