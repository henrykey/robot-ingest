package com.ibcai.ingest.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 主题工作线程 - 步骤2：处理特定topic+objectKey的消息队列
 */
public class TopicWorker {
    
    private static final Logger log = LoggerFactory.getLogger(TopicWorker.class);
    
    private final String groupKey;
    private final BlockingQueue<Message> inputQueue;
    private final Thread workerThread;
    private volatile boolean running = false;
    
    // 统计
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    
    // 队列容量限制
    private static final int QUEUE_CAPACITY = 1000;
    
    public TopicWorker(String groupKey) {
        this.groupKey = groupKey;
        this.inputQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.workerThread = new Thread(this::processMessages, "ingest-topic-" + groupKey);
        this.workerThread.setDaemon(true);
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
     * 处理单条消息（步骤2暂时只丢弃）
     */
    private void processMessage(Message message) {
        // 步骤2：暂时只做计数，不做实际处理
        // 后续步骤会在这里添加去重、lastone发布、Redis写入等逻辑
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("TopicWorker[%s: processed=%d, dropped=%d, queueSize=%d]", 
                           groupKey, processedCount.get(), droppedCount.get(), inputQueue.size());
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
