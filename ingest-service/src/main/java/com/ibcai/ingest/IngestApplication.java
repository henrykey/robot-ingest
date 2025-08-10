package com.ibcai.ingest;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.ibcai.common.Cfg;
import com.ibcai.common.ConfigLoader;
import com.ibcai.common.JsonCoreHasher;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SpringBootApplication
public class IngestApplication {

    private static final Logger log = LoggerFactory.getLogger(IngestApplication.class);
    
    // 设备ID提取正则
    private static final Pattern DEVICE_ID_PATTERN = Pattern.compile("robots/([^/]+)/");
    
    // 统计计数器
    private static final Map<String, AtomicLong> rxCounter = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> enqCounter = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> dropCounter = new ConcurrentHashMap<>();
    private static final long startTime = System.currentTimeMillis();
    
    // 🚀 自适应高频处理组件
    private static boolean adaptiveEnabled = false;
    private static int maxMessageSizeKB = 16;
    private static int normalToHighFreqThreshold = 1000;
    private static int highFreqToNormalThreshold = 500;
    private static int statisticsIntervalSec = 2;
    private static int queueMaxSize = 5000;
    private static int batchSize = 100;
    private static int batchTimeoutMs = 100;
    
    // 高频模式状态
    private static final AtomicBoolean isHighFreqMode = new AtomicBoolean(false);
    private static final Map<String, BlockingQueue<MessageBuffer>> messageQueues = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> lastThroughputCheck = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    
    // 消息缓冲类
    static class MessageBuffer {
        final String topic;
        final String payload;
        final long timestamp;
        final String deviceId;
        
        MessageBuffer(String topic, String payload, String deviceId) {
            this.topic = topic;
            this.payload = payload;
            this.deviceId = deviceId;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(IngestApplication.class, args);

        Map<String,Object> cfg = ConfigLoader.load(args);
        String broker = Cfg.get(cfg, "mqtt.brokerUrl", "tcp://localhost:1883");
        String clientIdPrefix = Cfg.get(cfg, "mqtt.clientIdPrefix", "ingest-");
        String clientId = clientIdPrefix + UUID.randomUUID();

        // 🚀 MQTT高频优化配置
        int connectionTimeoutSec = Cfg.get(cfg, "mqtt.connectionTimeoutSec", 30);
        int keepAliveSec = Cfg.get(cfg, "mqtt.keepAliveSec", 60);
        int maxInflight = Cfg.get(cfg, "mqtt.maxInflight", 65535);  // 关键参数
        boolean cleanSession = Cfg.get(cfg, "mqtt.cleanSession", true);
        
        // 🚀 自适应高频处理配置加载
        Map<String, Object> adaptive = (Map<String, Object>) cfg.getOrDefault("adaptive", Collections.emptyMap());
        adaptiveEnabled = Cfg.get(adaptive, "enable", false);
        maxMessageSizeKB = Cfg.get(adaptive, "maxMessageSizeKB", 16);
        normalToHighFreqThreshold = Cfg.get(adaptive, "thresholds.normalToHighFreq", 1000);
        highFreqToNormalThreshold = Cfg.get(adaptive, "thresholds.highFreqToNormal", 500);
        statisticsIntervalSec = Cfg.get(adaptive, "statisticsWindow.entranceIntervalSec", 2);
        queueMaxSize = Cfg.get(adaptive, "messageQueue.maxSize", 5000);
        batchSize = Cfg.get(adaptive, "messageQueue.batchSize", 100);
        batchTimeoutMs = Cfg.get(adaptive, "messageQueue.timeoutMs", 100);
        
        log.info("🚀 Adaptive High-Frequency Processing: enabled={}, normalToHigh={}msg/s, highToNormal={}msg/s, queueSize={}, batchSize={}", 
            adaptiveEnabled, normalToHighFreqThreshold, highFreqToNormalThreshold, queueMaxSize, batchSize);

        String redisHost = Cfg.get(cfg, "redis.host", "127.0.0.1");
        int redisPort = Cfg.get(cfg, "redis.port", 6379);

        RedisClient rclient = RedisClient.create("redis://" + redisHost + ":" + redisPort);
        var rconn = rclient.connect();
        RedisCommands<String,String> R = rconn.sync();

        // 订阅主题和队列名
        Map<String, String> topicMap = Map.of(
            "state",      Cfg.get(cfg, "mqtt.topics.state", "robots/+/state"),
            "connection", Cfg.get(cfg, "mqtt.topics.connection", "robots/+/connection"),
            "network_ip", Cfg.get(cfg, "mqtt.topics.networkIp", "robots/+/network/ip"),
            "error",      Cfg.get(cfg, "mqtt.topics.error", "robots/+/error"),
            "cargo",      Cfg.get(cfg, "mqtt.topics.cargo", "robots/+/cargo")
        );
        Map<String, String> queueMap = Map.of(
            "state",      Cfg.get(cfg, "redis.queues.state", "q:state"),
            "connection", Cfg.get(cfg, "redis.queues.connection", "q:connection"),
            "network_ip", Cfg.get(cfg, "redis.queues.networkIp", "q:network_ip"),
            "error",      Cfg.get(cfg, "redis.queues.error", "q:error"),
            "cargo",      Cfg.get(cfg, "redis.queues.cargo", "q:cargo")
        );
        Map<String, Integer> qosMap = Map.of(
            "state",      Cfg.get(cfg, "mqtt.qos.state", 1),      // 🚀 默认QoS=1
            "connection", Cfg.get(cfg, "mqtt.qos.connection", 1),
            "network_ip", Cfg.get(cfg, "mqtt.qos.networkIp", 1),
            "error",      Cfg.get(cfg, "mqtt.qos.error", 1),
            "cargo",      Cfg.get(cfg, "mqtt.qos.cargo", 1)       // 🚀 默认QoS=1
        );

        // dedupe 配置
        Map<String, Object> dedupe = (Map<String, Object>) cfg.getOrDefault("dedupe", Collections.emptyMap());
        boolean dedupeEnable = Boolean.TRUE.equals(dedupe.getOrDefault("enable", false));
        int globalWindowMin = ((Number)dedupe.getOrDefault("timeWindowMinutes", 10)).intValue();
        Map<String, Object> perTopic = (Map<String, Object>) dedupe.getOrDefault("perTopic", Collections.emptyMap());

        // logAll 配置
        Map<String, Object> logging = (Map<String, Object>) cfg.getOrDefault("logging", Collections.emptyMap());
        boolean logAll = Boolean.TRUE.equals(logging.getOrDefault("logAll", false));

        // 🚀 初始化自适应高频处理组件
        if (adaptiveEnabled) {
            initializeAdaptiveProcessing(R, queueMap, dedupeEnable, globalWindowMin, perTopic, logAll);
        }

        // 🚀 构建优化的MQTT客户端 - 应用高频连接参数
        String host = broker.replace("tcp://","").split(":")[0];
        int port = Integer.parseInt(broker.substring(broker.lastIndexOf(':')+1));
        
        Mqtt3AsyncClient mqtt = MqttClient.builder()
                .useMqttVersion3()
                .identifier(clientId)
                .serverHost(host)
                .serverPort(port)
                .automaticReconnectWithDefaultConfig()
                .buildAsync();

        // 定义订阅函数供连接和重连时使用
        Runnable subscribeAll = () -> {
            log.info("🚀 MQTT connected to {}, starting optimized subscriptions with maxInflight={}", broker, maxInflight);
            for (String topicKey : topicMap.keySet()) {
                String topic = topicMap.get(topicKey);
                int qos = qosMap.get(topicKey);
                String queue = queueMap.get(topicKey);
                
                // 初始化计数器
                rxCounter.putIfAbsent(topicKey, new AtomicLong(0));
                enqCounter.putIfAbsent(topicKey, new AtomicLong(0));
                dropCounter.putIfAbsent(topicKey, new AtomicLong(0));
                lastThroughputCheck.putIfAbsent(topicKey, new AtomicLong(System.currentTimeMillis()));
                
                log.info("🚀 Subscribing to topic: {} qos: {} (adaptive high-frequency enabled={})", topic, qos, adaptiveEnabled);
                subscribe(mqtt, topic, qos, (realTopic, payload) -> {
                    if (adaptiveEnabled) {
                        handleMessageAdaptive(R, topicKey, realTopic, payload, queue, dedupeEnable, globalWindowMin, perTopic, logAll, qos);
                    } else {
                        handleMessageOptimized(R, topicKey, realTopic, payload, queue, dedupeEnable, globalWindowMin, perTopic, logAll, qos);
                    }
                });
            }
            
            String mode = adaptiveEnabled ? "ADAPTIVE HIGH-FREQUENCY" : "OPTIMIZED";
            log.info("🚀 All topics subscribed - {} MQTT processing ACTIVE", mode);
        };

        // 🚀 优化的连接配置 - 应用超时和心跳参数
        mqtt.connectWith()
                .cleanSession(cleanSession)
                .keepAlive(keepAliveSec)  // 应用配置的心跳间隔
                .send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        log.error("❌ Failed to connect to MQTT: {}", throwable.getMessage());
                        return;
                    }
                    log.info("🚀 MQTT connected with optimized settings: keepAlive={}s, cleanSession={}", 
                        keepAliveSec, cleanSession);
                    subscribeAll.run();
                })
                .join();

        log.info("🚀 HIGH-FREQUENCY MQTT INGEST ACTIVE - Connected to MQTT {} and Redis {}:{}", 
            broker, redisHost, redisPort);
    }

    // 🚀 初始化自适应高频处理组件
    private static void initializeAdaptiveProcessing(RedisCommands<String,String> R, Map<String, String> queueMap,
                                                     boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll) {
        log.info("🚀 Initializing Adaptive High-Frequency Processing...");
        
        // 为每个主题创建消息队列
        for (String topicKey : queueMap.keySet()) {
            messageQueues.put(topicKey, new LinkedBlockingQueue<>(queueMaxSize));
            
            // 启动消息处理线程
            scheduler.submit(() -> processMessageQueue(R, topicKey, queueMap.get(topicKey), dedupeEnable, globalWindowMin, perTopic, logAll));
        }
        
        // 启动吞吐量监控和模式切换线程
        scheduler.scheduleAtFixedRate(() -> {
            for (String topicKey : queueMap.keySet()) {
                checkThroughputAndSwitchMode(topicKey);
            }
        }, statisticsIntervalSec, statisticsIntervalSec, TimeUnit.SECONDS);
        
        // 启动统计输出线程
        scheduler.scheduleAtFixedRate(() -> {
            for (String topicKey : queueMap.keySet()) {
                printAdaptiveStats(topicKey);
            }
        }, 5, 5, TimeUnit.SECONDS);
        
        log.info("🚀 Adaptive High-Frequency Processing initialized successfully");
    }

    // 🚀 自适应消息处理器 - 动态模式切换
    private static void handleMessageAdaptive(RedisCommands<String,String> R, String topicKey, String topic, byte[] payload, String queue,
                                             boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll, int qos) {
        try {
            String payloadStr = new String(payload, StandardCharsets.UTF_8);
            
            // 检查消息大小限制
            if (payloadStr.length() > maxMessageSizeKB * 1024) {
                log.warn("🚀 [SIZE-LIMIT] topic={} messageSize={}KB > limit={}KB, dropped", 
                    topicKey, payloadStr.length() / 1024, maxMessageSizeKB);
                return;
            }
            
            // 更新接收计数
            long rxCount = rxCounter.get(topicKey).incrementAndGet();
            String deviceId = extractDeviceId(payloadStr, topic);
            
            if (isHighFreqMode.get()) {
                // 🚀 高频模式：放入队列缓冲
                BlockingQueue<MessageBuffer> queue_buffer = messageQueues.get(topicKey);
                MessageBuffer msgBuffer = new MessageBuffer(topic, payloadStr, deviceId);
                
                if (!queue_buffer.offer(msgBuffer)) {
                    // 队列满，丢弃消息
                    dropCounter.get(topicKey).incrementAndGet();
                    if (rxCount % 1000 == 0) {
                        log.warn("🚀 [HIGH-FREQ] topic={} queue_full, dropped message #{}", topicKey, rxCount);
                    }
                    return;
                }
                
                // 简化日志
                if (rxCount % 5000 == 0) {
                    log.info("🚀 [HIGH-FREQ] topic={} rx={} queue_size={}", topicKey, rxCount, queue_buffer.size());
                }
            } else {
                // 🚀 正常模式：直接处理
                handleMessageDirect(R, topicKey, topic, payloadStr, deviceId, queue, dedupeEnable, globalWindowMin, perTopic, logAll);
                
                // 正常频率日志
                if (rxCount % 100 == 0) {
                    log.debug("🚀 [NORMAL] topic={} rx={} processed_directly", topicKey, rxCount);
                }
            }
            
        } catch (Exception e) {
            log.error("❌ [ADAPTIVE-ERROR] topic={} error={}", topic, e.getMessage());
        }
    }
    private static void handleMessageOptimized(RedisCommands<String,String> R, String topicKey, String topic, byte[] payload, String queue,
                                               boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll, int qos) {
        try {
            String payloadStr = new String(payload, StandardCharsets.UTF_8);
            
            // 更新接收计数
            long rxCount = rxCounter.get(topicKey).incrementAndGet();
            
            // 🚀 FAST MODE: 如果不启用去重，直接入队（最高性能）
            if (!dedupeEnable) {
                R.lpush(queue, payloadStr);
                if (logAll) {
                    R.lpush("q:raw:" + topicKey, payloadStr);
                }
                enqCounter.get(topicKey).incrementAndGet();
                
                // 📊 统计输出（降低频率）
                if (rxCount % 1000 == 0) {
                    printStatsOptimized(topicKey);
                    log.info("🚀 [FAST-MODE] topic={} rx={} - NO DEDUP, MAX THROUGHPUT", topicKey, rxCount);
                }
                return;
            }
            
            // 🚀 去重模式：简化设备ID提取和去重逻辑
            String deviceId = extractDeviceId(payloadStr, topic);
            if (deviceId == null || deviceId.isEmpty()) {
                // 无deviceId直接入队，不做去重
                R.lpush(queue, payloadStr);
                enqCounter.get(topicKey).incrementAndGet();
                return;
            }

            // 🚀 简化去重：只使用基于时间的去重，不计算复杂hash
            String lastTsKey = "dedupe:" + topicKey + ":" + deviceId;
            long nowMs = System.currentTimeMillis();
            
            // 读取窗口配置（简化）
            Map<String,Object> topicConf = (Map<String,Object>) perTopic.getOrDefault(topicKey, Collections.emptyMap());
            int windowMin = topicConf.containsKey("timeWindowMinutes") ?
                    ((Number)topicConf.get("timeWindowMinutes")).intValue() : globalWindowMin;
            
            // 🚀 单次Redis查询检查时间窗口
            String lastAcceptMsStr = R.get(lastTsKey);
            long lastAcceptMs = lastAcceptMsStr == null ? 0 : Long.parseLong(lastAcceptMsStr);
            
            boolean within = lastAcceptMs > 0 && (nowMs - lastAcceptMs) < windowMin * 60_000;
            
            if (within) {
                // 🚀 时间窗口内丢弃 - 最少Redis操作
                dropCounter.get(topicKey).incrementAndGet();
                
                // � 降低日志频率
                if (rxCount % 1000 == 0) {
                    printStatsOptimized(topicKey);
                }
                return;
            }

            // 🚀 有效消息 - 最少Redis操作
            R.set(lastTsKey, String.valueOf(nowMs));  // 只更新时间戳
            R.lpush(queue, payloadStr);               // 入队
            if (logAll) {
                R.lpush("q:raw:" + topicKey, payloadStr);
            }
            
            enqCounter.get(topicKey).incrementAndGet();
            
            // � 统计输出（降低频率）
            if (rxCount % 1000 == 0) {
                printStatsOptimized(topicKey);
                if (log.isDebugEnabled()) {
                    log.debug("🚀 [BATCH-1000] topic={} deviceId={} processed_1000_messages", topic, deviceId);
                }
            }
            
        } catch (Exception e) {
            log.error("❌ [ERROR] topic={} error={}", topic, e.getMessage());
        }
    }

    // 🚀 生成 payload 预览（优化版本）
    private static String createPayloadPreview(String payloadStr) {
        if (payloadStr == null) return "null";
        String preview = payloadStr.length() > 100 ? payloadStr.substring(0, 100) + "..." : payloadStr;
        return preview.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", ".");
    }
    
    // 🚀 优化的统计输出 - 专为高频场景设计
    private static void printStatsOptimized(String topicKey) {
        long rx = rxCounter.get(topicKey).get();
        long enq = enqCounter.get(topicKey).get();
        long drop = dropCounter.get(topicKey).get();
        
        // 🚀 修正：处理总数 = 入队 + 去重丢弃（都是成功接收并处理的消息）
        long processed = enq + drop;
        double processRate = rx > 0 ? (double) processed / rx * 100 : 0;
        double dropRate = processed > 0 ? (double) drop / processed * 100 : 0;
        double throughput = rx / ((System.currentTimeMillis() - startTime) / 1000.0); // MQTT接收吞吐量（消息/秒）
        
        log.info("🚀 [STATS] topic={} rx={} processed={} enq={} drop={} process_rate={}% dedup_rate={}% throughput={}/s", 
            topicKey, rx, processed, enq, drop, 
            Math.round(processRate * 10) / 10.0, 
            Math.round(dropRate * 10) / 10.0, 
            Math.round(throughput * 10) / 10.0);
    }

    // 设备ID提取：优先 payload.deviceId，否则从 topic 解析
    private static String extractDeviceId(String payloadStr, String topic) {
        try {
            // 尝试从 JSON payload 提取 deviceId
            int idx = payloadStr.indexOf("\"deviceId\"");
            if (idx != -1) {
                int colon = payloadStr.indexOf(":", idx);
                if (colon != -1) {
                    int start = payloadStr.indexOf("\"", colon);
                    int end = payloadStr.indexOf("\"", start + 1);
                    if (start != -1 && end != -1) {
                        return payloadStr.substring(start + 1, end);
                    }
                }
            }
        } catch (Exception ignore) {}
        // 从 topic 解析
        Matcher m = DEVICE_ID_PATTERN.matcher(topic);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    // 🚀 吞吐量检查和模式切换
    private static void checkThroughputAndSwitchMode(String topicKey) {
        try {
            long currentTime = System.currentTimeMillis();
            long lastCheckTime = lastThroughputCheck.get(topicKey).get();
            long timeDiffSec = (currentTime - lastCheckTime) / 1000;
            
            if (timeDiffSec < statisticsIntervalSec) return;
            
            long currentRx = rxCounter.get(topicKey).get();
            long previousRx = lastThroughputCheck.get(topicKey).getAndSet(currentTime);
            
            // 计算吞吐量 (msg/s)
            double throughput = timeDiffSec > 0 ? (double)(currentRx) / ((currentTime - startTime) / 1000.0) : 0;
            
            boolean currentMode = isHighFreqMode.get();
            boolean shouldBeHighFreq = throughput > normalToHighFreqThreshold;
            boolean shouldBeNormal = throughput < highFreqToNormalThreshold;
            
            if (!currentMode && shouldBeHighFreq) {
                // 切换到高频模式
                isHighFreqMode.set(true);
                log.info("🚀 [MODE-SWITCH] topic={} NORMAL -> HIGH-FREQ, throughput={}/s > threshold={}/s", 
                    topicKey, Math.round(throughput), normalToHighFreqThreshold);
            } else if (currentMode && shouldBeNormal) {
                // 切换到正常模式
                isHighFreqMode.set(false);
                log.info("🚀 [MODE-SWITCH] topic={} HIGH-FREQ -> NORMAL, throughput={}/s < threshold={}/s", 
                    topicKey, Math.round(throughput), highFreqToNormalThreshold);
            }
            
        } catch (Exception e) {
            log.error("❌ [MODE-SWITCH-ERROR] topic={} error={}", topicKey, e.getMessage());
        }
    }
    
    // 🚀 自适应统计输出
    private static void printAdaptiveStats(String topicKey) {
        try {
            long rx = rxCounter.get(topicKey).get();
            long enq = enqCounter.get(topicKey).get();
            long drop = dropCounter.get(topicKey).get();
            
            long processed = enq + drop;
            double processRate = rx > 0 ? (double) processed / rx * 100 : 0;
            double dropRate = processed > 0 ? (double) drop / processed * 100 : 0;
            double throughput = rx / ((System.currentTimeMillis() - startTime) / 1000.0);
            
            BlockingQueue<MessageBuffer> queue = messageQueues.get(topicKey);
            int queueSize = queue != null ? queue.size() : 0;
            String mode = isHighFreqMode.get() ? "HIGH-FREQ" : "NORMAL";
            
            if (isHighFreqMode.get()) {
                // 高频模式简化日志
                log.info("🚀 [ADAPTIVE-{}] topic={} rx={} queue={} throughput={}/s", 
                    mode, topicKey, rx, queueSize, Math.round(throughput));
            } else {
                // 正常模式详细日志
                log.info("🚀 [ADAPTIVE-{}] topic={} rx={} processed={} enq={} drop={} process_rate={}% dedup_rate={}% throughput={}/s", 
                    mode, topicKey, rx, processed, enq, drop, 
                    Math.round(processRate * 10) / 10.0, 
                    Math.round(dropRate * 10) / 10.0, 
                    Math.round(throughput * 10) / 10.0);
            }
            
        } catch (Exception e) {
            log.error("❌ [ADAPTIVE-STATS-ERROR] topic={} error={}", topicKey, e.getMessage());
        }
    }

    // 🚀 消息队列处理线程 - 批量去重处理
    private static void processMessageQueue(RedisCommands<String,String> R, String topicKey, String queue,
                                           boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll) {
        BlockingQueue<MessageBuffer> messageQueue = messageQueues.get(topicKey);
        List<MessageBuffer> batch = new ArrayList<>(batchSize);
        
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 收集一批消息
                MessageBuffer first = messageQueue.poll(batchTimeoutMs, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                
                batch.clear();
                batch.add(first);
                
                // 尽量收集更多消息形成批次
                messageQueue.drainTo(batch, batchSize - 1);
                
                if (dedupeEnable) {
                    processBatchWithDedupe(R, topicKey, queue, batch, globalWindowMin, perTopic, logAll);
                } else {
                    processBatchNoDedupe(R, topicKey, queue, batch, logAll);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("🚀 [QUEUE-PROCESSOR] topic={} interrupted", topicKey);
        } catch (Exception e) {
            log.error("❌ [QUEUE-PROCESSOR] topic={} error={}", topicKey, e.getMessage());
        }
    }
    
    // 🚀 批量处理（带去重）
    private static void processBatchWithDedupe(RedisCommands<String,String> R, String topicKey, String queue, 
                                              List<MessageBuffer> batch, int globalWindowMin, Map<String,Object> perTopic, boolean logAll) {
        if (batch.isEmpty()) return;
        
        long nowMs = System.currentTimeMillis();
        Map<String,Object> topicConf = (Map<String,Object>) perTopic.getOrDefault(topicKey, Collections.emptyMap());
        int windowMin = topicConf.containsKey("timeWindowMinutes") ?
                ((Number)topicConf.get("timeWindowMinutes")).intValue() : globalWindowMin;
        
        // 批量检查去重状态
        Map<String, String> dedupeKeys = new HashMap<>();
        Set<String> deviceIds = new HashSet<>();
        
        for (MessageBuffer msg : batch) {
            if (msg.deviceId != null && !msg.deviceId.isEmpty()) {
                String dedupeKey = "dedupe:" + topicKey + ":" + msg.deviceId;
                dedupeKeys.put(msg.deviceId, dedupeKey);
                deviceIds.add(dedupeKey);
            }
        }
        
        // 批量获取去重状态
        Map<String, String> lastAcceptTimes = new HashMap<>();
        if (!deviceIds.isEmpty()) {
            List<String> keysList = new ArrayList<>(deviceIds);
            for (int i = 0; i < keysList.size(); i++) {
                String key = keysList.get(i);
                String value = R.get(key);  // 单独获取每个key
                if (value != null) {
                    lastAcceptTimes.put(key, value);
                }
            }
        }
        
        // 处理每条消息
        List<String> toEnqueue = new ArrayList<>();
        Map<String, String> toUpdateDedupe = new HashMap<>();
        
        for (MessageBuffer msg : batch) {
            if (msg.deviceId == null || msg.deviceId.isEmpty()) {
                // 无deviceId直接入队
                toEnqueue.add(msg.payload);
                enqCounter.get(topicKey).incrementAndGet();
                continue;
            }
            
            String dedupeKey = dedupeKeys.get(msg.deviceId);
            String lastAcceptMsStr = lastAcceptTimes.get(dedupeKey);
            long lastAcceptMs = lastAcceptMsStr == null ? 0 : Long.parseLong(lastAcceptMsStr);
            
            boolean within = lastAcceptMs > 0 && (nowMs - lastAcceptMs) < windowMin * 60_000;
            
            if (within) {
                dropCounter.get(topicKey).incrementAndGet();
            } else {
                toEnqueue.add(msg.payload);
                toUpdateDedupe.put(dedupeKey, String.valueOf(nowMs));
                enqCounter.get(topicKey).incrementAndGet();
            }
        }
        
        // 批量操作Redis
        if (!toEnqueue.isEmpty()) {
            R.lpush(queue, toEnqueue.toArray(new String[0]));
            if (logAll) {
                R.lpush("q:raw:" + topicKey, toEnqueue.toArray(new String[0]));
            }
        }
        
        if (!toUpdateDedupe.isEmpty()) {
            R.mset(toUpdateDedupe);
        }
    }
    
    // 🚀 批量处理（无去重）
    private static void processBatchNoDedupe(RedisCommands<String,String> R, String topicKey, String queue, 
                                            List<MessageBuffer> batch, boolean logAll) {
        if (batch.isEmpty()) return;
        
        List<String> payloads = batch.stream().map(msg -> msg.payload).toList();
        R.lpush(queue, payloads.toArray(new String[0]));
        
        if (logAll) {
            R.lpush("q:raw:" + topicKey, payloads.toArray(new String[0]));
        }
        
        enqCounter.get(topicKey).addAndGet(batch.size());
    }
    
    // 🚀 直接处理模式（正常模式使用）
    private static void handleMessageDirect(RedisCommands<String,String> R, String topicKey, String topic, String payloadStr, String deviceId,
                                           String queue, boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll) {
        
        if (!dedupeEnable) {
            R.lpush(queue, payloadStr);
            if (logAll) {
                R.lpush("q:raw:" + topicKey, payloadStr);
            }
            enqCounter.get(topicKey).incrementAndGet();
            return;
        }
        
        if (deviceId == null || deviceId.isEmpty()) {
            R.lpush(queue, payloadStr);
            enqCounter.get(topicKey).incrementAndGet();
            return;
        }

        // 去重逻辑
        String lastTsKey = "dedupe:" + topicKey + ":" + deviceId;
        long nowMs = System.currentTimeMillis();
        
        Map<String,Object> topicConf = (Map<String,Object>) perTopic.getOrDefault(topicKey, Collections.emptyMap());
        int windowMin = topicConf.containsKey("timeWindowMinutes") ?
                ((Number)topicConf.get("timeWindowMinutes")).intValue() : globalWindowMin;
        
        String lastAcceptMsStr = R.get(lastTsKey);
        long lastAcceptMs = lastAcceptMsStr == null ? 0 : Long.parseLong(lastAcceptMsStr);
        
        boolean within = lastAcceptMs > 0 && (nowMs - lastAcceptMs) < windowMin * 60_000;
        
        if (within) {
            dropCounter.get(topicKey).incrementAndGet();
            return;
        }

        R.set(lastTsKey, String.valueOf(nowMs));
        R.lpush(queue, payloadStr);
        if (logAll) {
            R.lpush("q:raw:" + topicKey, payloadStr);
        }
        
        enqCounter.get(topicKey).incrementAndGet();
    }

    interface MsgHandler { void handle(String topic, byte[] payload) throws Exception; }

    private static void subscribe(Mqtt3AsyncClient client, String topic, int qos, MsgHandler handler) {
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.fromCode(qos))
                .callback(msg -> {
                    try { handler.handle(msg.getTopic().toString(), msg.getPayloadAsBytes()); }
                    catch (Exception e) { 
                        log.error("❌ Message handler error: {}", e.getMessage());
                    }
                })
                .send();
    }
}
