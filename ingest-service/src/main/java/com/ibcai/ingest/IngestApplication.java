package com.ibcai.ingest;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.ibcai.common.Cfg;
import com.ibcai.common.ConfigLoader;
import com.ibcai.common.JsonCoreHasher;
import com.ibcai.ingest.queue.GlobalQueue;
import com.ibcai.ingest.queue.Message;
import com.ibcai.ingest.queue.SimpleQueueProcessor;
import com.ibcai.ingest.queue.Dispatcher;
import com.ibcai.ingest.queue.Step3ConfigManager;
import com.ibcai.ingest.queue.DedupeService;
import com.ibcai.ingest.queue.RedisOutputService;
import com.ibcai.ingest.queue.LastonePublisher;
import com.ibcai.ingest.config.IngestFeatureConfig;
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
    
    // 🚀 异步消息处理线程池 (参考测试客户端优化)
    private static final ExecutorService messageProcessor = Executors.newFixedThreadPool(10);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    
    // 🚀 步骤1&2：全局统计变量（总数+吞吐量）
    private static final AtomicLong globalTotalMessages = new AtomicLong(0);
    private static final AtomicLong globalMessagesInWindow = new AtomicLong(0);
    private static volatile long lastGlobalStatTime = System.currentTimeMillis();
    private static volatile long throughputStartTime = System.currentTimeMillis();
    private static volatile long lastGlobalTotalMessages = 0; // 用于判断globalTotalMessages是否增加
    private static final List<Long> recentMessageTimes = Collections.synchronizedList(new ArrayList<>());
    
    // 可配置的统计时间窗口（毫秒）
    private static int statsThroughputWindowMs = IngestFeatureConfig.getStatsThroughputWindowSec() * 1000;
    private static int statsOutputIntervalMs = IngestFeatureConfig.getStatsOutputIntervalSec() * 1000;
    
    // 🚀 步骤3：动态日志模式（附加防抖动机制）
    private static volatile String currentLogMode = "LOW";
    private static long lowModeStartTime = 0;
    private static long midModeStartTime = 0;  // HIGH/ULTRA→MID的稳定期计时器
    private static long highModeStartTime = 0; // ULTRA→HIGH的稳定期计时器
    private static final long LOW_MODE_DELAY = 30000; // MID→LOW需要30秒稳定期
    private static final long MID_MODE_DELAY = 30000; // HIGH/ULTRA→MID需要30秒稳定期
    private static final long HIGH_MODE_DELAY = 30000; // ULTRA→HIGH需要30秒稳定期
    private static final Object modeLock = new Object(); // 🔒 模式切换同步锁
    
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

        // 获取去重配置（保留参数避免编译错误）
        Map<String, Object> dedupeMap = (Map<String, Object>) cfg.getOrDefault("dedupe", Collections.emptyMap());
        boolean dedupeEnable = Boolean.TRUE.equals(dedupeMap.getOrDefault("enable", false));
        int globalWindowMin = ((Number) dedupeMap.getOrDefault("timeWindowMinutes", 10)).intValue();
        Map<String, Object> perTopic = (Map<String, Object>) dedupeMap.getOrDefault("perTopic", Collections.emptyMap());

        // logAll 配置
        Map<String, Object> logging = (Map<String, Object>) cfg.getOrDefault("logging", Collections.emptyMap());
        boolean logAll = Boolean.TRUE.equals(logging.getOrDefault("logAll", false));

        // 🚀 初始化自适应高频处理组件
        if (adaptiveEnabled) {
            initializeAdaptiveProcessing(R, queueMap, dedupeEnable, globalWindowMin, perTopic, logAll);
        }

        // 🚀 构建优化的Paho MQTT客户端 - 高性能配置
        try {
            MqttClient mqtt = new MqttClient(broker, clientId, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(cleanSession);
            options.setKeepAliveInterval(keepAliveSec);
            options.setConnectionTimeout(30);  // 30秒连接超时
            options.setAutomaticReconnect(true);  // 自动重连
            options.setMaxInflight(maxInflight);  // 高并发设置
            
            // 设置回调处理器
            mqtt.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    log.warn("❌ MQTT connection lost: {}", cause.getMessage());
                }
                
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // 🚀 全局入队 Tap（步骤1）- 零处理镜像式侧挂，不影响原链路
                    GlobalQueue.offer(new Message(topic, message.getPayload(), System.currentTimeMillis()));
                    
                    // 🚀 异步处理消息以提高吞吐量 (参考测试客户端优化)
                    messageProcessor.submit(() -> {
                        try {
                            // 根据topic确定topicKey
                            String topicKey = getTopicKeyFromRealTopic(topic, topicMap);
                            if (topicKey != null) {
                                String queue = queueMap.get(topicKey);
                                byte[] payload = message.getPayload();
                                
                                // 🚀 所有消息都通过新的sidecar架构处理
                                handleMessageAdaptive(R, topicKey, topic, payload, queue, dedupeEnable, globalWindowMin, perTopic, logAll, message.getQos());
                            }
                        } catch (Exception e) {
                            log.error("❌ Error processing message asynchronously: {}", e.getMessage());
                        }
                    });
                }
                
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // 发布消息完成回调（我们只订阅，不需要处理）
                }
            });
            
            // 连接到MQTT broker
            log.info("🚀 Connecting to MQTT broker: {}", broker);
            mqtt.connect(options);
            log.info("🚀 MQTT connected with optimized settings: keepAlive={}s, cleanSession={}, maxInflight={}", 
                keepAliveSec, cleanSession, maxInflight);
            
            // 订阅所有主题
            for (String topicKey : topicMap.keySet()) {
                String topic = topicMap.get(topicKey);
                int qos = qosMap.get(topicKey);
                
                // 初始化计数器
                lastThroughputCheck.putIfAbsent(topicKey, new AtomicLong(System.currentTimeMillis()));
                
                log.info("🚀 Subscribing to topic: {} qos: {} (adaptive high-frequency enabled={})", topic, qos, adaptiveEnabled);
                mqtt.subscribe(topic, qos);
            }
            
            String mode = adaptiveEnabled ? "ADAPTIVE HIGH-FREQUENCY" : "OPTIMIZED";
            log.info("🚀 All topics subscribed - {} MQTT processing ACTIVE", mode);
            log.info("🚀 HIGH-FREQUENCY MQTT INGEST ACTIVE - Connected to MQTT {} and Redis {}:{}", 
                    broker, redisHost, redisPort);
            
            // 🚀 步骤3：初始化侧挂架构配置管理器
            Step3ConfigManager.initialize(cfg);
            
            // 🚀 Step 4: 初始化 LastonePublisher
            initializeLastonePublisher(mqtt, cfg);
            
            // 🚀 启动步骤1的简单队列处理器
            SimpleQueueProcessor.start();
            
        } catch (Exception e) {
            log.error("❌ Failed to initialize MQTT client: {}", e.getMessage());
            throw new RuntimeException("MQTT initialization failed", e);
        }
    }
    
    // 🚀 Step 4: 初始化 LastonePublisher
    private static void initializeLastonePublisher(MqttClient mqtt, Map<String, Object> cfg) {
        try {
            // 从配置读取 lastone 设置
            Map<String, Object> mqttConfig = (Map<String, Object>) cfg.get("mqtt");
            Map<String, Object> lastoneConfig = (Map<String, Object>) mqttConfig.get("lastone");
            
            boolean enabled = false;
            String topicPrefix = "lastone";
            
            if (lastoneConfig != null) {
                enabled = (boolean) lastoneConfig.getOrDefault("enabled", false);
                topicPrefix = (String) lastoneConfig.getOrDefault("topicPrefix", "lastone");
            }
            
            LastonePublisher lastonePublisher = new LastonePublisher(mqtt, topicPrefix, enabled);
            Step3ConfigManager.setLastonePublisher(lastonePublisher);
            
            log.info("🚀 Step 4: LastonePublisher initialized - enabled={}, prefix={}", enabled, topicPrefix);
            
        } catch (Exception e) {
            log.error("❌ Failed to initialize LastonePublisher: {}", e.getMessage(), e);
            // 设置一个禁用的 LastonePublisher 以避免空指针
            LastonePublisher disabledPublisher = new LastonePublisher(null, "lastone", false);
            Step3ConfigManager.setLastonePublisher(disabledPublisher);
        }
    }

    // 🚀 辅助函数：从实际topic匹配到topicKey
    private static String getTopicKeyFromRealTopic(String realTopic, Map<String, String> topicMap) {
        for (Map.Entry<String, String> entry : topicMap.entrySet()) {
            String pattern = entry.getValue();
            // 将MQTT通配符模式转换为正则表达式
            String regex = pattern.replace("+", "[^/]+").replace("#", ".*");
            if (realTopic.matches(regex)) {
                return entry.getKey();
            }
        }
        return null; // 未匹配到
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
        
        // 启动全局统计输出线程（每10秒输出一次）
        scheduler.scheduleAtFixedRate(() -> {
            long totalMsgs = globalTotalMessages.get();
            if (totalMsgs > 0) {
                int instantRate;
                synchronized (recentMessageTimes) {
                    instantRate = recentMessageTimes.size() / 2;  // 2秒窗口内的消息数/2
                }
                String mode = isHighFreqMode.get() ? "HIGH-FREQ" : "NORMAL";
                log.info("📊 [PERIODIC-STATS] total={}msgs, instantRate={}msg/s, mode={}", totalMsgs, instantRate, mode);
            }
        }, 10, 10, TimeUnit.SECONDS);
        
        // 🚀 启动日志模式切换检查任务（每5秒检查一次）
        scheduler.scheduleAtFixedRate(() -> {
            try {
                determineLogMode();  // 定期检查并切换模式，避免每条消息都检查
            } catch (Exception e) {
                log.error("❌ Error in log mode determination: {}", e.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
        
        log.info("🚀 Adaptive High-Frequency Processing initialized successfully");
    }

    // 🚀 步骤1&2：总数统计 + 吞吐量计算（瞬时+平滑）
    private static void recordMessageReceived(String topicKey) {
        long currentTime = System.currentTimeMillis();
        
        // 1. 累积全局接收数量统计
        long totalMsgs = globalTotalMessages.incrementAndGet();
        long msgsInWindow = globalMessagesInWindow.incrementAndGet();
        
        // 2. 瞬时吞吐量统计（基于最近2秒）
        synchronized (recentMessageTimes) {
            recentMessageTimes.add(currentTime);
            // 清理超过2秒的记录
            recentMessageTimes.removeIf(time -> currentTime - time > 2000);
        }
        
        // 3. 每配置间隔输出统计（仅当globalTotalMessages增加时）
        long timeSinceLastStat = currentTime - lastGlobalStatTime;
        long currentGlobalTotal = globalTotalMessages.get();
        if (timeSinceLastStat >= statsOutputIntervalMs && currentGlobalTotal > lastGlobalTotalMessages) {
            long timeSinceStart = currentTime - throughputStartTime;
            double avgThroughputPerSec = msgsInWindow / (timeSinceLastStat / 1000.0);
            double totalAvgThroughput = (totalMsgs * 1000.0) / timeSinceStart;
            
            lastGlobalStatTime = currentTime;
            lastGlobalTotalMessages = currentGlobalTotal; // 更新上次记录的总数
            
            // 获取所有类型的drop计数
            long queueDropped = GlobalQueue.getDroppedCount();       // 队列满时丢弃
            long redisDropped = RedisOutputService.getDroppedCount(); // Redis输出失败丢弃
            long dedupeDropped = DedupeService.getDuplicatedCount();  // 去重丢弃
            long totalDropped = queueDropped + redisDropped + dedupeDropped;
            long effectiveCount = totalMsgs - totalDropped;
            
            log.info("📊 [GLOBAL-STATS] Total: {} msgs, Dropped: {} msgs (Queue: {}, Redis: {}, Dedupe: {}), Effective: {} msgs, CurrentPeriod: {} msgs, Throughput: {} msg/s (current), {} msg/s (total avg)", 
                totalMsgs, totalDropped, queueDropped, redisDropped, dedupeDropped, effectiveCount, msgsInWindow, String.format("%.1f", avgThroughputPerSec), String.format("%.1f", totalAvgThroughput));
            
            // 重置当前周期计数，为下一个统计周期准备
            globalMessagesInWindow.set(0);
        }
    }
    
    // 🚀 步骤3：log模式判断 - 根据吞吐量确定日志级别（附加防抖动机制）
    private static String determineLogMode() {
        synchronized (modeLock) {  // 🔒 确保模式切换的线程安全
            int instantRate;
            synchronized (recentMessageTimes) {
                instantRate = recentMessageTimes.size() / 2; // 瞬时速率（msg/s）
            }
            
            // 动态调整日志模式
            String newMode;
            if (instantRate >= 500) {
                newMode = "ULTRA"; // 超高频：>500 msg/s
            } else if (instantRate >= 100) {
                newMode = "HIGH"; // 高频：100-500 msg/s  
            } else if (instantRate >= 10) {
                newMode = "MID"; // 中频：10-100 msg/s
            } else {
                newMode = "LOW"; // 低频：<10 msg/s
            }
        
        // 🔒 防抖动机制：ULTRA→HIGH需要30秒稳定期
        if (newMode.equals("HIGH") && currentLogMode.equals("ULTRA")) {
            if (highModeStartTime == 0) {
                highModeStartTime = System.currentTimeMillis();
                // 保持ULTRA模式，不立即切换
                return currentLogMode;
            } else if (System.currentTimeMillis() - highModeStartTime < HIGH_MODE_DELAY) {
                // 还没到30秒，继续保持ULTRA模式
                return currentLogMode;
            }
            // 已经稳定30秒，允许切换到HIGH
            highModeStartTime = 0;
        } else if (!newMode.equals("HIGH") || !currentLogMode.equals("ULTRA")) {
            // 不是从ULTRA切换到HIGH，重置计时器
            highModeStartTime = 0;
        }
        
        // 🔒 防抖动机制：HIGH/ULTRA→MID需要30秒稳定期
        if (newMode.equals("MID") && (currentLogMode.equals("HIGH") || currentLogMode.equals("ULTRA"))) {
            if (midModeStartTime == 0) {
                midModeStartTime = System.currentTimeMillis();
                // 保持HIGH/ULTRA模式，不立即切换
                return currentLogMode;
            } else if (System.currentTimeMillis() - midModeStartTime < MID_MODE_DELAY) {
                // 还没到30秒，继续保持HIGH/ULTRA模式
                return currentLogMode;
            }
            // 已经稳定30秒，允许切换到MID
            midModeStartTime = 0;
        } else if (!newMode.equals("MID") || !currentLogMode.equals("HIGH") && !currentLogMode.equals("ULTRA")) {
            // 不是从HIGH/ULTRA切换到MID，重置计时器
            midModeStartTime = 0;
        }
        
        // 🔒 防抖动机制：MID→LOW需要30秒稳定期
        if (newMode.equals("LOW") && currentLogMode.equals("MID")) {
            if (lowModeStartTime == 0) {
                lowModeStartTime = System.currentTimeMillis();
                // 保持MID模式，不立即切换
                return currentLogMode;
            } else if (System.currentTimeMillis() - lowModeStartTime < LOW_MODE_DELAY) {
                // 还没到30秒，继续保持MID模式
                return currentLogMode;
            }
            // 已经稳定30秒，允许切换到LOW
            lowModeStartTime = 0;
        } else if (!newMode.equals("LOW")) {
            // 不是切换到LOW，重置计时器
            lowModeStartTime = 0;
        }
        
            // 更新全局模式（记录切换）
            if (!newMode.equals(currentLogMode)) {
                log.info("🚀 [MODE-SWITCH] {} -> {} (throughput: {} msg/s)", currentLogMode, newMode, instantRate);
                currentLogMode = newMode;
            }
            
            return currentLogMode;
        }  // 🔒 同步块结束
    }
    
    // 🚀 步骤4：log输出分级 - 精细化控制，最大化高频接收能力
    private static class LogSampler {
        private long lastOutputTime = 0;
        private long lastOutputCount = 0;
        
        boolean shouldOutput(String mode, long currentCount) {
            long now = System.currentTimeMillis();
            long timeDiff = now - lastOutputTime;
            long countDiff = currentCount - lastOutputCount;
            
            boolean shouldOutput = false;
            switch (mode) {
                case "LOW":
                    // LOW模式：每10条且间隔≥5秒
                    shouldOutput = (countDiff >= 10 && timeDiff >= 5000);
                    break;
                case "MID":
                    // MID模式：每100条且间隔≥10秒
                    shouldOutput = (countDiff >= 100 && timeDiff >= 10000);
                    break;
                case "HIGH":
                    // HIGH模式：每1000条且间隔≥10秒
                    shouldOutput = (countDiff >= 1000 && timeDiff >= 10000);
                    break;
                case "ULTRA":
                    // ULTRA模式：每5000条且间隔≥10秒
                    shouldOutput = (countDiff >= 5000 && timeDiff >= 10000);
                    break;
            }
            
            if (shouldOutput) {
                lastOutputTime = now;
                lastOutputCount = currentCount;
            }
            return shouldOutput;
        }
    }
    
    // 为各种日志动作分别设置采样器
    private static LogSampler receivedSampler = new LogSampler();
    private static LogSampler normalSampler = new LogSampler();
    private static LogSampler highFreqSampler = new LogSampler();
    
    private static void outputMessage(String logMode, String action, String topic, String topicKey, String payload, String details) {
        long currentTotal = globalTotalMessages.get();
        
        // 根据动作类型选择采样器
        LogSampler sampler = null;
        if ("RECEIVED".equals(action)) {
            sampler = receivedSampler;
        } else if ("NORMAL".equals(action)) {
            sampler = normalSampler;
        } else if ("HIGH-FREQ".equals(action)) {
            sampler = highFreqSampler;
        }
        
        // 高频模式下使用采样控制，低频模式正常输出
        if (sampler != null && !sampler.shouldOutput(logMode, currentTotal)) {
            return; // 不输出
        }
        
        switch (logMode) {
            case "LOW": // 低频：详细日志
                String preview = payload.length() > 50 ? payload.substring(0, 50) + "..." : payload;
                log.info("� [{}] topic={} preview=[{}] details={}", action, topic, preview, details);
                break;
                
            case "MID": // 中频：精简日志（已通过采样控制）
                log.info("🚀 [{}] topic={} count={} details={}", action, topicKey, currentTotal, details);
                break;
                
            case "HIGH": // 高频：最少日志（已通过采样控制）
            case "ULTRA": // 超高频：最少日志（已通过采样控制）
                int instantRate = recentMessageTimes.size() / 2;
                log.info("📊 [{}] throughput={}msg/s total={} mode={} {}", action, instantRate, currentTotal, logMode, details);
                break;
            
            default:
                // 未知模式，降低输出频率
                if (currentTotal % 1000 == 0) {
                    log.warn("🔍 [OUTPUT-DEBUG] Unknown logMode={} total={} action={}", logMode, currentTotal, action);
                }
                break;
        }
    }

    // 🚀 自适应消息处理器 - 动态模式切换
    private static void handleMessageAdaptive(RedisCommands<String,String> R, String topicKey, String topic, byte[] payload, String queue,
                                             boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll, int qos) {
        try {
            // 📊 步骤1：总数统计 - 在所有处理之前记录消息接收
            recordMessageReceived(topicKey);
            
            // 📊 步骤2：吞吐量计算并检查模式切换
            checkThroughputAndSwitchMode();
            
            // 📊 步骤3：获取当前log模式（不重复计算，避免竞态条件）
            String logMode = currentLogMode;
            
            String payloadStr = new String(payload, StandardCharsets.UTF_8);
            
            // 检查消息大小限制
            if (payloadStr.length() > maxMessageSizeKB * 1024) {
                // outputMessage(logMode, "SIZE-LIMIT", topic, topicKey, payloadStr, 
                //     String.format("messageSize=%dKB > limit=%dKB, dropped", payloadStr.length() / 1024, maxMessageSizeKB));
                return;
            }
            
            // 📊 步骤4：log输出分级 - 根据模式输出不同级别的日志
            long currentTotal = globalTotalMessages.get();
            if (currentTotal % 1000 == 0) {
                log.debug("🔍 [DEBUG-STATS] mode={} total={} recent_times_size={}", logMode, currentTotal, recentMessageTimes.size());
            }
            // outputMessage(logMode, "RECEIVED", topic, topicKey, payloadStr, "message arrived");
            
            // 📦 步骤5：入队列 - 消息进入处理队列
            String deviceId = extractDeviceId(payloadStr, topic);
            
            if (isHighFreqMode.get()) {
                // 🚀 高频模式：放入队列缓冲
                BlockingQueue<MessageBuffer> queue_buffer = messageQueues.get(topicKey);
                MessageBuffer msgBuffer = new MessageBuffer(topic, payloadStr, deviceId);
                
                if (!queue_buffer.offer(msgBuffer)) {
                    // 队列满，丢弃消息
                    if (globalTotalMessages.get() % 1000 == 0) {
                        log.debug("🚀 [HIGH-FREQ] topic={} queue_full, dropped message #{}", topicKey, globalTotalMessages.get());
                    }
                    return;
                }
                
                // 使用采样控制的日志输出
                // outputMessage(logMode, "HIGH-FREQ", topic, topicKey, payloadStr, 
                //     "queue_size=" + queue_buffer.size());
            } else {
                // 🚀 正常模式：直接处理
                handleMessageDirect(R, topicKey, topic, payloadStr, deviceId, queue, dedupeEnable, globalWindowMin, perTopic, logAll);
                
                // 使用采样控制的日志输出
                // outputMessage(logMode, "NORMAL", topic, topicKey, payloadStr, "processed_directly");
            }
            
        } catch (Exception e) {
            log.error("❌ [ADAPTIVE-ERROR] topic={} error={}", topic, e.getMessage());
        }
    }

    // 🚀 生成 payload 预览（优化版本）
    private static String createPayloadPreview(String payloadStr) {
        if (payloadStr == null) return "null";
        String preview = payloadStr.length() > 100 ? payloadStr.substring(0, 100) + "..." : payloadStr;
        return preview.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", ".");
    }
    
    // 🚀 优化的统计输出 - 专为高频场景设计


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

    // 🚀 吞吐量检查和模式切换 - 使用瞬时吞吐量（2秒窗口）
    private static void checkThroughputAndSwitchMode() {
        try {
            // 获取瞬时吞吐量（基于2秒窗口的消息数）
            int instantThroughput;
            synchronized (recentMessageTimes) {
                instantThroughput = recentMessageTimes.size() / 2; // 瞬时速率（msg/s）
            }
            
            boolean currentMode = isHighFreqMode.get();
            boolean shouldBeHighFreq = instantThroughput > normalToHighFreqThreshold;
            boolean shouldBeNormal = instantThroughput < highFreqToNormalThreshold;
            
            if (!currentMode && shouldBeHighFreq) {
                // 切换到高频模式
                isHighFreqMode.set(true);
                log.debug("🚀 [MODE-SWITCH] NORMAL -> HIGH-FREQ, instantThroughput={}msg/s > threshold={}msg/s", 
                    instantThroughput, normalToHighFreqThreshold);
            } else if (currentMode && shouldBeNormal) {
                // 切换到正常模式
                isHighFreqMode.set(false);
                log.debug("🚀 [MODE-SWITCH] HIGH-FREQ -> NORMAL, instantThroughput={}msg/s < threshold={}msg/s", 
                    instantThroughput, highFreqToNormalThreshold);
                
                // 如果吞吐量回落到很低的区域（比如 < 100 msg/s），重置计数器
                if (instantThroughput < 100) {
                    long totalBeforeReset = globalTotalMessages.get();
                    globalMessagesInWindow.set(0);
                    throughputStartTime = System.currentTimeMillis();
                    lastGlobalStatTime = System.currentTimeMillis();
                    log.info("🔄 [COUNTER-RESET] Low throughput detected ({}msg/s), counters reset. Total before reset: {}", 
                        instantThroughput, totalBeforeReset);
                }
            }
            
        } catch (Exception e) {
            log.error("❌ [MODE-SWITCH-ERROR] error={}", e.getMessage());
        }
    }
    
    // 🚀 自适应统计输出
    private static void printAdaptiveStats(String topicKey) {
        try {
            BlockingQueue<MessageBuffer> queue = messageQueues.get(topicKey);
            int queueSize = queue != null ? queue.size() : 0;
            String mode = isHighFreqMode.get() ? "HIGH-FREQ" : "NORMAL";
            
            if (isHighFreqMode.get()) {
                // 高频模式简化日志
                log.info("🚀 [ADAPTIVE-{}] topic={} queue={}", 
                    mode, topicKey, queueSize);
            } else {
                // 正常模式简化日志
                log.info("🚀 [ADAPTIVE-{}] topic={} queue={}", 
                    mode, topicKey, queueSize);
            }
            
        } catch (Exception e) {
            log.error("❌ [ADAPTIVE-STATS-ERROR] topic={} error={}", topicKey, e.getMessage());
        }
    }

    // 🚀 消息队列处理线程 - 批量处理
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
                
                processBatchNoDedupe(R, topicKey, queue, batch, logAll);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("🚀 [QUEUE-PROCESSOR] topic={} interrupted", topicKey);
        } catch (Exception e) {
            log.error("❌ [QUEUE-PROCESSOR] topic={} error={}", topicKey, e.getMessage());
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
    }
    
    // 🚀 直接处理模式（正常模式使用）
    private static void handleMessageDirect(RedisCommands<String,String> R, String topicKey, String topic, String payloadStr, String deviceId,
                                           String queue, boolean dedupeEnable, int globalWindowMin, Map<String,Object> perTopic, boolean logAll) {
        
        R.lpush(queue, payloadStr);
        if (logAll) {
            R.lpush("q:raw:" + topicKey, payloadStr);
        }
    }

    interface MsgHandler { void handle(String topic, byte[] payload) throws Exception; }

    // Paho MQTT client subscription helper
    private static void subscribe(MqttClient client, String topic, int qos, MsgHandler handler) {
        try {
            client.subscribe(topic, qos, (receivedTopic, message) -> {
                try { 
                    handler.handle(receivedTopic, message.getPayload()); 
                }
                catch (Exception e) { 
                    log.error("❌ Message handler error: {}", e.getMessage());
                }
            });
        } catch (MqttException e) {
            log.error("❌ Failed to subscribe to topic {}: {}", topic, e.getMessage());
        }
    }
}
