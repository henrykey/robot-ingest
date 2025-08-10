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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
                
                log.info("🚀 Subscribing to topic: {} qos: {} (optimized for high-frequency)", topic, qos);
                subscribe(mqtt, topic, qos, (realTopic, payload) -> {
                    handleMessageOptimized(R, topicKey, realTopic, payload, queue, dedupeEnable, globalWindowMin, perTopic, logAll, qos);
                });
            }
            log.info("🚀 All topics subscribed - High-frequency MQTT optimization ACTIVE");
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

    // 🚀 超高频消息处理器 - 极简版本
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
