package com.ibcai.ingest.config;

/**
 * 特性开关配置类
 */
public class IngestFeatureConfig {
    
    // 总开关
    public static final String INGEST_FEATURE_ENABLED = "INGEST_FEATURE_ENABLED";
    
    // 队列配置
    public static final String INGEST_QUEUE_CAPACITY = "INGEST_QUEUE_CAPACITY";
    public static final String INGEST_QUEUE_DROP_WHEN_FULL = "INGEST_QUEUE_DROP_WHEN_FULL";
    
    // 批量配置
    public static final String INGEST_BATCH_SIZE = "INGEST_BATCH_SIZE";
    public static final String INGEST_BATCH_INTERVAL_MS = "INGEST_BATCH_INTERVAL_MS";
    
    // 统计配置
    public static final String INGEST_STATS_THROUGHPUT_WINDOW_SEC = "INGEST_STATS_THROUGHPUT_WINDOW_SEC";
    public static final String INGEST_STATS_OUTPUT_INTERVAL_SEC = "INGEST_STATS_OUTPUT_INTERVAL_SEC";
    
    // Step 4: Lastone 配置
    public static final String INGEST_LASTONE_ENABLED = "INGEST_LASTONE_ENABLED";
    public static final String INGEST_LASTONE_TOPIC_PREFIX = "INGEST_LASTONE_TOPIC_PREFIX";
    
    // 默认值
    public static final boolean DEFAULT_FEATURE_ENABLED = false;
    public static final int DEFAULT_QUEUE_CAPACITY = 10240;
    public static final boolean DEFAULT_DROP_WHEN_FULL = true;
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_BATCH_INTERVAL_MS = 100;
    public static final int DEFAULT_STATS_THROUGHPUT_WINDOW_SEC = 60;
    public static final int DEFAULT_STATS_OUTPUT_INTERVAL_SEC = 60;
    public static final boolean DEFAULT_LASTONE_ENABLED = true;
    public static final String DEFAULT_LASTONE_TOPIC_PREFIX = "lastone";
    
    /**
     * 获取环境变量或默认值
     */
    public static boolean isFeatureEnabled() {
        String env = System.getenv(INGEST_FEATURE_ENABLED);
        return env != null ? Boolean.parseBoolean(env) : DEFAULT_FEATURE_ENABLED;
    }
    
    public static int getQueueCapacity() {
        String env = System.getenv(INGEST_QUEUE_CAPACITY);
        return env != null ? Integer.parseInt(env) : DEFAULT_QUEUE_CAPACITY;
    }
    
    public static boolean isDropWhenFull() {
        String env = System.getenv(INGEST_QUEUE_DROP_WHEN_FULL);
        return env != null ? Boolean.parseBoolean(env) : DEFAULT_DROP_WHEN_FULL;
    }
    
    public static int getBatchSize() {
        String env = System.getenv(INGEST_BATCH_SIZE);
        return env != null ? Integer.parseInt(env) : DEFAULT_BATCH_SIZE;
    }
    
    public static int getBatchIntervalMs() {
        String env = System.getenv(INGEST_BATCH_INTERVAL_MS);
        return env != null ? Integer.parseInt(env) : DEFAULT_BATCH_INTERVAL_MS;
    }
    
    public static int getStatsThroughputWindowSec() {
        String env = System.getenv(INGEST_STATS_THROUGHPUT_WINDOW_SEC);
        return env != null ? Integer.parseInt(env) : DEFAULT_STATS_THROUGHPUT_WINDOW_SEC;
    }
    
    public static int getStatsOutputIntervalSec() {
        String env = System.getenv(INGEST_STATS_OUTPUT_INTERVAL_SEC);
        return env != null ? Integer.parseInt(env) : DEFAULT_STATS_OUTPUT_INTERVAL_SEC;
    }
    
    public static boolean isLastoneEnabled() {
        String env = System.getenv(INGEST_LASTONE_ENABLED);
        return env != null ? Boolean.parseBoolean(env) : DEFAULT_LASTONE_ENABLED;
    }
    
    public static String getLastoneTopicPrefix() {
        String env = System.getenv(INGEST_LASTONE_TOPIC_PREFIX);
        return env != null ? env : DEFAULT_LASTONE_TOPIC_PREFIX;
    }
}
