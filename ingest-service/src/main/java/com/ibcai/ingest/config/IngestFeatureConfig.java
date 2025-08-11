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
    
    // Step 5: RedisWriter 配置
    public static final String INGEST_REDIS_WRITER_ENABLED = "INGEST_REDIS_WRITER_ENABLED";
    public static final String INGEST_REDIS_WRITER_KEY_TEMPLATE = "INGEST_REDIS_WRITER_KEY_TEMPLATE";
    public static final String INGEST_REDIS_WRITER_TTL_SEC = "INGEST_REDIS_WRITER_TTL_SEC";
    public static final String INGEST_REDIS_WRITER_RETRY_ATTEMPTS = "INGEST_REDIS_WRITER_RETRY_ATTEMPTS";
    public static final String INGEST_REDIS_WRITER_RETRY_DELAY_MS = "INGEST_REDIS_WRITER_RETRY_DELAY_MS";
    
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
    public static final boolean DEFAULT_REDIS_WRITER_ENABLED = true;
    public static final String DEFAULT_REDIS_WRITER_KEY_TEMPLATE = "ingest:{topic}:{objectKey}";
    public static final int DEFAULT_REDIS_WRITER_TTL_SEC = 86400;
    public static final int DEFAULT_REDIS_WRITER_RETRY_ATTEMPTS = 2;
    public static final long DEFAULT_REDIS_WRITER_RETRY_DELAY_MS = 10;
    
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
    
    public static boolean isRedisWriterEnabled() {
        String env = System.getenv(INGEST_REDIS_WRITER_ENABLED);
        return env != null ? Boolean.parseBoolean(env) : DEFAULT_REDIS_WRITER_ENABLED;
    }
    
    public static String getRedisWriterKeyTemplate() {
        String env = System.getenv(INGEST_REDIS_WRITER_KEY_TEMPLATE);
        return env != null ? env : DEFAULT_REDIS_WRITER_KEY_TEMPLATE;
    }
    
    public static int getRedisWriterTtlSec() {
        String env = System.getenv(INGEST_REDIS_WRITER_TTL_SEC);
        return env != null ? Integer.parseInt(env) : DEFAULT_REDIS_WRITER_TTL_SEC;
    }
    
    public static int getRedisWriterRetryAttempts() {
        String env = System.getenv(INGEST_REDIS_WRITER_RETRY_ATTEMPTS);
        return env != null ? Integer.parseInt(env) : DEFAULT_REDIS_WRITER_RETRY_ATTEMPTS;
    }
    
    public static long getRedisWriterRetryDelayMs() {
        String env = System.getenv(INGEST_REDIS_WRITER_RETRY_DELAY_MS);
        return env != null ? Long.parseLong(env) : DEFAULT_REDIS_WRITER_RETRY_DELAY_MS;
    }
}
