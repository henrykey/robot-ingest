package com.ibcai.writer;

import com.ibcai.common.Cfg;
import com.ibcai.common.ConfigLoader;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@SpringBootApplication
public class WriterApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(WriterApplication.class, args);

        Map<String,Object> cfg = ConfigLoader.load(args);

        String redisHost = Cfg.get(cfg, "redis.host", "127.0.0.1");
        int redisPort = Cfg.get(cfg, "redis.port", 6379);

        String mongoUri = Cfg.get(cfg, "mongodb.uri", "mongodb://127.0.0.1:27017");
        String mongoDb = Cfg.get(cfg, "mongodb.database", "robotdb");
        String stateEvents = Cfg.get(cfg, "mongodb.collections.stateEvents", "state_events");

        // 旧的batch配置（保持兼容性）
        int sizeTrigger = Cfg.get(cfg, "batch.sizeTrigger", 1000);
        int timeTrigger = Cfg.get(cfg, "batch.timeTriggerSec", 60);
        int maxPerFlush = Cfg.get(cfg, "batch.maxPerFlush", 5000);

        RedisClient rclient = RedisClient.create("redis://" + redisHost + ":" + redisPort);
        var rconn = rclient.connect();
        RedisCommands<String,String> R = rconn.sync();

        MongoTemplate mongo = new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoUri + "/" + mongoDb));

        // Step 6: 启动新的批量Writer（如果启用）
        Map<String, Object> writerConfig = (Map<String, Object>) cfg.getOrDefault("writer", new HashMap<>());
        boolean newWriterEnabled = Cfg.get(writerConfig, "enabled", false);
        
        BatchWriter batchWriter = null;
        if (newWriterEnabled) {
            batchWriter = new BatchWriter(R, mongo, cfg);
            batchWriter.start();
            System.out.println("🚀 Step 6: BatchWriter started for ingest:* queues");
        }

        System.out.println("🚀 Writer service started - Legacy writer for q:state queue");
        
        long lastFlush = System.currentTimeMillis();
        long lastStats = System.currentTimeMillis();

        while (true) {
            // 旧的Writer逻辑（处理 q:state 队列，保持兼容性）
            long qlen = R.llen("q:state");
            if (qlen >= sizeTrigger || (System.currentTimeMillis() - lastFlush) / 1000 >= timeTrigger) {
                int toPop = (int)Math.min(qlen, maxPerFlush);
                List<String> batch = new ArrayList<>(toPop);
                for (int i = 0; i < toPop; i++) {
                    String v = R.rpop("q:state"); // LPUSH in ingest -> RPOP here
                    if (v == null) break;
                    batch.add(v);
                }
                if (!batch.isEmpty()) {
                    Instant t0 = Instant.now();
                    BulkOperations ops = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, stateEvents);
                    for (String json : batch) {
                        Map<String,Object> doc = new HashMap<>();
                        doc.put("raw", json);
                        doc.put("ingestedAt", new Date());
                        doc.put("source", "legacy-writer");  // 标识来源
                        ops.insert(doc);
                    }
                    ops.execute();
                    long ms = Duration.between(t0, Instant.now()).toMillis();
                    System.out.println("Legacy Writer: Flushed " + batch.size() + " state events in " + ms + " ms");
                }
                lastFlush = System.currentTimeMillis();
            }
            
            // 每30秒输出统计信息
            if (batchWriter != null && (System.currentTimeMillis() - lastStats) >= 30000) {
                System.out.println("📊 Writer Stats: " + batchWriter.getStats());
                lastStats = System.currentTimeMillis();
            }
            
            Thread.sleep(50);
        }
    }
}
