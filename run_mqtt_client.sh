#!/bin/bash

# 进入项目目录
cd /home/kehongwei/works/robot-ingest-skeleton

# 使用docker运行独立的MQTT客户端
docker run --rm --network host \
  -v $(pwd):/workspace \
  -w /workspace \
  gradle:8.4-jdk21 bash -c "
    cd ingest-service && 
    gradle --no-daemon --quiet execute -PmainClass=com.ibcai.test.MQTTClient 2>/dev/null || 
    java -cp 'build/libs/ingest-service-0.1.0.jar' org.springframework.boot.loader.JarLauncher com.ibcai.test.MQTTClient 2>/dev/null || 
    gradle --no-daemon bootRun --args='com.ibcai.test.MQTTClient' 2>/dev/null ||
    echo 'Trying alternative approach...' &&
    java -Djava.ext.dirs=build/libs -cp build/classes/java/main com.ibcai.test.MQTTClient
  "
