#!/bin/bash

# 步骤1验收测试：只监控日志，不发送MQTT消息

echo "🚀 步骤1验收测试开始..."

# 测试1：特性关闭状态
echo ""
echo "📋 测试1：特性关闭状态下运行服务"
echo "INGEST_FEATURE_ENABLED=false (默认)"

docker run -d --name test-ingest-off \
  --network host \
  -v $(pwd)/config.yml:/app/config.yml:ro \
  -e INGEST_FEATURE_ENABLED=false \
  ibcai/robot-ingest:ingest

sleep 5

echo "🔍 特性关闭状态的服务日志："
docker logs test-ingest-off 2>&1 | tail -10

if docker logs test-ingest-off 2>&1 | grep -q "HIGH-FREQUENCY MQTT INGEST ACTIVE"; then
    echo "✅ 服务正常启动（特性关闭）"
else
    echo "❌ 服务启动失败"
fi

docker rm -f test-ingest-off

echo ""
echo "📋 测试2：特性开启状态下运行服务"
echo "INGEST_FEATURE_ENABLED=true"

docker run -d --name test-ingest-on \
  --network host \
  -v $(pwd)/config.yml:/app/config.yml:ro \
  -e INGEST_FEATURE_ENABLED=true \
  ibcai/robot-ingest:ingest

sleep 5

echo "🔍 特性开启状态的服务日志："
docker logs test-ingest-on 2>&1 | tail -15

if docker logs test-ingest-on 2>&1 | grep -q "SimpleQueueProcessor started"; then
    echo "✅ SimpleQueueProcessor已启动"
else
    echo "❌ SimpleQueueProcessor未启动"
fi

echo ""
echo "🎯 现在服务已准备就绪，请发送高频MQTT消息进行测试"
echo "📡 MQTT Broker: 192.168.123.61:1883"
echo "📋 测试主题: robots/+/state, robots/+/connection, robots/+/network/ip, robots/+/error, robots/+/cargo"
echo ""
echo "⏱️  将持续监控30秒的日志输出..."

# 监控30秒的日志
for i in {1..30}; do
    echo "⏱️  监控中... ${i}/30秒"
    sleep 1
    
    # 每5秒输出一次最新的统计日志
    if [ $((i % 5)) -eq 0 ]; then
        echo ""
        echo "📊 第${i}秒统计日志："
        docker logs test-ingest-on 2>&1 | grep -E "(SimpleQueueProcessor stats|GlobalQueue.*offered|Sample message)" | tail -3
        echo ""
    fi
done

echo ""
echo "📊 最终测试结果："
docker logs test-ingest-on 2>&1 | grep -E "(SimpleQueueProcessor stats|GlobalQueue.*offered)" | tail -5

echo ""
echo "🔍 完整日志输出（最后50行）："
docker logs test-ingest-on 2>&1 | tail -50

echo ""
echo "🎉 步骤1监控完成！"
echo "🔧 服务容器仍在运行，容器名: test-ingest-on"
echo "🔧 如需继续测试，请发送更多MQTT消息"
echo "🔧 如需停止，运行: docker rm -f test-ingest-on"
