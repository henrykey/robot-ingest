#!/bin/bash

# 🚀 MQTT高频消息发送测试脚本
echo "🚀 开始MQTT高频消息发送测试..."

MQTT_BROKER="192.168.123.61"
MQTT_PORT="1883"
QOS="1"
MESSAGE_COUNT=1000

echo "📋 测试配置:"
echo "   MQTT Broker: $MQTT_BROKER:$MQTT_PORT"
echo "   QoS级别: $QOS"
echo "   发送消息数: $MESSAGE_COUNT"
echo ""

# 检查mosquitto_pub是否可用
if ! command -v mosquitto_pub >/dev/null 2>&1; then
    echo "❌ mosquitto_pub未安装，无法进行测试"
    echo "请安装: sudo apt install mosquitto-clients"
    exit 1
fi

echo "🚀 开始发送高频消息..."
start_time=$(date +%s)

# 发送高频消息
for i in $(seq 1 $MESSAGE_COUNT); do
    # 生成不同的消息内容以避免去重
    battery=$((40 + $i % 60))
    task_id=$((1000 + $i))
    lat=$(echo "39.123456 + $i * 0.000001" | bc -l)
    lon=$(echo "116.123456 + $i * 0.000001" | bc -l)
    timestamp=$(date -Iseconds)
    
    # 轮换不同的robot ID
    robot_id="robot$(printf %03d $((($i % 10) + 1)))"
    
    # 发送到state主题
    mosquitto_pub -h $MQTT_BROKER -p $MQTT_PORT -t "robots/$robot_id/state" -q $QOS \
        -m "{\"deviceId\":\"$robot_id\",\"battery\":$battery,\"taskStatus\":\"moving\",\"taskId\":$task_id,\"autonomousMode\":true,\"fault\":false,\"binsNum\":2,\"coordinateType\":\"GPS\",\"position\":{\"lat\":$lat,\"lon\":$lon},\"timestamp\":\"$timestamp\"}" &
    
    # 每100条显示进度
    if [ $((i % 100)) -eq 0 ]; then
        echo "   已发送: $i/$MESSAGE_COUNT"
    fi
    
    # 控制发送频率 - 高频测试
    if [ $((i % 50)) -eq 0 ]; then
        sleep 0.1  # 每50条消息稍微暂停
    fi
done

# 等待所有后台进程完成
wait

end_time=$(date +%s)
duration=$((end_time - start_time))
throughput=$(echo "scale=2; $MESSAGE_COUNT / $duration" | bc -l)

echo ""
echo "📊 发送完成:"
echo "   发送消息数: $MESSAGE_COUNT"
echo "   耗时: ${duration}秒"
echo "   发送速率: $throughput 消息/秒"
echo ""
echo "🔍 请在另一个终端运行以下命令查看接收情况:"
echo "   docker-compose logs -f ingest | grep STATS"
echo ""
echo "⏱️ 等待5秒让消息处理完成..."
sleep 5
