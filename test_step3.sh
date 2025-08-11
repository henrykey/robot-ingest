#!/bin/bash

echo "🚀 开始步骤3测试：去重与Redis输出验证"
echo "发送测试消息验证：1) 去重功能 2) Redis输出功能 3) 统计输出"

# 测试消息1：新消息（应该被处理）
echo "📤 发送测试消息1（新消息）..."
mosquitto_pub -h 192.168.123.61 -p 1883 -t "robots/test001/state" -m '{
  "deviceId": "test001",
  "battery": 75,
  "taskStatus": "running", 
  "taskId": "task_001",
  "autonomousMode": true,
  "fault": false,
  "binsNum": 3,
  "coordinateType": "global",
  "position": {"x": 10.123456, "y": 20.654321, "z": 0.0}
}'

echo "⏳ 等待3秒..."
sleep 3

# 测试消息2：相同消息（应该被去重）
echo "📤 发送测试消息2（重复消息）..."
mosquitto_pub -h 192.168.123.61 -p 1883 -t "robots/test001/state" -m '{
  "deviceId": "test001",
  "battery": 75,
  "taskStatus": "running", 
  "taskId": "task_001",
  "autonomousMode": true,
  "fault": false,
  "binsNum": 3,
  "coordinateType": "global",
  "position": {"x": 10.123456, "y": 20.654321, "z": 0.0}
}'

echo "⏳ 等待3秒..."
sleep 3

# 测试消息3：不同内容（应该被处理）
echo "📤 发送测试消息3（不同内容）..."
mosquitto_pub -h 192.168.123.61 -p 1883 -t "robots/test001/state" -m '{
  "deviceId": "test001",
  "battery": 80,
  "taskStatus": "running", 
  "taskId": "task_001",
  "autonomousMode": true,
  "fault": false,
  "binsNum": 3,
  "coordinateType": "global",
  "position": {"x": 10.123456, "y": 20.654321, "z": 0.0}
}'

echo "⏳ 等待3秒..."
sleep 3

# 测试消息4：不同设备（应该被处理）
echo "📤 发送测试消息4（不同设备）..."
mosquitto_pub -h 192.168.123.61 -p 1883 -t "robots/test002/state" -m '{
  "deviceId": "test002",
  "battery": 75,
  "taskStatus": "running", 
  "taskId": "task_002",
  "autonomousMode": true,
  "fault": false,
  "binsNum": 2,
  "coordinateType": "global",
  "position": {"x": 15.123456, "y": 25.654321, "z": 0.0}
}'

echo "⏳ 等待5秒以查看统计..."
sleep 5

echo ""
echo "🎉 步骤3测试消息发送完成！"
echo "请检查日志中的去重处理结果："
echo "- 消息1: 应显示为 unique (DIFFERENT_CONTENT)"
echo "- 消息2: 应显示为 duplicate (DUPLICATE_WITHIN_WINDOW)"
echo "- 消息3: 应显示为 unique (DIFFERENT_CONTENT - battery不同)"
echo "- 消息4: 应显示为 unique (不同设备)"
echo ""
echo "应该创建2个TopicWorker: state:test001 和 state:test002"
