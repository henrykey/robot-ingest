#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import json
import time
import sys

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Connected to MQTT broker")
        # 订阅 lastone 主题来验证
        client.subscribe("lastone/+/+")
        print("📡 Subscribed to: lastone/+/+")
    else:
        print(f"❌ Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"📨 Received LASTONE: {topic}")
    print(f"   Payload: {payload}")
    print(f"   Retained: {msg.retain}")

def on_publish(client, userdata, mid):
    print(f"📤 Message published (mid: {mid})")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 test_lastone.py <device_id>")
        sys.exit(1)
    
    device_id = sys.argv[1]
    
    # 创建MQTT客户端
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    
    # 连接到MQTT broker
    try:
        client.connect("192.168.123.61", 1883, 60)
        client.loop_start()
        
        time.sleep(1)  # 等待连接建立
        
        # 发送一条状态消息
        topic = f"robots/{device_id}/state"
        payload = {
            "battery": 85,
            "taskStatus": "running",
            "taskId": f"task_{int(time.time())}",
            "autonomousMode": True,
            "fault": False,
            "binsNum": 3,
            "coordinateType": "GPS",
            "position": {"x": 12.345678, "y": 67.890123, "z": 0.5},
            "timestamp": int(time.time() * 1000)
        }
        
        payload_str = json.dumps(payload)
        print(f"📤 Publishing to: {topic}")
        print(f"   Payload: {payload_str}")
        
        client.publish(topic, payload_str, qos=0)
        
        # 等待消息处理和lastone发布
        print("⏳ Waiting for lastone message...")
        time.sleep(5)
        
        client.loop_stop()
        client.disconnect()
        print("🔌 Disconnected")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
