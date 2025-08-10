#!/usr/bin/env python3
"""
简单的MQTT性能测试脚本，用于对比不同客户端的接收性能
"""
import paho.mqtt.client as mqtt
import json
import time
import threading
from collections import defaultdict

class MQTTPerformanceTest:
    def __init__(self, broker_host="192.168.123.61", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = None
        self.message_count = 0
        self.start_time = None
        self.last_report_time = None
        self.last_report_count = 0
        self.lock = threading.Lock()
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"✅ Connected to MQTT broker {self.broker_host}:{self.broker_port}")
            # 订阅所有测试topic
            topics = [
                "robots/+/error",
                "robots/+/state", 
                "robots/+/cargo",
                "robots/+/network/ip",
                "robots/+/connection"
            ]
            for topic in topics:
                client.subscribe(topic, qos=1)
                print(f"📡 Subscribed to: {topic}")
        else:
            print(f"❌ Failed to connect, return code {rc}")

    def on_message(self, client, userdata, msg):
        with self.lock:
            self.message_count += 1
            current_time = time.time()
            
            # 第一条消息记录开始时间
            if self.start_time is None:
                self.start_time = current_time
                self.last_report_time = current_time
                
            # 每10秒报告一次统计
            if current_time - self.last_report_time >= 10.0:
                time_elapsed = current_time - self.start_time
                total_rate = self.message_count / time_elapsed if time_elapsed > 0 else 0
                
                period_count = self.message_count - self.last_report_count
                period_rate = period_count / 10.0
                
                print(f"📊 [PYTHON-CLIENT] Total: {self.message_count} msgs, "
                      f"Period: {period_count} msgs, "
                      f"Throughput: {period_rate:.1f} msg/s (current), "
                      f"{total_rate:.1f} msg/s (total avg)")
                
                self.last_report_time = current_time
                self.last_report_count = self.message_count

    def on_disconnect(self, client, userdata, rc):
        print(f"🔌 Disconnected from MQTT broker")

    def start_test(self, duration_seconds=300):
        """开始性能测试"""
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # 连接到broker
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            
            print(f"🚀 MQTT Performance Test Started - Running for {duration_seconds} seconds")
            print("Press Ctrl+C to stop early")
            
            time.sleep(duration_seconds)
            
        except KeyboardInterrupt:
            print("\n⏹️  Test stopped by user")
        finally:
            self.stop_test()

    def stop_test(self):
        """停止测试并显示最终统计"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            
        if self.start_time:
            total_time = time.time() - self.start_time
            avg_rate = self.message_count / total_time if total_time > 0 else 0
            print(f"\n📈 Final Results:")
            print(f"   Total Messages: {self.message_count}")
            print(f"   Total Time: {total_time:.1f}s")
            print(f"   Average Rate: {avg_rate:.1f} msg/s")

if __name__ == "__main__":
    test = MQTTPerformanceTest()
    test.start_test(300)  # 运行5分钟
