package com.ibcai.test;

import org.eclipse.paho.client.mqttv3.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MQTTClient {

    private static final String BROKER_URL = "tcp://192.168.123.61:1883";
    private static final String CLIENT_ID = "TestMQTTClient";
    private static final String[] TOPIC_FILTERS = {
        "robots/+/state",
        "robots/+/connection", 
        "robots/+/network/ip",
        "robots/+/error",
        "robots/+/cargo"
    };

    // 线程池用于异步处理消息
    private static final ExecutorService messageProcessor = Executors.newFixedThreadPool(10);

    // 计数收到多个消息，服务运行2分钟后停止并打印结果
    public static void main(String[] args) throws Exception {
        MqttClient client = null;
        try {
            client = new MqttClient(BROKER_URL, CLIENT_ID);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            // 优化连接参数以支持高吞吐量
            options.setConnectionTimeout(30);
            options.setKeepAliveInterval(20);
            options.setMaxInflight(10000); // 增加最大未确认消息数量
            options.setAutomaticReconnect(true);

            // 使用原子类确保线程安全的计数
            final AtomicLong messageCount = new AtomicLong(0);
            final AtomicLong lastReportTime = new AtomicLong(System.currentTimeMillis());
            final AtomicLong lastReportCount = new AtomicLong(0);

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // 异步处理消息以提高吞吐量
                    messageProcessor.submit(() -> {
                        try {
                            long count = messageCount.incrementAndGet();
                            if (count % 1000 == 0) {
                                long currentTime = System.currentTimeMillis();
                                long timeDiff = currentTime - lastReportTime.get();
                                long countDiff = count - lastReportCount.get();

                                if (timeDiff > 0) {
                                    double messagesPerSecond = (countDiff * 1000.0) / timeDiff;
                                    System.out.println("Received message count: " + count +
                                            ", 当前速率: " + String.format("%.2f", messagesPerSecond) + " messages/sec");
                                }

                                lastReportTime.set(currentTime);
                                lastReportCount.set(count);
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing message: " + e.getMessage());
                        }
                    });
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // 不需要实现
                }
            });

            client.connect(options);
            
            // 订阅所有主题
            for (String topic : TOPIC_FILTERS) {
                client.subscribe(topic);
                System.out.println("Subscribed to topic: " + topic);
            }

            // 运行2分钟后停止服务并打印结果
            long startTime = System.currentTimeMillis();
            long runTime = 2 * 60 * 1000; // 2分钟
            while (System.currentTimeMillis() - startTime < runTime) {
                Thread.sleep(1000);
            }

            client.disconnect();
            messageProcessor.shutdown();
            System.out.println("服务已停止，收到消息总数: " + messageCount.get());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (client != null && client.isConnected()) {
                try {
                    client.disconnect();
                } catch (MqttException e) {
                    System.err.println("Error disconnecting: " + e.getMessage());
                }
            }
            messageProcessor.shutdown();
        }
    }
}

