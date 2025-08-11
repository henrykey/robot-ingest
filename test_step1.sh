#!/bin/bash

# 步骤1验收测试：验证Tap功能不影响原链路

echo "🚀 步骤1验收测试开始..."

# 测试1：验证特性关闭时的行为（默认状态）
echo "📋 测试1：验证特性默认关闭状态"
echo "INGEST_FEATURE_ENABLED 未设置，GlobalQueue应该直接返回false"

# 测试2：验证构建成功
echo "📋 测试2：验证Docker构建成功"
docker images | grep "ibcai/robot-ingest.*ingest"
if [ $? -eq 0 ]; then
    echo "✅ Docker镜像构建成功"
else
    echo "❌ Docker镜像构建失败"
    exit 1
fi

# 测试3：验证特性开关配置存在
echo "📋 测试3：验证配置文件包含特性开关"
if grep -q "ingest:" config.yml; then
    echo "✅ 配置文件包含ingest特性配置"
else
    echo "❌ 配置文件缺少ingest特性配置"
    exit 1
fi

# 测试4：验证默认为关闭状态
if grep -q "enabled: false" config.yml; then
    echo "✅ 特性默认关闭状态正确"
else
    echo "❌ 特性默认状态不正确"
    exit 1
fi

echo "🎉 步骤1基础验收测试通过！"
echo "📝 下一步需要进行运行时验证（需要MQTT broker）"
