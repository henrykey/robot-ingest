#!/bin/bash

# 🚀 自适应高频MQTT消息处理测试脚本
# 测试场景：正常模式 -> 高频模式 -> 正常模式的自动切换

set -e

# 配置参数
MQTT_BROKER="192.168.123.61"
MQTT_PORT="1883"
BASE_TOPIC="robots"
DEVICE_ID="test_robot_$(date +%s)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] ✅${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠️${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ❌${NC} $1"
}

# 检查依赖
check_dependencies() {
    log "检查依赖工具..."
    
    if ! command -v mosquitto_pub >/dev/null 2>&1; then
        log_error "mosquitto_pub 未安装，请安装: sudo apt-get install mosquitto-clients"
        exit 1
    fi
    
    if ! command -v docker-compose >/dev/null 2>&1; then
        log_error "docker-compose 未安装"
        exit 1
    fi
    
    log_success "所有依赖检查完成"
}

# 发送测试消息
send_message() {
    local topic=$1
    local message=$2
    local qos=${3:-1}
    
    mosquitto_pub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "$topic" -m "$message" -q "$qos"
}

# 生成机器人状态消息
generate_state_message() {
    local device_id=$1
    local battery=$((RANDOM % 100 + 1))
    local task_id=$((RANDOM % 1000))
    
    cat <<EOF
{
    "deviceId": "$device_id",
    "timestamp": "$(date -Iseconds)",
    "battery": $battery,
    "taskStatus": "running",
    "taskId": "$task_id",
    "autonomousMode": true,
    "fault": false,
    "binsNum": 5,
    "coordinateType": "cartesian",
    "position": {
        "x": $((RANDOM % 1000)).123456,
        "y": $((RANDOM % 1000)).654321,
        "z": 0.0,
        "orientation": $((RANDOM % 360))
    }
}
EOF
}

# 发送低频消息测试（正常模式）
test_normal_mode() {
    log "📊 阶段1: 测试正常模式 (低频消息, <500 msg/s)"
    local count=0
    local max_count=50
    
    while [ $count -lt $max_count ]; do
        local device="robot_normal_$((count % 10))"
        local topic="$BASE_TOPIC/$device/state"
        local message=$(generate_state_message "$device")
        
        send_message "$topic" "$message"
        
        count=$((count + 1))
        echo -ne "\r发送消息: $count/$max_count"
        
        # 低频发送：每秒2-3条消息
        sleep 0.4
    done
    
    echo ""
    log_success "正常模式测试完成，发送了 $max_count 条消息"
    log "等待10秒观察统计..."
    sleep 10
}

# 发送高频消息测试（触发高频模式）
test_high_freq_mode() {
    log "🚀 阶段2: 测试高频模式触发 (高频消息, >1000 msg/s)"
    local count=0
    local max_count=3000
    local start_time=$(date +%s)
    
    while [ $count -lt $max_count ]; do
        local device="robot_high_$((count % 20))"
        local topic="$BASE_TOPIC/$device/state"
        local message=$(generate_state_message "$device")
        
        send_message "$topic" "$message"
        
        count=$((count + 1))
        
        # 显示进度
        if [ $((count % 200)) -eq 0 ]; then
            local elapsed=$(($(date +%s) - start_time))
            local rate=$((count / (elapsed + 1)))
            echo -ne "\r发送消息: $count/$max_count (速率: ~${rate} msg/s)"
        fi
        
        # 高频发送：无延迟
    done
    
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    local rate=$((max_count / elapsed))
    
    echo ""
    log_success "高频模式测试完成，发送了 $max_count 条消息"
    log "📈 实际发送速率: $rate msg/s (耗时: ${elapsed}s)"
    log "等待15秒观察模式切换和统计..."
    sleep 15
}

# 发送低频消息测试（触发回到正常模式）
test_return_normal_mode() {
    log "📉 阶段3: 测试返回正常模式 (低频消息, <500 msg/s)"
    local count=0
    local max_count=100
    
    while [ $count -lt $max_count ]; do
        local device="robot_return_$((count % 5))"
        local topic="$BASE_TOPIC/$device/state"
        local message=$(generate_state_message "$device")
        
        send_message "$topic" "$message"
        
        count=$((count + 1))
        echo -ne "\r发送消息: $count/$max_count"
        
        # 低频发送：每秒1条消息
        sleep 1
    done
    
    echo ""
    log_success "返回正常模式测试完成，发送了 $max_count 条消息"
    log "等待10秒观察模式切换..."
    sleep 10
}

# 显示日志监控命令
show_monitoring_commands() {
    log "📊 监控命令："
    echo "  实时日志: docker-compose logs -f ingest"
    echo "  统计信息: docker-compose logs ingest | grep 'ADAPTIVE'"
    echo "  模式切换: docker-compose logs ingest | grep 'MODE-SWITCH'"
    echo "  Redis队列: docker exec robot-ingest-ingest redis-cli -h 192.168.123.20 llen q:state"
}

# 主测试流程
main() {
    echo "🚀 自适应高频MQTT消息处理测试"
    echo "========================================"
    
    check_dependencies
    
    log "测试设备ID: $DEVICE_ID"
    log "MQTT Broker: $MQTT_BROKER:$MQTT_PORT"
    log "开始自适应处理测试..."
    
    show_monitoring_commands
    echo ""
    
    # 等待用户准备
    log_warning "请确保ingest服务正在运行，按Enter继续..."
    read -r
    
    # 执行测试阶段
    test_normal_mode
    test_high_freq_mode  
    test_return_normal_mode
    
    log_success "🎉 自适应高频处理测试完成！"
    echo ""
    echo "📋 测试总结:"
    echo "   阶段1: 正常模式 (50条消息, ~2.5 msg/s)"
    echo "   阶段2: 高频模式 (3000条消息, >1000 msg/s)"
    echo "   阶段3: 返回正常 (100条消息, 1 msg/s)"
    echo ""
    echo "🔍 查看结果:"
    echo "   docker-compose logs ingest | grep -E '(ADAPTIVE|MODE-SWITCH)'"
}

# 运行主函数
main "$@"
