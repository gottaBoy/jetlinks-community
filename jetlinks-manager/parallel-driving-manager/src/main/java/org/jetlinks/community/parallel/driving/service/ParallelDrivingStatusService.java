package org.jetlinks.community.parallel.driving.service;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;

/**
 * 平行驾驶状态服务
 * 负责车辆状态实时上报和转发
 *
 * @author JetLinks
 */
@Service
@Slf4j
public class ParallelDrivingStatusService {
    
    // 注意：此服务已废弃，车辆状态上报不应该转发给驾驶舱设备
    // 状态上报只用于 WebSocket 推送到前端（ParallelDrivingWebSocketHandler）
    
    /**
     * 初始化状态订阅
     * 
     * 注意：车辆状态上报不应该转发给驾驶舱设备
     * 状态上报只用于 WebSocket 推送到前端
     * 此服务已废弃，状态上报由 ParallelDrivingWebSocketHandler 处理
     */
    @PostConstruct
    public void init() {
        // 不再订阅车辆状态上报并转发给驾驶舱
        // 状态上报只用于 WebSocket 推送到前端（ParallelDrivingWebSocketHandler）
        // 如果需要转发，应该只转发自定义消息响应（如 emergencystopresp）
        log.info("平行驾驶状态服务已启动（已废弃，状态上报不转发给驾驶舱）");
    }
    
    
    /**
     * 处理车辆状态上报（已废弃）
     * 
     * 注意：车辆状态上报不应该转发给驾驶舱设备
     * 状态上报只用于：
     * 1. WebSocket 推送到前端（ParallelDrivingWebSocketHandler）
     * 2. 平台数据存储和查询
     * 
     * 只有自定义消息响应（如 emergencystopresp）才需要转发给驾驶舱
     * 自定义消息响应由 ParallelDrivingCustomMessageHandler 处理
     *
     * @param message 属性上报消息
     * @return Mono<Void>
     * @deprecated 车辆状态上报不应该转发给驾驶舱，此方法已废弃
     */
    @Deprecated
    public Mono<Void> handleVehicleStatusReport(ReportPropertyMessage message) {
        // 不再转发车辆状态上报给驾驶舱
        // 状态上报只用于 WebSocket 推送到前端
        log.debug("收到车辆状态上报（不转发给驾驶舱）: vehicle={}, properties={}", 
            message.getDeviceId(), message.getProperties());
        return Mono.empty();
    }
    
    /**
     * 获取车辆状态（通过设备属性查询）
     *
     * @param vehicleDeviceId 车辆设备ID
     * @return Mono<Map<String, Object>> 车辆状态属性
     */
    public Mono<java.util.Map<String, Object>> getVehicleStatus(String vehicleDeviceId) {
        // 这里可以通过 DeviceOperator 读取设备属性
        // 实际实现需要根据 JetLinks 的 API 调整
        return Mono.empty();
    }
}
