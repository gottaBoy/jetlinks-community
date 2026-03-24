package org.jetlinks.community.parallel.driving.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.parallel.driving.message.ParallelDrivingControlMessage;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 平行驾驶控制日志服务
 * 记录控制指令的发送日志和统计信息
 *
 * @author JetLinks
 */
@Service
@Slf4j
@AllArgsConstructor
public class ParallelDrivingControlLogService {
    
    // 控制日志缓存（可以后续改为持久化存储）
    private final ConcurrentLinkedQueue<ControlLog> controlLogs = new ConcurrentLinkedQueue<>();
    
    // 统计信息
    private final Map<String, ControlStatistics> statistics = new java.util.concurrent.ConcurrentHashMap<>();
    
    /**
     * 记录控制指令日志
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车辆设备ID
     * @param controlMessage 控制指令消息
     * @param success 是否成功
     * @param errorMessage 错误信息（如果失败）
     */
    public void logControlCommand(String cockpitDeviceId, 
                                 String vehicleDeviceId,
                                 ParallelDrivingControlMessage controlMessage,
                                 boolean success,
                                 String errorMessage) {
        ControlLog controlLog = ControlLog.builder()
            .id(java.util.UUID.randomUUID().toString())
            .cockpitDeviceId(cockpitDeviceId)
            .vehicleDeviceId(vehicleDeviceId)
            .controlType(controlMessage.getControlType() != null 
                ? controlMessage.getControlType().getValue() 
                : "unknown")
            .controlParams(controlMessage.getControlParams())
            .success(success)
            .errorMessage(errorMessage)
            .timestamp(System.currentTimeMillis())
            .build();
        
        controlLogs.offer(controlLog);
        
        // 保持最近 1000 条日志
        while (controlLogs.size() > 1000) {
            controlLogs.poll();
        }
        
        // 更新统计信息
        String key = cockpitDeviceId + "-" + vehicleDeviceId;
        statistics.computeIfAbsent(key, k -> new ControlStatistics())
            .increment(controlMessage.getControlType() != null 
                ? controlMessage.getControlType().getValue() 
                : "unknown", success);
        
        if (success) {
            log.info("控制指令发送成功: cockpit={}, vehicle={}, type={}, params={}", 
                cockpitDeviceId, vehicleDeviceId, controlLog.getControlType(), controlLog.getControlParams());
        } else {
            log.warn("控制指令发送失败: cockpit={}, vehicle={}, type={}, error={}", 
                cockpitDeviceId, vehicleDeviceId, controlLog.getControlType(), errorMessage);
        }
    }
    
    /**
     * 获取控制日志
     *
     * @param cockpitDeviceId 驾驶舱设备ID（可选）
     * @param vehicleDeviceId 车辆设备ID（可选）
     * @param limit 限制数量
     * @return 控制日志列表
     */
    public java.util.List<ControlLog> getControlLogs(String cockpitDeviceId, 
                                                     String vehicleDeviceId, 
                                                     int limit) {
        return controlLogs.stream()
            .filter(log -> {
                if (cockpitDeviceId != null && !log.getCockpitDeviceId().equals(cockpitDeviceId)) {
                    return false;
                }
                if (vehicleDeviceId != null && !log.getVehicleDeviceId().equals(vehicleDeviceId)) {
                    return false;
                }
                return true;
            })
            .sorted((a, b) -> Long.compare(b.getTimestamp(), a.getTimestamp()))
            .limit(limit)
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * 获取统计信息
     *
     * @param cockpitDeviceId 驾驶舱设备ID（可选）
     * @param vehicleDeviceId 车辆设备ID（可选）
     * @return 统计信息
     */
    public ControlStatistics getStatistics(String cockpitDeviceId, String vehicleDeviceId) {
        String key = cockpitDeviceId + "-" + vehicleDeviceId;
        return statistics.getOrDefault(key, new ControlStatistics());
    }
    
    /**
     * 控制日志实体
     */
    @lombok.Data
    @lombok.Builder
    public static class ControlLog {
        private String id;
        private String cockpitDeviceId;
        private String vehicleDeviceId;
        private String controlType;
        private Map<String, Object> controlParams;
        private boolean success;
        private String errorMessage;
        private long timestamp;
    }
    
    /**
     * 控制统计信息
     */
    @lombok.Data
    public static class ControlStatistics {
        private final Map<String, Long> successCount = new HashMap<>();
        private final Map<String, Long> failureCount = new HashMap<>();
        private long totalSuccess = 0;
        private long totalFailure = 0;
        
        public void increment(String controlType, boolean success) {
            if (success) {
                successCount.merge(controlType, 1L, (a, b) -> a + b);
                totalSuccess++;
            } else {
                failureCount.merge(controlType, 1L, (a, b) -> a + b);
                totalFailure++;
            }
        }
        
        public long getTotal() {
            return totalSuccess + totalFailure;
        }
        
        public double getSuccessRate() {
            long total = getTotal();
            return total > 0 ? (double) totalSuccess / total : 0.0;
        }
    }
}
