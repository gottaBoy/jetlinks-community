package org.jetlinks.community.parallel.driving.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 平行驾驶加密服务
 * 用于查询设备的加密状态和配置
 *
 * @author JetLinks
 */
@Service
@Slf4j
@AllArgsConstructor
public class ParallelDrivingEncryptionService {
    
    private final DeviceRegistry deviceRegistry;
    
    /**
     * 协议配置键：是否启用加密
     */
    public static final ConfigKey<Boolean> CONFIG_KEY_ENABLE_ENCRYPT = ConfigKey.of("enableEncrypt");
    
    /**
     * 检查设备是否启用加密
     *
     * @param deviceId 设备ID
     * @return Mono<Boolean> true 表示启用加密，false 表示未启用
     */
    public Mono<Boolean> isEncryptionEnabled(String deviceId) {
        return deviceRegistry.getDevice(deviceId)
            .flatMap(device -> device.getSelfConfig(CONFIG_KEY_ENABLE_ENCRYPT.getKey())
                .map(value -> {
                    Object configValue = value.as(Object.class);
                    if (configValue == null) {
                        return false; // 默认不启用加密
                    }
                    
                    // 处理类型转换
                    if (configValue instanceof Boolean) {
                        return (Boolean) configValue;
                    } else if (configValue instanceof String) {
                        String strValue = ((String) configValue).trim().toLowerCase();
                        return "true".equals(strValue) || "1".equals(strValue) || "yes".equals(strValue);
                    } else if (configValue instanceof Number) {
                        return ((Number) configValue).intValue() != 0;
                    }
                    
                    return false;
                })
            )
            .defaultIfEmpty(false)
            .doOnNext(enabled -> log.debug("设备[{}]加密状态: {}", deviceId, enabled ? "已启用" : "未启用"));
    }
    
    /**
     * 检查设备是否已认证（用于加密通信）
     * 注意：实际的认证状态存储在协议编解码器中，这里只能通过设备在线状态推断
     *
     * @param deviceId 设备ID
     * @return Mono<Boolean> true 表示设备在线（可能已认证），false 表示设备离线
     */
    public Mono<Boolean> isDeviceAuthenticated(String deviceId) {
        return deviceRegistry.getDevice(deviceId)
            .flatMap(DeviceOperator::isOnline)
            .defaultIfEmpty(false)
            .doOnNext(online -> log.debug("设备[{}]在线状态: {}", deviceId, online ? "在线" : "离线"));
    }
    
    /**
     * 检查设备是否支持加密通信
     * 需要同时满足：启用加密配置 + 设备在线
     *
     * @param deviceId 设备ID
     * @return Mono<Boolean> true 表示支持加密通信
     */
    public Mono<Boolean> isEncryptionSupported(String deviceId) {
        return Mono.zip(
            isEncryptionEnabled(deviceId),
            isDeviceAuthenticated(deviceId)
        )
        .map(tuple -> tuple.getT1() && tuple.getT2())
        .doOnNext(supported -> log.debug("设备[{}]加密通信支持: {}", deviceId, supported ? "支持" : "不支持"));
    }
}
