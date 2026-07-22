package org.jetlinks.community.parallel.driving.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * 平行驾驶延迟指标
 * 用于监控 remotejoystick 等消息的端到端延迟
 *
 * @see org.jetlinks.community.parallel.driving.message.ParallelDrivingCustomMessageHandler
 */
@Component
public class ParallelDrivingLatencyMetrics {

    public static final String METRIC_PLATFORM_LATENCY = "parallel_driving.remotejoystick.platform_latency";

    /** 车端功能回复因白名单未转发驾驶舱（按 functionId 打点） */
    public static final String METRIC_VEHICLE_REPLY_COCKPIT_SKIPPED = "parallel_driving.vehicle_reply.cockpit_forward_skipped";

    private final MeterRegistry registry;

    @Autowired(required = false)
    public ParallelDrivingLatencyMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * 记录 remotejoystick 平台处理延迟（驾驶舱消息到达平台 -> publish 到设备网关）
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @param durationMs 耗时（毫秒）
     */
    public void recordRemoteJoystickPlatformLatency(String cockpitId, String vehicleId, long durationMs) {
        if (registry == null) {
            return;
        }
        Timer.builder(METRIC_PLATFORM_LATENCY)
            .tag("cockpit", cockpitId != null ? cockpitId : "unknown")
            .tag("vehicle", vehicleId != null ? vehicleId : "unknown")
            .description("remotejoystick 平台处理延迟：收到驾驶舱消息到 publish 到设备网关")
            .register(registry)
            .record(durationMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 记录被白名单拦截、未转发至驾驶舱的车端功能回复（便于观测优化效果）。
     *
     * @param functionId 车端回复中的 functionId，未知时为 {@code unknown}
     */
    public void recordVehicleReplyCockpitForwardSkipped(String functionId) {
        if (registry == null) {
            return;
        }
        String fn = StringUtils.hasText(functionId) ? functionId : "unknown";
        registry.counter(METRIC_VEHICLE_REPLY_COCKPIT_SKIPPED,
                "function", fn)
            .increment();
    }
}
