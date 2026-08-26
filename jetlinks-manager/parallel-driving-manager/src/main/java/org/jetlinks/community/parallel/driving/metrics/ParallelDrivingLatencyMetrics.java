package org.jetlinks.community.parallel.driving.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger activeStatusWebSocketSessions = new AtomicInteger();

    @Autowired(required = false)
    public ParallelDrivingLatencyMetrics(MeterRegistry registry) {
        this.registry = registry;
        if (registry != null) {
            registry.gauge(
                "parallel_driving.status_websocket.active_sessions",
                activeStatusWebSocketSessions
            );
        }
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

    /** remotejoystick 重复帧被去重丢弃的计数 */
    public static final String METRIC_REMOTE_JOYSTICK_DEDUP_DROPPED = "parallel_driving.remotejoystick.dedup_dropped";

    /** remotejoystick 信箱合并（积压时旧帧被覆盖）计数 */
    public static final String METRIC_REMOTE_JOYSTICK_MAILBOX_COALESCED = "parallel_driving.remotejoystick.mailbox_coalesced";

    public void recordRemoteJoystickDedupDropped(String cockpitId) {
        if (registry == null) {
            return;
        }
        registry.counter(METRIC_REMOTE_JOYSTICK_DEDUP_DROPPED,
                "cockpit", cockpitId != null ? cockpitId : "unknown")
            .increment();
    }

    public void recordRemoteJoystickMailboxCoalesced(String cockpitId, String vehicleId) {
        if (registry == null) {
            return;
        }
        registry.counter(METRIC_REMOTE_JOYSTICK_MAILBOX_COALESCED,
                "cockpit", cockpitId != null ? cockpitId : "unknown",
                "vehicle", vehicleId != null ? vehicleId : "unknown")
            .increment();
    }

    public void statusWebSocketOpened() {
        activeStatusWebSocketSessions.incrementAndGet();
        incrementStatusCounter("parallel_driving.status_websocket.connections", "result", "opened");
    }

    public void statusWebSocketClosed() {
        activeStatusWebSocketSessions.updateAndGet(current -> Math.max(0, current - 1));
        incrementStatusCounter("parallel_driving.status_websocket.connections", "result", "closed");
    }

    public void recordStatusMessageReceived(String source, long vehicleTimestamp, long eventBusReceivedAt) {
        String normalizedSource = normalizeStatusSource(source);
        incrementStatusCounter(
            "parallel_driving.status_websocket.messages",
            "stage", "eventbus_received",
            "source", normalizedSource
        );
        recordPlausibleTimestampLatency(
            "parallel_driving.status_websocket.vehicle_to_eventbus_latency",
            "车辆消息时间戳到云端 EventBus 接收的延迟；依赖车云时钟同步",
            normalizedSource,
            vehicleTimestamp,
            eventBusReceivedAt
        );
    }

    public void recordStatusQueued(String source, long eventBusReceivedAt, long queuedAt) {
        recordStatusQueued(
            source,
            eventBusReceivedAt,
            queuedAt,
            Math.max(0, queuedAt - eventBusReceivedAt)
        );
    }

    public void recordStatusQueued(
        String source,
        long eventBusReceivedAt,
        long queuedAt,
        long monotonicDurationMs
    ) {
        recordStatusDuration(
            "parallel_driving.status_websocket.eventbus_to_queue_latency",
            "云端 EventBus 接收到进入 WebSocket 最新值队列的延迟",
            source,
            monotonicDurationMs
        );
    }

    public void recordStatusSent(
        String source,
        long eventBusReceivedAt,
        long queuedAt,
        long sentAt
    ) {
        recordStatusSent(
            source,
            eventBusReceivedAt,
            queuedAt,
            sentAt,
            Math.max(0, sentAt - eventBusReceivedAt),
            Math.max(0, sentAt - queuedAt)
        );
    }

    public void recordStatusSent(
        String source,
        long eventBusReceivedAt,
        long queuedAt,
        long sentAt,
        long eventbusToSendDurationMs,
        long queueToSendDurationMs
    ) {
        String normalizedSource = normalizeStatusSource(source);
        incrementStatusCounter(
            "parallel_driving.status_websocket.messages",
            "stage", "websocket_sent",
            "source", normalizedSource
        );
        recordStatusDuration(
            "parallel_driving.status_websocket.eventbus_to_send_latency",
            "云端 EventBus 接收到 WebSocket 发送的延迟，包含 latest/sample 等待",
            normalizedSource,
            eventbusToSendDurationMs
        );
        recordStatusDuration(
            "parallel_driving.status_websocket.queue_to_send_latency",
            "进入 WebSocket 最新值队列到发送的延迟",
            normalizedSource,
            queueToSendDurationMs
        );
    }

    public void recordStatusSinkFailure(String result) {
        incrementStatusCounter(
            "parallel_driving.status_websocket.sink_failures",
            "result", StringUtils.hasText(result) ? result : "unknown"
        );
    }

    public void recordStatusSubscriptionError(String source) {
        incrementStatusCounter(
            "parallel_driving.status_websocket.subscription_errors",
            "source", normalizeStatusSource(source)
        );
    }

    private void recordPlausibleTimestampLatency(
        String metric,
        String description,
        String source,
        long fromTimestamp,
        long toTimestamp
    ) {
        long duration = toTimestamp - fromTimestamp;
        // 车端时钟未同步时避免污染直方图；原始时间戳仍随 WebSocket 消息下发用于事件排查。
        if (fromTimestamp <= 0 || duration < 0 || duration > TimeUnit.MINUTES.toMillis(10)) {
            incrementStatusCounter(
                "parallel_driving.status_websocket.invalid_vehicle_timestamp",
                "source", normalizeStatusSource(source)
            );
            return;
        }
        recordStatusDuration(metric, description, source, duration);
    }

    private void recordStatusDuration(
        String metric,
        String description,
        String source,
        long durationMs
    ) {
        if (registry == null) {
            return;
        }
        Timer.builder(metric)
            .tag("source", normalizeStatusSource(source))
            .description(description)
            .register(registry)
            .record(durationMs, TimeUnit.MILLISECONDS);
    }

    private void incrementStatusCounter(String metric, String... tags) {
        if (registry == null) {
            return;
        }
        registry.counter(metric, tags).increment();
    }

    private String normalizeStatusSource(String source) {
        return "cockpit".equals(source) ? "cockpit" : "vehicle";
    }
}
