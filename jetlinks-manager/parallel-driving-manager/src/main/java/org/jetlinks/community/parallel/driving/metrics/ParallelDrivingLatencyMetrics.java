package org.jetlinks.community.parallel.driving.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 平行驾驶延迟指标
 * 用于监控 remotejoystick 等消息的端到端延迟
 *
 * @see org.jetlinks.community.parallel.driving.message.ParallelDrivingCustomMessageHandler
 */
@Component
public class ParallelDrivingLatencyMetrics {

    public static final String METRIC_PLATFORM_LATENCY = "parallel_driving.remotejoystick.platform_latency";

    /** 车端功能回复因白名单未转发驾驶舱 */
    public static final String METRIC_VEHICLE_REPLY_COCKPIT_SKIPPED = "parallel_driving.vehicle_reply.cockpit_forward_skipped";
    public static final String METRIC_CONTROL_REQUESTS = "parallel_driving.control.requests";
    public static final String METRIC_CONTROL_DURATION = "parallel_driving.control.duration";
    public static final String METRIC_CONTROL_STAGE_DURATION = "parallel_driving.control.stage_duration";
    public static final String METRIC_CONTROL_REDIS_LOCK = "parallel_driving.control.redis_lock";

    private final MeterRegistry registry;
    private final AtomicInteger activeStatusWebSocketSessions = new AtomicInteger();
    private final AtomicInteger remoteJoystickInflightSends = new AtomicInteger();
    private final Timer remoteJoystickPlatformLatency;
    private final Counter remoteJoystickDedupDropped;
    private final Counter remoteJoystickMailboxCoalesced;
    private final Counter remoteJoystickSendStarted;
    private final Timer remoteJoystickMailboxPendingAge;
    private final Timer remoteJoystickSendCompletionOnComplete;
    private final Timer remoteJoystickSendCompletionOnError;
    private final Timer remoteJoystickSendCompletionCancel;
    private final Timer remoteJoystickSendCompletionOther;
    private final Counter remoteJoystickInflightSlow;
    private final Timer remoteJoystickInflightSlowDuration;
    private final Timer statusVehicleToEventBusVehicle;
    private final Timer statusVehicleToEventBusCockpit;
    private final Timer statusEventBusToQueueVehicle;
    private final Timer statusEventBusToQueueCockpit;
    private final Timer statusEventBusToSendVehicle;
    private final Timer statusEventBusToSendCockpit;
    private final Timer statusQueueToSendVehicle;
    private final Timer statusQueueToSendCockpit;
    private final ConcurrentMap<String, Counter> controlRequestCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> controlDurationTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> controlStageDurationTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> controlRedisLockCounters = new ConcurrentHashMap<>();

    private static final Set<String> CONTROL_OPERATIONS = Set.of(
        "bind", "unbind", "takeover", "release", "control"
    );
    private static final Set<String> CONTROL_STAGES = Set.of(
        "redis_lock",
        "device_lookup",
        "remove_old_sessions",
        "delete_stale_session",
        "create_session",
        "create_room",
        "notify_devices",
        "update_session_state",
        "wait_active",
        "close_room",
        "delete_session",
        "session_room_lookup",
        "forward"
    );

    @Autowired(required = false)
    public ParallelDrivingLatencyMetrics(MeterRegistry registry) {
        this.registry = registry;
        if (registry != null) {
            remoteJoystickPlatformLatency = Timer.builder(METRIC_PLATFORM_LATENCY)
                .description("remotejoystick 平台处理延迟：收到驾驶舱消息到 publish 到设备网关")
                .register(registry);
            remoteJoystickDedupDropped = registry.counter(METRIC_REMOTE_JOYSTICK_DEDUP_DROPPED);
            remoteJoystickMailboxCoalesced = registry.counter(METRIC_REMOTE_JOYSTICK_MAILBOX_COALESCED);
            remoteJoystickSendStarted = registry.counter(METRIC_REMOTE_JOYSTICK_SEND_STARTED);
            remoteJoystickMailboxPendingAge = Timer.builder(METRIC_REMOTE_JOYSTICK_MAILBOX_PENDING_AGE)
                .description("remotejoystick latest-only 信箱中最新帧被取出发送前的等待时间")
                .register(registry);
            remoteJoystickSendCompletionOnComplete = completionTimer(registry, "on_complete");
            remoteJoystickSendCompletionOnError = completionTimer(registry, "on_error");
            remoteJoystickSendCompletionCancel = completionTimer(registry, "cancel");
            remoteJoystickSendCompletionOther = completionTimer(registry, "other");
            remoteJoystickInflightSlow = registry.counter(METRIC_REMOTE_JOYSTICK_INFLIGHT_SLOW);
            remoteJoystickInflightSlowDuration = Timer.builder(METRIC_REMOTE_JOYSTICK_INFLIGHT_SLOW + ".duration")
                .description("remotejoystick latest-only 当前在途发送被观察到的持续时间")
                .register(registry);
            statusVehicleToEventBusVehicle = statusTimer(
                registry,
                "parallel_driving.status_websocket.vehicle_to_eventbus_latency",
                "车辆消息时间戳到云端 EventBus 接收的延迟；依赖车云时钟同步",
                "vehicle"
            );
            statusVehicleToEventBusCockpit = statusTimer(
                registry,
                "parallel_driving.status_websocket.vehicle_to_eventbus_latency",
                "车辆消息时间戳到云端 EventBus 接收的延迟；依赖车云时钟同步",
                "cockpit"
            );
            statusEventBusToQueueVehicle = statusTimer(
                registry,
                "parallel_driving.status_websocket.eventbus_to_queue_latency",
                "云端 EventBus 接收到进入 WebSocket 最新值队列的延迟",
                "vehicle"
            );
            statusEventBusToQueueCockpit = statusTimer(
                registry,
                "parallel_driving.status_websocket.eventbus_to_queue_latency",
                "云端 EventBus 接收到进入 WebSocket 最新值队列的延迟",
                "cockpit"
            );
            statusEventBusToSendVehicle = statusTimer(
                registry,
                "parallel_driving.status_websocket.eventbus_to_send_latency",
                "云端 EventBus 接收到 WebSocket 发送的延迟，包含 latest/sample 等待",
                "vehicle"
            );
            statusEventBusToSendCockpit = statusTimer(
                registry,
                "parallel_driving.status_websocket.eventbus_to_send_latency",
                "云端 EventBus 接收到 WebSocket 发送的延迟，包含 latest/sample 等待",
                "cockpit"
            );
            statusQueueToSendVehicle = statusTimer(
                registry,
                "parallel_driving.status_websocket.queue_to_send_latency",
                "进入 WebSocket 最新值队列到发送的延迟",
                "vehicle"
            );
            statusQueueToSendCockpit = statusTimer(
                registry,
                "parallel_driving.status_websocket.queue_to_send_latency",
                "进入 WebSocket 最新值队列到发送的延迟",
                "cockpit"
            );
            registry.gauge(
                "parallel_driving.status_websocket.active_sessions",
                activeStatusWebSocketSessions
            );
            registry.gauge(
                METRIC_REMOTE_JOYSTICK_INFLIGHT,
                remoteJoystickInflightSends
            );
        } else {
            remoteJoystickPlatformLatency = null;
            remoteJoystickDedupDropped = null;
            remoteJoystickMailboxCoalesced = null;
            remoteJoystickSendStarted = null;
            remoteJoystickMailboxPendingAge = null;
            remoteJoystickSendCompletionOnComplete = null;
            remoteJoystickSendCompletionOnError = null;
            remoteJoystickSendCompletionCancel = null;
            remoteJoystickSendCompletionOther = null;
            remoteJoystickInflightSlow = null;
            remoteJoystickInflightSlowDuration = null;
            statusVehicleToEventBusVehicle = null;
            statusVehicleToEventBusCockpit = null;
            statusEventBusToQueueVehicle = null;
            statusEventBusToQueueCockpit = null;
            statusEventBusToSendVehicle = null;
            statusEventBusToSendCockpit = null;
            statusQueueToSendVehicle = null;
            statusQueueToSendCockpit = null;
        }
    }

    private static Timer completionTimer(MeterRegistry registry, String result) {
        return Timer.builder(METRIC_REMOTE_JOYSTICK_SEND_COMPLETION_LATENCY)
            .tag("result", result)
            .description("remotejoystick 从开始转发到设备发送器完成的耗时")
            .register(registry);
    }

    private static Timer statusTimer(
        MeterRegistry registry,
        String metric,
        String description,
        String source
    ) {
        return Timer.builder(metric)
            .tag("source", source)
            .description(description)
            .register(registry);
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
        runMetricSafely(() -> remoteJoystickPlatformLatency.record(durationMs, TimeUnit.MILLISECONDS));
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
        // functionId 来自车端协议，不能作为 Prometheus label，避免无界基数。
        runMetricSafely(() -> registry.counter(METRIC_VEHICLE_REPLY_COCKPIT_SKIPPED).increment());
    }

    /** remotejoystick 重复帧被去重丢弃的计数 */
    public static final String METRIC_REMOTE_JOYSTICK_DEDUP_DROPPED = "parallel_driving.remotejoystick.dedup_dropped";

    /** remotejoystick 信箱合并（积压时旧帧被覆盖）计数 */
    public static final String METRIC_REMOTE_JOYSTICK_MAILBOX_COALESCED = "parallel_driving.remotejoystick.mailbox_coalesced";

    /** remotejoystick 当前正在等待设备发送完成的数量 */
    public static final String METRIC_REMOTE_JOYSTICK_INFLIGHT = "parallel_driving.remotejoystick.inflight";

    /** remotejoystick 在 latest-only 信箱中的等待时间 */
    public static final String METRIC_REMOTE_JOYSTICK_MAILBOX_PENDING_AGE = "parallel_driving.remotejoystick.mailbox_pending_age";

    /** remotejoystick 从开始转发到设备发送器完成的耗时 */
    public static final String METRIC_REMOTE_JOYSTICK_SEND_COMPLETION_LATENCY = "parallel_driving.remotejoystick.send_completion_latency";

    /** remotejoystick 在途发送超过安全观测阈值的次数 */
    public static final String METRIC_REMOTE_JOYSTICK_INFLIGHT_SLOW = "parallel_driving.remotejoystick.inflight_slow";

    /** remotejoystick 开始调用车辆设备发送器的次数 */
    public static final String METRIC_REMOTE_JOYSTICK_SEND_STARTED = "parallel_driving.remotejoystick.send_started";

    public void recordRemoteJoystickDedupDropped(String cockpitId) {
        if (registry == null) {
            return;
        }
        runMetricSafely(remoteJoystickDedupDropped::increment);
    }

    public void recordRemoteJoystickMailboxCoalesced(String cockpitId, String vehicleId) {
        if (registry == null) {
            return;
        }
        runMetricSafely(remoteJoystickMailboxCoalesced::increment);
    }

    public void remoteJoystickSendStarted() {
        if (registry == null) {
            return;
        }
        remoteJoystickInflightSends.incrementAndGet();
    }

    public void recordRemoteJoystickSendStarted(String cockpitId, String vehicleId) {
        if (registry == null) {
            return;
        }
        runMetricSafely(remoteJoystickSendStarted::increment);
        remoteJoystickInflightSends.incrementAndGet();
    }

    public void remoteJoystickSendFinished() {
        if (registry == null) {
            return;
        }
        remoteJoystickInflightSends.updateAndGet(current -> Math.max(0, current - 1));
    }

    public void recordRemoteJoystickMailboxPendingAge(String cockpitId, String vehicleId, long durationMs) {
        if (registry == null) {
            return;
        }
        runMetricSafely(() -> remoteJoystickMailboxPendingAge.record(
            Math.max(0, durationMs), TimeUnit.MILLISECONDS));
    }

    public void recordRemoteJoystickSendCompletion(String cockpitId,
                                                   String vehicleId,
                                                   long durationMs,
                                                   String result) {
        if (registry == null) {
            return;
        }
        runMetricSafely(() -> timerForCompletion(result)
            .record(Math.max(0, durationMs), TimeUnit.MILLISECONDS));
    }

    public void recordRemoteJoystickInflightSlow(String cockpitId, String vehicleId, long durationMs) {
        if (registry == null) {
            return;
        }
        runMetricSafely(remoteJoystickInflightSlow::increment);
        runMetricSafely(() -> remoteJoystickInflightSlowDuration.record(
            Math.max(0, durationMs), TimeUnit.MILLISECONDS));
    }

    private Timer timerForCompletion(String result) {
        if ("on_complete".equals(result)) {
            return remoteJoystickSendCompletionOnComplete;
        }
        if ("on_error".equals(result)) {
            return remoteJoystickSendCompletionOnError;
        }
        if ("cancel".equals(result)) {
            return remoteJoystickSendCompletionCancel;
        }
        return remoteJoystickSendCompletionOther;
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
        String normalizedSource = normalizeStatusSource(source);
        runMetricSafely(() -> {
            Timer timer = statusTimer(metric, normalizedSource);
            if (timer == null) {
                // Keep forward compatibility if a new status metric is added without a cached timer.
                Timer.builder(metric)
                    .tag("source", normalizedSource)
                    .description(description)
                    .register(registry)
                    .record(durationMs, TimeUnit.MILLISECONDS);
                return;
            }
            timer.record(durationMs, TimeUnit.MILLISECONDS);
        });
    }

    private Timer statusTimer(String metric, String source) {
        if ("parallel_driving.status_websocket.vehicle_to_eventbus_latency".equals(metric)) {
            return "cockpit".equals(source)
                ? statusVehicleToEventBusCockpit
                : statusVehicleToEventBusVehicle;
        }
        if ("parallel_driving.status_websocket.eventbus_to_queue_latency".equals(metric)) {
            return "cockpit".equals(source)
                ? statusEventBusToQueueCockpit
                : statusEventBusToQueueVehicle;
        }
        if ("parallel_driving.status_websocket.eventbus_to_send_latency".equals(metric)) {
            return "cockpit".equals(source)
                ? statusEventBusToSendCockpit
                : statusEventBusToSendVehicle;
        }
        if ("parallel_driving.status_websocket.queue_to_send_latency".equals(metric)) {
            return "cockpit".equals(source)
                ? statusQueueToSendCockpit
                : statusQueueToSendVehicle;
        }
        return null;
    }

    private void incrementStatusCounter(String metric, String... tags) {
        if (registry == null) {
            return;
        }
        runMetricSafely(() -> registry.counter(metric, tags).increment());
    }

    private String normalizeStatusSource(String source) {
        return "cockpit".equals(source) ? "cockpit" : "vehicle";
    }

    /**
     * 观察控制面操作。计时从响应式链真正订阅开始，避免冷 Mono 在创建时提前计时。
     */
    public <T> Mono<T> observeControlOperation(String operation, Mono<T> publisher) {
        if (registry == null) {
            return publisher;
        }
        return Mono.defer(() -> {
            long started = System.nanoTime();
            return publisher.doFinally(signal -> runMetricSafely(() -> {
                String result = controlResult(signal);
                String normalizedOperation = normalizeControlOperation(operation);
                controlRequestCounter(normalizedOperation, result).increment();
                controlDurationTimer(normalizedOperation, result)
                    .record(Math.max(0, System.nanoTime() - started), TimeUnit.NANOSECONDS);
            }));
        });
    }

    /**
     * 观察控制面阶段。阶段名必须来自固定枚举，不能传入设备 ID 或请求 ID。
     */
    public <T> Mono<T> observeControlStage(String operation, String stage, Mono<T> publisher) {
        if (registry == null) {
            return publisher;
        }
        return Mono.defer(() -> {
            long started = System.nanoTime();
            return publisher.doFinally(signal -> runMetricSafely(() -> {
                controlStageDurationTimer(
                    normalizeControlOperation(operation),
                    normalizeControlStage(stage),
                    controlResult(signal)
                )
                    .record(Math.max(0, System.nanoTime() - started), TimeUnit.NANOSECONDS);
            }));
        });
    }

    public void recordRedisLock(String operation, boolean acquired) {
        if (registry == null) {
            return;
        }
        String normalizedOperation = normalizeControlOperation(operation);
        runMetricSafely(() -> controlRedisLockCounter(normalizedOperation, acquired ? "acquired" : "busy")
            .increment());
    }

    private String controlResult(SignalType signal) {
        if (signal == SignalType.ON_COMPLETE) {
            return "success";
        }
        if (signal == SignalType.ON_ERROR) {
            return "failure";
        }
        if (signal == SignalType.CANCEL) {
            return "cancel";
        }
        return "other";
    }

    private String normalizeControlOperation(String operation) {
        return StringUtils.hasText(operation) && CONTROL_OPERATIONS.contains(operation)
            ? operation
            : "unknown";
    }

    private String normalizeControlStage(String stage) {
        return StringUtils.hasText(stage) && CONTROL_STAGES.contains(stage)
            ? stage
            : "unknown";
    }

    private Counter controlRequestCounter(String operation, String result) {
        return controlRequestCounters.computeIfAbsent(
            metricKey(operation, result),
            ignored -> registry.counter(
                METRIC_CONTROL_REQUESTS,
                "operation", operation,
                "result", result
            )
        );
    }

    private Timer controlDurationTimer(String operation, String result) {
        return controlDurationTimers.computeIfAbsent(
            metricKey(operation, result),
            ignored -> Timer.builder(METRIC_CONTROL_DURATION)
                .tag("operation", operation)
                .tag("result", result)
                .description("并行驾驶控制面操作耗时")
                .register(registry)
        );
    }

    private Timer controlStageDurationTimer(String operation, String stage, String result) {
        return controlStageDurationTimers.computeIfAbsent(
            metricKey(operation, stage, result),
            ignored -> Timer.builder(METRIC_CONTROL_STAGE_DURATION)
                .tag("operation", operation)
                .tag("stage", stage)
                .tag("result", result)
                .description("并行驾驶控制面阶段耗时")
                .register(registry)
        );
    }

    private Counter controlRedisLockCounter(String operation, String result) {
        return controlRedisLockCounters.computeIfAbsent(
            metricKey(operation, result),
            ignored -> registry.counter(
                METRIC_CONTROL_REDIS_LOCK,
                "operation", operation,
                "result", result
            )
        );
    }

    private static String metricKey(String... values) {
        return String.join("\u0000", values);
    }

    /**
     * 观测必须是旁路能力。注册表配置冲突或 exporter 异常不能改变业务 publisher 的结果。
     */
    private void runMetricSafely(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException ignored) {
            // Metrics are best effort and must never affect the media/control path.
        }
    }
}
