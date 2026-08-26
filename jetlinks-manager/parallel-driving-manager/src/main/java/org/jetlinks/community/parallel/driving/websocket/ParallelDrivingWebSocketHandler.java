package org.jetlinks.community.parallel.driving.websocket;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.parallel.driving.metrics.ParallelDrivingLatencyMetrics;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingRelationService;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;

/**
 * 平行驾驶 WebSocket 处理器
 * 用于实时推送车辆状态到前端
 *
 * @author yi.min@zeron.ai
 */
@Component
@Slf4j
public class ParallelDrivingWebSocketHandler implements WebSocketHandler {

    private static final String INTERNAL_EVENTBUS_RECEIVED_NANOS = "_eventbusReceivedMonoNanos";
    private static final String INTERNAL_WEBSOCKET_QUEUED_NANOS = "_websocketQueuedMonoNanos";
    private static final String EVENTBUS_TO_QUEUE_DURATION_MS = "eventbusToQueueDurationMs";
    private static final String QUEUE_TO_SEND_DURATION_MS = "queueToSendDurationMs";
    private static final String EVENTBUS_TO_SEND_DURATION_MS = "eventbusToSendDurationMs";
    
    private final ParallelDrivingRelationService relationService;
    private final EventBus eventBus;
    private final ParallelDrivingLatencyMetrics metrics;

    @Autowired
    public ParallelDrivingWebSocketHandler(ParallelDrivingRelationService relationService,
                                          @Qualifier("eventBus") EventBus eventBus,
                                          ParallelDrivingLatencyMetrics metrics) {
        this.relationService = relationService;
        this.eventBus = eventBus;
        this.metrics = metrics;
    }
    
    // 存储 WebSocket 连接（key: sessionId, value: WebSocketSessionInfo）
    private final Map<String, WebSocketSessionInfo> sessions = new ConcurrentHashMap<>();
    
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String sessionId = session.getId();
        log.info("WebSocket 连接建立: sessionId={}", sessionId);
        
        // 从查询参数中获取车辆设备ID或驾驶舱设备ID
        String query = session.getHandshakeInfo().getUri().getQuery();
        String vehicleId = query != null && query.contains("vehicleId=")
            ? extractQueryParam(query, "vehicleId")
            : null;
        String cockpitId = query != null && query.contains("cockpitId=")
            ? extractQueryParam(query, "cockpitId")
            : null;
        
        if (vehicleId == null && cockpitId == null) {
            log.warn("WebSocket 连接缺少设备ID参数: sessionId={}", sessionId);
            return session.close();
        }
        
        // 创建会话信息
        WebSocketSessionInfo sessionInfo = new WebSocketSessionInfo(sessionId, vehicleId, cockpitId);
        sessions.put(sessionId, sessionInfo);
        metrics.statusWebSocketOpened();
        AtomicBoolean cleaned = new AtomicBoolean();
        
        // 订阅车辆状态上报
        Disposable subscription = subscribeVehicleStatus(vehicleId, cockpitId, session);
        Runnable cleanup = () -> {
            if (cleaned.compareAndSet(false, true)) {
                sessions.remove(sessionId);
                metrics.statusWebSocketClosed();
                if (subscription != null) {
                    subscription.dispose();
                }
            }
        };
        
        // 订阅车辆状态并推送到 WebSocket
        Flux<WebSocketMessage> statusMessages = sessionInfo.getStatusFlux()
            .map(status -> {
                long sentAt = System.currentTimeMillis();
                long sentAtNanos = System.nanoTime();
                Map<String, Object> outgoing = new HashMap<>(status);
                outgoing.put("websocketSentAt", sentAt);
                long eventbusReceivedAtNanos = numberValue(
                    outgoing.get(INTERNAL_EVENTBUS_RECEIVED_NANOS),
                    sentAtNanos
                );
                long queuedAtNanos = numberValue(
                    outgoing.get(INTERNAL_WEBSOCKET_QUEUED_NANOS),
                    sentAtNanos
                );
                long eventbusToSendDurationMs =
                    monotonicDurationMs(eventbusReceivedAtNanos, sentAtNanos);
                long queueToSendDurationMs =
                    monotonicDurationMs(queuedAtNanos, sentAtNanos);
                outgoing.put(EVENTBUS_TO_SEND_DURATION_MS, eventbusToSendDurationMs);
                outgoing.put(QUEUE_TO_SEND_DURATION_MS, queueToSendDurationMs);
                outgoing.put("serverMonotonicTimingAvailable",
                    outgoing.containsKey(INTERNAL_EVENTBUS_RECEIVED_NANOS)
                        && outgoing.containsKey(INTERNAL_WEBSOCKET_QUEUED_NANOS));
                outgoing.remove(INTERNAL_EVENTBUS_RECEIVED_NANOS);
                outgoing.remove(INTERNAL_WEBSOCKET_QUEUED_NANOS);
                String source = String.valueOf(outgoing.remove("_observationSource"));
                metrics.recordStatusSent(
                    source,
                    numberValue(outgoing.get("eventbusReceivedAt"), sentAt),
                    numberValue(outgoing.get("websocketQueuedAt"), sentAt),
                    sentAt,
                    eventbusToSendDurationMs,
                    queueToSendDurationMs
                );
                return session.textMessage(JSON.toJSONString(outgoing));
            })
            .doOnComplete(() -> {
                log.debug("WebSocket 状态推送完成: sessionId={}", sessionId);
            })
            .doOnError(error -> {
                log.error("WebSocket 状态推送失败: sessionId={}", sessionId, error);
            });
        
        // 处理客户端消息（可选，用于接收控制指令）
        Mono<Void> receiveHandler = session.receive()
            .doOnNext(message -> {
                String payload = message.getPayloadAsText();
                log.debug("收到 WebSocket 客户端消息: sessionId={}, payload={}", sessionId, payload);
                // 可以在这里处理客户端发送的控制指令
            })
            .then();
        
        // 监听连接关闭（无论正常/异常，都要清理订阅）
        session.closeStatus()
            .doFinally((SignalType signalType) -> {
                log.info("WebSocket 连接关闭: sessionId={}, signal={}", sessionId, signalType);
                cleanup.run();
            })
            .subscribe();
        
        // 发送状态消息并处理客户端消息
        return Mono.zip(
            session.send(statusMessages),
            receiveHandler
        )
            .then()
            .doFinally(signalType -> cleanup.run());
    }
    
    /**
     * 订阅车辆状态上报
     *
     * @param vehicleId 车辆设备ID
     * @param cockpitId 驾驶舱设备ID
     * @param session WebSocket 会话
     * @return Disposable 用于取消订阅
     */
    private Disposable subscribeVehicleStatus(String vehicleId, String cockpitId, WebSocketSession session) {
        String sessionId = session.getId();
        WebSocketSessionInfo sessionInfo = sessions.get(sessionId);
        
        if (sessionInfo == null) {
            return null;
        }
        
        // 如果提供了车辆ID，订阅该车辆的状态
        if (vehicleId != null) {
            Subscription subscription = Subscription
                .builder()
                .subscriberId("parallel-driving-websocket-" + sessionId)
                .topics("/device/*/" + vehicleId + "/message/property/report")
                .features(Subscription.Feature.local, Subscription.Feature.broker)
                .build();
            
            return eventBus.subscribe(subscription, DeviceMessage.class)
                .ofType(ReportPropertyMessage.class)
                .map(message -> {
                    long eventBusReceivedAt = System.currentTimeMillis();
                    long eventBusReceivedAtNanos = System.nanoTime();
                    metrics.recordStatusMessageReceived(
                        "vehicle",
                        message.getTimestamp(),
                        eventBusReceivedAt
                    );
                    Map<String, Object> status = new java.util.HashMap<>();
                    status.put("type", "vehicle-status");
                    status.put("deviceId", message.getDeviceId());
                    status.put("properties", message.getProperties());
                    status.put("timestamp", message.getTimestamp());
                    status.put("eventbusReceivedAt", eventBusReceivedAt);
                    status.put(INTERNAL_EVENTBUS_RECEIVED_NANOS, eventBusReceivedAtNanos);
                    status.put("_observationSource", "vehicle");
                    return status;
                })
                .doOnNext(status -> {
                    // chassis_status 100ms 上报时，这里会非常频繁：不要打印完整 status
                    log.debug("推送车辆状态到 WebSocket: sessionId={}, vehicleId={}, timestamp={}",
                        sessionId, vehicleId, status.get("timestamp"));
                    long queuedAt = System.currentTimeMillis();
                    long queuedAtNanos = System.nanoTime();
                    status.put("websocketQueuedAt", queuedAt);
                    status.put(INTERNAL_WEBSOCKET_QUEUED_NANOS, queuedAtNanos);
                    long eventbusReceivedAtNanos = numberValue(
                        status.get(INTERNAL_EVENTBUS_RECEIVED_NANOS),
                        queuedAtNanos
                    );
                    long eventbusToQueueDurationMs =
                        monotonicDurationMs(eventbusReceivedAtNanos, queuedAtNanos);
                    status.put(EVENTBUS_TO_QUEUE_DURATION_MS, eventbusToQueueDurationMs);
                    metrics.recordStatusQueued(
                        "vehicle",
                        numberValue(status.get("eventbusReceivedAt"), queuedAt),
                        queuedAt,
                        eventbusToQueueDurationMs
                    );
                    Sinks.EmitResult result = sessionInfo.addStatus(status);
                    if (result.isFailure()) {
                        metrics.recordStatusSinkFailure(result.name());
                    }
                })
                .doOnError(error -> {
                    metrics.recordStatusSubscriptionError("vehicle");
                    log.error("WebSocket 订阅车辆状态失败: sessionId={}, vehicleId={}",
                        sessionId, vehicleId, error);
                })
                .subscribe();
        }
        
        // 如果提供了驾驶舱ID，订阅该驾驶舱绑定的车辆状态
        if (cockpitId != null) {
            return relationService.getSessionByCockpit(cockpitId)
                .flatMapMany(sessionEntity -> {
                    String boundVehicleId = sessionEntity.getVehicleDeviceId();
                    if (boundVehicleId == null) {
                        return Flux.empty();
                    }
                    
                    Subscription subscription = Subscription
                        .builder()
                        .subscriberId("parallel-driving-websocket-" + sessionId)
                        .topics("/device/*/" + boundVehicleId + "/message/property/report")
                        .features(Subscription.Feature.local, Subscription.Feature.broker)
                        .build();
                    
                    return eventBus.subscribe(subscription, DeviceMessage.class)
                        .ofType(ReportPropertyMessage.class)
                        .map(message -> {
                            long eventBusReceivedAt = System.currentTimeMillis();
                            long eventBusReceivedAtNanos = System.nanoTime();
                            metrics.recordStatusMessageReceived(
                                "cockpit",
                                message.getTimestamp(),
                                eventBusReceivedAt
                            );
                            Map<String, Object> status = new java.util.HashMap<>();
                            status.put("type", "vehicle-status");
                            status.put("deviceId", message.getDeviceId());
                            status.put("properties", message.getProperties());
                            status.put("timestamp", message.getTimestamp());
                            status.put("cockpitId", cockpitId);
                            status.put("eventbusReceivedAt", eventBusReceivedAt);
                            status.put(INTERNAL_EVENTBUS_RECEIVED_NANOS, eventBusReceivedAtNanos);
                            status.put("_observationSource", "cockpit");
                            return status;
                        })
                        .doOnNext(status -> {
                            log.debug("推送车辆状态到 WebSocket: sessionId={}, cockpitId={}, vehicleId={}, timestamp={}",
                                sessionId, cockpitId, boundVehicleId, status.get("timestamp"));
                            long queuedAt = System.currentTimeMillis();
                            long queuedAtNanos = System.nanoTime();
                            status.put("websocketQueuedAt", queuedAt);
                            status.put(INTERNAL_WEBSOCKET_QUEUED_NANOS, queuedAtNanos);
                            long eventbusReceivedAtNanos = numberValue(
                                status.get(INTERNAL_EVENTBUS_RECEIVED_NANOS),
                                queuedAtNanos
                            );
                            long eventbusToQueueDurationMs =
                                monotonicDurationMs(eventbusReceivedAtNanos, queuedAtNanos);
                            status.put(EVENTBUS_TO_QUEUE_DURATION_MS, eventbusToQueueDurationMs);
                            metrics.recordStatusQueued(
                                "cockpit",
                                numberValue(status.get("eventbusReceivedAt"), queuedAt),
                                queuedAt,
                                eventbusToQueueDurationMs
                            );
                            Sinks.EmitResult result = sessionInfo.addStatus(status);
                            if (result.isFailure()) {
                                metrics.recordStatusSinkFailure(result.name());
                            }
                        })
                        .doOnError(error -> {
                            metrics.recordStatusSubscriptionError("cockpit");
                            log.error("WebSocket 订阅车辆状态失败: sessionId={}, cockpitId={}",
                                sessionId, cockpitId, error);
                        });
                })
                .subscribe();
        }
        
        return null;
    }
    
    /**
     * 从查询字符串中提取参数值
     */
    private String extractQueryParam(String query, String paramName) {
        if (query == null || query.isEmpty()) {
            return null;
        }
        String[] params = query.split("&");
        for (String param : params) {
            if (param.startsWith(paramName + "=")) {
                return param.substring(paramName.length() + 1);
            }
        }
        return null;
    }

    private static long numberValue(Object value, long fallback) {
        return value instanceof Number ? ((Number) value).longValue() : fallback;
    }

    private static long monotonicDurationMs(long startNanos, long endNanos) {
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos));
    }
    
    /**
     * WebSocket 会话信息
     */
    @SuppressWarnings("unused")
    private static class WebSocketSessionInfo {
        @SuppressWarnings("unused")
        private final String sessionId;
        @SuppressWarnings("unused")
        private final String vehicleId;
        @SuppressWarnings("unused")
        private final String cockpitId;
        private final Sinks.Many<Map<String, Object>> statusSink;
        private final Flux<Map<String, Object>> statusFlux;
        private final Map<String, Object> latestProperties = new HashMap<>();
        
        public WebSocketSessionInfo(String sessionId, String vehicleId, String cockpitId) {
            this.sessionId = sessionId;
            this.vehicleId = vehicleId;
            this.cockpitId = cockpitId;
            // 对 100ms 高频状态：只保留最新，避免无界 buffer 占用内存
            this.statusSink = Sinks.many().replay().latest();
            // 降频推送：默认 200ms 推一次最新状态（前端更稳，网络更省）
            this.statusFlux = statusSink.asFlux().sample(Duration.ofMillis(200));
        }
        
        public synchronized Sinks.EmitResult addStatus(Map<String, Object> status) {
            Object properties = status.get("properties");
            if (properties instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propertyMap = (Map<String, Object>) properties;
                latestProperties.putAll(propertyMap);
            }

            Map<String, Object> snapshot = new HashMap<>(status);
            snapshot.put("properties", new HashMap<>(latestProperties));
            Sinks.EmitResult result = statusSink.tryEmitNext(snapshot);
            if (result.isFailure()) {
                log.debug("WebSocket 状态入队失败: sessionId={}, result={}", sessionId, result);
            }
            return result;
        }
        
        public Flux<Map<String, Object>> getStatusFlux() {
            return statusFlux;
        }
    }
}
