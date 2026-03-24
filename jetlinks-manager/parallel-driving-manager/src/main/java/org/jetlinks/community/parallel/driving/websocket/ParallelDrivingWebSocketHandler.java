package org.jetlinks.community.parallel.driving.websocket;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    
    private final ParallelDrivingRelationService relationService;
    private final EventBus eventBus;

    @Autowired
    public ParallelDrivingWebSocketHandler(ParallelDrivingRelationService relationService,
                                          @Qualifier("eventBus") EventBus eventBus) {
        this.relationService = relationService;
        this.eventBus = eventBus;
    }
    
    // 存储 WebSocket 连接（key: sessionId, value: WebSocketSessionInfo）
    private final Map<String, WebSocketSessionInfo> sessions = new ConcurrentHashMap<>();
    
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String sessionId = session.getId();
        log.info("WebSocket 连接建立: sessionId={}", sessionId);
        
        // 从查询参数中获取车辆设备ID或驾驶舱设备ID
        String vehicleId = session.getHandshakeInfo().getUri().getQuery().contains("vehicleId=")
            ? extractQueryParam(session.getHandshakeInfo().getUri().getQuery(), "vehicleId")
            : null;
        String cockpitId = session.getHandshakeInfo().getUri().getQuery().contains("cockpitId=")
            ? extractQueryParam(session.getHandshakeInfo().getUri().getQuery(), "cockpitId")
            : null;
        
        if (vehicleId == null && cockpitId == null) {
            log.warn("WebSocket 连接缺少设备ID参数: sessionId={}", sessionId);
            return session.close();
        }
        
        // 创建会话信息
        WebSocketSessionInfo sessionInfo = new WebSocketSessionInfo(sessionId, vehicleId, cockpitId);
        sessions.put(sessionId, sessionInfo);
        
        // 订阅车辆状态上报
        Disposable subscription = subscribeVehicleStatus(vehicleId, cockpitId, session);
        
        // 订阅车辆状态并推送到 WebSocket
        Flux<WebSocketMessage> statusMessages = sessionInfo.getStatusFlux()
            .map(status -> session.textMessage(JSON.toJSONString(status)))
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
                sessions.remove(sessionId);
                if (subscription != null) {
                    subscription.dispose();
                }
            })
            .subscribe();
        
        // 发送状态消息并处理客户端消息
        return Mono.zip(
            session.send(statusMessages),
            receiveHandler
        ).then();
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
                .broker()
                .local()
                .build();
            
            return eventBus.subscribe(subscription, DeviceMessage.class)
                .cast(ReportPropertyMessage.class)
                .map(message -> {
                    Map<String, Object> status = new java.util.HashMap<>();
                    status.put("type", "vehicle-status");
                    status.put("deviceId", message.getDeviceId());
                    status.put("properties", message.getProperties());
                    status.put("timestamp", message.getTimestamp());
                    return status;
                })
                .doOnNext(status -> {
                    // chassis_status 100ms 上报时，这里会非常频繁：不要打印完整 status
                    log.debug("推送车辆状态到 WebSocket: sessionId={}, vehicleId={}, timestamp={}",
                        sessionId, vehicleId, status.get("timestamp"));
                    sessionInfo.addStatus(status);
                })
                .doOnError(error -> log.error("WebSocket 订阅车辆状态失败: sessionId={}, vehicleId={}", 
                    sessionId, vehicleId, error))
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
                        .broker()
                        .local()
                        .build();
                    
                    return eventBus.subscribe(subscription, DeviceMessage.class)
                        .cast(ReportPropertyMessage.class)
                        .map(message -> {
                            Map<String, Object> status = new java.util.HashMap<>();
                            status.put("type", "vehicle-status");
                            status.put("deviceId", message.getDeviceId());
                            status.put("properties", message.getProperties());
                            status.put("timestamp", message.getTimestamp());
                            status.put("cockpitId", cockpitId);
                            return status;
                        })
                        .doOnNext(status -> {
                            log.debug("推送车辆状态到 WebSocket: sessionId={}, cockpitId={}, vehicleId={}, timestamp={}",
                                sessionId, cockpitId, boundVehicleId, status.get("timestamp"));
                            sessionInfo.addStatus(status);
                        })
                        .doOnError(error -> log.error("WebSocket 订阅车辆状态失败: sessionId={}, cockpitId={}", 
                            sessionId, cockpitId, error));
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
        private final reactor.core.publisher.Sinks.Many<Map<String, Object>> statusSink;
        private final Flux<Map<String, Object>> statusFlux;
        
        public WebSocketSessionInfo(String sessionId, String vehicleId, String cockpitId) {
            this.sessionId = sessionId;
            this.vehicleId = vehicleId;
            this.cockpitId = cockpitId;
            // 对 100ms 高频状态：只保留最新，避免无界 buffer 占用内存
            this.statusSink = reactor.core.publisher.Sinks.many().multicast().directBestEffort();
            // 降频推送：默认 200ms 推一次最新状态（前端更稳，网络更省）
            this.statusFlux = statusSink.asFlux().sample(Duration.ofMillis(200));
        }
        
        public void addStatus(Map<String, Object> status) {
            statusSink.tryEmitNext(status);
        }
        
        public Flux<Map<String, Object>> getStatusFlux() {
            return statusFlux;
        }
    }
}
