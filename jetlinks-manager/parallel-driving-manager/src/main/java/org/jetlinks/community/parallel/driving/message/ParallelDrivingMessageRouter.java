package org.jetlinks.community.parallel.driving.message;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.exception.AccessDenyException;
import org.jetlinks.community.parallel.driving.configuration.ParallelDrivingVehicleToCockpitProperties;
import org.jetlinks.community.parallel.driving.metrics.ParallelDrivingLatencyMetrics;
import org.jetlinks.community.parallel.driving.room.ParallelDrivingRoomManager;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingRelationService;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.trace.DeviceTracer;
import org.jetlinks.core.trace.MonoTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ParallelDrivingMessageRouter {

    private final ParallelDrivingRelationService relationService;
    private final ParallelDrivingRoomManager roomManager;
    private final EventBus eventBus;
    private final ParallelDrivingCustomMessageHandler customMessageHandler;
    private final ParallelDrivingVehicleToCockpitProperties vehicleToCockpitProperties;
    private final ParallelDrivingLatencyMetrics latencyMetrics;

    /** Message dedup cache: messageId → timestamp. Prevents duplicate processing in cluster mode. */
    private final ConcurrentHashMap<String, Long> processedMessages = new ConcurrentHashMap<>();
    private static final long DEDUP_TTL_MS = 5000;
    private static final int DEDUP_MAX_SIZE = 10000;

    /**
     * Small bounded scheduler for the remotejoystick adapter path. The room
     * mailbox is latest-only, so a large queue would preserve stale commands
     * and turn overload into delayed bursts.
     */
    private static final Scheduler REMOTE_JOYSTICK_SCHEDULER = Schedulers.newBoundedElastic(
        4, 256, "remotejoystick", 60);

    /** remotejoystick 重复帧去重开关（parallel-driving.control.dedup，默认 false） */
    @Value("${parallel-driving.control.dedup:false}")
    private boolean remoteJoystickDedupEnabled = false;

    /** remotejoystick 重复样本去重：(cockpitId, 源timestamp, 控制值指纹) → 命中时间 */
    private final ConcurrentHashMap<String, Long> processedJoystickSamples = new ConcurrentHashMap<>();
    private static final long JOYSTICK_DEDUP_TTL_MS = 2000;
    private static final int JOYSTICK_DEDUP_MAX_SIZE = 10000;

    @Autowired
    public ParallelDrivingMessageRouter(ParallelDrivingRelationService relationService,
                                        ParallelDrivingRoomManager roomManager,
                                        @Qualifier("eventBus") EventBus eventBus,
                                        ParallelDrivingCustomMessageHandler customMessageHandler,
                                        ParallelDrivingVehicleToCockpitProperties vehicleToCockpitProperties,
                                        @Autowired(required = false) ParallelDrivingLatencyMetrics latencyMetrics) {
        this.relationService = relationService;
        this.roomManager = roomManager;
        this.eventBus = eventBus;
        this.customMessageHandler = customMessageHandler;
        this.vehicleToCockpitProperties = vehicleToCockpitProperties;
        this.latencyMetrics = latencyMetrics;
    }

    private Disposable disposable;

    /**
     * 初始化消息订阅
     */
    @PostConstruct
    public void init() {
        // 订阅驾驶舱发送的控制指令
        // 注意：驾驶舱通过 TCP 发送的 INVOKE_FUNCTION 是上行消息，topic 格式为 /device/{productId}/{deviceId}/message/send/function
        // 平台可能发布到 /device/... 或 /org/{orgId}/device/...，需同时订阅两种格式
        Subscription cockpitSubscription = Subscription
            .builder()
            .subscriberId("parallel-driving-cockpit-router")
            .topics(
                "/device/parallel-driving-joystick/*/message/send/function",  // 手柄驾驶舱
                "/device/parallel-driving-cockpit/*/message/send/function",   // 标准驾驶舱
                "/org/*/device/parallel-driving-joystick/*/message/send/function",  // 带 org 前缀
                "/org/*/device/parallel-driving-cockpit/*/message/send/function"
            )
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        // 订阅车端上报的状态和回复
        // 车端上报的状态 topic 格式为 /device/{productId}/{deviceId}/message/property/report
        // 车端回复的 FUNCTION_REPLY topic 格式为 /device/{productId}/{deviceId}/message/function/reply
        Subscription vehicleSubscription = Subscription
            .builder()
            .subscriberId("parallel-driving-vehicle-router")
            .topics(
                "/device/parallel-driving-vehicle/*/message/property/report",      // 车辆属性上报
                "/device/parallel-driving-vehicle/*/message/function/reply",        // 车辆功能回复
                "/device/parallel-driving-vehicle/*/message/send/function",        // 车端上行功能调用（如 cloudLinkPing RTT）
                "/device/parallel-driving-product/*/message/property/report",      // 兼容旧产品ID
                "/device/parallel-driving-product/*/message/function/reply",        // 兼容旧产品ID
                "/device/parallel-driving-product/*/message/send/function",
                "/org/*/device/parallel-driving-vehicle/*/message/send/function",
                "/org/*/device/parallel-driving-product/*/message/send/function"
            )
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        // 拆分 cockpit 与 vehicle 流，避免互相抢占；cockpit 使用更高并发与 prefetch，车端 noReply 后主要瓶颈在驾驶舱
        disposable = Flux.merge(
            eventBus.subscribe(cockpitSubscription, DeviceMessage.class)
                .doOnNext(msg -> {
                    boolean isRemoteJoystick = msg instanceof FunctionInvokeMessage
                        && "remotejoystick".equals(((FunctionInvokeMessage) msg).getFunctionId());
                    if (isRemoteJoystick) {
                        log.debug("[驾驶舱->云端] ParallelDrivingMessageRouter 收到驾驶舱消息: deviceId={}, messageType={}, messageId={}",
                            msg.getDeviceId(), msg.getMessageType(), msg.getMessageId());
                    } else {
                        log.info("[驾驶舱->云端] ParallelDrivingMessageRouter 收到驾驶舱消息: deviceId={}, messageType={}, messageId={}",
                            msg.getDeviceId(), msg.getMessageType(), msg.getMessageId());
                    }
                })
                .publishOn(Schedulers.parallel(), 64)
                .flatMap(msg -> {
                    boolean isRemoteJoystick = msg instanceof FunctionInvokeMessage
                        && "remotejoystick".equals(((FunctionInvokeMessage) msg).getFunctionId());
                    boolean isEmergencyStop = msg instanceof FunctionInvokeMessage
                        && "emergencystop".equals(((FunctionInvokeMessage) msg).getFunctionId());
                    if (isRemoteJoystick) {
                        if (remoteJoystickDedupEnabled && isDuplicateRemoteJoystick((FunctionInvokeMessage) msg)) {
                            log.debug("[驾驶舱->云端] 跳过重复 remotejoystick 帧: deviceId={}, timestamp={}",
                                msg.getDeviceId(), getStringInputOrHeader((FunctionInvokeMessage) msg, "timestamp"));
                            return Mono.empty();
                        }
                        // remotejoystick 专用高并发流：独立调度器，避免与协议层 boundedElastic 竞争
                        return customMessageHandler.handleCustomMessage(msg)
                            .subscribeOn(REMOTE_JOYSTICK_SCHEDULER)
                            .onErrorResume(e -> {
                                log.warn("[驾驶舱->云端] remotejoystick 处理失败: deviceId={}, error={}", msg.getDeviceId(), e.getMessage());
                                return Mono.empty();
                            });
                    }
                    if (isDuplicate(msg.getMessageId())) {
                        log.debug("[驾驶舱->云端] 跳过重复消息: deviceId={}, messageId={}", msg.getDeviceId(), msg.getMessageId());
                        return Mono.empty();
                    }
                    if (isEmergencyStop) {
                        return customMessageHandler.handleCustomMessage(msg)
                            .subscribeOn(Schedulers.parallel());
                    }
                    return customMessageHandler.handleCustomMessage(msg)
                        .then(Mono.defer(() -> {
                            log.debug("[驾驶舱->云端] ParallelDrivingMessageRouter 自定义消息处理完成，继续处理标准消息: deviceId={}", msg.getDeviceId());
                            return handleCockpitControlMessage(msg);
                        }))
                        .onErrorResume(error -> {
                            log.warn("[驾驶舱->云端] ParallelDrivingMessageRouter 自定义消息处理失败，继续处理标准消息: deviceId={}, error={}", 
                                msg.getDeviceId(), error.getMessage());
                            return handleCockpitControlMessage(msg);
                        });
                }, 32, 32)
                .onErrorContinue((error, obj) -> log.error("[驾驶舱->云端] 处理驾驶舱消息失败", error)),
            eventBus.subscribe(vehicleSubscription, DeviceMessage.class)
                .doOnNext(msg -> {
                    // 详细记录收到的车端消息
                    boolean isCustomProtocol = msg.getHeader("customProtocol")
                        .map(v -> Boolean.parseBoolean(String.valueOf(v)))
                        .orElse(false);
                    String messageTypeHeader = msg.getHeader("messageType")
                        .map(String::valueOf)
                        .orElse("null");
                    log.debug("[车辆->云端] ParallelDrivingMessageRouter 收到车端消息: deviceId={}, messageType={}, messageId={}, isCustomProtocol={}, messageTypeHeader={}", 
                        msg.getDeviceId(), msg.getMessageType(), msg.getMessageId(), isCustomProtocol, messageTypeHeader);
                    
                    // 如果是 FunctionInvokeMessageReply，记录详细信息
                    if (msg instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply) {
                        org.jetlinks.core.message.function.FunctionInvokeMessageReply reply = 
                            (org.jetlinks.core.message.function.FunctionInvokeMessageReply) msg;
                        log.debug("[车辆->云端] ParallelDrivingMessageRouter 收到车端 FunctionInvokeMessageReply: functionId={}, success={}, requestId={}", 
                            reply.getFunctionId(), reply.isSuccess(), 
                            reply.getHeader("requestMessageId").or(() -> reply.getHeader("requestId")).map(String::valueOf).orElse("null"));
                    }
                })
                .flatMap(msg -> {
                    // 方向过滤：带 sourceType=cockpit 头的是驾驶舱→车辆下行转发帧，
                    // 不应作为车辆上行处理，跳过以避免下行控制帧自消费触发"未找到激活房间"。
                    if ("cockpit".equals(msg.getHeader("sourceType").map(String::valueOf).orElse(null))) {
                        log.debug("[车辆->云端] 跳过下行转发帧（方向过滤）: deviceId={}, messageId={}",
                            msg.getDeviceId(), msg.getMessageId());
                        return Mono.<Void>empty();
                    }
                    // 车云 RTT：车端上行 INVOKE_FUNCTION cloudLinkPing，平台立即回 FunctionInvokeMessageReply（与 MQTT/TCP 业务链路一致）
                    if (msg instanceof FunctionInvokeMessage) {
                        FunctionInvokeMessage inv = (FunctionInvokeMessage) msg;
                        if ("cloudLinkPing".equals(inv.getFunctionId())) {
                            long serverReceiveTimeMs = System.currentTimeMillis();
                            long serverReceiveTimeNanos = System.nanoTime();
                            Object clientTs = inv.getInput("clientSendTimeMs");
                            log.info("[cloudLinkPing] recv_ping deviceId={} requestMessageId={} clientSendTimeMs={} serverReceiveTimeMs={}",
                                inv.getDeviceId(), inv.getMessageId(), clientTs, serverReceiveTimeMs);
                            return customMessageHandler.replyToCloudLinkPing(
                                inv, serverReceiveTimeMs, serverReceiveTimeNanos);
                        }
                    }
                    if (isDuplicate(msg.getMessageId())) {
                        log.debug("[车辆->云端] 跳过重复消息: deviceId={}, messageId={}", msg.getDeviceId(), msg.getMessageId());
                        return Mono.<Void>empty();
                    }
                    boolean isCustomProtocol = msg.getHeader("customProtocol")
                        .map(v -> Boolean.parseBoolean(String.valueOf(v)))
                        .orElse(false);
                    
                    log.debug("[车辆->云端] ParallelDrivingMessageRouter 处理车端消息: deviceId={}, messageType={}, isCustomProtocol={}, messageId={}", 
                        msg.getDeviceId(), msg.getMessageType(), isCustomProtocol, msg.getMessageId());
                    
                    if (isCustomProtocol) {
                        // 1. 如果是自定义消息，只处理自定义消息，不处理标准消息（避免重复转发）
                        return customMessageHandler.handleCustomMessage(msg)
                            .timeout(java.time.Duration.ofSeconds(5)) // 5秒超时
                            .onErrorResume(error -> {
                                log.warn("[车辆->云端] 自定义消息处理失败或超时: deviceId={}, error={}", 
                                    msg.getDeviceId(), error.getMessage());
                                return Mono.<Void>empty(); // 忽略错误，避免影响其他消息处理
                            });
                    } else {
                        // 2. 如果不是自定义消息，检查是否是标准的 FunctionInvokeMessageReply（控制指令回包）
                        // 需要转发给驾驶舱，让驾驶舱知道控制指令的执行结果
                        if (msg instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply) {
                            org.jetlinks.core.message.function.FunctionInvokeMessageReply reply = 
                                (org.jetlinks.core.message.function.FunctionInvokeMessageReply) msg;
                            log.debug("[车辆->云端] ParallelDrivingMessageRouter 收到车端功能调用回复: deviceId={}, functionId={}, success={}, messageId={}", 
                                reply.getDeviceId(), reply.getFunctionId(), reply.isSuccess(), reply.getMessageId());
                            return handleVehicleFunctionReply(reply);
                        }
                        // 3. 车辆状态上报（ReportPropertyMessage）不应该转发给驾驶舱，只用于 WebSocket 推送到前端
                        log.debug("[车辆->云端] ParallelDrivingMessageRouter 忽略车端消息（非功能调用回复）: deviceId={}, messageType={}", 
                            msg.getDeviceId(), msg.getMessageType());
                        return Mono.<Void>empty();
                    }
                })
                .onErrorContinue((error, obj) -> log.error("[车辆->云端] 处理车端消息失败", error))
        ).subscribe();

        log.info("ParallelDrivingMessageRouter 已初始化订阅: cockpit=[/device/parallel-driving-joystick|cockpit/*/message/send/function], vehicle=[/device/parallel-driving-vehicle|product/*/message/...]");
    }

    /**
     * remotejoystick 重复帧去重：同一驾驶舱 + 同一源时间戳 + 同一控制值指纹 → 同一样本重复发送。
     * 驾驶舱 seq 递增上线前,用(源 timestamp + 控制值)丢弃完全重复帧。
     */
    private boolean isDuplicateRemoteJoystick(FunctionInvokeMessage msg) {
        String cockpitId = msg.getDeviceId();
        String timestamp = getStringInputOrHeader(msg, "timestamp");
        if (cockpitId == null || timestamp == null || timestamp.isEmpty()) {
            return false; // 缺关键字段，不去重（安全兜底）
        }
        String fingerprint = JSON.toJSONString(msg.getInput("joystickdata"));
        String key = cockpitId + "|" + timestamp + "|" + fingerprint;
        long now = System.currentTimeMillis();
        Long prev = processedJoystickSamples.putIfAbsent(key, now);
        if (prev != null && now - prev < JOYSTICK_DEDUP_TTL_MS) {
            if (latencyMetrics != null) {
                latencyMetrics.recordRemoteJoystickDedupDropped(cockpitId);
            }
            return true;
        }
        if (prev != null) {
            processedJoystickSamples.put(key, now);
        }
        if (processedJoystickSamples.size() > JOYSTICK_DEDUP_MAX_SIZE) {
            long cutoff = now - JOYSTICK_DEDUP_TTL_MS;
            processedJoystickSamples.entrySet().removeIf(e -> e.getValue() < cutoff);
        }
        return false;
    }

    private String getStringInputOrHeader(FunctionInvokeMessage msg, String key) {
        Object v = msg.getInput(key);
        if (v == null) {
            v = msg.getHeader(key).map(String::valueOf).orElse(null);
        }
        return v == null ? null : String.valueOf(v);
    }

    @PreDestroy
    public void destroy() {
        if (disposable != null) {
            disposable.dispose();
        }
        processedMessages.clear();
        processedJoystickSamples.clear();
    }

    private boolean isDuplicate(String messageId) {
        if (messageId == null || messageId.isEmpty()) {
            return false;
        }
        long now = System.currentTimeMillis();
        Long prev = processedMessages.putIfAbsent(messageId, now);
        if (prev != null && now - prev < DEDUP_TTL_MS) {
            return true;
        }
        if (prev != null) {
            processedMessages.put(messageId, now);
        }
        if (processedMessages.size() > DEDUP_MAX_SIZE) {
            long cutoff = now - DEDUP_TTL_MS;
            processedMessages.entrySet().removeIf(e -> e.getValue() < cutoff);
        }
        return false;
    }

    /**
     * 处理驾驶舱发送的控制指令
     * 通过消息头中的 targetDeviceId 判断是否是并行驾驶控制消息
     */
    public Mono<Void> handleCockpitControlMessage(DeviceMessage message) {
        String cockpitDeviceId = message.getDeviceId();
        
        // 检查是否是并行驾驶控制消息（通过消息头判断）
        String targetVehicleId = message.getHeader("targetDeviceId")
            .map(String::valueOf)
            .orElse(null);

        // 兼容：TCP 驾驶舱直接发送 INVOKE_FUNCTION 时，通常不会带 targetDeviceId header。
        // 此时尝试从 FunctionInvokeMessage.inputs 中提取目标车辆（优先 vin，其次 vehicleDeviceId/targetDeviceId）。
        if (!StringUtils.hasText(targetVehicleId) && message instanceof FunctionInvokeMessage) {
            FunctionInvokeMessage invoke = (FunctionInvokeMessage) message;
            Object vinObj = invoke.getInput("vin");
            if (vinObj == null) {
                vinObj = invoke.getInput("vehicleDeviceId");
            }
            if (vinObj == null) {
                vinObj = invoke.getInput("targetDeviceId");
            }
            if (vinObj != null) {
                // 处理 Optional 对象：如果 getInput 返回的是 Optional，需要先解包
                String vinStr = null;
                if (vinObj instanceof java.util.Optional) {
                    @SuppressWarnings("unchecked")
                    java.util.Optional<Object> opt = (java.util.Optional<Object>) vinObj;
                    if (opt.isPresent()) {
                        vinStr = String.valueOf(opt.get());
                    }
                } else {
                    vinStr = String.valueOf(vinObj);
                }
                
                if (StringUtils.hasText(vinStr)) {
                    targetVehicleId = vinStr;
                    // 给后续逻辑补上 header，确保 room/自定义处理都能统一识别
                    invoke.addHeaderIfAbsent("targetDeviceId", targetVehicleId);
                }
            }
        }

        // 如果仍然没有 targetVehicleId，说明不是并行驾驶控制消息，直接返回
        if (!StringUtils.hasText(targetVehicleId)) {
            return Mono.empty();
        }
        final String finalTargetVehicleId = targetVehicleId;

        log.info("[驾驶舱->云端] 收到驾驶舱控制指令: cockpit={}, targetVehicle={}", cockpitDeviceId, finalTargetVehicleId);

        // 1. 验证权限（优先检查活跃会话，如果没有则检查绑定关系）
        // 对于紧急指令（如 emergencystop），允许有绑定关系就可以发送
        Mono<Boolean> hasPermission = relationService.checkControlPermission(cockpitDeviceId, finalTargetVehicleId)
            .flatMap(hasActiveSession -> {
                if (hasActiveSession) {
                    return Mono.just(true);
                }
                // 如果没有活跃会话，检查是否有绑定关系（授权关系）
                return relationService.checkBindingPermission(cockpitDeviceId, finalTargetVehicleId)
                    .doOnNext(hasBinding -> {
                        if (hasBinding) {
                            log.debug("[驾驶舱->云端] 驾驶舱[{}]没有活跃会话，但存在绑定关系，允许发送控制指令: vehicle={}", 
                                cockpitDeviceId, finalTargetVehicleId);
                        }
                    });
            });

        Mono<Void> operation = hasPermission
            .filter(Boolean::booleanValue)
            .switchIfEmpty(Mono.error(new AccessDenyException("驾驶舱[" + cockpitDeviceId + "]无权限控制车端[" + finalTargetVehicleId + "]（既无活跃会话也无绑定关系）")))
            // 2. 获取或创建房间
            .then(roomManager.getRoom(cockpitDeviceId, finalTargetVehicleId)
                .switchIfEmpty(
                    // 如果没有房间，尝试创建（对于紧急指令，允许自动创建房间）
                    Mono.defer(() -> {
                        log.info("[驾驶舱->云端->车辆] 房间不存在，尝试自动创建: cockpit={}, vehicle={}", cockpitDeviceId, finalTargetVehicleId);
                        return roomManager.createRoom(cockpitDeviceId, finalTargetVehicleId)
                            .doOnNext(room -> log.info("[驾驶舱->云端->车辆] 自动创建房间成功: roomId={}", room.getRoomId()))
                            .doOnError(error -> log.warn("自动创建房间失败: cockpit={}, vehicle={}, error={}", 
                                cockpitDeviceId, finalTargetVehicleId, error.getMessage()));
                    })
                )
            )
            // 3. 通过房间转发消息
            .flatMap(room -> {
                // 更新最后活动时间
                relationService.updateLastActiveTime(cockpitDeviceId, finalTargetVehicleId)
                    .subscribe();  // 异步更新，不阻塞消息转发
                return room.forwardCockpitToVehicle(message);
            })
            .doOnSuccess(v -> log.info("[驾驶舱->云端->车辆] 驾驶舱控制指令路由成功: cockpit={}, vehicle={}", cockpitDeviceId, finalTargetVehicleId))
            .doOnError(error -> log.error("[驾驶舱->云端->车辆] 驾驶舱控制指令路由失败: cockpit={}, vehicle={}", cockpitDeviceId, finalTargetVehicleId, error))
            .onErrorResume(error -> {
                log.error("[驾驶舱->云端->车辆] 处理驾驶舱控制指令失败", error);
                return Mono.empty(); // 忽略错误，避免影响其他消息处理
            });
        return traceMessage(
            operation,
            message,
            "/parallel-driving/control/message",
            "cockpit-to-vehicle",
            finalTargetVehicleId,
            functionId(message));
    }

    /**
     * 处理车端上报的状态（已废弃）
     * 
     * 注意：车辆状态上报（ReportPropertyMessage）不应该转发给驾驶舱设备
     * 状态上报只用于：
     * 1. WebSocket 推送到前端（ParallelDrivingWebSocketHandler）
     * 2. 平台数据存储和查询
     * 
     * 只有自定义消息响应（如 emergencystopresp）才需要转发给驾驶舱
     * 自定义消息响应由 ParallelDrivingCustomMessageHandler 处理
     * 
     * @deprecated 车辆状态上报不应该转发给驾驶舱，此方法已废弃
     */
    @Deprecated
    public Mono<Void> handleVehicleStatusMessage(DeviceMessage message) {
        // 不再转发车辆状态上报给驾驶舱
        // 状态上报只用于 WebSocket 推送到前端
        log.debug("收到车端消息（不转发状态上报）: vehicle={}, messageType={}", 
            message.getDeviceId(), message.getMessageType());
        return Mono.empty();
    }

    /**
     * 处理车端返回的功能调用回复（FunctionInvokeMessageReply）
     * 将控制指令的执行结果转发给驾驶舱
     * 
     * @param reply 功能调用回复消息
     * @return Mono<Void>
     */
    public Mono<Void> handleVehicleFunctionReply(org.jetlinks.core.message.function.FunctionInvokeMessageReply reply) {
        String vehicleDeviceId = reply.getDeviceId();
        String functionId = reply.getFunctionId();

        log.info("[车辆->云端] 收到车端功能调用回复: vehicle={}, functionId={}, success={}, messageId={}",
            vehicleDeviceId, functionId, reply.isSuccess(), reply.getMessageId());

        if (!vehicleToCockpitProperties.shouldForwardStandardFunctionReplyToCockpit(functionId)) {
            log.debug("[车辆->云端->驾驶舱] 跳过转发车端标准功能回复(不在白名单 parallel-driving.vehicle-to-cockpit.forward-reply-function-ids): vehicle={}, functionId={}, messageId={}",
                vehicleDeviceId, functionId, reply.getMessageId());
            if (latencyMetrics != null) {
                latencyMetrics.recordVehicleReplyCockpitForwardSkipped(functionId);
            }
            return Mono.empty();
        }

        // 1. 获取房间（通过车辆ID）
        Mono<Void> operation = roomManager.getRoomByVehicle(vehicleDeviceId)
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("[车辆->云端->驾驶舱] 车端功能调用回复：房间不存在，无法转发给驾驶舱: vehicle={}, functionId={}, messageId={}", 
                    vehicleDeviceId, reply.getFunctionId(), reply.getMessageId());
                return Mono.empty();  // 如果没有房间，忽略消息
            }))
            // 2. 通过房间转发消息并更新最后活动时间
            .flatMap(room -> {
                log.info("[车辆->云端->驾驶舱] 车端功能调用回复：找到房间，准备转发给驾驶舱: vehicle={}, cockpit={}, functionId={}, messageId={}", 
                    vehicleDeviceId, room.getCockpitDeviceId(), reply.getFunctionId(), reply.getMessageId());
                // 异步更新最后活动时间，不阻塞消息转发
                relationService.updateLastActiveTime(room.getCockpitDeviceId(), vehicleDeviceId)
                    .subscribe();
                return room.forwardVehicleToCockpit(reply);
            })
            .doOnSuccess(v -> log.info("[车辆->云端->驾驶舱] 车端功能调用回复转发成功: vehicle={}, functionId={}, messageId={}", 
                vehicleDeviceId, reply.getFunctionId(), reply.getMessageId()))
            .doOnError(error -> log.error("[车辆->云端->驾驶舱] 车端功能调用回复转发失败: vehicle={}, functionId={}, messageId={}", 
                vehicleDeviceId, reply.getFunctionId(), reply.getMessageId(), error))
            .onErrorResume(error -> {
                log.error("[车辆->云端->驾驶舱] 处理车端功能调用回复失败: vehicle={}, functionId={}, messageId={}", 
                    vehicleDeviceId, reply.getFunctionId(), reply.getMessageId(), error);
                return Mono.empty(); // 忽略错误，避免影响其他消息处理
            });
        return traceMessage(
            operation,
            reply,
            "/parallel-driving/reply/vehicle-to-cockpit",
            "vehicle-to-cockpit",
            vehicleDeviceId,
            functionId(reply));
    }

    private String functionId(DeviceMessage message) {
        if (message instanceof FunctionInvokeMessage) {
            return ((FunctionInvokeMessage) message).getFunctionId();
        }
        if (message instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply) {
            return ((org.jetlinks.core.message.function.FunctionInvokeMessageReply) message).getFunctionId();
        }
        return message.getHeader("functionId").map(String::valueOf).orElse(null);
    }

    private <T> Mono<T> traceMessage(Mono<T> operation,
                                     DeviceMessage message,
                                     String spanName,
                                     String direction,
                                     String vehicleDeviceId,
                                     String functionId) {
        Mono<T> traced = operation.as(MonoTracer.create(spanName, builder -> {
            builder
                .setAttribute("parallel.driving.direction", direction)
                .setAttribute("parallel.driving.cockpit.id",
                    message.getHeader("sourceDeviceId")
                        .map(String::valueOf)
                        .orElseGet(() -> "cockpit-to-vehicle".equals(direction)
                            ? message.getDeviceId()
                            : ""));
            if (message.getMessageId() != null && !message.getMessageId().isEmpty()) {
                builder.setAttribute("messaging.message.id", message.getMessageId());
            }
            if (vehicleDeviceId != null && !vehicleDeviceId.isEmpty()) {
                builder.setAttribute("parallel.driving.vehicle.id", vehicleDeviceId);
            }
            if (functionId != null && !functionId.isEmpty()) {
                builder.setAttribute("parallel.driving.control.type", functionId);
            }
        }));
        return traced.as(DeviceTracer.fromMessage(message));
    }
}
