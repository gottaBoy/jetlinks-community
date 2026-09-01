package org.jetlinks.community.parallel.driving.message;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.exception.BusinessException;
import org.jetlinks.community.parallel.driving.configuration.ParallelDrivingVehicleToCockpitProperties;
import org.jetlinks.community.parallel.driving.metrics.ParallelDrivingLatencyMetrics;
import org.jetlinks.community.parallel.driving.room.ParallelDrivingRoom;
import org.jetlinks.community.parallel.driving.room.ParallelDrivingRoomManager;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingRelationService;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.function.FunctionParameter;
import org.jetlinks.core.trace.DeviceTracer;
import org.jetlinks.core.trace.MonoTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

/**
 * 平行驾驶自定义消息处理器
 * 处理自定义协议格式的消息（如 emergencystop）
 * 
 * 支持的消息格式：
 * - emergencystop: 紧急停车指令
 * - emergencystopresp: 紧急停车响应
 * 
 * @author yi.min@zeron.com
 */
@Component
@Slf4j
public class ParallelDrivingCustomMessageHandler {

    private final ParallelDrivingRelationService relationService;
    private final ParallelDrivingRoomManager roomManager;
    private final DeviceRegistry deviceRegistry;
    private final ParallelDrivingLatencyMetrics latencyMetrics;
    private final ParallelDrivingVehicleToCockpitProperties vehicleToCockpitProperties;

    /** remotejoystick 高频时缓存 cockpit->room，避免每条消息都查权限和房间，TTL 2 秒 */
    private final ConcurrentHashMap<String, CachedRoom> remotejoystickRoomCache = new ConcurrentHashMap<>();
    private static final long ROOM_CACHE_TTL_MS = 2000;

    /** updateLastActiveTime 防抖：500ms 内同一 cockpit+vehicle 只更新一次，降低 DB 压力 */
    private static final long LAST_ACTIVE_DEBOUNCE_MS = 500;
    private final ConcurrentHashMap<String, AtomicLong> lastActiveTimeUpdateCache = new ConcurrentHashMap<>();

    private static class CachedRoom {
        final ParallelDrivingRoom room;
        final String vehicleId;
        final long expiryTime;

        CachedRoom(ParallelDrivingRoom room, String vehicleId) {
            this.room = room;
            this.vehicleId = vehicleId;
            this.expiryTime = System.currentTimeMillis() + ROOM_CACHE_TTL_MS;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
    }

    private void recordPlatformLatency(String cockpitId, String vehicleId, long startMs) {
        if (latencyMetrics != null) {
            latencyMetrics.recordRemoteJoystickPlatformLatency(cockpitId, vehicleId,
                System.currentTimeMillis() - startMs);
        }
    }

    /**
     * 防抖更新最后活动时间：500ms 内同一 cockpit+vehicle 只触发一次 DB 更新，fire-and-forget 不阻塞主流程。
     * 用于 remotejoystick 高频热路径，降低 DB 压力。
     */
    private void scheduleUpdateLastActiveTimeDebounced(String cockpitId, String vehicleId) {
        long now = System.currentTimeMillis();
        String key = cockpitId + ":" + vehicleId;
        AtomicLong lastUpdated = lastActiveTimeUpdateCache.computeIfAbsent(
            key, ignored -> new AtomicLong(Long.MIN_VALUE));
        while (true) {
            long previous = lastUpdated.get();
            if (previous != Long.MIN_VALUE && now - previous < LAST_ACTIVE_DEBOUNCE_MS) {
                return;
            }
            if (lastUpdated.compareAndSet(previous, now)) {
                break;
            }
        }
        relationService.updateLastActiveTime(cockpitId, vehicleId)
            .subscribe(v -> {}, e -> log.debug("updateLastActiveTime debounced failed: {}", e.getMessage()));
    }

    private void removeCachedRoomIfSame(String cockpitId, ParallelDrivingRoom room, String vehicleId) {
        remotejoystickRoomCache.computeIfPresent(cockpitId, (key, current) ->
            current.room == room && java.util.Objects.equals(current.vehicleId, vehicleId)
                ? null
                : current);
    }

    @Autowired
    public ParallelDrivingCustomMessageHandler(ParallelDrivingRelationService relationService,
                                                ParallelDrivingRoomManager roomManager,
                                                DeviceRegistry deviceRegistry,
                                                ParallelDrivingVehicleToCockpitProperties vehicleToCockpitProperties,
                                                @Autowired(required = false) ParallelDrivingLatencyMetrics latencyMetrics) {
        this.relationService = relationService;
        this.roomManager = roomManager;
        this.deviceRegistry = deviceRegistry;
        this.vehicleToCockpitProperties = vehicleToCockpitProperties;
        this.latencyMetrics = latencyMetrics;
    }

    /**
     * 处理自定义消息（从设备消息中提取自定义协议消息）
     * 
     * @param deviceMessage 设备消息
     * @return Mono<Void>
     */
    public Mono<Void> handleCustomMessage(DeviceMessage deviceMessage) {
        // 尝试从消息中提取自定义协议消息
        String customMessageJson = extractCustomMessage(deviceMessage);
        
        log.debug("ParallelDrivingCustomMessageHandler.handleCustomMessage: deviceId={}, messageType={}, messageId={}, customMessageJson={}", 
            deviceMessage.getDeviceId(), deviceMessage.getMessageType(), deviceMessage.getMessageId(), 
            customMessageJson != null && customMessageJson.length() > 0 
                ? customMessageJson.substring(0, Math.min(200, customMessageJson.length())) 
                : "null");
        
        if (!StringUtils.hasText(customMessageJson)) {
            // remotejoystick 若 joystickdata 为空会走这里，导致不下发，需排查
            boolean looksLikeRemoteJoystick = deviceMessage instanceof FunctionInvokeMessage
                && "remotejoystick".equals(((FunctionInvokeMessage) deviceMessage).getFunctionId());
            if (looksLikeRemoteJoystick) {
                log.warn("[驾驶舱->云端->车辆] remotejoystick 提取失败被忽略(joystickdata可能为空): deviceId={}, messageId={}",
                    deviceMessage.getDeviceId(), deviceMessage.getMessageId());
            } else {
                log.debug("ParallelDrivingCustomMessageHandler: 不是自定义消息，忽略: deviceId={}, messageType={}",
                    deviceMessage.getDeviceId(), deviceMessage.getMessageType());
            }
            return Mono.empty(); // 不是自定义消息，忽略
        }

        try {
            JSONObject customMsg = JSON.parseObject(customMessageJson);
            String messageName = customMsg.getString("name");
            
            // 判断消息方向：根据消息类型和来源判断
            String direction = "未知";
            if (deviceMessage instanceof FunctionInvokeMessage) {
                direction = "[驾驶舱->云端->车辆]";
            } else if (deviceMessage instanceof FunctionInvokeMessageReply) {
                direction = "[车辆->云端->驾驶舱]";
            }
            
            // remotejoystick 高频（约100ms/条），使用 DEBUG 避免日志 I/O 影响吞吐
            if ("remotejoystick".equals(messageName)) {
                log.debug("{} ParallelDrivingCustomMessageHandler: 处理自定义消息: deviceId={}, messageName={}, messageId={}",
                    direction, deviceMessage.getDeviceId(), messageName, deviceMessage.getMessageId());
            } else {
                log.info("{} ParallelDrivingCustomMessageHandler: 处理自定义消息: deviceId={}, messageName={}, messageId={}",
                    direction, deviceMessage.getDeviceId(), messageName, deviceMessage.getMessageId());
            }
            
            if ("emergencystop".equals(messageName)) {
                return traceCustomMessage(
                    handleEmergencyStop(customMsg, deviceMessage),
                    deviceMessage,
                    messageName,
                    customMsg.getString("vin"));
            } else if ("emergencystopresp".equals(messageName)) {
                log.info("[车辆->云端->驾驶舱] ParallelDrivingCustomMessageHandler: 处理 emergencystopresp: deviceId={}, messageId={}", 
                    deviceMessage.getDeviceId(), deviceMessage.getMessageId());
                return traceCustomMessage(
                    handleEmergencyStopResponse(customMsg, deviceMessage),
                    deviceMessage,
                    messageName,
                    customMsg.getString("vin"));
            } else if ("remotejoystick".equals(messageName)) {
                return traceCustomMessage(
                    handleRemoteJoystick(customMsg, deviceMessage),
                    deviceMessage,
                    messageName,
                    customMsg.getString("vin"));
            } else if ("remotejoystickresp".equals(messageName)) {
                log.debug("[车辆->云端->驾驶舱] ParallelDrivingCustomMessageHandler: 处理 remotejoystickresp: deviceId={}, messageId={}", 
                    deviceMessage.getDeviceId(), deviceMessage.getMessageId());
                return traceCustomMessage(
                    handleRemoteJoystickResponse(customMsg, deviceMessage),
                    deviceMessage,
                    messageName,
                    customMsg.getString("vin"));
            }
            
            log.warn("ParallelDrivingCustomMessageHandler: 未知的自定义消息类型: deviceId={}, messageName={}", 
                deviceMessage.getDeviceId(), messageName);
            return Mono.empty();
        } catch (Exception e) {
            log.error("解析自定义消息失败: deviceId={}, messageId={}", 
                deviceMessage.getDeviceId(), deviceMessage.getMessageId(), e);
            return Mono.empty();
        }
    }

    private <T> Mono<T> traceCustomMessage(Mono<T> operation,
                                           DeviceMessage deviceMessage,
                                           String messageName,
                                           String vehicleDeviceId) {
        String direction = deviceMessage instanceof FunctionInvokeMessageReply
            ? "vehicle-to-cockpit"
            : "cockpit-to-vehicle";
        Mono<T> traced = operation.as(MonoTracer.create(
            "/parallel-driving/message/" + messageName,
            builder -> {
                builder
                    .setAttribute("parallel.driving.direction", direction)
                    .setAttribute("parallel.driving.message.name", messageName)
                    .setAttribute("parallel.driving.cockpit.id",
                        "cockpit-to-vehicle".equals(direction)
                            ? deviceMessage.getDeviceId()
                            : deviceMessage.getHeader("sourceDeviceId")
                                .map(String::valueOf)
                                .orElse(""));
                if (deviceMessage.getMessageId() != null && !deviceMessage.getMessageId().isEmpty()) {
                    builder.setAttribute("messaging.message.id", deviceMessage.getMessageId());
                }
                if (vehicleDeviceId != null && !vehicleDeviceId.isEmpty()) {
                    builder.setAttribute("parallel.driving.vehicle.id", vehicleDeviceId);
                }
            }));
        return traced.as(DeviceTracer.fromMessage(deviceMessage));
    }

    /**
     * 从设备消息中提取自定义协议消息
     * 优先从消息头中获取，如果没有则尝试从消息体中获取
     */
    private String extractCustomMessage(DeviceMessage message) {
        // 驾驶舱 TCP 发送的 remotejoystick 为 INVOKE_FUNCTION，无 customProtocol，需从 functionId+inputs 重构
        if (message instanceof FunctionInvokeMessage) {
            FunctionInvokeMessage funcMsg = (FunctionInvokeMessage) message;
            if ("remotejoystick".equals(funcMsg.getFunctionId())) {
                String reconstructed = reconstructRemoteJoystickMessage(funcMsg);
                if (reconstructed != null) {
                    return reconstructed;
                }
            }
        }

        // 1. 检查是否是自定义协议消息
        boolean isCustomProtocol = message.getHeader("customProtocol")
            .map(v -> Boolean.parseBoolean(String.valueOf(v)))
            .orElse(false);
        
        if (!isCustomProtocol) {
            return null;
        }

        // 2. 尝试从消息头中获取原始消息
        return message.getHeader("originalMessage")
            .map(String::valueOf)
            .orElseGet(() -> {
                // 3. 如果是 FunctionInvokeMessage，尝试从输入参数中获取
                if (message instanceof FunctionInvokeMessage) {
                    FunctionInvokeMessage funcMsg = (FunctionInvokeMessage) message;
                    Object customMsg = funcMsg.getInput("customMessage");
                    if (customMsg != null) {
                        return customMsg.toString();
                    }
                    // 尝试从 functionId 和 inputs 重构消息
                    if ("emergencystop".equals(funcMsg.getFunctionId())) {
                        return reconstructEmergencyStopMessage(funcMsg);
                    }
                }
                // 4. 如果是 FunctionInvokeMessageReply，尝试从输出参数中获取
                if (message instanceof FunctionInvokeMessageReply) {
                    FunctionInvokeMessageReply replyMsg = (FunctionInvokeMessageReply) message;
                    Object output = replyMsg.getOutput();
                    if (output instanceof Map) {
                        Object customMsg = ((Map<?, ?>) output).get("customMessage");
                        if (customMsg != null) {
                            return customMsg.toString();
                        }
                    }
                }
                return null;
            });
    }

    /**
     * 从 FunctionInvokeMessage 重构紧急停车消息
     */
    private String reconstructEmergencyStopMessage(FunctionInvokeMessage message) {
        try {
            JSONObject customMsg = new JSONObject();
            customMsg.put("name", "emergencystop");
            customMsg.put("vin", message.getDeviceId()); // 简化处理
            customMsg.put("id", message.getMessageId());
            customMsg.put("type", "oms");
            customMsg.put("omsno", message.getDeviceId()); // 简化处理
            customMsg.put("seq", message.getHeader("seq").map(String::valueOf).orElse("1"));
            customMsg.put("version", message.getHeader("version").map(String::valueOf).orElse("1.0"));
            customMsg.put("timestamp", String.valueOf(System.currentTimeMillis()));
            Object emergencystatus = message.getInput("emergencystatus");
            customMsg.put("emergencystatus", emergencystatus != null ? emergencystatus.toString() : "");
            return customMsg.toJSONString();
        } catch (Exception e) {
            log.error("重构紧急停车消息失败", e);
            return null;
        }
    }

    /**
     * 从 FunctionInvokeMessage 重构 remotejoystick 消息（驾驶舱 TCP 发送的 INVOKE_FUNCTION 格式）
     */
    private String reconstructRemoteJoystickMessage(FunctionInvokeMessage message) {
        try {
            Object joystickdataObj = message.getInput("joystickdata");
            if (joystickdataObj == null) {
                return null;
            }
            JSONObject customMsg = new JSONObject();
            customMsg.put("name", "remotejoystick");
            customMsg.put("vin", getInputOrHeader(message, "vin", message.getDeviceId()));
            customMsg.put("id", getInputOrHeader(message, "id", message.getMessageId()));
            customMsg.put("omsno", getInputOrHeader(message, "omsno", ""));
            customMsg.put("seq", getInputOrHeader(message, "seq", "1"));
            customMsg.put("version", getInputOrHeader(message, "version", "1.0"));
            customMsg.put("timestamp", getInputOrHeader(message, "timestamp", String.valueOf(System.currentTimeMillis())));
            customMsg.put("joystickdata", joystickdataObj);
            return customMsg.toJSONString();
        } catch (Exception e) {
            log.error("重构 remotejoystick 消息失败", e);
            return null;
        }
    }

    private String getInputOrHeader(FunctionInvokeMessage msg, String key, String defaultValue) {
        Object v = msg.getInput(key);
        if (v == null) {
            v = msg.getHeader(key).map(String::valueOf).orElse(null);
        }
        return unwrapValue(v, defaultValue);
    }

    /** 从 inputs 解包值，避免 Optional 等被 String.valueOf 转为 "Optional[xxx]" */
    private String unwrapValue(Object v, String defaultValue) {
        if (v == null) return defaultValue;
        if (v instanceof java.util.Optional) {
            java.util.Optional<?> opt = (java.util.Optional<?>) v;
            return opt.map(o -> unwrapValue(o, defaultValue)).orElse(defaultValue);
        }
        String s = String.valueOf(v);
        if (s == null || s.isEmpty()) return defaultValue;
        // 若已被转为 "Optional[xxx]" 字符串，提取内部值
        if (s.startsWith("Optional[") && s.endsWith("]")) {
            s = s.substring(9, s.length() - 1);
        }
        return s.isEmpty() ? defaultValue : s;
    }

    /**
     * 处理紧急停车指令
     * 
     * @param customMsg 自定义消息 JSON（可能来自自定义协议或规则引擎）
     * @param deviceMessage 设备消息
     */
    private Mono<Void> handleEmergencyStop(JSONObject customMsg, DeviceMessage deviceMessage) {
        final String cockpitDeviceId = deviceMessage.getDeviceId();
        
        // 从自定义消息或 FunctionInvokeMessage 的 inputs 中提取参数
        String emergencystatus = customMsg.getString("emergencystatus"); // start 或 stop（必填）
        String vin = customMsg.getString("vin");
        String omsno = customMsg.getString("omsno");
        String id = customMsg.getString("id");
        String seq = customMsg.getString("seq");
        String version = customMsg.getString("version");
        String timestamp = customMsg.getString("timestamp");
        
        // 如果 emergencystatus 为空，尝试从 FunctionInvokeMessage 的 inputs 中获取
        if (deviceMessage instanceof FunctionInvokeMessage && (emergencystatus == null || emergencystatus.isEmpty())) {
            FunctionInvokeMessage funcMsg = (FunctionInvokeMessage) deviceMessage;
            Object statusObj = funcMsg.getInput("emergencystatus");
            if (statusObj != null) {
                emergencystatus = String.valueOf(statusObj);
            }
            // 也从 inputs 中提取其他参数（如果自定义消息中没有）
            if (vin == null || vin.isEmpty()) {
                Object vinObj = funcMsg.getInput("vin");
                if (vinObj != null) {
                    vin = String.valueOf(vinObj);
                }
            }
            if (omsno == null || omsno.isEmpty()) {
                Object omsnoObj = funcMsg.getInput("omsno");
                if (omsnoObj != null) {
                    omsno = String.valueOf(omsnoObj);
                }
            }
            if (seq == null || seq.isEmpty()) {
                Object seqObj = funcMsg.getInput("seq");
                if (seqObj != null) {
                    seq = String.valueOf(seqObj);
                }
            }
            if (version == null || version.isEmpty()) {
                Object versionObj = funcMsg.getInput("version");
                if (versionObj != null) {
                    version = String.valueOf(versionObj);
                }
            }
            if (timestamp == null || timestamp.isEmpty()) {
                Object timestampObj = funcMsg.getInput("timestamp");
                if (timestampObj != null) {
                    timestamp = String.valueOf(timestampObj);
                }
            }
            if (id == null || id.isEmpty()) {
                id = funcMsg.getMessageId(); // 使用消息ID
            }
        }
        
        // 验证必填参数
        if (emergencystatus == null || emergencystatus.isEmpty()) {
            return Mono.error(new BusinessException("emergencystatus 参数不能为空"));
        }
        
        // 使用 final 变量，以便在 lambda 中使用
        final String finalEmergencystatus = emergencystatus;
        final String finalVin = vin;
        final String finalOmsno = omsno;
        final String finalId = id;
        final String finalSeq = seq;
        final String finalVersion = version;
        final String finalTimestamp = timestamp;
        
        // 1. 根据 vin 或 targetDeviceId 查找车辆设备ID
        // 优先使用 vin，如果没有则从消息头中获取 targetDeviceId
        final String finalVehicleDeviceId;
        if (finalVin != null && !finalVin.isEmpty()) {
            finalVehicleDeviceId = finalVin;
        } else {
            String targetId = deviceMessage.getHeader("targetDeviceId")
                .map(String::valueOf)
                .orElse(null);
            if (targetId == null || targetId.isEmpty()) {
                return Mono.error(new BusinessException("无法确定目标车辆设备ID，请提供 vin 参数或 targetDeviceId 消息头"));
            }
            finalVehicleDeviceId = targetId;
        }
        
        log.info("收到紧急停车指令: cockpit={}, vin={}, omsno={}, status={}, id={}, seq={}", 
            cockpitDeviceId, finalVin, finalOmsno, finalEmergencystatus, finalId, finalSeq);
        
        // 2. 验证权限
        return relationService.checkControlPermission(cockpitDeviceId, finalVehicleDeviceId)
            .filter(Boolean::booleanValue)
            .switchIfEmpty(Mono.error(new BusinessException("无权限控制车辆: " + finalVehicleDeviceId)))
            // 3. 获取房间
            .then(roomManager.getRoomByCockpit(cockpitDeviceId))
            .switchIfEmpty(Mono.error(new BusinessException("房间不存在或未激活")))
            // 4. 构造转发消息
            .flatMap(room -> {
                // 构造自定义消息，转发到车辆
                JSONObject forwardMsg = new JSONObject();
                forwardMsg.put("name", "emergencystop");
                forwardMsg.put("vin", finalVehicleDeviceId); // 车辆 VIN
                // 关键：确保 id 字段等于 messageId，这样车辆回复时能够正确匹配
                // 优先使用 messageId，如果没有则使用提供的 id
                String customMessageId = deviceMessage.getMessageId();
                if (customMessageId == null || customMessageId.isEmpty()) {
                    customMessageId = finalId != null && !finalId.isEmpty() ? finalId : java.util.UUID.randomUUID().toString();
                }
                forwardMsg.put("id", customMessageId); // 使用 messageId 作为自定义消息的 id
                log.debug("[驾驶舱->云端->车辆] 设置自定义消息 id: id={}, messageId={}, originalId={}", 
                    customMessageId, deviceMessage.getMessageId(), finalId);
                forwardMsg.put("type", "oms"); // 从云端发送到车辆
                forwardMsg.put("omsno", finalOmsno != null && !finalOmsno.isEmpty() ? finalOmsno : cockpitDeviceId); // 使用提供的omsno或驾驶舱ID
                forwardMsg.put("seq", finalSeq != null && !finalSeq.isEmpty() ? finalSeq : "1"); // 使用提供的seq或默认值
                forwardMsg.put("version", finalVersion != null && !finalVersion.isEmpty() ? finalVersion : "1.0"); // 使用提供的version或默认值
                forwardMsg.put("timestamp", finalTimestamp != null && !finalTimestamp.isEmpty() ? finalTimestamp : 
                    java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))); // 使用提供的时间戳或当前时间
                forwardMsg.put("emergencystatus", finalEmergencystatus);

                // 将自定义消息转换为设备消息
                FunctionInvokeMessage controlMessage = new FunctionInvokeMessage();
                controlMessage.setDeviceId(cockpitDeviceId);
                controlMessage.setFunctionId("emergencystop");
                
                // 验证 functionId 是否设置成功
                String verifyFunctionId = controlMessage.getFunctionId();
                log.info("ParallelDrivingCustomMessageHandler 创建消息: functionId={}, messageId={}, deviceId={}", 
                    verifyFunctionId, controlMessage.getMessageId(), controlMessage.getDeviceId());
                if (verifyFunctionId == null || verifyFunctionId.isEmpty() || !"emergencystop".equals(verifyFunctionId)) {
                    log.error("ParallelDrivingCustomMessageHandler ❌ functionId 设置失败！期望=emergencystop, 实际={}", 
                        verifyFunctionId);
                    // 强制重新设置
                    controlMessage.setFunctionId("emergencystop");
                    log.warn("ParallelDrivingCustomMessageHandler 强制重新设置 functionId: functionId={}", 
                        controlMessage.getFunctionId());
                }
                
                controlMessage.addInput("customMessage", forwardMsg.toJSONString());
                controlMessage.addInput("emergencystatus", finalEmergencystatus);
                // 添加所有参数到 inputs（供规则引擎使用）
                if (finalVin != null && !finalVin.isEmpty()) {
                    controlMessage.addInput("vin", finalVin);
                }
                if (finalOmsno != null && !finalOmsno.isEmpty()) {
                    controlMessage.addInput("omsno", finalOmsno);
                }
                if (finalSeq != null && !finalSeq.isEmpty()) {
                    controlMessage.addInput("seq", finalSeq);
                }
                if (finalVersion != null && !finalVersion.isEmpty()) {
                    controlMessage.addInput("version", finalVersion);
                }
                if (finalTimestamp != null && !finalTimestamp.isEmpty()) {
                    controlMessage.addInput("timestamp", finalTimestamp);
                }
                controlMessage.addHeader("targetDeviceId", finalVehicleDeviceId);
                controlMessage.addHeader("customProtocol", "true");
                controlMessage.addHeader("messageType", "emergencystop");
                // 在 header 中也添加 functionId（双重保险）
                controlMessage.addHeader("functionId", "emergencystop");
                controlMessage.addHeader("function", "emergencystop");
                // 设置 force 头，跳过物模型验证（因为这是自定义协议消息，不需要在物模型中定义）
                controlMessage.addHeaderIfAbsent(Headers.force, true);
                
                // 发送前最后一次验证
                String finalCheckFunctionId = controlMessage.getFunctionId();
                log.info("ParallelDrivingCustomMessageHandler 发送前验证: functionId={}, messageId={}, force={}", 
                    finalCheckFunctionId, controlMessage.getMessageId(), 
                    controlMessage.getHeader(Headers.force).orElse(false));
                if (finalCheckFunctionId == null || finalCheckFunctionId.isEmpty()) {
                    log.error("ParallelDrivingCustomMessageHandler ❌ 发送前 functionId 仍然为空！强制设置");
                    controlMessage.setFunctionId("emergencystop");
                }

                // 通过房间转发
                return room.forwardCockpitToVehicle(controlMessage);
            })
            // 5. 更新最后活动时间
            .then(relationService.updateLastActiveTime(cockpitDeviceId, finalVehicleDeviceId))
            .doOnSuccess(v -> log.info("[驾驶舱->云端->车辆] 紧急停车指令转发成功: cockpit={}, vehicle={}, status={}", 
                cockpitDeviceId, finalVehicleDeviceId, finalEmergencystatus))
            .doOnError(error -> log.error("[驾驶舱->云端->车辆] 紧急停车指令转发失败: cockpit={}, vehicle={}", 
                cockpitDeviceId, finalVehicleDeviceId, error));
    }

    /**
     * 处理紧急停车响应
     * 
     * @param customMsg 自定义消息 JSON
     * @param deviceMessage 设备消息
     */
    private Mono<Void> handleEmergencyStopResponse(JSONObject customMsg, DeviceMessage deviceMessage) {
        String vehicleDeviceId = deviceMessage.getDeviceId();
        String vin = customMsg.getString("vin");
        String response = customMsg.getString("response");
        String ack = customMsg.getString("ack");

        log.info("[车辆->云端] 收到紧急停车响应: vehicle={}, vin={}, response={}, ack={}", 
            vehicleDeviceId, vin, response, ack);

        // 1. 获取房间（通过车辆ID）
        return roomManager.getRoomByVehicle(vehicleDeviceId)
            .switchIfEmpty(Mono.empty()) // 如果没有房间，忽略消息
            // 2. 构造响应消息，转发到驾驶舱
            .flatMap(room -> {
                // 构造响应消息
                JSONObject responseMsg = new JSONObject();
                responseMsg.put("name", "emergencystopresp");
                responseMsg.put("vin", vin);
                responseMsg.put("id", customMsg.getString("id"));
                responseMsg.put("type", "vehicle");
                responseMsg.put("omsno", customMsg.getString("omsno"));
                responseMsg.put("seq", customMsg.getString("seq"));
                responseMsg.put("version", customMsg.getString("version"));
                responseMsg.put("timestamp", customMsg.getString("timestamp"));
                responseMsg.put("response", response);
                responseMsg.put("ack", ack);

                // 将自定义消息转换为设备消息
                FunctionInvokeMessageReply replyMessage = new FunctionInvokeMessageReply();
                replyMessage.setDeviceId(vehicleDeviceId);
                
                // 关键：requestId 应该与请求消息的 id 相同，用于匹配请求
                // 自定义协议中，响应消息的 id 字段就是请求消息的 id
                String requestIdStr = customMsg.getString("id");
                // 确保 requestId 非空（避免类型安全警告）
                final String finalRequestId = java.util.Objects.requireNonNull(
                    (requestIdStr != null && !requestIdStr.isEmpty()) 
                        ? requestIdStr 
                        : java.util.UUID.randomUUID().toString(),
                    "requestId must not be null"
                );
                // 设置 requestId header（用于匹配原始请求）
                replyMessage.addHeader("requestMessageId", finalRequestId);
                replyMessage.addHeader("requestId", finalRequestId);
                
                // messageId 应该是新的唯一ID（响应消息的唯一标识）
                String messageId = java.util.UUID.randomUUID().toString();
                replyMessage.setMessageId(messageId);
                
                replyMessage.setFunctionId("emergencystop");
                replyMessage.setSuccess("true".equals(response));
                
                // 将自定义消息放在消息头中
                String originalMsg = responseMsg.toJSONString();
                replyMessage.addHeader("originalMessage", originalMsg != null ? originalMsg : "");
                replyMessage.addHeader("customProtocol", "true");
                replyMessage.addHeader("messageType", "emergencystopresp");
                replyMessage.addHeader("response", response != null ? response : "");
                replyMessage.addHeader("ack", ack != null ? ack : "");
                
                log.debug("[车辆->云端->驾驶舱] 构造 FunctionInvokeMessageReply: vehicle={}, functionId={}, messageId={}, requestId={}, success={}", 
                    vehicleDeviceId, replyMessage.getFunctionId(), messageId, finalRequestId, replyMessage.isSuccess());

                // 通过房间转发
                return room.forwardVehicleToCockpit(replyMessage)
                    // 3. 更新最后活动时间
                    .then(relationService.updateLastActiveTime(room.getCockpitDeviceId(), vehicleDeviceId));
            })
            .doOnSuccess(v -> log.info("[车辆->云端->驾驶舱] 紧急停车响应转发成功: vehicle={}", vehicleDeviceId))
            .doOnError(error -> log.error("[车辆->云端->驾驶舱] 紧急停车响应转发失败: vehicle={}", vehicleDeviceId, error));
    }

    /**
     * 处理远程摇杆控制指令
     * 支持两个方向：驾驶舱->车辆（ cockpit 发送控制指令）、车辆->驾驶舱（ vehicle 上报摇杆数据）
     * 
     * @param customMsg 自定义消息 JSON
     * @param deviceMessage 设备消息
     */
    private Mono<Void> handleRemoteJoystick(JSONObject customMsg, DeviceMessage deviceMessage) {
        String senderDeviceId = deviceMessage.getDeviceId();
        
        // 先判断方向：若 getRoomByVehicle(sender) 有房间，则 sender 是车辆，方向为 车辆->驾驶舱
        return roomManager.getRoomByVehicle(senderDeviceId)
            .flatMap(room -> {
                // 车辆->驾驶舱：车辆上报 remotejoystick，转发给驾驶舱
                return handleRemoteJoystickVehicleToCockpit(customMsg, deviceMessage, room);
            })
            .switchIfEmpty(Mono.defer(() -> {
                // 驾驶舱->车辆：驾驶舱发送 remotejoystick，转发给车辆
                return handleRemoteJoystickCockpitToVehicle(customMsg, deviceMessage);
            }));
    }

    /**
     * 车辆->驾驶舱：车辆上报 remotejoystick 数据，转发给驾驶舱
     */
    private Mono<Void> handleRemoteJoystickVehicleToCockpit(JSONObject customMsg, DeviceMessage deviceMessage,
            org.jetlinks.community.parallel.driving.room.ParallelDrivingRoom room) {
        if (!vehicleToCockpitProperties.isForwardVehicleRemoteJoystickMirror()) {
            log.debug("[车辆->云端->驾驶舱] 跳过车端 remotejoystick 镜像转发(未开启 parallel-driving.vehicle-to-cockpit.forward-vehicle-remote-joystick-mirror): vehicle={}",
                deviceMessage.getDeviceId());
            if (latencyMetrics != null) {
                latencyMetrics.recordVehicleReplyCockpitForwardSkipped("remotejoystick_mirror");
            }
            return Mono.empty();
        }
        String vehicleDeviceId = deviceMessage.getDeviceId();
        Object joystickdataObj = customMsg.get("joystickdata");
        if (joystickdataObj == null && deviceMessage instanceof FunctionInvokeMessage) {
            joystickdataObj = ((FunctionInvokeMessage) deviceMessage).getInput("joystickdata");
        }
        if (joystickdataObj == null) {
            log.warn("[车辆->云端->驾驶舱] remotejoystick 缺少 joystickdata，忽略: vehicle={}", vehicleDeviceId);
            return Mono.empty();
        }
        // 构造 FunctionInvokeMessage 转发给驾驶舱（驾驶舱需要收到 remotejoystick 格式）
        org.jetlinks.core.message.function.FunctionInvokeMessage forwardMsg = 
            new org.jetlinks.core.message.function.FunctionInvokeMessage();
        forwardMsg.setDeviceId(vehicleDeviceId);
        forwardMsg.setFunctionId("remotejoystick");
        forwardMsg.setMessageId(deviceMessage.getMessageId());
        forwardMsg.setTimestamp(deviceMessage.getTimestamp());
        forwardMsg.addInput("joystickdata", joystickdataObj);
        forwardMsg.addInput("vin", customMsg.getString("vin"));
        forwardMsg.addInput("id", customMsg.getString("id"));
        forwardMsg.addInput("omsno", customMsg.getString("omsno"));
        forwardMsg.addInput("seq", customMsg.getString("seq"));
        forwardMsg.addInput("version", customMsg.getString("version"));
        forwardMsg.addInput("timestamp", customMsg.getString("timestamp"));
        forwardMsg.addHeader(Headers.force, true);
        forwardMsg.addHeader(Headers.async, true);
        return room.forwardVehicleToCockpit(forwardMsg)
            .doOnSuccess(v -> scheduleUpdateLastActiveTimeDebounced(room.getCockpitDeviceId(), vehicleDeviceId))
            .doOnSuccess(v -> log.debug("[车辆->云端->驾驶舱] remotejoystick 转发成功: vehicle={}, cockpit={}", 
                vehicleDeviceId, room.getCockpitDeviceId()))
            .doOnError(e -> log.error("[车辆->云端->驾驶舱] remotejoystick 转发失败: vehicle={}", vehicleDeviceId, e));
    }

    /**
     * 驾驶舱->车辆：驾驶舱发送 remotejoystick 控制指令，转发给车辆
     */
    private Mono<Void> handleRemoteJoystickCockpitToVehicle(JSONObject customMsg, DeviceMessage deviceMessage) {
        String cockpitDeviceId = deviceMessage.getDeviceId();
        
        // 从自定义消息中提取参数
        String vin = customMsg.getString("vin");
        String id = customMsg.getString("id");
        String omsno = customMsg.getString("omsno");
        String seq = customMsg.getString("seq");
        String version = customMsg.getString("version");
        String timestamp = customMsg.getString("timestamp");
        Object joystickdataObj = customMsg.get("joystickdata");
        
        // 如果是从 FunctionInvokeMessage 中提取，尝试从 inputs 中获取
        if (deviceMessage instanceof FunctionInvokeMessage) {
            FunctionInvokeMessage funcMsg = (FunctionInvokeMessage) deviceMessage;
            // 尝试从 inputs 中提取 joystickdata
            if (joystickdataObj == null) {
                joystickdataObj = funcMsg.getInput("joystickdata");
            }
            // 也从 inputs 中提取其他参数（如果自定义消息中没有），使用 unwrapValue 避免 Optional 被转为 "Optional[xxx]"
            if (vin == null || vin.isEmpty()) {
                vin = unwrapValue(funcMsg.getInput("vin"), null);
            }
            if (omsno == null || omsno.isEmpty()) {
                omsno = unwrapValue(funcMsg.getInput("omsno"), null);
            }
            if (seq == null || seq.isEmpty()) {
                seq = unwrapValue(funcMsg.getInput("seq"), null);
            }
            if (version == null || version.isEmpty()) {
                version = unwrapValue(funcMsg.getInput("version"), null);
            }
            if (timestamp == null || timestamp.isEmpty()) {
                timestamp = unwrapValue(funcMsg.getInput("timestamp"), null);
            }
            if (id == null || id.isEmpty()) {
                id = funcMsg.getMessageId(); // 使用消息ID
            }
        }
        
        // 验证必填参数
        if (joystickdataObj == null) {
            return Mono.error(new BusinessException("joystickdata 参数不能为空"));
        }
        
        // 使用 final 变量，以便在 lambda 中使用
        final Object finalJoystickdata = joystickdataObj;
        final String finalVin = vin;
        final String finalOmsno = omsno;
        final String finalId = id;
        final String finalSeq = seq;
        final String finalVersion = version;
        final String finalTimestamp = timestamp;
        
        // 1. 目标车辆选择策略（防串车）：
        // - 若驾驶舱已有激活房间：以房间 vehicleId 为准（忽略上行消息中的 vin/targetDeviceId，避免旧 vin 导致误控）
        // - 否则：可回退使用 vin 或 targetDeviceId（用于尚未建立房间的早期阶段/调试）
        String targetFromMessage = (finalVin != null && !finalVin.isEmpty()) ? finalVin : null;
        if (targetFromMessage == null) {
            targetFromMessage = deviceMessage.getHeader("targetDeviceId").map(String::valueOf).orElse(null);
        }
        // vin 等于 cockpit（如手柄 JYS001 误填了自己的 id）时，认为无效
        final boolean vinEqualsCockpit = cockpitDeviceId != null && cockpitDeviceId.equals(targetFromMessage);
        final String finalVehicleDeviceId =
            (targetFromMessage != null && !targetFromMessage.isEmpty() && !vinEqualsCockpit)
                ? targetFromMessage
                : null;
        
        log.debug("[驾驶舱->云端] 收到远程摇杆控制指令: cockpit={}, vin={}, omsno={}, id={}, seq={}, vinEqualsCockpit={}",
            cockpitDeviceId, finalVin, finalOmsno, finalId, finalSeq, vinEqualsCockpit);
        
        // 2. 优先使用缓存（remotejoystick 高频，避免每条都查权限和房间）
        CachedRoom cached = remotejoystickRoomCache.get(cockpitDeviceId);
        if (cached != null && !cached.isExpired() && cached.room.isActive()) {
            String vehicleId = cached.vehicleId;
            if (cockpitDeviceId != null && cockpitDeviceId.equals(vehicleId)) {
                return Mono.empty();
            }
            long startMs = System.currentTimeMillis();
            return doForwardRemoteJoystick(deviceMessage, finalJoystickdata, finalOmsno, finalId, finalSeq,
                finalVersion, finalTimestamp, cockpitDeviceId, cached.room, vehicleId)
                .doOnSuccess(v -> recordPlatformLatency(cockpitDeviceId, vehicleId, startMs));
        }
        if (cached != null && (cached.isExpired() || !cached.room.isActive())) {
            remotejoystickRoomCache.remove(cockpitDeviceId, cached);
        }
        
        // 3. 获取目标车辆：优先房间，其次消息 vin/targetDeviceId
        Mono<String> vehicleIdSource = roomManager.getRoomByCockpit(cockpitDeviceId)
            .filter(ParallelDrivingRoom::isActive)
            .map(room -> {
                String roomVehicleId = room.getVehicleDeviceId();
                if (finalVehicleDeviceId != null && !finalVehicleDeviceId.equals(roomVehicleId)) {
                    log.warn("[驾驶舱->云端] remotejoystick 消息中的 vin/targetDeviceId 与房间不一致，已忽略消息目标，使用房间车辆: cockpit={}, msgTarget={}, roomVehicle={}",
                        cockpitDeviceId, finalVehicleDeviceId, roomVehicleId);
                }
                // 写入缓存，加速后续高频路径
                remotejoystickRoomCache.put(cockpitDeviceId, new CachedRoom(room, roomVehicleId));
                return roomVehicleId;
            })
            .switchIfEmpty(finalVehicleDeviceId != null
                ? Mono.just(finalVehicleDeviceId)
                : Mono.error(new BusinessException("无法确定目标车辆：未找到激活房间，且消息未提供有效 vin/targetDeviceId。请先完成接管，或确保消息中 vin 为目标车辆ID")));
        
        return vehicleIdSource
            .flatMap(vehicleId -> {
                if (cockpitDeviceId != null && cockpitDeviceId.equals(vehicleId)) {
                    log.debug("[驾驶舱->云端] 目标车辆与驾驶舱相同，跳过: cockpit={}", cockpitDeviceId);
                    return Mono.empty();
                }
                return relationService.checkControlPermission(cockpitDeviceId, vehicleId)
            .filter(Boolean::booleanValue)
            .switchIfEmpty(Mono.error(new BusinessException("无权限控制车辆: " + vehicleId)))
            // 3. 获取房间
            .then(roomManager.getRoomByCockpit(cockpitDeviceId))
            .switchIfEmpty(Mono.error(new BusinessException("房间不存在或未激活")))
            // 4. 构造转发消息并缓存 room（供后续 remotejoystick 快速路径使用）
            .flatMap(room -> {
                remotejoystickRoomCache.put(cockpitDeviceId, new CachedRoom(room, vehicleId));
                // 构造自定义消息，转发到车辆（vin 必须为目标车辆ID，如 L584C4VC5SD001331，不能用手柄 JYS001）
                JSONObject forwardMsg = new JSONObject();
                forwardMsg.put("name", "remotejoystick");
                forwardMsg.put("vin", vehicleId); // 车辆 VIN（目标车辆设备ID）
                // 关键：确保 id 字段等于 messageId，这样车辆回复时能够正确匹配
                // 优先使用 messageId，如果没有则使用提供的 id
                String customMessageId = deviceMessage.getMessageId();
                if (customMessageId == null || customMessageId.isEmpty()) {
                    customMessageId = finalId != null && !finalId.isEmpty() ? finalId : java.util.UUID.randomUUID().toString();
                }
                String effectiveSeq = finalSeq != null && !finalSeq.isEmpty() ? finalSeq : "1";
                String correlationId = deviceMessage.getHeader("correlationId")
                    .map(String::valueOf)
                    .orElse(customMessageId);
                forwardMsg.put("id", customMessageId); // 使用 messageId 作为自定义消息的 id
                log.debug("[驾驶舱->云端->车辆] 设置自定义消息 id: id={}, messageId={}, originalId={}", 
                    customMessageId, deviceMessage.getMessageId(), finalId);
                forwardMsg.put("type", "oms"); // 从云端发送到车辆
                forwardMsg.put("omsno", finalOmsno != null && !finalOmsno.isEmpty() ? finalOmsno : cockpitDeviceId); // 使用提供的omsno或驾驶舱ID
                forwardMsg.put("seq", effectiveSeq); // 使用提供的seq或默认值
                forwardMsg.put("correlationId", correlationId);
                forwardMsg.put("version", finalVersion != null && !finalVersion.isEmpty() ? finalVersion : "1.0"); // 使用提供的version或默认值
                forwardMsg.put("timestamp", finalTimestamp != null && !finalTimestamp.isEmpty() ? finalTimestamp : 
                    java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))); // 使用提供的时间戳或当前时间
                forwardMsg.put("joystickdata", finalJoystickdata);

                // 将自定义消息转换为设备消息
                FunctionInvokeMessage controlMessage = new FunctionInvokeMessage();
                controlMessage.setDeviceId(cockpitDeviceId);
                controlMessage.setFunctionId("remotejoystick");
                controlMessage.setMessageId(customMessageId);
                
                // 验证 functionId 是否设置成功
                String verifyFunctionId = controlMessage.getFunctionId();
                log.debug("[驾驶舱->云端] ParallelDrivingCustomMessageHandler 创建消息: functionId={}, messageId={}, deviceId={}",
                    verifyFunctionId, controlMessage.getMessageId(), controlMessage.getDeviceId());
                if (verifyFunctionId == null || verifyFunctionId.isEmpty() || !"remotejoystick".equals(verifyFunctionId)) {
                    log.error("[驾驶舱->云端] ParallelDrivingCustomMessageHandler ❌ functionId 设置失败！期望=remotejoystick, 实际={}", 
                        verifyFunctionId);
                    // 强制重新设置
                    controlMessage.setFunctionId("remotejoystick");
                    log.warn("[驾驶舱->云端] ParallelDrivingCustomMessageHandler 强制重新设置 functionId: functionId={}", 
                        controlMessage.getFunctionId());
                }
                
                controlMessage.addInput("customMessage", forwardMsg.toJSONString());
                controlMessage.addInput("joystickdata", finalJoystickdata);
                // 添加所有参数到 inputs（供规则引擎使用），vin 必须为目标车辆ID
                controlMessage.addInput("vin", vehicleId);
                if (finalOmsno != null && !finalOmsno.isEmpty()) {
                    controlMessage.addInput("omsno", finalOmsno);
                }
                controlMessage.addInput("seq", effectiveSeq);
                controlMessage.addInput("correlationId", correlationId);
                controlMessage.addHeader("seq", effectiveSeq);
                if (finalVersion != null && !finalVersion.isEmpty()) {
                    controlMessage.addInput("version", finalVersion);
                }
                if (finalTimestamp != null && !finalTimestamp.isEmpty()) {
                    controlMessage.addInput("timestamp", finalTimestamp);
                }
                controlMessage.addHeader("targetDeviceId", vehicleId);
                controlMessage.addHeader("customProtocol", "true");
                controlMessage.addHeader("messageType", "remotejoystick");
                controlMessage.addHeader("correlationId", correlationId);
                // 在 header 中也添加 functionId（双重保险）
                controlMessage.addHeader("functionId", "remotejoystick");
                controlMessage.addHeader("function", "remotejoystick");
                // 设置 force 头，跳过物模型验证（因为这是自定义协议消息，不需要在物模型中定义）
                controlMessage.addHeaderIfAbsent(Headers.force, true);
                
                // 发送前最后一次验证
                String finalCheckFunctionId = controlMessage.getFunctionId();
                log.debug("[驾驶舱->云端] ParallelDrivingCustomMessageHandler 发送前验证: functionId={}, messageId={}, force={}",
                    finalCheckFunctionId, controlMessage.getMessageId(),
                    controlMessage.getHeader(Headers.force).orElse(false));
                if (finalCheckFunctionId == null || finalCheckFunctionId.isEmpty()) {
                    log.error("[驾驶舱->云端] ParallelDrivingCustomMessageHandler ❌ 发送前 functionId 仍然为空！强制设置");
                    controlMessage.setFunctionId("remotejoystick");
                }

                // 通过房间转发
                long startMs = System.currentTimeMillis();
                return room.forwardCockpitToVehicle(controlMessage)
                    .doOnSuccess(v -> recordPlatformLatency(cockpitDeviceId, vehicleId, startMs));
            })
            // 5. 防抖更新最后活动时间（fire-and-forget，不阻塞转发完成）
            .doOnSuccess(v -> scheduleUpdateLastActiveTimeDebounced(cockpitDeviceId, vehicleId))
            .doOnSuccess(v -> log.debug("[驾驶舱->云端->车辆] 远程摇杆控制指令转发成功: cockpit={}, vehicle={}",
                cockpitDeviceId, vehicleId))
            .doOnError(error -> {
                CachedRoom cachedOnError = remotejoystickRoomCache.get(cockpitDeviceId);
                if (cachedOnError != null
                    && java.util.Objects.equals(cachedOnError.vehicleId, vehicleId)) {
                    removeCachedRoomIfSame(cockpitDeviceId, cachedOnError.room, vehicleId);
                }
                log.error("[驾驶舱->云端->车辆] 远程摇杆控制指令转发失败: cockpit={}, vehicle={}", 
                    cockpitDeviceId, vehicleId, error);
            });
        });
    }

    /** 构造并转发 remotejoystick 到车辆（供缓存命中时快速路径使用） */
    private Mono<Void> doForwardRemoteJoystick(DeviceMessage deviceMessage, Object finalJoystickdata,
            String finalOmsno, String finalId, String finalSeq, String finalVersion, String finalTimestamp,
            String cockpitDeviceId, ParallelDrivingRoom room, String vehicleId) {
        JSONObject forwardMsg = new JSONObject();
        forwardMsg.put("name", "remotejoystick");
        forwardMsg.put("vin", vehicleId);
        String customMessageId = deviceMessage.getMessageId();
        if (customMessageId == null || customMessageId.isEmpty()) {
            customMessageId = finalId != null && !finalId.isEmpty() ? finalId : java.util.UUID.randomUUID().toString();
        }
        String effectiveSeq = finalSeq != null && !finalSeq.isEmpty() ? finalSeq : "1";
        String correlationId = deviceMessage.getHeader("correlationId")
            .map(String::valueOf)
            .orElse(customMessageId);
        forwardMsg.put("id", customMessageId);
        forwardMsg.put("type", "oms");
        forwardMsg.put("omsno", finalOmsno != null && !finalOmsno.isEmpty() ? finalOmsno : cockpitDeviceId);
        forwardMsg.put("seq", effectiveSeq);
        forwardMsg.put("correlationId", correlationId);
        forwardMsg.put("version", finalVersion != null && !finalVersion.isEmpty() ? finalVersion : "1.0");
        forwardMsg.put("timestamp", finalTimestamp != null && !finalTimestamp.isEmpty() ? finalTimestamp :
            java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        forwardMsg.put("joystickdata", finalJoystickdata);

        FunctionInvokeMessage controlMessage = new FunctionInvokeMessage();
        controlMessage.setDeviceId(cockpitDeviceId);
        controlMessage.setFunctionId("remotejoystick");
        controlMessage.setMessageId(customMessageId);
        controlMessage.addInput("customMessage", forwardMsg.toJSONString());
        controlMessage.addInput("joystickdata", finalJoystickdata);
        controlMessage.addInput("vin", vehicleId);
        if (finalOmsno != null && !finalOmsno.isEmpty()) controlMessage.addInput("omsno", finalOmsno);
        controlMessage.addInput("seq", effectiveSeq);
        controlMessage.addInput("correlationId", correlationId);
        controlMessage.addHeader("seq", effectiveSeq);
        if (finalVersion != null && !finalVersion.isEmpty()) controlMessage.addInput("version", finalVersion);
        if (finalTimestamp != null && !finalTimestamp.isEmpty()) controlMessage.addInput("timestamp", finalTimestamp);
        controlMessage.addHeader("targetDeviceId", vehicleId);
        controlMessage.addHeader("customProtocol", "true");
        controlMessage.addHeader("messageType", "remotejoystick");
        controlMessage.addHeader("correlationId", correlationId);
        controlMessage.addHeader("functionId", "remotejoystick");
        controlMessage.addHeader("function", "remotejoystick");
        controlMessage.addHeaderIfAbsent(Headers.force, true);

        return room.forwardCockpitToVehicle(controlMessage)
            .doOnSuccess(v -> scheduleUpdateLastActiveTimeDebounced(cockpitDeviceId, vehicleId))
            .doOnSuccess(v -> log.debug("[驾驶舱->云端->车辆] 远程摇杆控制指令转发成功(缓存): cockpit={}, vehicle={}", cockpitDeviceId, vehicleId))
            .doOnError(error -> {
                removeCachedRoomIfSame(cockpitDeviceId, room, vehicleId);
                log.error("[驾驶舱->云端->车辆] 远程摇杆控制指令转发失败(缓存): cockpit={}, vehicle={}", cockpitDeviceId, vehicleId, error);
            });
    }

    /**
     * 处理远程摇杆控制响应
     * 
     * @param customMsg 自定义消息 JSON
     * @param deviceMessage 设备消息
     */
    private Mono<Void> handleRemoteJoystickResponse(JSONObject customMsg, DeviceMessage deviceMessage) {
        String vehicleDeviceId = deviceMessage.getDeviceId();
        if (!vehicleToCockpitProperties.shouldForwardStandardFunctionReplyToCockpit("remotejoystick")) {
            log.debug("[车辆->云端->驾驶舱] 跳过 remotejoystickresp 转发(不在白名单 parallel-driving.vehicle-to-cockpit.forward-reply-function-ids): vehicle={}",
                vehicleDeviceId);
            if (latencyMetrics != null) {
                latencyMetrics.recordVehicleReplyCockpitForwardSkipped("remotejoystickresp");
            }
            return Mono.empty();
        }

        // 从自定义消息中提取参数
        String id = customMsg.getString("id");
        String status = customMsg.getString("status");
        Object responseObj = customMsg.get("response");
        String ack = customMsg.getString("ack");
        
        // 判断响应是否成功
        boolean success = false;
        if (responseObj != null) {
            String response = String.valueOf(responseObj).trim().toLowerCase();
            success = "true".equals(response) || "1".equals(response) || "ok".equals(response);
        }
        
        // 确保 requestId 非空（避免类型安全警告）
        final String finalRequestId = java.util.Objects.requireNonNull(
            (id != null && !id.isEmpty()) ? id : deviceMessage.getMessageId(),
            "requestId cannot be null");
        final boolean finalSuccess = success;
        final String finalStatus = status;
        final String finalAck = ack;
        
        log.debug("[车辆->云端] 收到远程摇杆控制响应: vehicle={}, id={}, success={}, status={}, ack={}", 
            vehicleDeviceId, finalRequestId, finalSuccess, finalStatus, finalAck);
        
        // 1. 查找房间
        return roomManager.getRoomByVehicle(vehicleDeviceId)
            .switchIfEmpty(Mono.error(new BusinessException("房间不存在或未激活")))
            // 2. 构造 FunctionInvokeMessageReply
            .flatMap(room -> {
                String cockpitDeviceId = room.getCockpitDeviceId();
                
                FunctionInvokeMessageReply replyMessage = new FunctionInvokeMessageReply();
                replyMessage.setDeviceId(vehicleDeviceId);
                replyMessage.setFunctionId("remotejoystick");
                replyMessage.setSuccess(finalSuccess);
                
                // 设置 messageId（生成新的唯一ID，用于响应消息）
                String messageId = java.util.UUID.randomUUID().toString();
                replyMessage.setMessageId(messageId);
                
                // 设置 requestId 和 requestMessageId（从原始请求的 id 获取）
                replyMessage.addHeader("requestMessageId", finalRequestId);
                replyMessage.addHeader("requestId", finalRequestId);
                
                // 设置 output（将整个消息作为 output，保留原始信息）
                Map<String, Object> output = new java.util.HashMap<>();
                output.put("status", finalStatus);
                output.put("response", responseObj);
                output.put("ack", finalAck);
                output.put("id", finalRequestId);
                replyMessage.setOutput(output);
                
                // 设置 timestamp
                Object timestampObj = customMsg.get("timestamp");
                if (timestampObj != null) {
                    try {
                        // 尝试解析时间戳（格式：YYYY-MM-DD HH:mm:ss）
                        String timestampStr = String.valueOf(timestampObj);
                        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        java.util.Date date = sdf.parse(timestampStr);
                        replyMessage.setTimestamp(date.getTime());
                    } catch (Exception e) {
                        // 如果解析失败，使用当前时间
                        replyMessage.setTimestamp(System.currentTimeMillis());
                    }
                } else {
                    replyMessage.setTimestamp(System.currentTimeMillis());
                }
                
                // 添加自定义协议标识
                replyMessage.addHeader("customProtocol", "true");
                replyMessage.addHeader("messageType", "remotejoystickresp");
                
                log.debug("[车辆->云端->驾驶舱] 准备转发远程摇杆控制响应: vehicle={} -> cockpit={}, functionId={}, messageId={}, requestId={}, success={}", 
                    vehicleDeviceId, cockpitDeviceId, replyMessage.getFunctionId(), messageId, finalRequestId, replyMessage.isSuccess());

                // 通过房间转发
                return room.forwardVehicleToCockpit(replyMessage)
                    // 3. 防抖更新最后活动时间（fire-and-forget）
                    .doOnSuccess(v -> scheduleUpdateLastActiveTimeDebounced(cockpitDeviceId, vehicleDeviceId))
                    .doOnSuccess(v -> log.debug("[车辆->云端->驾驶舱] 远程摇杆控制响应转发成功: vehicle={} -> cockpit={}, functionId={}, success={}", 
                        vehicleDeviceId, cockpitDeviceId, replyMessage.getFunctionId(), finalSuccess))
                    .doOnError(error -> log.error("[车辆->云端->驾驶舱] 远程摇杆控制响应转发失败: vehicle={} -> cockpit={}", 
                        vehicleDeviceId, cockpitDeviceId, error));
            })
            .then()
            .doOnError(error -> log.error("[车辆->云端->驾驶舱] 远程摇杆控制响应转发失败: vehicle={}", 
                vehicleDeviceId, error));
    }

    /**
     * 车云链路 RTT：响应车端上行的 {@code cloudLinkPing}（与 MQTT/网关业务路径一致）。
     * <p>
     * 车端流程建议：定时发起 functionId=cloudLinkPing（inputs 可带 pingId、clientSendTimeMs），
     * 收到本回复后计算 RTT=收包时刻-发包时刻（建议用单调时钟），再将结果写入属性 {@code cloud_link_rtt_ms}
     * 或 {@code chassis_status.cloud_link_rtt_ms}，随状态上报即可在远控页展示。
     * <p>
     * 另输出 {@code serverReceiveTimeMs}、{@code serverReplyTimeMs} 和
     * {@code serverProcessingMs}。其中处理耗时使用平台 JVM 单调时钟测量，不受 NTP 或系统
     * wall clock 调整影响；车端可用 {@code RTT - serverProcessingMs} 估算网络往返。
     */
    public Mono<Void> replyToCloudLinkPing(FunctionInvokeMessage invoke,
                                           long serverReceiveTimeMs,
                                           long serverReceiveTimeNanos) {
        String deviceId = invoke.getDeviceId();
        if (deviceId == null || deviceId.isEmpty()) {
            log.warn("[cloudLinkPing] skip pong: missing deviceId requestMessageId={}", invoke.getMessageId());
            return Mono.empty();
        }
        Map<String, Object> output = new HashMap<>();
        for (FunctionParameter p : invoke.getInputs()) {
            if (p == null || p.getName() == null) {
                continue;
            }
            if ("pingId".equals(p.getName()) || "clientSendTimeMs".equals(p.getName())) {
                output.put(p.getName(), p.getValue());
            }
        }
        long serverReplyTimeMs = System.currentTimeMillis();
        long serverProcessingMs = Math.max(
            0,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - serverReceiveTimeNanos));
        output.put("serverReceiveTimeMs", serverReceiveTimeMs);
        output.put("serverReplyTimeMs", serverReplyTimeMs);
        output.put("serverTimeMs", serverReplyTimeMs);
        output.put("serverProcessingMs", serverProcessingMs);

        FunctionInvokeMessageReply reply = new FunctionInvokeMessageReply();
        reply.setDeviceId(deviceId);
        reply.setFunctionId("cloudLinkPing");
        reply.setMessageId(java.util.UUID.randomUUID().toString());
        reply.addHeader("requestMessageId", invoke.getMessageId());
        reply.addHeader("requestId", invoke.getMessageId());
        reply.setTimestamp(serverReplyTimeMs);
        reply.setSuccess(true);
        reply.setOutput(output);

        String replyMessageId = reply.getMessageId();
        log.info("[cloudLinkPing] send_pong deviceId={} requestMessageId={} replyMessageId={} serverReceiveTimeMs={} serverReplyTimeMs={} serverProcessingMs={}",
            deviceId, invoke.getMessageId(), replyMessageId, serverReceiveTimeMs, serverReplyTimeMs,
            serverProcessingMs);

        // 使用 sendAndForget：send() 会等待设备对下行消息的协议应答，TCP 车端收到 INVOKE_FUNCTION_REPLY 后通常不回包，导致 DefaultDeviceMessageSender 超时
        Mono<Void> operation = deviceRegistry.getDevice(deviceId)
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("[cloudLinkPing] skip pong: device not in registry deviceId={} requestMessageId={}",
                    deviceId, invoke.getMessageId());
                return Mono.empty();
            }))
            // The vehicle does not send a protocol ACK for this reply. Keep
            // the low-frequency ping path bounded if the gateway/socket is
            // unhealthy, while leaving the high-frequency joystick path
            // untouched.
            .flatMap(op -> op.messageSender().sendAndForget(reply)
                .timeout(Duration.ofSeconds(1))
                .then())
            .doOnSuccess(v -> log.debug("[cloudLinkPing] send_pong_done deviceId={} requestMessageId={}",
                deviceId, invoke.getMessageId()))
            .onErrorResume(err -> {
                log.warn("[cloudLinkPing] send_pong_fail deviceId={} requestMessageId={} err={}",
                    deviceId, invoke.getMessageId(), err.getMessage());
                return Mono.empty();
            });
        return operation.as(MonoTracer.create(
            "/parallel-driving/cloud-link-ping",
            builder -> {
                builder
                    .setAttribute("parallel.driving.direction", "vehicle-to-vehicle")
                    .setAttribute("parallel.driving.vehicle.id", deviceId)
                    .setAttribute("parallel.driving.control.type", "cloudLinkPing");
                if (invoke.getMessageId() != null && !invoke.getMessageId().isEmpty()) {
                    builder.setAttribute("messaging.message.id", invoke.getMessageId());
                }
            }))
            .as(DeviceTracer.fromMessage(invoke));
    }
}
