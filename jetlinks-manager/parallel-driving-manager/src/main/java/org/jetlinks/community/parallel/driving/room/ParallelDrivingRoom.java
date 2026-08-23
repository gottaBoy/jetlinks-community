package org.jetlinks.community.parallel.driving.room;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.parallel.driving.enums.ParallelDrivingSessionState;
import org.jetlinks.community.parallel.driving.metrics.ParallelDrivingLatencyMetrics;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingEncryptionService;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Headers;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 平行驾驶房间
 * 管理驾驶舱-车辆之间的 TCP 连接和消息转发
 *
 * @author JetLinks
 */
@Slf4j
@Getter
public class ParallelDrivingRoom {
    
    private final String roomId;  // 房间ID：cockpitId-vehicleId
    private final String cockpitDeviceId;
    private final String vehicleDeviceId;
    
    private DeviceOperator cockpitDevice;
    private DeviceOperator vehicleDevice;
    
    private ParallelDrivingSessionState state;
    private long createTime;
    private long lastActiveTime;
    
    // 消息统计
    private final AtomicLong cockpitToVehicleMessages = new AtomicLong(0);
    private final AtomicLong vehicleToCockpitMessages = new AtomicLong(0);
    
    // 加密服务（可选，用于查询加密状态）
    private ParallelDrivingEncryptionService encryptionService;
    
    // 设备注册表：用于每次转发时重新获取车辆设备，避免车端重启后使用过期会话
    private DeviceRegistry deviceRegistry;

    /** remotejoystick 高频时缓存 vehicleDevice，TTL 1 秒，减少 getDevice 调用 */
    private final AtomicReference<DeviceOperator> cachedVehicleDevice = new AtomicReference<>(null);
    private volatile long cachedVehicleDeviceTime = 0;
    private static final long DEVICE_CACHE_TTL_MS = 1000;

    /** latest-only 信箱开关（parallel-driving.control.latest-only，默认 false 保持原行为） */
    private volatile boolean latestOnlyEnabled = false;
    /** remotejoystick latest-only 信箱：待发的最新一帧（覆盖式） */
    private final AtomicReference<DeviceMessage> pendingRemoteJoystick = new AtomicReference<>();
    /** 单飞标记：是否已有帧在途 */
    private final AtomicBoolean remoteJoystickSending = new AtomicBoolean(false);
    /** latest-only 信箱合并(旧帧被覆盖)计数，@Getter 暴露 */
    private final AtomicLong coalescedRemoteJoystickMessages = new AtomicLong(0);
    /** 指标记录器（可选注入，null 时跳过埋点） */
    private volatile ParallelDrivingLatencyMetrics latencyMetrics;
    
    public ParallelDrivingRoom(String cockpitDeviceId, String vehicleDeviceId) {
        this.roomId = cockpitDeviceId + "-" + vehicleDeviceId;
        this.cockpitDeviceId = cockpitDeviceId;
        this.vehicleDeviceId = vehicleDeviceId;
        this.state = ParallelDrivingSessionState.BINDING;
        this.createTime = System.currentTimeMillis();
        this.lastActiveTime = System.currentTimeMillis();
    }
    
    /**
     * 初始化房间（获取设备操作器）
     *
     * @param cockpitDevice 驾驶舱设备操作器
     * @param vehicleDevice 车辆设备操作器
     */
    public void initialize(DeviceOperator cockpitDevice, DeviceOperator vehicleDevice) {
        this.cockpitDevice = cockpitDevice;
        this.vehicleDevice = vehicleDevice;
        this.state = ParallelDrivingSessionState.ACTIVE;
        this.lastActiveTime = System.currentTimeMillis();
        log.info("房间[{}]初始化成功: cockpit={}, vehicle={}", roomId, cockpitDeviceId, vehicleDeviceId);
    }
    
    /**
     * 设置加密服务（可选）
     *
     * @param encryptionService 加密服务
     */
    public void setEncryptionService(ParallelDrivingEncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }
    
    /**
     * 设置设备注册表（用于每次转发时重新获取车辆设备，解决车端重启后收不到消息的问题）
     *
     * @param deviceRegistry 设备注册表
     */
    public void setDeviceRegistry(DeviceRegistry deviceRegistry) {
        this.deviceRegistry = deviceRegistry;
    }

    /** 开启/关闭 remotejoystick latest-only 信箱（默认关闭，保持单节点原行为） */
    public void setLatestOnlyEnabled(boolean latestOnlyEnabled) {
        this.latestOnlyEnabled = latestOnlyEnabled;
    }

    /** 设置指标记录器（可选） */
    public void setLatencyMetrics(ParallelDrivingLatencyMetrics latencyMetrics) {
        this.latencyMetrics = latencyMetrics;
    }
    
    /** 从注册表获取车辆设备并缓存（供 remotejoystick 高频转发使用） */
    private Mono<DeviceOperator> fetchAndCacheDevice() {
        if (deviceRegistry == null) {
            return vehicleDevice != null ? Mono.just(vehicleDevice) : Mono.error(new IllegalStateException("车辆设备未初始化"));
        }
        return deviceRegistry.getDevice(vehicleDeviceId)
            .switchIfEmpty(Mono.error(new IllegalStateException("车辆设备不存在或已离线: " + vehicleDeviceId)))
            .doOnNext(device -> {
                cachedVehicleDevice.set(device);
                cachedVehicleDeviceTime = System.currentTimeMillis();
            });
    }

    /**
     * 转发驾驶舱消息到车辆
     *
     * @param message 设备消息
     * @return Mono<Void>
     */
    public reactor.core.publisher.Mono<Void> forwardCockpitToVehicle(DeviceMessage message) {
        if (latestOnlyEnabled && isRemoteJoystickMessage(message)) {
            return forwardRemoteJoystickLatestOnly(message);
        }
        return forwardCockpitToVehicleDirect(message);
    }

    private static boolean isRemoteJoystickMessage(DeviceMessage message) {
        return message instanceof org.jetlinks.core.message.function.FunctionInvokeMessage
            && "remotejoystick".equals(((org.jetlinks.core.message.function.FunctionInvokeMessage) message).getFunctionId());
    }

    /** latest-only 信箱：同一时刻最多一帧在途；新帧覆盖待发帧，发完只补发最新的，跳过中间旧帧。 */
    private reactor.core.publisher.Mono<Void> forwardRemoteJoystickLatestOnly(DeviceMessage message) {
        DeviceMessage replaced = pendingRemoteJoystick.getAndSet(message);
        if (replaced != null) {
            coalescedRemoteJoystickMessages.incrementAndGet();
            if (latencyMetrics != null) {
                latencyMetrics.recordRemoteJoystickMailboxCoalesced(cockpitDeviceId, vehicleDeviceId);
            }
        }
        if (!remoteJoystickSending.compareAndSet(false, true)) {
            return reactor.core.publisher.Mono.empty();
        }
        return drainRemoteJoystick();
    }

    private reactor.core.publisher.Mono<Void> drainRemoteJoystick() {
        DeviceMessage next = pendingRemoteJoystick.getAndSet(null);
        if (next == null) {
            remoteJoystickSending.set(false);
            return reactor.core.publisher.Mono.empty();
        }
        return forwardCockpitToVehicleDirect(next)
            .doFinally(s -> {
                remoteJoystickSending.set(false);
                if (pendingRemoteJoystick.get() != null
                    && remoteJoystickSending.compareAndSet(false, true)) {
                    drainRemoteJoystick().subscribe(
                        null,
                        e -> log.error("[驾驶舱->云端->车辆] remotejoystick 信箱 drain 失败: {}", e.getMessage()));
                }
            });
    }

    private reactor.core.publisher.Mono<Void> forwardCockpitToVehicleDirect(DeviceMessage message) {
        if (state != ParallelDrivingSessionState.ACTIVE) {
            return reactor.core.publisher.Mono.error(
                new IllegalStateException("房间状态不正确: " + state)
            );
        }
        
        if (vehicleDevice == null && deviceRegistry == null) {
            return Mono.error(
                new IllegalStateException("车辆设备未初始化")
            );
        }
        
        // 根本解决方案：不依赖 copy()，直接创建新消息，确保所有字段都被正确设置
        DeviceMessage forwarded;
        
        if (message instanceof org.jetlinks.core.message.function.FunctionInvokeMessage) {
            org.jetlinks.core.message.function.FunctionInvokeMessage originalFuncMsg = 
                (org.jetlinks.core.message.function.FunctionInvokeMessage) message;
            
            // 获取原消息的所有关键信息
            String functionId = originalFuncMsg.getFunctionId();
            String messageId = originalFuncMsg.getMessageId();
            // 若驾驶舱未设置 messageId，生成一个便于追踪与 requestId 匹配
            if (messageId == null || messageId.isEmpty()) {
                messageId = java.util.UUID.randomUUID().toString();
                log.debug("房间[{}]原消息 messageId 为空，已生成: messageId={}, functionId={}", roomId, messageId, functionId);
            }
            Long timestamp = originalFuncMsg.getTimestamp();
            java.util.List<org.jetlinks.core.message.function.FunctionParameter> inputs = originalFuncMsg.getInputs();
            Boolean forceHeader = message.getHeader(Headers.force).orElse(false);
            
            // 如果 functionId 为空，尝试从 header 中获取
            if (functionId == null || functionId.isEmpty()) {
                functionId = message.getHeader("functionId")
                    .map(String::valueOf)
                    .orElse(null);
                if (functionId == null || functionId.isEmpty()) {
                    functionId = message.getHeader("function")
                        .map(String::valueOf)
                        .orElse(null);
                }
                if (functionId != null && !functionId.isEmpty()) {
                    log.warn("房间[{}]从 header 恢复 functionId: functionId={}, messageId={}", 
                        roomId, functionId, messageId);
                } else {
                    log.error("房间[{}]❌ 原消息 functionId 为空且 header 中也没有！messageId={}, 原functionId={}", 
                        roomId, messageId, originalFuncMsg.getFunctionId());
                }
            }
            
            if ("remotejoystick".equals(functionId)) {
                log.debug("[驾驶舱->云端->车辆] 房间[{}]创建新 FunctionInvokeMessage: functionId={}, messageId={}, deviceId={}->{}, force={}", 
                    roomId, functionId, messageId, message.getDeviceId(), vehicleDeviceId, forceHeader);
            } else {
                log.info("[驾驶舱->云端->车辆] 房间[{}]创建新 FunctionInvokeMessage: functionId={}, messageId={}, deviceId={}->{}, force={}", 
                    roomId, functionId, messageId, message.getDeviceId(), vehicleDeviceId, forceHeader);
            }
            
            // 创建新的 FunctionInvokeMessage，确保所有字段都被正确设置
            org.jetlinks.core.message.function.FunctionInvokeMessage newFuncMsg = 
                new org.jetlinks.core.message.function.FunctionInvokeMessage();
            
            // 设置关键字段
            newFuncMsg.setDeviceId(vehicleDeviceId);  // 关键：设置为车辆ID
            // 关键：如果 functionId 仍然为空，尝试从其他来源获取
            if (functionId == null || functionId.isEmpty()) {
                // 1. 尝试从 inputs 中获取（某些协议可能将 functionId 或 name 放在 inputs 中）
                if (inputs != null && !inputs.isEmpty()) {
                    for (org.jetlinks.core.message.function.FunctionParameter param : inputs) {
                        if (param != null) {
                            String paramName = param.getName();
                            Object value = param.getValue();
                            // 检查是否是 functionId 参数
                            if ("functionId".equals(paramName) && value != null) {
                                functionId = String.valueOf(value);
                                log.info("房间[{}]从 inputs 中获取 functionId: functionId={}, messageId={}", 
                                    roomId, functionId, messageId);
                                break;
                            }
                            // 检查是否是 name 参数（自定义协议消息可能使用 name 字段）
                            if ("name".equals(paramName) && value != null) {
                                String nameValue = String.valueOf(value);
                                // 验证是否是已知的功能名称
                                if ("emergencystop".equals(nameValue) || "remotejoystick".equals(nameValue)) {
                                    functionId = nameValue;
                                    log.info("房间[{}]从 inputs 的 name 字段获取 functionId: functionId={}, messageId={}", 
                                        roomId, functionId, messageId);
                                    break;
                                }
                            }
                        }
                    }
                }
                
                // 2. 如果仍然为空，尝试从 messageType header 推断（自定义协议消息）
                if ((functionId == null || functionId.isEmpty())) {
                    String messageType = message.getHeader("messageType")
                        .map(String::valueOf)
                        .orElse(null);
                    if ("emergencystop".equals(messageType) || "remotejoystick".equals(messageType)) {
                        functionId = messageType;
                        log.info("房间[{}]从 messageType header 推断 functionId: functionId={}, messageId={}", 
                            roomId, functionId, messageId);
                    }
                }
                
                // 3. 如果仍然为空，记录错误（不应该发生，因为 functionId 应该在消息中设置）
                if (functionId == null || functionId.isEmpty()) {
                    log.error("房间[{}]❌ 无法确定 functionId！messageId={}, messageType={}, inputs={}", 
                        roomId, messageId, message.getHeader("messageType").orElse(null),
                        inputs != null ? inputs.stream()
                            .map(p -> p != null ? p.getName() + "=" + p.getValue() : "null")
                            .collect(java.util.stream.Collectors.joining(", ")) : "null");
                    // 不设置默认值，让协议层或物模型验证来处理
                    // 如果必须设置，可以根据业务需求选择一个默认值，但这不是推荐的做法
                    throw new IllegalStateException("无法确定 functionId，消息格式不正确: messageId=" + messageId);
                }
            }
            newFuncMsg.setFunctionId(functionId);  // 确保 functionId 不为空
            newFuncMsg.setMessageId(messageId);
            if (timestamp != null) {
                newFuncMsg.setTimestamp(timestamp);
            }
            
            // 复制所有 inputs
            if (inputs != null && !inputs.isEmpty()) {
                for (org.jetlinks.core.message.function.FunctionParameter param : inputs) {
                    if (param != null) {
                        newFuncMsg.addInput(param.getName(), param.getValue());
                    }
                }
            }
            
            // 复制所有 headers（除了 targetDeviceId）
            java.util.Map<String, Object> headers = message.getHeaders();
            if (headers != null) {
                headers.forEach((key, value) -> {
                    if (key != null && !"targetDeviceId".equals(key) && value != null) {
                        newFuncMsg.addHeader(key, value);
                    }
                });
            }
            
            // 确保 Headers.force 被设置（跳过物模型验证）
            if (forceHeader) {
                newFuncMsg.addHeaderIfAbsent(Headers.force, true);
                log.debug("房间[{}]设置 Headers.force: messageId={}", roomId, messageId);
            }
            
            // 关键：设置 async 头，表示异步调用，不需要等待响应（避免超时）
            // 对于控制指令，通常不需要等待响应，使用异步模式可以提高性能并避免超时
            newFuncMsg.addHeaderIfAbsent(org.jetlinks.core.message.Headers.async, true);
            // remotejoystick 高频：显式告知车端不回复，从源头减少消息量（见 docs/remotejoystick无回复实现方案.md）
            if ("remotejoystick".equals(functionId)) {
                newFuncMsg.addHeader("noReply", true);
                newFuncMsg.addHeaderIfAbsent(org.jetlinks.core.message.Headers.sendAndForget, true);
            }
            log.debug("房间[{}]设置 Headers.async: messageId={}", roomId, messageId);
            
            // 添加房间相关的 headers
            if (cockpitDeviceId != null) {
                newFuncMsg.addHeader("sourceDeviceId", cockpitDeviceId);
            }
            newFuncMsg.addHeader("sourceType", "cockpit");
            if (roomId != null) {
                newFuncMsg.addHeader("roomId", roomId);
            }
            newFuncMsg.addHeader("forwardTime", System.currentTimeMillis());
            
            // 在消息头中添加 functionId（兼容性处理）
            if (functionId != null && !functionId.isEmpty()) {
                newFuncMsg.addHeader("functionId", functionId);
                newFuncMsg.addHeader("function", functionId);
            }
            
            // 关键：设置 requestId 和 requestMessageId，确保车辆回复时能够匹配
            // requestId 应该等于 messageId，这样车辆回复时可以通过 requestMessageId 匹配
            if (messageId != null && !messageId.isEmpty()) {
                newFuncMsg.addHeader("requestId", messageId);
                newFuncMsg.addHeader("requestMessageId", messageId);
                log.debug("房间[{}]设置 requestId header: requestId={}, messageId={}", 
                    roomId, messageId, messageId);
            }
            
            forwarded = newFuncMsg;
            
            // 验证最终状态
            String finalFunctionId = newFuncMsg.getFunctionId();
            if ("remotejoystick".equals(finalFunctionId)) {
                log.debug("房间[{}]新消息创建完成: messageId={}, functionId={}, deviceId={}, force={}", 
                    roomId, messageId, finalFunctionId, newFuncMsg.getDeviceId(), 
                    newFuncMsg.getHeader(Headers.force).orElse(false));
            } else {
                log.info("房间[{}]新消息创建完成: messageId={}, functionId={}, deviceId={}, force={}", 
                    roomId, messageId, finalFunctionId, newFuncMsg.getDeviceId(), 
                    newFuncMsg.getHeader(Headers.force).orElse(false));
            }
            
            if (finalFunctionId == null || finalFunctionId.isEmpty()) {
                log.error("房间[{}]❌ 新消息 functionId 仍然为空！messageId={}", roomId, messageId);
            }
        } else {
            // 对于非 FunctionInvokeMessage，使用 copy() 并设置目标设备ID
            forwarded = message.copy();
            forwarded.removeHeader("targetDeviceId");
            forwarded.addHeader("targetDeviceId", vehicleDeviceId);
            forwarded.addHeader("sourceDeviceId", cockpitDeviceId);
            forwarded.addHeader("sourceType", "cockpit");
            forwarded.addHeader("roomId", roomId);
            forwarded.addHeader("forwardTime", System.currentTimeMillis());
            log.warn("房间[{}]转发非 FunctionInvokeMessage 消息，通过 header 设置目标设备ID: deviceId={}", 
                roomId, vehicleDeviceId);
        }
        
        // 查询车辆设备的加密状态（用于日志和监控）
        // 注意：实际的加密/解密由协议编解码器自动处理，这里只是查询状态用于日志
        if (encryptionService != null) {
            encryptionService.isEncryptionSupported(vehicleDeviceId)
                .doOnNext(enabled -> {
                    if (enabled) {
                        log.debug("房间[{}]转发消息（加密模式）: cockpit -> vehicle, messageId={}", 
                            roomId, forwarded.getMessageId());
                    } else {
                        log.debug("房间[{}]转发消息（明文模式）: cockpit -> vehicle, messageId={}", 
                            roomId, forwarded.getMessageId());
                    }
                })
                .subscribe(); // 异步查询，不阻塞消息转发
        }
        
        // 最终验证：确保 FunctionInvokeMessage 的 functionId 不为空
        if (forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessage) {
            org.jetlinks.core.message.function.FunctionInvokeMessage finalFuncMsg = 
                (org.jetlinks.core.message.function.FunctionInvokeMessage) forwarded;
            String finalFunctionId = finalFuncMsg.getFunctionId();
            if (finalFunctionId == null || finalFunctionId.isEmpty()) {
                log.error("房间[{}]❌ 发送前 functionId 仍然为空！messageId={}, 尝试从 header 恢复", 
                    roomId, finalFuncMsg.getMessageId());
                // 尝试从 header 恢复
                String headerFunctionId = finalFuncMsg.getHeader("functionId")
                    .map(String::valueOf)
                    .orElse(null);
                if (headerFunctionId != null && !headerFunctionId.isEmpty()) {
                    finalFuncMsg.setFunctionId(headerFunctionId);
                    log.warn("房间[{}]从 header 恢复 functionId: functionId={}", roomId, headerFunctionId);
                }
            }
            // 记录最终状态；remotejoystick 高频（约100ms/条），使用 DEBUG 避免日志 I/O 影响吞吐
            if ("remotejoystick".equals(finalFuncMsg.getFunctionId())) {
                log.debug("[驾驶舱->云端->车辆] 房间[{}]准备发送消息: messageId={}, functionId={}, deviceId={}, force={}",
                    roomId, finalFuncMsg.getMessageId(), finalFuncMsg.getFunctionId(),
                    finalFuncMsg.getDeviceId(), finalFuncMsg.getHeader(Headers.force).orElse(false));
            } else {
                log.info("[驾驶舱->云端->车辆] 房间[{}]准备发送消息: messageId={}, functionId={}, deviceId={}, force={}",
                    roomId, finalFuncMsg.getMessageId(), finalFuncMsg.getFunctionId(),
                    finalFuncMsg.getDeviceId(), finalFuncMsg.getHeader(Headers.force).orElse(false));
            }
        }
        
        // 每次转发时从注册表重新获取车辆设备，避免车端重启后使用过期会话导致收不到消息
        // remotejoystick 高频：缓存 device 1 秒，减少 getDevice 调用
        boolean isRemoteJoystick = forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessage
            && "remotejoystick".equals(((org.jetlinks.core.message.function.FunctionInvokeMessage) forwarded).getFunctionId());
        Mono<DeviceOperator> deviceSource;
        if (isRemoteJoystick && (System.currentTimeMillis() - cachedVehicleDeviceTime) < DEVICE_CACHE_TTL_MS) {
            DeviceOperator cached = cachedVehicleDevice.get();
            deviceSource = cached != null ? Mono.just(cached) : fetchAndCacheDevice();
        } else {
            deviceSource = fetchAndCacheDevice();
        }
        
        // 如果是 FunctionInvokeMessage 且设置了 async 头，使用 sendAndForget 避免超时
        // 否则使用 send() 等待响应
        if (forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessage) {
            org.jetlinks.core.message.function.FunctionInvokeMessage funcMsg = 
                (org.jetlinks.core.message.function.FunctionInvokeMessage) forwarded;
            Boolean isAsync = funcMsg.getHeader(org.jetlinks.core.message.Headers.async).orElse(false);
            
            if (isAsync) {
                // 异步模式：不等待响应，避免超时
                String requestId = funcMsg.getHeader("requestId")
                    .map(String::valueOf)
                    .orElse("null");
                String requestMessageId = funcMsg.getHeader("requestMessageId")
                    .map(String::valueOf)
                    .orElse("null");
                if ("remotejoystick".equals(funcMsg.getFunctionId())) {
                    log.debug("[驾驶舱->云端->车辆] 房间[{}]准备发送消息（异步）: messageId={}, functionId={}, requestId={}, requestMessageId={}",
                        roomId, forwarded.getMessageId(), funcMsg.getFunctionId(), requestId, requestMessageId);
                } else {
                    log.info("[驾驶舱->云端->车辆] 房间[{}]准备发送消息（异步）: messageId={}, functionId={}, requestId={}, requestMessageId={}",
                        roomId, forwarded.getMessageId(), funcMsg.getFunctionId(), requestId, requestMessageId);
                }
                
                return deviceSource.flatMap(device -> device.messageSender().sendAndForget(forwarded))
                    .doOnSuccess(v -> {
                        cockpitToVehicleMessages.incrementAndGet();
                        lastActiveTime = System.currentTimeMillis();
                        if ("remotejoystick".equals(funcMsg.getFunctionId())) {
                            log.debug("[驾驶舱->云端->车辆] 房间[{}]转发消息成功（异步）: messageId={}, functionId={}, requestId={}, requestMessageId={}",
                                roomId, forwarded.getMessageId(), funcMsg.getFunctionId(), requestId, requestMessageId);
                        } else {
                            log.info("[驾驶舱->云端->车辆] 房间[{}]转发消息成功（异步）: messageId={}, functionId={}, requestId={}, requestMessageId={}",
                                roomId, forwarded.getMessageId(), funcMsg.getFunctionId(), requestId, requestMessageId);
                        }
                    })
                    .doOnError(error -> {
                        if (isRemoteJoystick) cachedVehicleDevice.set(null);  // 发送失败时清除缓存
                        log.error("[驾驶舱->云端->车辆] 房间[{}]转发消息失败: messageId={}, functionId={}, requestId={}, requestMessageId={}, error={}", 
                            roomId, forwarded.getMessageId(), funcMsg.getFunctionId(), requestId, requestMessageId, error.getMessage());
                    });
            }
        }
        
        // 同步模式：等待响应（可能超时）
        return deviceSource.flatMapMany(device -> device.messageSender().send(forwarded))
            .doOnNext(reply -> {
                cockpitToVehicleMessages.incrementAndGet();
                lastActiveTime = System.currentTimeMillis();
                log.info("[驾驶舱->云端->车辆] 房间[{}]转发消息成功: messageId={}, reply={}", 
                    roomId, forwarded.getMessageId(), reply instanceof DeviceMessage 
                        ? ((DeviceMessage) reply).getMessageType() : String.valueOf(reply));
            })
            .doOnError(error -> log.error("[驾驶舱->云端->车辆] 房间[{}]转发消息失败: messageId={}", 
                roomId, forwarded.getMessageId(), error))
            .then();
    }
    
    /**
     * 转发车辆消息到驾驶舱
     *
     * @param message 设备消息
     * @return Mono<Void>
     */
    public reactor.core.publisher.Mono<Void> forwardVehicleToCockpit(DeviceMessage message) {
        if (state != ParallelDrivingSessionState.ACTIVE) {
            return reactor.core.publisher.Mono.error(
                new IllegalStateException("房间状态不正确: " + state)
            );
        }
        
        if (cockpitDevice == null) {
            return reactor.core.publisher.Mono.error(
                new IllegalStateException("驾驶舱设备未初始化")
            );
        }
        
        // 检查消息类型（在 copy() 之前检查，确保能正确识别）
        boolean isCustomProtocol = message.getHeader("customProtocol")
            .map(v -> Boolean.parseBoolean(String.valueOf(v)))
            .orElse(false);
        boolean isFunctionReply = message instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply;
        boolean isReportProperty = message instanceof org.jetlinks.core.message.property.ReportPropertyMessage;
        
        // 如果是 FunctionInvokeMessageReply，记录原始 functionId 和 requestId（在 copy() 之前）
        String originalFunctionId = null;
        String originalRequestId = null;
        if (isFunctionReply) {
            org.jetlinks.core.message.function.FunctionInvokeMessageReply originalReply = 
                (org.jetlinks.core.message.function.FunctionInvokeMessageReply) message;
            originalFunctionId = originalReply.getFunctionId();
            // requestId 通过 header 获取
            originalRequestId = originalReply.getHeader("requestMessageId")
                .or(() -> originalReply.getHeader("requestId"))
                .map(String::valueOf)
                .orElse(null);
            log.debug("房间[{}]转发前检查 FunctionInvokeMessageReply: functionId={}, requestId={}, messageId={}, success={}", 
                roomId, originalFunctionId, originalRequestId, message.getMessageId(), originalReply.isSuccess());
        }
        
        DeviceMessage forwarded = message.copy();
        
        // 如果是 FunctionInvokeMessageReply，确保 functionId 和 requestId 被正确保留
        if (isFunctionReply && forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply) {
            org.jetlinks.core.message.function.FunctionInvokeMessageReply forwardedReply = 
                (org.jetlinks.core.message.function.FunctionInvokeMessageReply) forwarded;
            
            // 如果 copy() 后 functionId 丢失，从原始消息恢复
            if ((forwardedReply.getFunctionId() == null || forwardedReply.getFunctionId().isEmpty()) 
                && originalFunctionId != null && !originalFunctionId.isEmpty()) {
                forwardedReply.setFunctionId(originalFunctionId);
                log.warn("房间[{}]copy() 后 functionId 丢失，已恢复: functionId={}, messageId={}", 
                    roomId, originalFunctionId, forwarded.getMessageId());
            }
            
            // 如果 copy() 后 requestId header 丢失，从原始消息恢复
            String forwardedRequestId = forwardedReply.getHeader("requestMessageId")
                .or(() -> forwardedReply.getHeader("requestId"))
                .map(String::valueOf)
                .orElse(null);
            if ((forwardedRequestId == null || forwardedRequestId.isEmpty()) 
                && originalRequestId != null && !originalRequestId.isEmpty()) {
                forwardedReply.addHeader("requestMessageId", originalRequestId);
                forwardedReply.addHeader("requestId", originalRequestId);
                log.warn("房间[{}]copy() 后 requestId header 丢失，已恢复: requestId={}, messageId={}", 
                    roomId, originalRequestId, forwarded.getMessageId());
            }
            
            // 验证最终状态
            String finalRequestId = forwardedReply.getHeader("requestMessageId")
                .or(() -> forwardedReply.getHeader("requestId"))
                .map(String::valueOf)
                .orElse("null");
            log.debug("房间[{}]copy() 后 FunctionInvokeMessageReply 状态: functionId={}, requestId={}, messageId={}, success={}", 
                roomId, forwardedReply.getFunctionId(), finalRequestId, 
                forwarded.getMessageId(), forwardedReply.isSuccess());
        }
        
        // 关键：确保消息的 deviceId 设置为驾驶舱ID（用于协议编解码器识别目标设备）
        // 注意：虽然通过 cockpitDevice.messageSender() 发送会自动设置，但为了确保协议编解码器正确识别，显式设置
        forwarded.thingId(org.jetlinks.core.device.DeviceThingType.device, cockpitDeviceId);
        
        forwarded.addHeader("sourceDeviceId", vehicleDeviceId);
        forwarded.addHeader("sourceType", "vehicle");
        forwarded.addHeader("roomId", roomId);
        forwarded.addHeader("forwardTime", System.currentTimeMillis());
        
        log.debug("[车辆->云端->驾驶舱] 房间[{}]转发消息前设置 deviceId: 原deviceId={} -> 新deviceId={}, messageId={}", 
            roomId, message.getDeviceId(), cockpitDeviceId, forwarded.getMessageId());
        
        // 关键：需要转发给驾驶舱的消息类型：
        // 1. 自定义消息响应（如 emergencystopresp）- 使用异步方式
        // 2. 标准功能调用回复（FunctionInvokeMessageReply）- 使用同步方式，让驾驶舱知道控制指令的执行结果
        // 
        // 不需要转发：
        // - 车辆状态上报（ReportPropertyMessage）- 只用于 WebSocket 推送到前端
        
        // 不转发状态上报
        if (isReportProperty) {
            log.debug("房间[{}]不转发状态上报: vehicle -> cockpit, messageId={}", 
                roomId, forwarded.getMessageId());
            return reactor.core.publisher.Mono.empty();
        }
        
        // 1. 自定义消息响应：使用异步方式（sendAndForget），因为它是响应消息，不需要等待回复
        if (isCustomProtocol && isFunctionReply) {
            return cockpitDevice.messageSender()
                .sendAndForget(forwarded)
                .doOnSuccess(v -> {
                    vehicleToCockpitMessages.incrementAndGet();
                    lastActiveTime = System.currentTimeMillis();
                    log.debug("房间[{}]转发自定义消息响应成功（异步）: vehicle -> cockpit, messageId={}, type={}", 
                        roomId, forwarded.getMessageId(), forwarded.getMessageType());
                })
                .doOnError(error -> log.error("房间[{}]转发消息失败: vehicle -> cockpit, messageId={}", 
                    roomId, forwarded.getMessageId(), error));
        }
        
        // 2a. 车辆上报的 remotejoystick（FunctionInvokeMessage）：使用 sendAndForget，数据流无需等待回复
        if (forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessage) {
            org.jetlinks.core.message.function.FunctionInvokeMessage funcMsg = 
                (org.jetlinks.core.message.function.FunctionInvokeMessage) forwarded;
            if ("remotejoystick".equals(funcMsg.getFunctionId())) {
                log.info("[车辆->云端->驾驶舱] 房间[{}]准备转发 remotejoystick 给驾驶舱: vehicle={} -> cockpit={}, messageId={}", 
                    roomId, vehicleDeviceId, cockpitDeviceId, forwarded.getMessageId());
                return cockpitDevice.messageSender()
                    .sendAndForget(forwarded)
                    .doOnSuccess(v -> {
                        vehicleToCockpitMessages.incrementAndGet();
                        lastActiveTime = System.currentTimeMillis();
                        log.info("[车辆->云端->驾驶舱] 房间[{}]转发 remotejoystick 成功: vehicle={} -> cockpit={}", 
                            roomId, vehicleDeviceId, cockpitDeviceId);
                    })
                    .doOnError(error -> log.error("[车辆->云端->驾驶舱] 房间[{}]转发 remotejoystick 失败: messageId={}", 
                        roomId, forwarded.getMessageId(), error));
            }
        }
        
        // 2b. 标准功能调用回复：使用异步方式（sendAndForget），因为这是回包，不需要等待响应
        // 使用异步方式可以避免设备离线时抛出异常，确保回包能够发送到驾驶舱
        // 这样驾驶舱的回调函数（on_invoke_reply）才能被调用
        if (isFunctionReply) {
            String functionId = forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply 
                ? ((org.jetlinks.core.message.function.FunctionInvokeMessageReply) forwarded).getFunctionId() 
                : "unknown";
            boolean success = forwarded instanceof org.jetlinks.core.message.function.FunctionInvokeMessageReply 
                ? ((org.jetlinks.core.message.function.FunctionInvokeMessageReply) forwarded).isSuccess() 
                : false;
            
            log.info("[车辆->云端->驾驶舱] 房间[{}]准备转发功能调用回复给驾驶舱: vehicle={} -> cockpit={}, functionId={}, messageId={}, success={}", 
                roomId, vehicleDeviceId, cockpitDeviceId, functionId, forwarded.getMessageId(), success);
            
            return cockpitDevice.messageSender()
                .sendAndForget(forwarded)
                .doOnSuccess(v -> {
                    vehicleToCockpitMessages.incrementAndGet();
                    lastActiveTime = System.currentTimeMillis();
                    log.info("[车辆->云端->驾驶舱] 房间[{}]转发功能调用回复成功（异步）: vehicle={} -> cockpit={}, functionId={}, messageId={}, success={}", 
                        roomId, vehicleDeviceId, cockpitDeviceId, functionId, forwarded.getMessageId(), success);
                })
                .doOnError(error -> log.error("[车辆->云端->驾驶舱] 房间[{}]转发功能调用回复失败: vehicle={} -> cockpit={}, functionId={}, messageId={}", 
                    roomId, vehicleDeviceId, cockpitDeviceId, functionId, forwarded.getMessageId(), error));
        }
        
        // 其他消息类型，使用 send() 同步方式等待响应
        return cockpitDevice.messageSender()
            .send(forwarded)
            .doOnNext(reply -> {
                vehicleToCockpitMessages.incrementAndGet();
                lastActiveTime = System.currentTimeMillis();
                log.debug("房间[{}]转发消息成功: vehicle -> cockpit, messageId={}, reply={}", 
                    roomId, forwarded.getMessageId(), reply != null ? reply.getMessageType() : "null");
            })
            .doOnError(error -> log.error("房间[{}]转发消息失败: vehicle -> cockpit, messageId={}", 
                roomId, forwarded.getMessageId(), error))
            .then();
    }
    
    /**
     * 检查房间是否活跃
     *
     * @return 是否活跃
     */
    public boolean isActive() {
        return state == ParallelDrivingSessionState.ACTIVE &&
               cockpitDevice != null &&
               vehicleDevice != null;
    }
    
    /**
     * 关闭房间
     *
     * @return Mono<Void>
     */
    public reactor.core.publisher.Mono<Void> close() {
        this.state = ParallelDrivingSessionState.RELEASED;
        log.info("房间[{}]已关闭: cockpit={}, vehicle={}, " +
                "cockpitToVehicle={}, vehicleToCockpit={}", 
            roomId, cockpitDeviceId, vehicleDeviceId,
            cockpitToVehicleMessages.get(), vehicleToCockpitMessages.get());
        return reactor.core.publisher.Mono.empty();
    }
    
    /**
     * 更新最后活动时间
     */
    public void touch() {
        this.lastActiveTime = System.currentTimeMillis();
    }
}
