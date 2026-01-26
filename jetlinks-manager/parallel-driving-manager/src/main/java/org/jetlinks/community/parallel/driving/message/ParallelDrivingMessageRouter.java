package org.jetlinks.community.parallel.driving.message;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.exception.AccessDenyException;
import org.hswebframework.web.exception.NotFoundException;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingRelationService;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.DeviceMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * 并行驾驶消息路由器
 * 负责驾驶舱-云端-车端的消息路由和转发
 *
 * @author JetLinks
 */
@Component
@Slf4j
public class ParallelDrivingMessageRouter {

    private final DeviceRegistry deviceRegistry;
    private final ParallelDrivingRelationService relationService;
    private final EventBus eventBus;

    @Autowired
    public ParallelDrivingMessageRouter(DeviceRegistry deviceRegistry,
                                        ParallelDrivingRelationService relationService,
                                        @Qualifier("eventBus") EventBus eventBus) {
        this.deviceRegistry = deviceRegistry;
        this.relationService = relationService;
        this.eventBus = eventBus;
    }

    private Disposable disposable;

    /**
     * 初始化消息订阅
     */
    @PostConstruct
    public void init() {
        // 订阅驾驶舱发送的控制指令（下行消息）
        Subscription cockpitSubscription = Subscription
            .builder()
            .subscriberId("parallel-driving-cockpit-router")
            .topics("/device/*/*/message/downstream")
            .broker()
            .local()
            .build();

        // 订阅车端上报的状态（上行消息）
        Subscription vehicleSubscription = Subscription
            .builder()
            .subscriberId("parallel-driving-vehicle-router")
            .topics("/device/*/*/message/upstream")
            .broker()
            .local()
            .build();

        disposable = Flux.merge(
            eventBus.subscribe(cockpitSubscription, DeviceMessage.class)
                .flatMap(this::handleCockpitControlMessage)
                .onErrorContinue((error, obj) -> log.error("处理驾驶舱消息失败", error)),
            eventBus.subscribe(vehicleSubscription, DeviceMessage.class)
                .flatMap(this::handleVehicleStatusMessage)
                .onErrorContinue((error, obj) -> log.error("处理车端消息失败", error))
        ).subscribe();
    }

    @PreDestroy
    public void destroy() {
        if (disposable != null) {
            disposable.dispose();
        }
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

        // 如果没有 targetDeviceId，说明不是并行驾驶控制消息，直接返回
        if (!StringUtils.hasText(targetVehicleId)) {
            return Mono.empty();
        }

        log.debug("收到驾驶舱控制指令: cockpit={}, targetVehicle={}", cockpitDeviceId, targetVehicleId);

        // 1. 验证权限
        return relationService.checkControlPermission(cockpitDeviceId, targetVehicleId)
            .filter(Boolean::booleanValue)
            .switchIfEmpty(Mono.error(new AccessDenyException("驾驶舱[" + cockpitDeviceId + "]无权限控制车端[" + targetVehicleId + "]")))
            // 2. 路由到车端
            .then(routeToVehicle(cockpitDeviceId, targetVehicleId, message))
            .doOnSuccess(v -> log.info("驾驶舱控制指令路由成功: cockpit={}, vehicle={}", cockpitDeviceId, targetVehicleId))
            .doOnError(error -> log.error("驾驶舱控制指令路由失败: cockpit={}, vehicle={}", cockpitDeviceId, targetVehicleId, error))
            .onErrorResume(error -> {
                log.error("处理驾驶舱控制指令失败", error);
                return Mono.empty(); // 忽略错误，避免影响其他消息处理
            });
    }

    /**
     * 处理车端上报的状态
     * 自动转发到绑定的驾驶舱
     */
    public Mono<Void> handleVehicleStatusMessage(DeviceMessage message) {
        String vehicleDeviceId = message.getDeviceId();

        log.debug("收到车端状态上报: vehicle={}", vehicleDeviceId);

        // 1. 查找绑定的驾驶舱（一对一，只有一个）
        return relationService.getBoundCockpit(vehicleDeviceId)
            .switchIfEmpty(Mono.empty())  // 如果没有绑定，忽略消息
            // 2. 转发到绑定的驾驶舱
            .flatMap(cockpitDeviceId -> 
                routeToCockpit(vehicleDeviceId, cockpitDeviceId, message)
            )
            .doOnSuccess(v -> log.debug("车端状态转发成功: vehicle={}", vehicleDeviceId))
            .doOnError(error -> log.error("车端状态转发失败: vehicle={}", vehicleDeviceId, error))
            .onErrorResume(error -> {
                log.error("处理车端状态消息失败", error);
                return Mono.empty(); // 忽略错误，避免影响其他消息处理
            });
    }

    /**
     * 路由消息到车端
     */
    private Mono<Void> routeToVehicle(String cockpitId, String vehicleId, DeviceMessage message) {
        return deviceRegistry.getDevice(vehicleId)
            .switchIfEmpty(Mono.error(new NotFoundException("车端设备不存在: " + vehicleId)))
            .flatMap(vehicle -> {
                // 构造转发消息
                DeviceMessage forwardedMessage = createForwardedMessage(message, cockpitId, "cockpit");
                
                // 发送到车端
                return vehicle.messageSender()
                    .send(forwardedMessage)
                    .then();
            });
    }

    /**
     * 路由消息到驾驶舱
     */
    private Mono<Void> routeToCockpit(String vehicleId, String cockpitId, DeviceMessage message) {
        return deviceRegistry.getDevice(cockpitId)
            .switchIfEmpty(Mono.empty()) // 驾驶舱离线时忽略
            .flatMap(cockpit -> {
                // 构造转发消息
                DeviceMessage forwardedMessage = createForwardedMessage(message, vehicleId, "vehicle");
                
                // 发送到驾驶舱
                return cockpit.messageSender()
                    .send(forwardedMessage)
                    .then();
            });
    }

    /**
     * 创建转发消息
     */
    private DeviceMessage createForwardedMessage(DeviceMessage original, String sourceDeviceId, String sourceType) {
        DeviceMessage forwarded = original.copy();
        
        // 添加转发相关的消息头
        forwarded.addHeader("sourceDeviceId", sourceDeviceId);
        forwarded.addHeader("sourceType", sourceType);
        forwarded.addHeader("forwarded", true);
        forwarded.addHeader("forwardTime", System.currentTimeMillis());
        
        return forwarded;
    }
}

