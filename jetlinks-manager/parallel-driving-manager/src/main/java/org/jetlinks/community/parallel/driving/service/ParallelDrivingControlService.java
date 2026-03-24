package org.jetlinks.community.parallel.driving.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.exception.BusinessException;
import org.hswebframework.web.exception.NotFoundException;
import org.jetlinks.community.parallel.driving.entity.ParallelDrivingSession;
import org.jetlinks.community.parallel.driving.message.ParallelDrivingControlMessage;
import org.jetlinks.community.parallel.driving.room.ParallelDrivingRoom;
import org.jetlinks.community.parallel.driving.room.ParallelDrivingRoomManager;
import org.jetlinks.core.device.DeviceRegistry;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 平行驾驶控制服务
 * 负责发送控制指令到车辆
 *
 * @author JetLinks
 */
@Service
@Slf4j
@AllArgsConstructor
public class ParallelDrivingControlService {
    
    private final DeviceRegistry deviceRegistry;
    private final ParallelDrivingRelationService relationService;
    private final ParallelDrivingRoomManager roomManager;
    private final ParallelDrivingControlLogService logService;
    
    /**
     * 发送控制指令到车辆
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车辆设备ID
     * @param controlMessage 控制指令消息
     * @return Mono<Void>
     */
    public Mono<Void> sendControlCommand(String cockpitDeviceId, 
                                         String vehicleDeviceId,
                                         ParallelDrivingControlMessage controlMessage) {
        log.info("发送控制指令: cockpit={}, vehicle={}, type={}", 
            cockpitDeviceId, vehicleDeviceId, 
            controlMessage.getControlType());
        
        // 1. 验证设备存在
        return Mono.zip(
            deviceRegistry.getDevice(cockpitDeviceId)
                .switchIfEmpty(Mono.error(new NotFoundException("驾驶舱设备不存在"))),
            deviceRegistry.getDevice(vehicleDeviceId)
                .switchIfEmpty(Mono.error(new NotFoundException("车辆设备不存在")))
        )
        // 2. 验证会话状态并获取房间
        .then(Mono.zip(
            relationService.getSession(cockpitDeviceId, vehicleDeviceId)
                .switchIfEmpty(Mono.error(new BusinessException("会话不存在或未激活"))),
            roomManager.getRoom(cockpitDeviceId, vehicleDeviceId)
                .switchIfEmpty(Mono.error(new BusinessException("房间不存在或未激活")))
        ))
        .flatMap(tuple -> {
            ParallelDrivingSession session = tuple.getT1();
            ParallelDrivingRoom room = tuple.getT2();
            
            // 3. 验证会话状态
            if (!session.isActive()) {
                return Mono.error(new BusinessException("会话未激活，当前状态: " + session.getState()));
            }
            
            // 4. 设置会话信息
            controlMessage.setSessionInfo(
                session.getId(),
                room.getRoomId(),
                session.getOperatorId()
            );
            
            // 5. 设置目标设备ID
            controlMessage.setDeviceId(cockpitDeviceId);
            if (vehicleDeviceId != null) {
                controlMessage.addHeader("targetDeviceId", vehicleDeviceId);
            }
            
            // 6. 通过房间转发消息（自动处理加密）
            return room.forwardCockpitToVehicle(controlMessage);
        })
        // 7. 更新最后活动时间
        .then(relationService.updateLastActiveTime(cockpitDeviceId, vehicleDeviceId))
        // 8. 记录控制日志
        .doOnSuccess(v -> {
            log.info("控制指令发送成功: cockpit={}, vehicle={}, type={}", 
                cockpitDeviceId, vehicleDeviceId, controlMessage.getControlType());
            logService.logControlCommand(cockpitDeviceId, vehicleDeviceId, controlMessage, true, null);
        })
        .doOnError(error -> {
            log.error("控制指令发送失败: cockpit={}, vehicle={}, type={}", 
                cockpitDeviceId, vehicleDeviceId, controlMessage.getControlType(), error);
            logService.logControlCommand(cockpitDeviceId, vehicleDeviceId, controlMessage, false, 
                error.getMessage());
        });
    }
    
    /**
     * 发送转向控制指令
     */
    public Mono<Void> steering(String cockpitDeviceId, String vehicleDeviceId, double angle) {
        return sendControlCommand(cockpitDeviceId, vehicleDeviceId, 
            ParallelDrivingControlMessage.steering(angle));
    }
    
    /**
     * 发送加速控制指令
     */
    public Mono<Void> accelerator(String cockpitDeviceId, String vehicleDeviceId, double value) {
        return sendControlCommand(cockpitDeviceId, vehicleDeviceId, 
            ParallelDrivingControlMessage.accelerator(value));
    }
    
    /**
     * 发送制动控制指令
     */
    public Mono<Void> brake(String cockpitDeviceId, String vehicleDeviceId, double value) {
        return sendControlCommand(cockpitDeviceId, vehicleDeviceId, 
            ParallelDrivingControlMessage.brake(value));
    }
    
    /**
     * 发送档位控制指令
     */
    public Mono<Void> gear(String cockpitDeviceId, String vehicleDeviceId, int gear) {
        return sendControlCommand(cockpitDeviceId, vehicleDeviceId, 
            ParallelDrivingControlMessage.gear(gear));
    }
    
    /**
     * 发送紧急停车指令
     */
    public Mono<Void> emergencyStop(String cockpitDeviceId, String vehicleDeviceId) {
        return sendControlCommand(cockpitDeviceId, vehicleDeviceId, 
            ParallelDrivingControlMessage.emergencyStop());
    }
    
    /**
     * 发送设置速度指令
     */
    public Mono<Void> setSpeed(String cockpitDeviceId, String vehicleDeviceId, double speed) {
        return sendControlCommand(cockpitDeviceId, vehicleDeviceId, 
            ParallelDrivingControlMessage.setSpeed(speed));
    }
}
