package org.jetlinks.community.parallel.driving.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.ResourceAction;
import org.jetlinks.community.parallel.driving.message.ParallelDrivingControlMessage;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingControlService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 平行驾驶控制指令控制器
 * 提供控制指令发送的 REST API
 *
 * @author JetLinks
 */
@RestController
@RequestMapping("/parallel-driving/control")
@Tag(name = "平行驾驶控制")
@Authorize
@Resource(id = "parallel-driving", name = "平行驾驶管理")
@AllArgsConstructor
public class ParallelDrivingControlController {
    
    private final ParallelDrivingControlService controlService;
    
    /**
     * 发送控制指令
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车辆设备ID
     * @param controlType 控制类型
     * @param params 控制参数
     * @return Mono<Void>
     */
    @PostMapping("/command")
    @Operation(summary = "发送控制指令")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> sendControlCommand(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId,
        @Parameter(description = "控制类型") @RequestParam String controlType,
        @Parameter(description = "控制参数") @RequestBody(required = false) Map<String, Object> params
    ) {
        ParallelDrivingControlMessage.ControlType type;
        try {
            type = ParallelDrivingControlMessage.ControlType.valueOf(controlType.toUpperCase());
        } catch (IllegalArgumentException e) {
            return Mono.error(new IllegalArgumentException("不支持的控制类型: " + controlType));
        }
        
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(type);
        if (params != null) {
            message.setControlParams(params);
        }
        
        return controlService.sendControlCommand(cockpitDeviceId, vehicleDeviceId, message);
    }
    
    /**
     * 转向控制
     */
    @PostMapping("/steering")
    @Operation(summary = "转向控制")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> steering(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId,
        @Parameter(description = "转向角度（度）") @RequestParam double angle
    ) {
        return controlService.steering(cockpitDeviceId, vehicleDeviceId, angle);
    }
    
    /**
     * 加速控制
     */
    @PostMapping("/accelerator")
    @Operation(summary = "加速控制")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> accelerator(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId,
        @Parameter(description = "加速值（0-1）") @RequestParam double value
    ) {
        return controlService.accelerator(cockpitDeviceId, vehicleDeviceId, value);
    }
    
    /**
     * 制动控制
     */
    @PostMapping("/brake")
    @Operation(summary = "制动控制")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> brake(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId,
        @Parameter(description = "制动力（0-1）") @RequestParam double value
    ) {
        return controlService.brake(cockpitDeviceId, vehicleDeviceId, value);
    }
    
    /**
     * 档位控制
     */
    @PostMapping("/gear")
    @Operation(summary = "档位控制")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> gear(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId,
        @Parameter(description = "档位（1-5）") @RequestParam int gear
    ) {
        return controlService.gear(cockpitDeviceId, vehicleDeviceId, gear);
    }
    
    /**
     * 紧急停车
     */
    @PostMapping("/emergency-stop")
    @Operation(summary = "紧急停车")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> emergencyStop(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId
    ) {
        return controlService.emergencyStop(cockpitDeviceId, vehicleDeviceId);
    }
    
    /**
     * 设置速度
     */
    @PostMapping("/set-speed")
    @Operation(summary = "设置速度")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Void> setSpeed(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam String vehicleDeviceId,
        @Parameter(description = "目标速度（km/h）") @RequestParam double speed
    ) {
        return controlService.setSpeed(cockpitDeviceId, vehicleDeviceId, speed);
    }
}
