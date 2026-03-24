package org.jetlinks.community.parallel.driving.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.ResourceAction;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingControlLogService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 平行驾驶控制日志控制器
 * 提供控制日志查询和统计的 REST API
 *
 * @author JetLinks
 */
@RestController
@RequestMapping("/parallel-driving/control")
@Tag(name = "平行驾驶控制日志")
@Authorize
@Resource(id = "parallel-driving-control-log", name = "平行驾驶控制日志")
@AllArgsConstructor
public class ParallelDrivingControlLogController {
    
    private final ParallelDrivingControlLogService logService;
    
    /**
     * 获取控制日志
     *
     * @param cockpitDeviceId 驾驶舱设备ID（可选）
     * @param vehicleDeviceId 车辆设备ID（可选）
     * @param limit 限制数量（默认100）
     * @return 控制日志列表
     */
    @GetMapping("/logs")
    @Operation(summary = "获取控制日志")
    @ResourceAction(id = "view", name = "查看日志")
    public Mono<List<ParallelDrivingControlLogService.ControlLog>> getControlLogs(
        @Parameter(description = "驾驶舱设备ID") @RequestParam(required = false) String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam(required = false) String vehicleDeviceId,
        @Parameter(description = "限制数量") @RequestParam(defaultValue = "100") int limit
    ) {
        return Mono.just(logService.getControlLogs(cockpitDeviceId, vehicleDeviceId, limit));
    }
    
    /**
     * 获取控制统计信息
     *
     * @param cockpitDeviceId 驾驶舱设备ID（可选）
     * @param vehicleDeviceId 车辆设备ID（可选）
     * @return 统计信息
     */
    @GetMapping("/statistics")
    @Operation(summary = "获取控制统计信息")
    @ResourceAction(id = "view", name = "查看统计")
    public Mono<ParallelDrivingControlLogService.ControlStatistics> getControlStatistics(
        @Parameter(description = "驾驶舱设备ID") @RequestParam(required = false) String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam(required = false) String vehicleDeviceId
    ) {
        return Mono.just(logService.getStatistics(cockpitDeviceId, vehicleDeviceId));
    }
}
