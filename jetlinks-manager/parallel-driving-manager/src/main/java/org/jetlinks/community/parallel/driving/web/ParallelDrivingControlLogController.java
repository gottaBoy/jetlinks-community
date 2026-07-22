package org.jetlinks.community.parallel.driving.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.ResourceAction;
import org.jetlinks.community.parallel.driving.entity.ParallelDrivingControlLog;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingControlLogService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/parallel-driving/control")
@Tag(name = "平行驾驶控制日志")
@Authorize
@Resource(id = "parallel-driving-control-log", name = "平行驾驶控制日志")
@AllArgsConstructor
public class ParallelDrivingControlLogController {

    private final ParallelDrivingControlLogService logService;

    @GetMapping("/logs")
    @Operation(summary = "获取控制日志")
    @ResourceAction(id = "view", name = "查看日志")
    public Mono<List<ParallelDrivingControlLog>> getControlLogs(
        @Parameter(description = "驾驶舱设备ID") @RequestParam(required = false) String cockpitDeviceId,
        @Parameter(description = "车辆设备ID") @RequestParam(required = false) String vehicleDeviceId,
        @Parameter(description = "限制数量") @RequestParam(defaultValue = "100") int limit
    ) {
        return logService.getControlLogs(cockpitDeviceId, vehicleDeviceId, limit);
    }

    @GetMapping("/logs/_query")
    @Operation(summary = "分页查询控制日志")
    @ResourceAction(id = "view", name = "查看日志")
    public Mono<PagerResult<ParallelDrivingControlLog>> queryControlLogs(
        QueryParamEntity query
    ) {
        return logService.getControlLogs(query);
    }

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
