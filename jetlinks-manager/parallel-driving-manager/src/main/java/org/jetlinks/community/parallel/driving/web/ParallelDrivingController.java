package org.jetlinks.community.parallel.driving.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryOperation;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.QueryAction;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.ResourceAction;
import org.hswebframework.web.exception.NotFoundException;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingRelationService;
import org.jetlinks.community.relation.entity.RelatedEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 并行驾驶控制器
 * 提供驾驶舱-车端绑定管理的REST API
 *
 * @author JetLinks
 */
@RestController
@RequestMapping("/parallel-driving")
@Tag(name = "并行驾驶管理")
@Authorize
@Resource(id = "parallel-driving", name = "并行驾驶管理")
@AllArgsConstructor
public class ParallelDrivingController {

    private final ParallelDrivingRelationService relationService;

    /**
     * 绑定驾驶舱到车端（一对一）
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return Mono<Void>
     */
    @PostMapping("/bind")
    @Operation(summary = "绑定驾驶舱到车端（一对一）")
    @ResourceAction(id = "bind", name = "绑定")
    public Mono<Void> bind(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车端设备ID") @RequestParam String vehicleDeviceId
    ) {
        return relationService.bind(cockpitDeviceId, vehicleDeviceId);
    }

    /**
     * 解绑
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return Mono<Void>
     */
    @DeleteMapping("/unbind")
    @Operation(summary = "解绑驾驶舱和车端")
    @ResourceAction(id = "unbind", name = "解绑")
    public Mono<Void> unbind(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车端设备ID") @RequestParam String vehicleDeviceId
    ) {
        return relationService.unbind(cockpitDeviceId, vehicleDeviceId);
    }

    /**
     * 查询驾驶舱绑定的车（完整信息）
     *
     * @param cockpitId 驾驶舱设备ID
     * @return 绑定关系详情
     */
    @GetMapping("/cockpit/{cockpitId}/vehicle")
    @Operation(summary = "查询驾驶舱绑定的车")
    @ResourceAction(id = "query", name = "查询")
    public Mono<RelatedEntity> getBoundVehicle(
        @Parameter(description = "驾驶舱设备ID") @PathVariable String cockpitId
    ) {
        return relationService.getBoundVehicleDetail(cockpitId)
            .switchIfEmpty(Mono.error(new NotFoundException("未绑定车辆")));
    }

    /**
     * 查询车被哪个驾驶舱绑定（完整信息）
     *
     * @param vehicleId 车端设备ID
     * @return 绑定关系详情
     */
    @GetMapping("/vehicle/{vehicleId}/cockpit")
    @Operation(summary = "查询车被哪个驾驶舱绑定")
    @ResourceAction(id = "query", name = "查询")
    public Mono<RelatedEntity> getBoundCockpit(
        @Parameter(description = "车端设备ID") @PathVariable String vehicleId
    ) {
        return relationService.getBoundCockpitDetail(vehicleId)
            .switchIfEmpty(Mono.error(new NotFoundException("未被绑定")));
    }

    /**
     * 检查控制权限
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return 是否有权限
     */
    @GetMapping("/permission/check")
    @Operation(summary = "检查控制权限")
    @ResourceAction(id = "query", name = "查询")
    public Mono<Boolean> checkPermission(
        @Parameter(description = "驾驶舱设备ID") @RequestParam String cockpitDeviceId,
        @Parameter(description = "车端设备ID") @RequestParam String vehicleDeviceId
    ) {
        return relationService.checkControlPermission(cockpitDeviceId, vehicleDeviceId);
    }

    /**
     * 查询绑定关系列表（GET）
     *
     * @param queryParam 查询参数
     * @return 分页结果
     */
    @GetMapping("/bind/_query")
    @QueryAction
    @QueryOperation(summary = "查询绑定关系列表")
    @ResourceAction(id = "query", name = "查询")
    public Mono<PagerResult<RelatedEntity>> queryBindRelations(
        @Parameter(hidden = true) QueryParamEntity queryParam
    ) {
        return relationService.queryBindRelations(queryParam);
    }

    /**
     * 查询绑定关系列表（POST）
     *
     * @param queryParam 查询参数
     * @return 分页结果
     */
    @PostMapping("/bind/_query")
    @QueryAction
    @Operation(summary = "(POST)查询绑定关系列表")
    @ResourceAction(id = "query", name = "查询")
    public Mono<PagerResult<RelatedEntity>> queryBindRelationsPost(
        @RequestBody Mono<QueryParamEntity> queryParam
    ) {
        return queryParam.flatMap(relationService::queryBindRelations);
    }
}

