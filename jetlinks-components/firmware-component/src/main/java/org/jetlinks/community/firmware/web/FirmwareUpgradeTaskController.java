package org.jetlinks.community.firmware.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Generated;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hswebframework.ezorm.rdb.mapping.defaults.SaveResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.Authentication;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.SaveAction;
import org.hswebframework.web.crud.web.reactive.ReactiveServiceCrudController;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeStatus;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.jetlinks.community.firmware.service.FirmwareUpgradeHistoryService;
import org.jetlinks.community.firmware.service.FirmwareUpgradeTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/firmware/upgrade")
@Authorize
@Resource(id = "firmware-upgrade-task-manager", name = "固件升级任务管理")
@Tag(name = "固件升级任务管理")
public class FirmwareUpgradeTaskController implements ReactiveServiceCrudController<FirmwareUpgradeTaskEntity, String> {

    @Autowired
    @Getter
    @Generated
    private FirmwareUpgradeTaskService service;

    @Autowired
    private FirmwareUpgradeHistoryService historyService;

    @PostMapping("/task")
    @SaveAction
    @Operation(summary = "创建升级任务")
    public Mono<SaveResult> createTask(@RequestBody Flux<FirmwareUpgradeTaskEntity> payload) {
        return Authentication
                .currentReactive()
                .flatMapMany(auth -> payload.map(entity -> applyAuthentication(entity, auth)))
                .switchIfEmpty(payload)
                .collectList()
                .flatMap(entities -> {
                    if (entities.isEmpty()) {
                        return Mono.just(SaveResult.of(0, 0));
                    }
                    if (entities.size() > 1) {
                        return Mono.error(new IllegalArgumentException("每次只能创建一个升级任务"));
                    }
                    return service.createTask(entities.get(0));
                });
    }

    /**
     * 显式查询端点 — 接口默认 @PostMapping 因自定义映射冲突未注册
     */
    @PostMapping("/task/_query")
    @Operation(summary = "分页查询升级任务")
    public Mono<Object> queryTask(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q -> getService().queryPager(q));
    }

    @PostMapping("/task/_query/no-paging")
    @Operation(summary = "不分页查询升级任务")
    public Mono<Object> queryTaskNoPaging(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q -> getService().createQuery().setParam(q).fetch()
                .collectList()
                .map(list -> (Object) list));
    }

    @DeleteMapping("/task/{id}")
    @SaveAction
    @Operation(summary = "删除升级任务")
    public Mono<Integer> deleteTask(@PathVariable String id) {
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getTaskId, id)
            .fetch()
            .any(history -> !FirmwareUpgradeStatus.isTerminal(history.getStatus()))
            .flatMap(active -> active
                ? Mono.error(new IllegalStateException("任务存在进行中的设备升级，不能删除"))
                : getService().deleteById(Mono.just(id)));
    }

    @PostMapping("/task/{id}/_start")
    @SaveAction
    @Operation(summary = "启动升级任务")
    public Mono<Void> startTask(@PathVariable String id, @RequestBody List<String> deviceIds) {
        return service.startTask(id, deviceIds);
    }

    @PostMapping("/task/{id}/_stop")
    @SaveAction
    @Operation(summary = "停止升级任务")
    public Mono<Void> stopTask(@PathVariable String id, @RequestBody List<String> deviceIds) {
        return service.stopTask(id, deviceIds);
    }

    @PostMapping("/task/{id}/devices/_retry")
    @SaveAction
    @Operation(summary = "重试任务中的指定设备")
    public Mono<Void> retryDevices(@PathVariable String id,
                                   @RequestBody(required = false) List<String> deviceIds) {
        return service.startTask(id, deviceIds == null ? Collections.emptyList() : deviceIds);
    }

    @PostMapping("/task/{id}/devices/_cancel")
    @SaveAction
    @Operation(summary = "取消任务中尚未下发的指定设备")
    public Mono<Void> cancelDevices(@PathVariable String id,
                                    @RequestBody(required = false) List<String> deviceIds) {
        return service.stopTask(id, deviceIds == null ? Collections.emptyList() : deviceIds);
    }

    @PostMapping("/history/{id}/_retry")
    @SaveAction
    @Operation(summary = "重试单条设备升级记录")
    public Mono<Void> retryHistory(@PathVariable String id) {
        return service.retryHistory(id);
    }

    @PostMapping("/history/{id}/_cancel")
    @SaveAction
    @Operation(summary = "取消单条尚未下发的设备升级记录")
    public Mono<Void> cancelHistory(@PathVariable String id) {
        return service.cancelHistory(id);
    }

    /**
     * 升级历史 — 分页查询
     */
    @PostMapping("/history/_query")
    @Operation(summary = "分页查询升级历史")
    public Mono<Object> queryHistory(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q -> historyService.queryPager(q));
    }

    @PostMapping("/history/detail/_query/no-paging")
    @Operation(summary = "不分页查询升级历史明细")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Mono<Object> queryHistoryNoPaging(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q ->
            historyService.createQuery().setParam(q).fetch()
                .collectList()
                .map(list -> (Object) list)
        );
    }

    @PostMapping("/history/_count")
    @Operation(summary = "升级历史计数")
    public Mono<Integer> countHistory(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q -> historyService.createQuery().setParam(q).count());
    }

    @DeleteMapping("/history/{id}")
    @SaveAction
    @Operation(summary = "删除升级历史记录")
    public Mono<Integer> deleteHistory(@PathVariable String id) {
        return historyService
            .findById(id)
            .flatMap(history -> FirmwareUpgradeStatus.isTerminal(history.getStatus())
                ? historyService.deleteById(Mono.just(id))
                : Mono.error(new IllegalStateException("进行中的升级记录不能删除")))
            .defaultIfEmpty(0);
    }

    /**
     * 升级任务明细 — 查询任务的设备级升级历史
     */
    @PostMapping("/task/detail/_query")
    @Operation(summary = "分页查询任务明细")
    public Mono<Object> queryTaskDetails(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q -> historyService.queryPager(q));
    }

    @PostMapping("/task/detail/_query/no-paging")
    @Operation(summary = "不分页查询任务明细")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Mono<Object> queryTaskDetailsNoPaging(@RequestBody Mono<QueryParamEntity> query) {
        return query.flatMap(q ->
            historyService.createQuery().setParam(q).fetch()
                .collectList()
                .map(list -> (Object) list)
        );
    }
}
