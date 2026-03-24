package org.jetlinks.community.parallel.driving.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.jetlinks.community.parallel.driving.dto.VehicleInfo;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * 平行驾驶车辆服务
 * 提供车辆列表查询（带在线状态和绑定信息）
 *
 * @author JetLinks
 */
@Service
@Slf4j
@AllArgsConstructor
public class ParallelDrivingVehicleService {

    private final LocalDeviceInstanceService deviceInstanceService;
    private final ParallelDrivingRelationService relationService;

    /**
     * 平行驾驶车辆产品ID
     * 仅该产品的设备会作为“车辆”出现在列表中
     */
    private static final String PARALLEL_DRIVING_VEHICLE_PRODUCT_ID = "parallel-driving-product";

    /**
     * 查询车辆列表（带在线状态和绑定信息）
     * 支持按驾驶舱ID过滤（只显示该驾驶舱已绑定的车辆）
     *
     * @param queryParam 查询参数（可包含 cockpitId 参数）
     * @return 分页结果
     */
    public Mono<PagerResult<VehicleInfo>> queryVehiclesWithState(QueryParamEntity queryParam) {
        // 提取 cockpitId 参数（如果存在）
        String cockpitId = queryParam.getTerms().stream()
            .filter(term -> "cockpitId".equals(term.getColumn()))
            .map(term -> String.valueOf(term.getValue()))
            .findFirst()
            .orElse(null);

        // 兼容前端详情页按 deviceId 精确查询：/parallel-driving/vehicles/_query
        // 注意：LocalDeviceInstanceService.createQuery().setParam(queryParam) 不一定会将 terms 自动转换为 where 条件，
        // 因此这里显式解析并应用 deviceId 过滤，避免出现“URL 是 A，但返回/展示成 B”的问题。
        String specifiedDeviceId = queryParam.getTerms().stream()
            .filter(term -> "deviceId".equals(term.getColumn()) || "id".equals(term.getColumn()))
            .map(term -> term.getValue())
            .filter(v -> v != null && !String.valueOf(v).isEmpty())
            .map(String::valueOf)
            .findFirst()
            .orElse(null);

        // 1. 如果指定了 cockpitId，先获取该驾驶舱绑定的车辆列表
        if (cockpitId != null) {
            return relationService.getBoundVehicles(cockpitId)
                .collectList()
                .flatMap(boundVehicleIds -> {
                    if (boundVehicleIds.isEmpty()) {
                        return Mono.just(PagerResult.empty());
                    }
                    // 若同时指定了 deviceId，则先做一次交集过滤，避免查出非绑定车辆
                    if (specifiedDeviceId != null && !boundVehicleIds.contains(specifiedDeviceId)) {
                        return Mono.just(PagerResult.empty());
                    }
                    // 只查询绑定的车辆
                    return deviceInstanceService.createQuery()
                        .setParam(queryParam)
                        .where()
                        .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getId,
                             specifiedDeviceId != null ? specifiedDeviceId : boundVehicleIds)
                        .count()
                        .flatMap(total -> {
                            if (total == 0) {
                                return Mono.just(PagerResult.empty());
                            }
                            return deviceInstanceService.createQuery()
                                .setParam(queryParam)
                                .where()
                                .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getId,
                                     specifiedDeviceId != null ? specifiedDeviceId : boundVehicleIds)
                                .fetch()
                                .flatMap(device -> buildVehicleInfo(device))
                                .collectList()
                                .map(list -> PagerResult.of(total.intValue(), list, queryParam));
                        });
                });
        } else {
            // 2. 查询所有车辆（仅 parallel-driving-vehicle 产品）
            var whereForCount = deviceInstanceService.createQuery()
                .setParam(queryParam)
                .where()
                .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getProductId,
                     PARALLEL_DRIVING_VEHICLE_PRODUCT_ID);

            if (specifiedDeviceId != null) {
                whereForCount.and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getId, specifiedDeviceId);
            }

            return whereForCount
                .count()
                .flatMap(total -> {
                    if (total == 0) {
                        return Mono.just(PagerResult.empty());
                    }
                    var whereForFetch = deviceInstanceService.createQuery()
                        .setParam(queryParam)
                        .where()
                        .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getProductId,
                             PARALLEL_DRIVING_VEHICLE_PRODUCT_ID);

                    if (specifiedDeviceId != null) {
                        whereForFetch.and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getId, specifiedDeviceId);
                    }

                    return whereForFetch
                        .fetch()
                        .flatMap(device -> buildVehicleInfo(device))
                        .collectList()
                        .map(list -> PagerResult.of(total.intValue(), list, queryParam));
                });
        }
    }

    /**
     * 构建车辆信息
     */
    private Mono<VehicleInfo> buildVehicleInfo(org.jetlinks.community.device.entity.DeviceInstanceEntity device) {
        return deviceInstanceService.getDeviceState(device.getId())
            .map(state -> {
                VehicleInfo info = new VehicleInfo();
                info.setDeviceId(device.getId());
                info.setDeviceName(device.getName());
                info.setProductId(device.getProductId());
                info.setState(state);

                // 查询接管会话（当前是否被接管）
                return relationService.getSessionByVehicle(device.getId())
                    .map(session -> {
                        info.setBoundCockpitId(session.getCockpitDeviceId());
                        info.setSessionState(session.getSessionState());
                        info.setBindTime(session.getBindTime());
                        info.setLastActiveTime(session.getLastActiveTime());
                        info.setOperatorId(session.getOperatorId());
                        info.setOperatorName(session.getOperatorName());
                        return info;
                    })
                    .defaultIfEmpty(info);
            })
            .flatMap(Function.identity());
    }

    /**
     * 查询车辆列表（不分页）
     *
     * @param queryParam 查询参数
     * @return 车辆信息列表
     */
    public Flux<VehicleInfo> queryVehiclesWithStateNoPaging(QueryParamEntity queryParam) {
        return deviceInstanceService.createQuery()
            .setParam(queryParam)
            .where()
            .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getProductId,
                 PARALLEL_DRIVING_VEHICLE_PRODUCT_ID)
            .fetch()
            .flatMap(device -> {
                return deviceInstanceService.getDeviceState(device.getId())
                    .map(state -> {
                        VehicleInfo info = new VehicleInfo();
                        info.setDeviceId(device.getId());
                        info.setDeviceName(device.getName());
                        info.setProductId(device.getProductId());
                        info.setState(state);

                        return relationService.getSessionByVehicle(device.getId())
                            .map(session -> {
                                info.setBoundCockpitId(session.getCockpitDeviceId());
                                info.setSessionState(session.getSessionState());
                                info.setBindTime(session.getBindTime());
                                info.setLastActiveTime(session.getLastActiveTime());
                                info.setOperatorId(session.getOperatorId());
                                info.setOperatorName(session.getOperatorName());
                                return info;
                            })
                            .defaultIfEmpty(info);
                    })
                    .flatMap(Function.identity());
            });
    }
}
