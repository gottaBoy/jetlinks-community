package org.jetlinks.community.parallel.driving.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.jetlinks.community.parallel.driving.dto.CockpitInfo;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * 平行驾驶驾驶舱服务
 * 提供驾驶舱列表查询（带在线状态和绑定信息）
 *
 * @author JetLinks
 */
@Service
@Slf4j
@AllArgsConstructor
public class ParallelDrivingCockpitService {
    
    private final LocalDeviceInstanceService deviceInstanceService;
    private final ParallelDrivingRelationService relationService;

    /**
     * 并行驾驶驾驶舱产品ID列表
     * 包含：手柄驾驶舱 parallel-driving-joystick、标准驾驶舱 parallel-driving-cockpit
     */
    private static final java.util.List<String> PARALLEL_DRIVING_COCKPIT_PRODUCT_IDS =
        java.util.Arrays.asList("parallel-driving-joystick", "parallel-driving-cockpit");
    
    /**
     * 查询驾驶舱列表（带在线状态和绑定信息）
     *
     * @param queryParam 查询参数
     * @return 分页结果
     */
    public Mono<PagerResult<CockpitInfo>> queryCockpitsWithState(QueryParamEntity queryParam) {
        // 1. 查询设备列表（仅驾驶舱产品）
        return deviceInstanceService.createQuery()
            .setParam(queryParam)
            .where()
            .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getProductId,
                 PARALLEL_DRIVING_COCKPIT_PRODUCT_IDS)
            .count()
            .flatMap(total -> {
                if (total == 0) {
                    return Mono.just(PagerResult.empty());
                }
                return deviceInstanceService.createQuery()
                    .setParam(queryParam)
                    .where()
                    .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getProductId,
                         PARALLEL_DRIVING_COCKPIT_PRODUCT_IDS)
                    .fetch()
                    .flatMap(device -> {
                        // 2. 获取设备在线状态
                        return deviceInstanceService.getDeviceState(device.getId())
                            .map(state -> {
                                CockpitInfo info = new CockpitInfo();
                                info.setDeviceId(device.getId());
                                info.setDeviceName(device.getName());
                                info.setProductId(device.getProductId());
                                info.setState(state);
                                
                                // 3. 查询绑定关系（使用 ParallelDrivingSession）
                                return relationService.getSessionByCockpit(device.getId())
                                    .map(session -> {
                                        info.setBoundVehicleId(session.getVehicleDeviceId());
                                        info.setSessionState(session.getSessionState());
                                        info.setBindTime(session.getBindTime());
                                        info.setLastActiveTime(session.getLastActiveTime());
                                        return info;
                                    })
                                    .defaultIfEmpty(info);
                            })
                            .flatMap(Function.identity());
                    })
                    .collectList()
                    .map(list -> PagerResult.of(total.intValue(), list, queryParam));
            });
    }
    
    /**
     * 查询驾驶舱列表（不分页）
     *
     * @param queryParam 查询参数
     * @return 驾驶舱信息列表
     */
    public Flux<CockpitInfo> queryCockpitsWithStateNoPaging(QueryParamEntity queryParam) {
        return deviceInstanceService.createQuery()
            .setParam(queryParam)
            .where()
            .and(org.jetlinks.community.device.entity.DeviceInstanceEntity::getProductId,
                 PARALLEL_DRIVING_COCKPIT_PRODUCT_IDS)
            .fetch()
            .flatMap(device -> {
                return deviceInstanceService.getDeviceState(device.getId())
                    .map(state -> {
                        CockpitInfo info = new CockpitInfo();
                        info.setDeviceId(device.getId());
                        info.setDeviceName(device.getName());
                        info.setProductId(device.getProductId());
                        info.setState(state);
                        
                        return relationService.getSessionByCockpit(device.getId())
                            .map(session -> {
                                info.setBoundVehicleId(session.getVehicleDeviceId());
                                info.setSessionState(session.getSessionState());
                                info.setBindTime(session.getBindTime());
                                info.setLastActiveTime(session.getLastActiveTime());
                                return info;
                            })
                            .defaultIfEmpty(info);
                    })
                    .flatMap(Function.identity());
            });
    }
}
