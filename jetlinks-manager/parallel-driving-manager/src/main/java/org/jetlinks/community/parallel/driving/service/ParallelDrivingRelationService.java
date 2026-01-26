package org.jetlinks.community.parallel.driving.service;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.exception.BusinessException;
import org.hswebframework.web.exception.NotFoundException;
import org.jetlinks.community.relation.entity.RelatedEntity;
import org.jetlinks.community.relation.service.RelatedObjectInfo;
import org.jetlinks.core.Value;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * 并行驾驶关系服务
 * 实现驾驶舱-车端的一对一绑定关系管理
 *
 * @author JetLinks
 */
@Service
@Slf4j
public class ParallelDrivingRelationService {

    private final ReactiveRepository<RelatedEntity, String> relatedRepository;
    private final DeviceRegistry deviceRegistry;

    @Autowired
    public ParallelDrivingRelationService(ReactiveRepository<RelatedEntity, String> relatedRepository,
                                         DeviceRegistry deviceRegistry) {
        this.relatedRepository = relatedRepository;
        this.deviceRegistry = deviceRegistry;
    }

    /**
     * 关系标识：控制
     */
    private static final String RELATION_CONTROLS = "controls";

    /**
     * 对象类型：设备
     */
    private static final String OBJECT_TYPE_DEVICE = "device";

    /**
     * 绑定驾驶舱到车端（一对一约束）
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return Mono<Void>
     */
    public Mono<Void> bind(String cockpitDeviceId, String vehicleDeviceId) {
        log.info("绑定驾驶舱到车端: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId);

        // 1. 验证设备存在和类型
        return Mono.zip(
            deviceRegistry.getDevice(cockpitDeviceId),
            deviceRegistry.getDevice(vehicleDeviceId)
        )
        .switchIfEmpty(Mono.error(new NotFoundException("设备不存在")))
        .flatMap(tuple -> validateDeviceTypes(tuple.getT1(), tuple.getT2()))
        // 2. 验证一对一约束
        .then(validateOneToOneConstraint(cockpitDeviceId, vehicleDeviceId))
        // 3. 解除旧的关系（如果存在）
        .then(removeOldRelations(cockpitDeviceId, vehicleDeviceId))
        // 4. 建立新关系
        .then(createRelation(cockpitDeviceId, vehicleDeviceId))
        .doOnSuccess(v -> log.info("绑定成功: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId))
        .doOnError(error -> log.error("绑定失败: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId, error));
    }

    /**
     * 验证一对一约束
     */
    private Mono<Void> validateOneToOneConstraint(String cockpitId, String vehicleId) {
        return Mono.zip(
            // 检查驾驶舱是否已经绑定了其他车
            checkCockpitBound(cockpitId),
            // 检查车是否已经被其他驾驶舱绑定
            checkVehicleBound(vehicleId)
        )
        .flatMap(tuple -> {
            String existingVehicle = tuple.getT1();
            String existingCockpit = tuple.getT2();

            // 如果驾驶舱已经绑定了其他车
            if (StringUtils.hasText(existingVehicle) && !existingVehicle.equals(vehicleId)) {
                return Mono.error(new BusinessException(
                    "驾驶舱[" + cockpitId + "]已经绑定了车辆[" + existingVehicle + "]"
                ));
            }

            // 如果车已经被其他驾驶舱绑定
            if (StringUtils.hasText(existingCockpit) && !existingCockpit.equals(cockpitId)) {
                return Mono.error(new BusinessException(
                    "车辆[" + vehicleId + "]已经被驾驶舱[" + existingCockpit + "]绑定"
                ));
            }

            return Mono.empty();
        });
    }

    /**
     * 检查驾驶舱是否已经绑定了车
     *
     * @param cockpitId 驾驶舱设备ID
     * @return 已绑定的车辆ID，如果没有则返回空字符串
     */
    private Mono<String> checkCockpitBound(String cockpitId) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getObjectId, cockpitId)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .fetch()
            // 注意：RelatedEntity 没有 expands 字段，这里直接返回所有关系
            // 如果需要状态管理，可以通过其他方式实现（如使用 relatedName 字段存储状态信息）
            .map(RelatedEntity::getRelatedId)
            .next()
            .cast(String.class)
            .defaultIfEmpty("");
    }

    /**
     * 检查车是否已经被驾驶舱绑定
     *
     * @param vehicleId 车端设备ID
     * @return 已绑定的驾驶舱ID，如果没有则返回空字符串
     */
    private Mono<String> checkVehicleBound(String vehicleId) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getRelatedId, vehicleId)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .fetch()
            // 注意：RelatedEntity 没有 expands 字段，这里直接返回所有关系
            .map(RelatedEntity::getObjectId)
            .next()
            .cast(String.class)
            .defaultIfEmpty("");
    }

    /**
     * 解除旧的关系
     */
    private Mono<Void> removeOldRelations(String cockpitId, String vehicleId) {
        return Mono.when(
            // 解除驾驶舱的旧关系（如果有其他车）
            relatedRepository.createDelete()
                .where(RelatedEntity::getObjectId, cockpitId)
                .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
                .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
                .and(RelatedEntity::getRelation, RELATION_CONTROLS)
                .and(RelatedEntity::getRelatedId, vehicleId, "!=")  // 排除当前要绑定的车
                .execute(),
            // 解除车的旧关系（如果有其他驾驶舱）
            relatedRepository.createDelete()
                .where(RelatedEntity::getRelatedId, vehicleId)
                .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
                .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
                .and(RelatedEntity::getRelation, RELATION_CONTROLS)
                .and(RelatedEntity::getObjectId, cockpitId, "!=")  // 排除当前要绑定的驾驶舱
                .execute()
        ).then();
    }

    /**
     * 创建新关系
     */
    private Mono<Void> createRelation(String cockpitId, String vehicleId) {
        // 同时获取驾驶舱和车端的设备信息
        return Mono.zip(
            deviceRegistry.getDevice(cockpitId)
                .switchIfEmpty(Mono.error(new NotFoundException("驾驶舱设备不存在: " + cockpitId)))
                .flatMap(cockpit -> cockpit.getSelfConfig(org.jetlinks.community.PropertyConstants.deviceName)
                    .switchIfEmpty(Mono.just(cockpitId))
                    .map(name -> (name == null || name.isEmpty()) ? cockpitId : name)),
            deviceRegistry.getDevice(vehicleId)
                .switchIfEmpty(Mono.error(new NotFoundException("车端设备不存在: " + vehicleId)))
                .flatMap(vehicle -> vehicle.getSelfConfig(org.jetlinks.community.PropertyConstants.deviceName)
                    .switchIfEmpty(Mono.just(vehicleId))
                    .map(name -> (name == null || name.isEmpty()) ? vehicleId : name))
        )
        .map(tuple -> {
            String cockpitName = tuple.getT1();
            String vehicleName = tuple.getT2();
            
            RelatedObjectInfo vehicleInfo = RelatedObjectInfo.of(vehicleId, vehicleName);

            RelatedEntity related = new RelatedEntity()
                .withObject(OBJECT_TYPE_DEVICE, cockpitId)
                .withRelated(OBJECT_TYPE_DEVICE, vehicleInfo, RELATION_CONTROLS);
            
            // 设置驾驶舱设备名称
            related.setObjectName(cockpitName);

            return related;
        })
        .flatMap(relatedRepository::insert)
        .then();
    }

    /**
     * 解绑
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return Mono<Void>
     */
    public Mono<Void> unbind(String cockpitDeviceId, String vehicleDeviceId) {
        log.info("解绑驾驶舱和车端: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId);

        return relatedRepository.createDelete()
            .where(RelatedEntity::getObjectId, cockpitDeviceId)
            .and(RelatedEntity::getRelatedId, vehicleDeviceId)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .execute()
            .then()
            .doOnSuccess(v -> log.info("解绑成功: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId))
            .doOnError(error -> log.error("解绑失败: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId, error));
    }

    /**
     * 查询驾驶舱绑定的车
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @return 已绑定的车辆ID，如果没有则返回空
     */
    public Mono<String> getBoundVehicle(String cockpitDeviceId) {
        return checkCockpitBound(cockpitDeviceId)
            .filter(StringUtils::hasText);
    }

    /**
     * 查询驾驶舱绑定的车（完整信息）
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @return 绑定关系实体，如果没有则返回空
     */
    public Mono<RelatedEntity> getBoundVehicleDetail(String cockpitDeviceId) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getObjectId, cockpitDeviceId)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .fetch()
            .next();
    }

    /**
     * 查询车被哪个驾驶舱绑定
     *
     * @param vehicleDeviceId 车端设备ID
     * @return 已绑定的驾驶舱ID，如果没有则返回空
     */
    public Mono<String> getBoundCockpit(String vehicleDeviceId) {
        return checkVehicleBound(vehicleDeviceId)
            .filter(StringUtils::hasText);
    }

    /**
     * 查询车被哪个驾驶舱绑定（完整信息）
     *
     * @param vehicleDeviceId 车端设备ID
     * @return 绑定关系实体，如果没有则返回空
     */
    public Mono<RelatedEntity> getBoundCockpitDetail(String vehicleDeviceId) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getRelatedId, vehicleDeviceId)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .fetch()
            .next();
    }

    /**
     * 检查控制权限
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return 是否有权限
     */
    public Mono<Boolean> checkControlPermission(String cockpitDeviceId, String vehicleDeviceId) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getObjectId, cockpitDeviceId)
            .and(RelatedEntity::getRelatedId, vehicleDeviceId)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .fetch()
            .hasElements();
    }

    /**
     * 查找绑定的驾驶舱（车端被哪些驾驶舱绑定）
     *
     * @param vehicleDeviceId 车端设备ID
     * @return 驾驶舱设备ID列表
     */
    public Flux<String> findBoundCockpits(String vehicleDeviceId) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getRelatedId, vehicleDeviceId)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelation, RELATION_CONTROLS)
            .fetch()
            .map(RelatedEntity::getObjectId);
    }

    /**
     * 查询绑定关系（分页）
     *
     * @param queryParam 查询参数
     * @return 分页结果
     */
    public Mono<PagerResult<RelatedEntity>> queryBindRelations(QueryParamEntity queryParam) {
        // 只查询 controls 关系的设备绑定
        return relatedRepository.createQuery()
            .where(RelatedEntity::getRelation, RELATION_CONTROLS)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .setParam(queryParam)
            .count()
            .flatMap(total -> {
                if (total == 0) {
                    return Mono.just(PagerResult.empty());
                }
                return relatedRepository.createQuery()
                    .where(RelatedEntity::getRelation, RELATION_CONTROLS)
                    .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
                    .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
                    .setParam(queryParam)
                    .fetch()
                    .collectList()
                    .map(list -> PagerResult.of(total.intValue(), list, queryParam));
            });
    }

    /**
     * 查询绑定关系（不分页）
     *
     * @param queryParam 查询参数
     * @return 绑定关系列表
     */
    public Flux<RelatedEntity> queryBindRelationsNoPaging(QueryParamEntity queryParam) {
        return relatedRepository.createQuery()
            .where(RelatedEntity::getRelation, RELATION_CONTROLS)
            .and(RelatedEntity::getObjectType, OBJECT_TYPE_DEVICE)
            .and(RelatedEntity::getRelatedType, OBJECT_TYPE_DEVICE)
            .setParam(queryParam)
            .fetch();
    }

    /**
     * 验证设备类型
     */
    private Mono<Void> validateDeviceTypes(DeviceOperator cockpit, DeviceOperator vehicle) {
        // 这里可以添加设备类型验证逻辑
        // 例如：验证驾驶舱和车端的产品类型
        return Mono.empty();
    }
}

