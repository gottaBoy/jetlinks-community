 package org.jetlinks.community.parallel.driving.service;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.Authentication;
import org.hswebframework.web.exception.BusinessException;
import org.hswebframework.web.exception.NotFoundException;
import org.jetlinks.community.parallel.driving.entity.ParallelDrivingBinding;
import org.jetlinks.community.parallel.driving.entity.ParallelDrivingSession;
import org.jetlinks.community.parallel.driving.enums.ParallelDrivingSessionState;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;

/**
 * 并行驾驶关系服务
 * 管理两种关系：
 * 1. 绑定关系（ParallelDrivingBinding）：多对多授权关系，一个驾驶舱可以绑定多个车辆
 * 2. 接管会话（ParallelDrivingSession）：一对一接管关系，一个驾驶舱同时只能接管一个车辆，一个车辆同时只能被一个驾驶舱接管
 *
 * @author JetLinks
 */
@Service
@Slf4j
public class ParallelDrivingRelationService {

    private final ReactiveRepository<ParallelDrivingBinding, String> bindingRepository;
    private final ReactiveRepository<ParallelDrivingSession, String> sessionRepository;
    private final DeviceRegistry deviceRegistry;
    private final org.jetlinks.community.parallel.driving.room.ParallelDrivingRoomManager roomManager;

    @Autowired
    public ParallelDrivingRelationService(ReactiveRepository<ParallelDrivingBinding, String> bindingRepository,
                                         ReactiveRepository<ParallelDrivingSession, String> sessionRepository,
                                         DeviceRegistry deviceRegistry,
                                         org.jetlinks.community.parallel.driving.room.ParallelDrivingRoomManager roomManager) {
        this.bindingRepository = bindingRepository;
        this.sessionRepository = sessionRepository;
        this.deviceRegistry = deviceRegistry;
        this.roomManager = roomManager;
    }

    /**
     * 绑定驾驶舱到车端（多对多授权关系）
     * 只创建绑定关系，不创建会话
     * 一个驾驶舱可以绑定多个车辆，一个车辆可以被多个驾驶舱绑定
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID（VIN号）
     * @return Mono<Void>
     */
    public Mono<Void> bind(String cockpitDeviceId, String vehicleDeviceId) {
        log.info("绑定驾驶舱到车端（授权关系）: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId);

        // 1. 验证设备存在和类型
        return Mono.zip(
            deviceRegistry.getDevice(cockpitDeviceId),
            deviceRegistry.getDevice(vehicleDeviceId)
        )
        .switchIfEmpty(Mono.error(new NotFoundException("设备不存在")))
        .flatMap(tuple -> validateDeviceTypes(tuple.getT1(), tuple.getT2()))
        // 2. 检查绑定关系是否已存在
        .then(checkBindingExists(cockpitDeviceId, vehicleDeviceId))
        .flatMap(exists -> {
            if (exists) {
                return Mono.error(new BusinessException(
                    "驾驶舱[" + cockpitDeviceId + "]已经绑定了车辆[" + vehicleDeviceId + "]"
                ));
            }
            // 3. 创建绑定关系
            return createBinding(cockpitDeviceId, vehicleDeviceId);
        })
        .doOnSuccess(v -> log.info("绑定成功: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId))
        .doOnError(error -> log.error("绑定失败: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId, error));
    }

    /**
     * 验证接管约束（一对一）
     * 确保：
     * 1. 驾驶舱当前没有其他活跃会话
     * 2. 车辆当前没有被其他驾驶舱接管
     */
    private Mono<Void> validateTakeoverConstraint(String cockpitId, String vehicleId) {
        return Mono.zip(
            // 检查驾驶舱是否已经接管了其他车（只检查活跃会话）
            checkCockpitTakeover(cockpitId),
            // 检查车是否已经被其他驾驶舱接管（只检查活跃会话）
            checkVehicleTakeover(vehicleId)
        )
        .flatMap(tuple -> {
            String existingVehicle = tuple.getT1();
            String existingCockpit = tuple.getT2();

            // 如果驾驶舱已经接管了其他车
            if (StringUtils.hasText(existingVehicle) && !existingVehicle.equals(vehicleId)) {
                return Mono.error(new BusinessException(
                    "驾驶舱[" + cockpitId + "]已经接管了车辆[" + existingVehicle + "]，请先释放"
                ));
            }

            // 如果车已经被其他驾驶舱接管
            if (StringUtils.hasText(existingCockpit) && !existingCockpit.equals(cockpitId)) {
                return Mono.error(new BusinessException(
                    "车辆[" + vehicleId + "]已经被驾驶舱[" + existingCockpit + "]接管，请先释放"
                ));
            }

            return Mono.empty();
        });
    }

    /**
     * 检查驾驶舱是否已经接管了车（只检查活跃会话）
     *
     * @param cockpitId 驾驶舱设备ID
     * @return 已接管的车辆ID，如果没有则返回空字符串
     */
    private Mono<String> checkCockpitTakeover(String cockpitId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
            .fetch()
            .map(ParallelDrivingSession::getVehicleDeviceId)
            .next()
            .defaultIfEmpty("");
    }

    /**
     * 检查车是否已经被驾驶舱接管（只检查活跃会话）
     *
     * @param vehicleId 车端设备ID
     * @return 已接管的驾驶舱ID，如果没有则返回空字符串
     */
    private Mono<String> checkVehicleTakeover(String vehicleId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
            .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
            .fetch()
            .map(ParallelDrivingSession::getCockpitDeviceId)
            .next()
            .defaultIfEmpty("");
    }

    /**
     * 检查绑定关系是否存在
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return 是否存在
     */
    private Mono<Boolean> checkBindingExists(String cockpitId, String vehicleId) {
        return bindingRepository.createQuery()
            .where(ParallelDrivingBinding::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingBinding::getVehicleDeviceId, vehicleId)
            .fetch()
            .hasElements();
    }

    /**
     * 创建绑定关系
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<Void>
     */
    private Mono<Void> createBinding(String cockpitId, String vehicleId) {
        return Mono.zip(
            deviceRegistry.getDevice(cockpitId)
                .switchIfEmpty(Mono.error(new NotFoundException("驾驶舱设备不存在: " + cockpitId)))
                .flatMap(cockpit -> cockpit.getSelfConfig(org.jetlinks.community.PropertyConstants.deviceName)
                    .switchIfEmpty(Mono.just(cockpitId))
                    .map(name -> (name == null || name.isEmpty()) ? cockpitId : name)),
            deviceRegistry.getDevice(vehicleId)
                .switchIfEmpty(Mono.error(new NotFoundException("车辆设备不存在: " + vehicleId)))
                .flatMap(vehicle -> vehicle.getSelfConfig(org.jetlinks.community.PropertyConstants.deviceName)
                    .switchIfEmpty(Mono.just(vehicleId))
                    .map(name -> (name == null || name.isEmpty()) ? vehicleId : name))
        )
        .flatMap(tuple -> {
            String cockpitName = tuple.getT1();
            String vehicleName = tuple.getT2();

            ParallelDrivingBinding binding = new ParallelDrivingBinding();
            binding.setCockpitDeviceId(cockpitId);
            binding.setCockpitDeviceName(cockpitName);
            binding.setVehicleDeviceId(vehicleId);
            binding.setVehicleDeviceName(vehicleName);
            binding.setBindTime(System.currentTimeMillis());

            return Authentication.currentReactive()
                .map(Authentication::getUser)
                .doOnNext(user -> {
                    binding.setCreatorId(user.getId());
                })
                .switchIfEmpty(Mono.empty())
                .then(bindingRepository.insert(binding))
                .then();
        });
    }

    /**
     * 解除旧的会话（如果存在）
     * 接管新车辆前，先释放驾驶舱的旧会话和车辆的旧会话
     */
    private Mono<Void> removeOldSessions(String cockpitId, String vehicleId) {
        return Mono.when(
            // 解除驾驶舱的旧会话（如果有其他车）
            sessionRepository.createQuery()
                .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
                .and(ParallelDrivingSession::getVehicleDeviceId, vehicleId, "!=")  // 排除当前要接管的车
                .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
                .fetch()
                .flatMap(session -> {
                    // 关闭房间
                    return roomManager.closeRoom(session.getCockpitDeviceId(), session.getVehicleDeviceId())
                        .then(deleteSession(session.getCockpitDeviceId(), session.getVehicleDeviceId()));
                })
                .then(),
            // 解除车的旧会话（如果有其他驾驶舱）
            sessionRepository.createQuery()
                .where(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
                .and(ParallelDrivingSession::getCockpitDeviceId, cockpitId, "!=")  // 排除当前要接管的驾驶舱
                .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
                .fetch()
                .flatMap(session -> {
                    // 关闭房间
                    return roomManager.closeRoom(session.getCockpitDeviceId(), session.getVehicleDeviceId())
                        .then(deleteSession(session.getCockpitDeviceId(), session.getVehicleDeviceId()));
                })
                .then()
        ).then();
    }

    /**
     * 创建新会话
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @param state 初始状态
     * @return Mono<ParallelDrivingSession>
     */
    private Mono<ParallelDrivingSession> createSession(String cockpitId, String vehicleId,
                                                       ParallelDrivingSessionState state) {
        // 先查询设备名称
        Mono<String> cockpitNameMono = deviceRegistry.getDevice(cockpitId)
            .flatMap(cockpit -> cockpit.getSelfConfig(org.jetlinks.community.PropertyConstants.deviceName)
                .switchIfEmpty(Mono.just(cockpitId)))
            .defaultIfEmpty(cockpitId);
        
        Mono<String> vehicleNameMono = deviceRegistry.getDevice(vehicleId)
            .flatMap(vehicle -> vehicle.getSelfConfig(org.jetlinks.community.PropertyConstants.deviceName)
                .switchIfEmpty(Mono.just(vehicleId)))
            .defaultIfEmpty(vehicleId);
        
        return Mono.zip(cockpitNameMono, vehicleNameMono)
            .flatMap(tuple -> {
                String cockpitName = tuple.getT1();
                String vehicleName = tuple.getT2();
                return Authentication.currentReactive()
                    .map(Authentication::getUser)
                    .map(user -> {
                        ParallelDrivingSession session = new ParallelDrivingSession();
                        session.setCockpitDeviceId(cockpitId);
                        session.setCockpitDeviceName(cockpitName);
                        session.setVehicleDeviceId(vehicleId);
                        session.setVehicleDeviceName(vehicleName);
                        session.setSessionState(state);
                        session.setBindTime(System.currentTimeMillis());
                        session.setLastActiveTime(System.currentTimeMillis());
                        session.setOperatorId(user.getId());
                        session.setOperatorName(user.getName());
                        return session;
                    })
                    .switchIfEmpty(Mono.fromSupplier(() -> {
                        ParallelDrivingSession session = new ParallelDrivingSession();
                        session.setCockpitDeviceId(cockpitId);
                        session.setCockpitDeviceName(cockpitName);
                        session.setVehicleDeviceId(vehicleId);
                        session.setVehicleDeviceName(vehicleName);
                        session.setSessionState(state);
                        session.setBindTime(System.currentTimeMillis());
                        session.setLastActiveTime(System.currentTimeMillis());
                        return session;
                    }));
            })
            // ezorm ReactiveRepository#insert 返回 Mono<Integer>，这里通过 thenReturn 保持返回实体类型
            .flatMap(session -> sessionRepository.insert(session).thenReturn(session))
            .doOnSuccess(session -> log.info("创建会话成功: cockpit={}, vehicle={}, state={}",
                cockpitId, vehicleId, state));
    }

    // NOTE: 旧版本通过 RelatedEntity 存储“绑定/会话”关系。
    // 当前版本已拆分为：
    // - ParallelDrivingBinding：授权绑定（多对多）
    // - ParallelDrivingSession：接管会话（一对一）

    /**
     * 解绑（删除授权关系）
     * 只删除绑定关系，如果存在活跃会话，需要先释放
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @param force 是否强制解绑（云端解绑）。为 true 时：若存在活跃会话则先云端 release，再删除绑定关系。
     * @return Mono<Void>
     */
    public Mono<Void> unbind(String cockpitDeviceId, String vehicleDeviceId, boolean force) {
        log.info("解绑驾驶舱和车端（删除授权关系）: cockpit={}, vehicle={}, force={}",
            cockpitDeviceId, vehicleDeviceId, force);

        // 1. 检查是否存在活跃会话
        return checkControlPermission(cockpitDeviceId, vehicleDeviceId)
            .flatMap(hasActiveSession -> {
                if (hasActiveSession) {
                    if (!force) {
                        return Mono.error(new BusinessException(
                            "驾驶舱[" + cockpitDeviceId + "]正在接管车辆[" + vehicleDeviceId + "]，请先释放控制，或使用 force=true 云端解绑"
                        ));
                    }
                    // force=true：先云端释放（即使设备离线也尽量完成；通知失败不会中断 release 主流程）
                    return release(cockpitDeviceId, vehicleDeviceId)
                        .onErrorResume(err -> {
                            log.warn("force 解绑前 release 失败，继续尝试删除绑定关系: cockpit={}, vehicle={}, error={}",
                                cockpitDeviceId, vehicleDeviceId, err.getMessage());
                            return Mono.empty();
                        })
                        .then();
                }
                return Mono.empty();
            })
            // 2. 删除绑定关系
            .then(bindingRepository.createDelete()
                .where(ParallelDrivingBinding::getCockpitDeviceId, cockpitDeviceId)
                .and(ParallelDrivingBinding::getVehicleDeviceId, vehicleDeviceId)
                .execute()
                .then())
            .doOnSuccess(v -> log.info("解绑成功: cockpit={}, vehicle={}, force={}", cockpitDeviceId, vehicleDeviceId, force))
            .doOnError(error -> log.error("解绑失败: cockpit={}, vehicle={}, force={}",
                cockpitDeviceId, vehicleDeviceId, force, error));
    }

    /**
     * 查询驾驶舱绑定的车
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @return 已绑定的车辆ID，如果没有则返回空
     */
    public Mono<String> getBoundVehicle(String cockpitDeviceId) {
        return getSessionByCockpit(cockpitDeviceId)
            .map(ParallelDrivingSession::getVehicleDeviceId);
    }

    /**
     * 查询驾驶舱绑定的车（完整信息）
     * 优先返回活跃会话，如果没有会话则返回第一个授权关系的车辆信息
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @return 会话实体，如果没有则返回空
     */
    public Mono<ParallelDrivingSession> getBoundVehicleDetail(String cockpitDeviceId) {
        // 1. 先查询活跃会话
        return getSessionByCockpit(cockpitDeviceId)
            // 2. 如果没有会话，查询授权关系，返回第一个绑定的车辆信息
            .switchIfEmpty(
                bindingRepository.createQuery()
                    .where(ParallelDrivingBinding::getCockpitDeviceId, cockpitDeviceId)
                    .fetch()
                    .sort((b1, b2) -> Long.compare(
                        b2.getBindTime() != null ? b2.getBindTime() : 0L,
                        b1.getBindTime() != null ? b1.getBindTime() : 0L
                    )) // 按绑定时间降序排序
                    .next()
                    .map(binding -> {
                        // 构造一个 session 对象（state 为 released，表示未创建会话）
                        ParallelDrivingSession session = new ParallelDrivingSession();
                        session.setCockpitDeviceId(binding.getCockpitDeviceId());
                        session.setCockpitDeviceName(binding.getCockpitDeviceName());
                        session.setVehicleDeviceId(binding.getVehicleDeviceId());
                        session.setVehicleDeviceName(binding.getVehicleDeviceName());
                        session.setSessionState(ParallelDrivingSessionState.RELEASED); // 表示未创建会话
                        session.setBindTime(binding.getBindTime() != null ? binding.getBindTime() : System.currentTimeMillis());
                        session.setLastActiveTime(binding.getBindTime() != null ? binding.getBindTime() : System.currentTimeMillis());
                        return session;
                    })
            );
    }

    /**
     * 查询车被哪个驾驶舱绑定
     *
     * @param vehicleDeviceId 车端设备ID
     * @return 已绑定的驾驶舱ID，如果没有则返回空
     */
    public Mono<String> getBoundCockpit(String vehicleDeviceId) {
        return getSessionByVehicle(vehicleDeviceId)
            .map(ParallelDrivingSession::getCockpitDeviceId);
    }

    /**
     * 查询车被哪个驾驶舱绑定（完整信息）
     *
     * @param vehicleDeviceId 车端设备ID
     * @return 会话实体，如果没有则返回空
     */
    public Mono<ParallelDrivingSession> getBoundCockpitDetail(String vehicleDeviceId) {
        return getSessionByVehicle(vehicleDeviceId);
    }

    /**
     * 检查控制权限
     * 检查是否存在活跃的会话（用于消息路由，必须已接管）
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车端设备ID
     * @return 是否有权限
     */
    public Mono<Boolean> checkControlPermission(String cockpitDeviceId, String vehicleDeviceId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitDeviceId)
            .and(ParallelDrivingSession::getVehicleDeviceId, vehicleDeviceId)
            .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
            .fetch()
            .hasElements();
    }

    /**
     * 检查绑定权限（授权关系）
     * 驾驶舱绑定了车辆后才允许发起接管
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车辆设备ID
     * @return 是否已绑定
     */
    public Mono<Boolean> checkBindingPermission(String cockpitDeviceId, String vehicleDeviceId) {
        return bindingRepository.createQuery()
            .where(ParallelDrivingBinding::getCockpitDeviceId, cockpitDeviceId)
            .and(ParallelDrivingBinding::getVehicleDeviceId, vehicleDeviceId)
            .fetch()
            .hasElements();
    }

    /**
     * 查找绑定的驾驶舱（车端被哪些驾驶舱绑定）
     * 只返回活跃会话的驾驶舱
     *
     * @param vehicleDeviceId 车端设备ID
     * @return 驾驶舱设备ID列表
     */
    public Flux<String> findBoundCockpits(String vehicleDeviceId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getVehicleDeviceId, vehicleDeviceId)
            .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
            .fetch()
            .map(ParallelDrivingSession::getCockpitDeviceId);
    }

    /**
     * 查询绑定关系（分页）
     * 使用 ParallelDrivingBinding 查询授权关系
     *
     * @param queryParam 查询参数
     * @return 分页结果
     */
    public Mono<PagerResult<ParallelDrivingBinding>> queryBindRelations(QueryParamEntity queryParam) {
        return bindingRepository.createQuery()
            .setParam(queryParam)
            .count()
            .flatMap(total -> {
                if (total == 0) {
                    return Mono.just(PagerResult.empty());
                }
                return bindingRepository.createQuery()
                    .setParam(queryParam)
                    .fetch()
                    .collectList()
                    .map(list -> PagerResult.of(total.intValue(), list, queryParam));
            });
    }

    /**
     * 查询绑定关系（不分页）
     * 使用 ParallelDrivingBinding 查询授权关系
     *
     * @param queryParam 查询参数
     * @return 绑定关系列表
     */
    public Flux<ParallelDrivingBinding> queryBindRelationsNoPaging(QueryParamEntity queryParam) {
        return bindingRepository.createQuery()
            .setParam(queryParam)
            .fetch();
    }

    /**
     * 查询驾驶舱绑定的所有车辆（授权关系）
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @return 车辆设备ID列表
     */
    public Flux<String> getBoundVehicles(String cockpitDeviceId) {
        return bindingRepository.createQuery()
            .where(ParallelDrivingBinding::getCockpitDeviceId, cockpitDeviceId)
            .fetch()
            .map(ParallelDrivingBinding::getVehicleDeviceId);
    }

    /**
     * 查询车辆被哪些驾驶舱绑定（授权关系）
     *
     * @param vehicleDeviceId 车辆设备ID
     * @return 驾驶舱设备ID列表
     */
    public Flux<String> getBoundCockpits(String vehicleDeviceId) {
        return bindingRepository.createQuery()
            .where(ParallelDrivingBinding::getVehicleDeviceId, vehicleDeviceId)
            .fetch()
            .map(ParallelDrivingBinding::getCockpitDeviceId);
    }

    /**
     * 验证设备类型
     */
    private Mono<Void> validateDeviceTypes(DeviceOperator cockpit, DeviceOperator vehicle) {
        // 这里可以添加设备类型验证逻辑
        // 例如：验证驾驶舱和车端的产品类型
        return Mono.empty();
    }

    // ========== 会话管理 ==========

    /**
     * 获取会话
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<ParallelDrivingSession>
     */
    public Mono<ParallelDrivingSession> getSession(String cockpitId, String vehicleId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
            .fetch()
            .next();
    }

    /**
     * 根据驾驶舱ID获取会话（只返回活跃会话）
     *
     * @param cockpitId 驾驶舱设备ID
     * @return Mono<ParallelDrivingSession>
     */
    public Mono<ParallelDrivingSession> getSessionByCockpit(String cockpitId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
            .fetch()
            .next();
    }

    /**
     * 根据车辆ID获取会话（只返回活跃会话）
     *
     * @param vehicleId 车辆设备ID
     * @return Mono<ParallelDrivingSession>
     */
    public Mono<ParallelDrivingSession> getSessionByVehicle(String vehicleId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
            .and(ParallelDrivingSession::getSessionState, ParallelDrivingSessionState.ACTIVE)
            .fetch()
            .next();
    }

    /**
     * 更新会话状态
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @param state 新状态
     * @return Mono<Void>
     */
    public Mono<Void> updateSessionState(String cockpitId, String vehicleId,
                                         ParallelDrivingSessionState state) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
            .fetch()
            .next()
            .flatMap(session -> {
                session.setSessionState(state);
                session.setLastActiveTime(System.currentTimeMillis());
                return sessionRepository.save(session);
            })
            .then()
            .doOnSuccess(v -> log.info("更新会话状态成功: cockpit={}, vehicle={}, state={}",
                cockpitId, vehicleId, state))
            .doOnError(error -> log.error("更新会话状态失败: cockpit={}, vehicle={}, state={}",
                cockpitId, vehicleId, state, error));
    }

    /**
     * 更新最后活动时间
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<Void>
     */
    public Mono<Void> updateLastActiveTime(String cockpitId, String vehicleId) {
        return sessionRepository.createQuery()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
            .fetch()
            .next()
            .flatMap(session -> {
                session.setLastActiveTime(System.currentTimeMillis());
                return sessionRepository.save(session);
            })
            .then();
    }

    /**
     * 获取会话状态
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<ParallelDrivingSessionState>
     */
    public Mono<ParallelDrivingSessionState> getSessionState(String cockpitId, String vehicleId) {
        return getSession(cockpitId, vehicleId)
            .map(ParallelDrivingSession::getSessionState)
            .defaultIfEmpty(null);
    }

    /**
     * 删除会话
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<Void>
     */
    public Mono<Void> deleteSession(String cockpitId, String vehicleId) {
        return sessionRepository.createDelete()
            .where(ParallelDrivingSession::getCockpitDeviceId, cockpitId)
            .and(ParallelDrivingSession::getVehicleDeviceId, vehicleId)
            .execute()
            .then();
    }

    /**
     * 远程接管（创建接管会话）
     * 前置条件：
     * 1. 驾驶舱和车辆必须存在绑定关系（授权关系）
     * 2. 驾驶舱当前没有其他活跃会话
     * 3. 车辆当前没有被其他驾驶舱接管
     * 4. 设备必须在线
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车辆设备ID（VIN号）
     * @return Mono<ParallelDrivingSession>
     */
    public Mono<ParallelDrivingSession> takeover(String cockpitDeviceId, String vehicleDeviceId) {
        log.info("开始远程接管: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId);

        // 1. 验证设备存在和在线
        return Mono.zip(
            deviceRegistry.getDevice(cockpitDeviceId)
                .switchIfEmpty(Mono.error(new NotFoundException("驾驶舱设备不存在"))),
            deviceRegistry.getDevice(vehicleDeviceId)
                .switchIfEmpty(Mono.error(new NotFoundException("车辆设备不存在")))
        )
        .flatMap(tuple -> {
            DeviceOperator cockpit = tuple.getT1();
            DeviceOperator vehicle = tuple.getT2();

            // 2. 检查设备在线状态
            return Mono.zip(
                cockpit.getState().defaultIfEmpty(org.jetlinks.core.device.DeviceState.offline),
                vehicle.getState().defaultIfEmpty(org.jetlinks.core.device.DeviceState.offline)
            )
            .flatMap(states -> {
                if (states.getT1() != org.jetlinks.core.device.DeviceState.online) {
                    return Mono.error(new BusinessException("驾驶舱设备未上线"));
                }
                if (states.getT2() != org.jetlinks.core.device.DeviceState.online) {
                    return Mono.error(new BusinessException("车辆设备未上线"));
                }

                // 3. 验证绑定关系是否存在（授权关系）
                return checkBindingExists(cockpitDeviceId, vehicleDeviceId)
                    .flatMap(exists -> {
                        if (!exists) {
                            return Mono.error(new BusinessException(
                                "驾驶舱[" + cockpitDeviceId + "]未绑定车辆[" + vehicleDeviceId + "]，请先绑定"
                            ));
                        }
                        // 4. 验证接管约束（一对一）
                        return validateTakeoverConstraint(cockpitDeviceId, vehicleDeviceId);
                    });
            });
        })
        // 5. 解除旧会话（如果存在）
        .then(removeOldSessions(cockpitDeviceId, vehicleDeviceId))
        // 6. 创建新会话（状态：BINDING）
        .then(createSession(cockpitDeviceId, vehicleDeviceId, ParallelDrivingSessionState.BINDING))
        // 7. 创建房间
        .then(roomManager.createRoom(cockpitDeviceId, vehicleDeviceId).then())
        // 8. 通知双方设备
        .then(notifyDevicesTakeover(cockpitDeviceId, vehicleDeviceId))
        // 9. 更新状态为 ACTIVE
        .then(updateSessionState(cockpitDeviceId, vehicleDeviceId,
                                 ParallelDrivingSessionState.ACTIVE))
        .then(getSession(cockpitDeviceId, vehicleDeviceId))
        .doOnSuccess(session -> log.info("远程接管成功: cockpit={}, vehicle={}, state={}",
            cockpitDeviceId, vehicleDeviceId, session != null ? session.getSessionState() : null))
        .doOnError(error -> log.error("远程接管失败: cockpit={}, vehicle={}",
            cockpitDeviceId, vehicleDeviceId, error));
    }

    /**
     * 释放控制（删除接管会话）
     * 只删除会话，绑定关系（授权关系）保留
     *
     * @param cockpitDeviceId 驾驶舱设备ID
     * @param vehicleDeviceId 车辆设备ID
     * @return Mono<Void>
     */
    public Mono<Void> release(String cockpitDeviceId, String vehicleDeviceId) {
        log.info("开始释放控制: cockpit={}, vehicle={}", cockpitDeviceId, vehicleDeviceId);

        // 1. 关闭房间
        return roomManager.closeRoom(cockpitDeviceId, vehicleDeviceId)
            // 2. 更新状态为 RELEASING
            .then(updateSessionState(cockpitDeviceId, vehicleDeviceId,
                                     ParallelDrivingSessionState.RELEASING))
            // 3. 通知双方设备
            .then(notifyDevicesRelease(cockpitDeviceId, vehicleDeviceId))
            // 4. 删除会话（绑定关系保留）
            .then(deleteSession(cockpitDeviceId, vehicleDeviceId))
            .then()
            .doOnSuccess(v -> log.info("释放控制成功: cockpit={}, vehicle={}",
                cockpitDeviceId, vehicleDeviceId))
            .doOnError(error -> log.error("释放控制失败: cockpit={}, vehicle={}",
                cockpitDeviceId, vehicleDeviceId, error));
    }

    /**
     * 通知设备接管
     */
    @SuppressWarnings("null")
    private Mono<Void> notifyDevicesTakeover(String cockpitId, String vehicleId) {
        return Mono.zip(
            deviceRegistry.getDevice(cockpitId),
            deviceRegistry.getDevice(vehicleId)
        )
        .flatMap(tuple -> {
            DeviceOperator cockpit = tuple.getT1();
            DeviceOperator vehicle = tuple.getT2();

            // 发送通知消息到驾驶舱，自定义协议字段放入 inputs
            String msgId = UUID.randomUUID().toString();
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            FunctionInvokeMessage cockpitNotify = new FunctionInvokeMessage();
            cockpitNotify.setDeviceId(cockpitId);
            cockpitNotify.setFunctionId("onTakeover");
            cockpitNotify.addInput("name", "onTakeover");
            cockpitNotify.addInput("vin", Objects.requireNonNullElse(vehicleId, ""));
            cockpitNotify.addInput("id", msgId);
            cockpitNotify.addInput("type", "oms");
            cockpitNotify.addInput("omsno", Objects.requireNonNullElse(cockpitId, ""));
            cockpitNotify.addInput("seq", "1");
            cockpitNotify.addInput("version", "1.0");
            cockpitNotify.addInput("timestamp", ts);
            cockpitNotify.addHeader(Headers.force, true);

            // 发送通知消息到车辆
            FunctionInvokeMessage vehicleNotify = new FunctionInvokeMessage();
            vehicleNotify.setDeviceId(vehicleId);
            vehicleNotify.setFunctionId("onTakeover");
            vehicleNotify.addInput("name", "onTakeover");
            vehicleNotify.addInput("vin", Objects.requireNonNullElse(cockpitId, ""));
            vehicleNotify.addInput("id", msgId);
            vehicleNotify.addInput("type", "oms");
            vehicleNotify.addInput("omsno", Objects.requireNonNullElse(vehicleId, ""));
            vehicleNotify.addInput("seq", "1");
            vehicleNotify.addInput("version", "1.0");
            vehicleNotify.addInput("timestamp", ts);
            vehicleNotify.addHeader(Headers.force, true);

            // 使用 sendAndForget 避免超时：onTakeover 为通知类消息，设备通常不回复
            return Mono.when(
                cockpit.messageSender().sendAndForget(cockpitNotify),
                vehicle.messageSender().sendAndForget(vehicleNotify)
            ).then();
        })
        .onErrorResume(error -> {
            log.warn("通知设备接管失败: cockpit={}, vehicle={}", cockpitId, vehicleId, error);
            return Mono.empty();  // 通知失败不影响主流程
        });
    }

    /**
     * 通知设备释放
     */
    @SuppressWarnings("null")
    private Mono<Void> notifyDevicesRelease(String cockpitId, String vehicleId) {
        return Mono.zip(
            deviceRegistry.getDevice(cockpitId),
            deviceRegistry.getDevice(vehicleId)
        )
        .flatMap(tuple -> {
            DeviceOperator cockpit = tuple.getT1();
            DeviceOperator vehicle = tuple.getT2();

            // 发送通知消息到驾驶舱，自定义协议字段放入 inputs
            String msgId = UUID.randomUUID().toString();
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            FunctionInvokeMessage cockpitNotify = new FunctionInvokeMessage();
            cockpitNotify.setDeviceId(cockpitId);
            cockpitNotify.setFunctionId("onRelease");
            cockpitNotify.addInput("name", "onRelease");
            cockpitNotify.addInput("vin", Objects.requireNonNullElse(vehicleId, ""));
            cockpitNotify.addInput("id", msgId);
            cockpitNotify.addInput("type", "oms");
            cockpitNotify.addInput("omsno", Objects.requireNonNullElse(cockpitId, ""));
            cockpitNotify.addInput("seq", "1");
            cockpitNotify.addInput("version", "1.0");
            cockpitNotify.addInput("timestamp", ts);
            cockpitNotify.addHeader(Headers.force, true);

            // 发送通知消息到车辆
            FunctionInvokeMessage vehicleNotify = new FunctionInvokeMessage();
            vehicleNotify.setDeviceId(vehicleId);
            vehicleNotify.setFunctionId("onRelease");
            vehicleNotify.addInput("name", "onRelease");
            vehicleNotify.addInput("vin", Objects.requireNonNullElse(cockpitId, ""));
            vehicleNotify.addInput("id", msgId);
            vehicleNotify.addInput("type", "oms");
            vehicleNotify.addInput("omsno", Objects.requireNonNullElse(vehicleId, ""));
            vehicleNotify.addInput("seq", "1");
            vehicleNotify.addInput("version", "1.0");
            vehicleNotify.addInput("timestamp", ts);
            vehicleNotify.addHeader(Headers.force, true);

            // 使用 sendAndForget 避免超时：onRelease 为通知类消息，设备通常不回复
            return Mono.when(
                cockpit.messageSender().sendAndForget(cockpitNotify),
                vehicle.messageSender().sendAndForget(vehicleNotify)
            ).then();
        })
        .onErrorResume(error -> {
            log.warn("通知设备释放失败: cockpit={}, vehicle={}", cockpitId, vehicleId, error);
            return Mono.empty();  // 通知失败不影响主流程
        });
    }
}

