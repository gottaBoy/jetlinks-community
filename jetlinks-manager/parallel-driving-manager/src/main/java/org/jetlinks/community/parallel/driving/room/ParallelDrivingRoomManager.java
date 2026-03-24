package org.jetlinks.community.parallel.driving.room;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingEncryptionService;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 平行驾驶房间管理器
 * 管理所有活跃的房间
 *
 * @author JetLinks
 */
@Component
@Slf4j
public class ParallelDrivingRoomManager {
    
    private final DeviceRegistry deviceRegistry;
    private final ParallelDrivingEncryptionService encryptionService;
    private final ConcurrentHashMap<String, ParallelDrivingRoom> rooms = new ConcurrentHashMap<>();
    
    @Autowired
    public ParallelDrivingRoomManager(DeviceRegistry deviceRegistry,
                                      ParallelDrivingEncryptionService encryptionService) {
        this.deviceRegistry = deviceRegistry;
        this.encryptionService = encryptionService;
    }
    
    /**
     * 创建房间
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<ParallelDrivingRoom>
     */
    public Mono<ParallelDrivingRoom> createRoom(String cockpitId, String vehicleId) {
        String roomId = cockpitId + "-" + vehicleId;
        
        // 如果房间已存在，先关闭
        ParallelDrivingRoom existingRoom = rooms.remove(roomId);
        if (existingRoom != null) {
            existingRoom.close().subscribe();
            log.info("关闭已存在的房间[{}]: cockpit={}, vehicle={}", roomId, cockpitId, vehicleId);
        }
        
        // 创建新房间
        ParallelDrivingRoom room = new ParallelDrivingRoom(cockpitId, vehicleId);
        
        // 获取设备操作器并初始化房间
        return Mono.zip(
            deviceRegistry.getDevice(cockpitId)
                .switchIfEmpty(Mono.error(new org.hswebframework.web.exception.NotFoundException(
                    "驾驶舱设备不存在: " + cockpitId))),
            deviceRegistry.getDevice(vehicleId)
                .switchIfEmpty(Mono.error(new org.hswebframework.web.exception.NotFoundException(
                    "车辆设备不存在: " + vehicleId)))
        )
        .doOnNext(tuple -> {
            DeviceOperator cockpit = tuple.getT1();
            DeviceOperator vehicle = tuple.getT2();
            room.initialize(cockpit, vehicle);
            // 设置加密服务（用于查询加密状态）
            room.setEncryptionService(encryptionService);
            // 设置设备注册表，用于每次转发时重新获取车辆设备（解决车端重启后收不到 remotejoystick 等问题）
            room.setDeviceRegistry(deviceRegistry);
            rooms.put(roomId, room);
            log.info("创建房间[{}]成功: cockpit={}, vehicle={}", roomId, cockpitId, vehicleId);
        })
        .thenReturn(room)
        .doOnError(error -> log.error("创建房间[{}]失败: cockpit={}, vehicle={}", 
            roomId, cockpitId, vehicleId, error));
    }
    
    /**
     * 获取房间
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<ParallelDrivingRoom>
     */
    public Mono<ParallelDrivingRoom> getRoom(String cockpitId, String vehicleId) {
        String roomId = cockpitId + "-" + vehicleId;
        ParallelDrivingRoom room = rooms.get(roomId);
        if (room != null && room.isActive()) {
            return Mono.just(room);
        }
        return Mono.empty();
    }
    
    /**
     * 根据驾驶舱ID获取房间
     *
     * @param cockpitId 驾驶舱设备ID
     * @return Mono<ParallelDrivingRoom>
     */
    public Mono<ParallelDrivingRoom> getRoomByCockpit(String cockpitId) {
        return Mono.fromCallable(() -> {
            return rooms.values().stream()
                .filter(room -> room.getCockpitDeviceId().equals(cockpitId))
                .filter(ParallelDrivingRoom::isActive)
                .findFirst()
                .orElse(null);
        })
        .cast(ParallelDrivingRoom.class)
        .switchIfEmpty(Mono.empty());
    }
    
    /**
     * 根据车辆ID获取房间
     *
     * @param vehicleId 车辆设备ID
     * @return Mono<ParallelDrivingRoom>
     */
    public Mono<ParallelDrivingRoom> getRoomByVehicle(String vehicleId) {
        return Mono.fromCallable(() -> {
            return rooms.values().stream()
                .filter(room -> room.getVehicleDeviceId().equals(vehicleId))
                .filter(ParallelDrivingRoom::isActive)
                .findFirst()
                .orElse(null);
        })
        .cast(ParallelDrivingRoom.class)
        .switchIfEmpty(Mono.empty());
    }
    
    /**
     * 关闭房间
     *
     * @param cockpitId 驾驶舱设备ID
     * @param vehicleId 车辆设备ID
     * @return Mono<Void>
     */
    public Mono<Void> closeRoom(String cockpitId, String vehicleId) {
        String roomId = cockpitId + "-" + vehicleId;
        ParallelDrivingRoom room = rooms.remove(roomId);
        if (room != null) {
            return room.close();
        }
        return Mono.empty();
    }
    
    /**
     * 获取所有活跃房间
     *
     * @return Flux<ParallelDrivingRoom>
     */
    public Flux<ParallelDrivingRoom> getAllActiveRooms() {
        return Flux.fromIterable(rooms.values())
            .filter(ParallelDrivingRoom::isActive);
    }
    
    /**
     * 获取房间统计信息
     *
     * @return 活跃房间数量
     */
    public int getActiveRoomCount() {
        return (int) rooms.values().stream()
            .filter(ParallelDrivingRoom::isActive)
            .count();
    }
    
    /**
     * 关闭所有房间（应用关闭时调用）
     */
    @PreDestroy
    public void shutdown() {
        log.info("关闭所有房间，共 {} 个", rooms.size());
        rooms.values().forEach(room -> room.close().subscribe());
        rooms.clear();
    }
}
