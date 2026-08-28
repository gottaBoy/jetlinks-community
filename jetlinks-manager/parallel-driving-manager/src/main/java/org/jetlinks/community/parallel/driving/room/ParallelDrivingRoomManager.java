package org.jetlinks.community.parallel.driving.room;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.parallel.driving.metrics.ParallelDrivingLatencyMetrics;
import org.jetlinks.community.parallel.driving.service.ParallelDrivingEncryptionService;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ParallelDrivingRoomManager {

    private static final String ROOM_INFO_PREFIX = "pd:room:info:";
    private static final String COCKPIT_INDEX_PREFIX = "pd:room:idx:cockpit:";
    private static final String VEHICLE_INDEX_PREFIX = "pd:room:idx:vehicle:";
    private static final Duration REDIS_TTL = Duration.ofHours(12);
    private static final long L1_CACHE_TTL_MS = 2000;
    /**
     * Delete the room and its indexes only if the room metadata is unchanged
     * since the close operation started. This prevents an old asynchronous
     * close from deleting a newly-created room for the same device pair.
     */
    private static final RedisScript<Object> CLOSE_ROOM_SCRIPT = RedisScript.of(
        "local current = redis.call('GET', KEYS[1])\n" +
        "local cockpit = redis.call('GET', KEYS[2])\n" +
        "local vehicle = redis.call('GET', KEYS[3])\n" +
        "if current == ARGV[1] and cockpit == ARGV[2] and vehicle == ARGV[2] then\n" +
        "  return redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])\n" +
        "end\n" +
        "return 0\n", Object.class);

    private final DeviceRegistry deviceRegistry;
    private final ParallelDrivingEncryptionService encryptionService;
    private final ReactiveRedisTemplate<Object, Object> redis;
    private final String nodeId;
    private final boolean latestOnlyEnabled;
    private final ParallelDrivingLatencyMetrics latencyMetrics;

    /** L1 local cache: roomKey → Room object (only for rooms owned by this node) */
    private final ConcurrentHashMap<String, ParallelDrivingRoom> localRooms = new ConcurrentHashMap<>();

    /** L1 index caches with TTL for high-frequency lookups */
    private final ConcurrentHashMap<String, CachedEntry<String>> cockpitIndexCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedEntry<String>> vehicleIndexCache = new ConcurrentHashMap<>();

    @Autowired
    public ParallelDrivingRoomManager(DeviceRegistry deviceRegistry,
                                      ParallelDrivingEncryptionService encryptionService,
                                      ReactiveRedisTemplate<Object, Object> redis,
                                      ParallelDrivingLatencyMetrics latencyMetrics,
                                      @Value("${jetlinks.server-id:standalone}") String nodeId,
                                      @Value("${parallel-driving.control.latest-only:false}") boolean latestOnlyEnabled) {
        this.deviceRegistry = deviceRegistry;
        this.encryptionService = encryptionService;
        this.redis = redis;
        this.latencyMetrics = latencyMetrics;
        this.nodeId = nodeId;
        this.latestOnlyEnabled = latestOnlyEnabled;
        log.info("ParallelDrivingRoomManager initialized: nodeId={}, latestOnlyEnabled={}", nodeId, latestOnlyEnabled);
    }

    public Mono<ParallelDrivingRoom> createRoom(String cockpitId, String vehicleId) {
        String roomKey = cockpitId + "-" + vehicleId;

        return closeRoom(cockpitId, vehicleId)
            .then(Mono.defer(() -> {
                ParallelDrivingRoom room = new ParallelDrivingRoom(cockpitId, vehicleId);

                return Mono.zip(
                    deviceRegistry.getDevice(cockpitId)
                        .switchIfEmpty(Mono.error(new org.hswebframework.web.exception.NotFoundException(
                            "驾驶舱设备不存在: " + cockpitId))),
                    deviceRegistry.getDevice(vehicleId)
                        .switchIfEmpty(Mono.error(new org.hswebframework.web.exception.NotFoundException(
                            "车辆设备不存在: " + vehicleId)))
                )
                .flatMap(tuple -> {
                    DeviceOperator cockpit = tuple.getT1();
                    DeviceOperator vehicle = tuple.getT2();
                    room.initialize(cockpit, vehicle);
                    room.setEncryptionService(encryptionService);
                    room.setDeviceRegistry(deviceRegistry);
                    room.setLatestOnlyEnabled(latestOnlyEnabled);
                    room.setLatencyMetrics(latencyMetrics);

                    JSONObject roomInfo = new JSONObject();
                    roomInfo.put("cockpitId", cockpitId);
                    roomInfo.put("vehicleId", vehicleId);
                    roomInfo.put("nodeId", nodeId);
                    roomInfo.put("state", "ACTIVE");
                    roomInfo.put("createTime", System.currentTimeMillis());

                    return redis.opsForValue().set(ROOM_INFO_PREFIX + roomKey, roomInfo.toJSONString(), REDIS_TTL)
                        .then(redis.opsForValue().set(COCKPIT_INDEX_PREFIX + cockpitId, roomKey, REDIS_TTL))
                        .then(redis.opsForValue().set(VEHICLE_INDEX_PREFIX + vehicleId, roomKey, REDIS_TTL))
                        .doOnSuccess(v -> {
                            localRooms.put(roomKey, room);
                            cockpitIndexCache.put(cockpitId, new CachedEntry<>(roomKey));
                            vehicleIndexCache.put(vehicleId, new CachedEntry<>(roomKey));
                            log.info("创建房间[{}]成功: cockpit={}, vehicle={}, node={}", roomKey, cockpitId, vehicleId, nodeId);
                        })
                        .thenReturn(room);
                })
                .doOnError(error -> log.error("创建房间[{}]失败: cockpit={}, vehicle={}", roomKey, cockpitId, vehicleId, error));
            }));
    }

    public Mono<ParallelDrivingRoom> getRoom(String cockpitId, String vehicleId) {
        String roomKey = cockpitId + "-" + vehicleId;

        ParallelDrivingRoom local = localRooms.get(roomKey);
        if (local != null && local.isActive()) {
            return Mono.just(local);
        }

        return getOrRecoverRoom(roomKey, cockpitId, vehicleId);
    }

    public Mono<ParallelDrivingRoom> getRoomByCockpit(String cockpitId) {
        CachedEntry<String> cached = cockpitIndexCache.get(cockpitId);
        if (cached != null && !cached.isExpired()) {
            ParallelDrivingRoom local = localRooms.get(cached.value);
            if (local != null && local.isActive()) {
                return Mono.just(local);
            }
        }

        return redis.opsForValue().get(COCKPIT_INDEX_PREFIX + cockpitId)
            .cast(String.class)
            .flatMap(roomKey -> {
                cockpitIndexCache.put(cockpitId, new CachedEntry<>(roomKey));
                ParallelDrivingRoom local = localRooms.get(roomKey);
                if (local != null && local.isActive()) {
                    return Mono.just(local);
                }
                String vehicleId = roomKey.contains("-") ? roomKey.substring(roomKey.indexOf('-') + 1) : null;
                if (vehicleId != null) {
                    return getOrRecoverRoom(roomKey, cockpitId, vehicleId);
                }
                return Mono.empty();
            });
    }

    public Mono<ParallelDrivingRoom> getRoomByVehicle(String vehicleId) {
        CachedEntry<String> cached = vehicleIndexCache.get(vehicleId);
        if (cached != null && !cached.isExpired()) {
            ParallelDrivingRoom local = localRooms.get(cached.value);
            if (local != null && local.isActive()) {
                return Mono.just(local);
            }
        }

        return redis.opsForValue().get(VEHICLE_INDEX_PREFIX + vehicleId)
            .cast(String.class)
            .flatMap(roomKey -> {
                vehicleIndexCache.put(vehicleId, new CachedEntry<>(roomKey));
                ParallelDrivingRoom local = localRooms.get(roomKey);
                if (local != null && local.isActive()) {
                    return Mono.just(local);
                }
                String cockpitId = roomKey.contains("-") ? roomKey.substring(0, roomKey.indexOf('-')) : null;
                if (cockpitId != null) {
                    return getOrRecoverRoom(roomKey, cockpitId, vehicleId);
                }
                return Mono.empty();
            });
    }

    public Mono<Void> closeRoom(String cockpitId, String vehicleId) {
        String roomKey = cockpitId + "-" + vehicleId;

        ParallelDrivingRoom local = localRooms.get(roomKey);
        if (local != null) {
            localRooms.remove(roomKey, local);
        }

        Mono<Void> closeLocal = (local != null) ? local.close() : Mono.empty();

        return closeLocal
            .then(redis.opsForValue().get(ROOM_INFO_PREFIX + roomKey)
                .cast(String.class)
                .defaultIfEmpty(""))
            .flatMap(snapshot -> {
                if (snapshot.isEmpty()) {
                    // No metadata means there is no safe generation to match.
                    // Leave stale indexes for the cleanup scheduler rather than
                    // risking deletion of a newer room.
                    return Mono.empty();
                }
                return redis.execute(
                        CLOSE_ROOM_SCRIPT,
                        Arrays.asList(
                            ROOM_INFO_PREFIX + roomKey,
                            COCKPIT_INDEX_PREFIX + cockpitId,
                            VEHICLE_INDEX_PREFIX + vehicleId),
                        snapshot,
                        roomKey)
                    .next()
                    .doOnNext(count -> log.info(
                        "关闭房间[{}]: 条件清理 Redis 键，结果={}", roomKey, count))
                    .then();
            });
    }

    public Flux<ParallelDrivingRoom> getAllActiveRooms() {
        return Flux.fromIterable(localRooms.values())
            .filter(ParallelDrivingRoom::isActive);
    }

    public int getActiveRoomCount() {
        return (int) localRooms.values().stream()
            .filter(ParallelDrivingRoom::isActive)
            .count();
    }

    /**
     * Passive recovery: rebuild a Room locally from Redis metadata.
     * Called when this node receives a control message for a room it doesn't own locally.
     */
    private Mono<ParallelDrivingRoom> getOrRecoverRoom(String roomKey, String cockpitId, String vehicleId) {
        return redis.opsForValue().get(ROOM_INFO_PREFIX + roomKey)
            .cast(String.class)
            .flatMap(json -> {
                JSONObject info = JSON.parseObject(json);
                if (info == null || !"ACTIVE".equals(info.getString("state"))) {
                    return Mono.empty();
                }

                log.info("被动恢复房间[{}]: 从 Redis 重建, originNode={}, currentNode={}",
                    roomKey, info.getString("nodeId"), nodeId);

                ParallelDrivingRoom room = new ParallelDrivingRoom(cockpitId, vehicleId);
                return Mono.zip(
                    deviceRegistry.getDevice(cockpitId).switchIfEmpty(Mono.empty()),
                    deviceRegistry.getDevice(vehicleId).switchIfEmpty(Mono.empty())
                )
                .flatMap(tuple -> {
                    room.initialize(tuple.getT1(), tuple.getT2());
                    room.setEncryptionService(encryptionService);
                    room.setDeviceRegistry(deviceRegistry);
                    room.setLatestOnlyEnabled(latestOnlyEnabled);
                    room.setLatencyMetrics(latencyMetrics);

                    info.put("nodeId", nodeId);
                    return redis.opsForValue().set(ROOM_INFO_PREFIX + roomKey, info.toJSONString(), REDIS_TTL)
                        .doOnSuccess(v -> {
                            localRooms.put(roomKey, room);
                            cockpitIndexCache.put(cockpitId, new CachedEntry<>(roomKey));
                            vehicleIndexCache.put(vehicleId, new CachedEntry<>(roomKey));
                        })
                        .thenReturn(room);
                });
            });
    }

    @PreDestroy
    public void shutdown() {
        log.info("关闭本节点[{}]所有房间，共 {} 个", nodeId, localRooms.size());
        for (Map.Entry<String, ParallelDrivingRoom> entry : localRooms.entrySet()) {
            ParallelDrivingRoom room = entry.getValue();
            room.close().subscribe();
        }
        localRooms.clear();
        cockpitIndexCache.clear();
        vehicleIndexCache.clear();
    }

    private static class CachedEntry<T> {
        final T value;
        final long timestamp;

        CachedEntry(T value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > L1_CACHE_TTL_MS;
        }
    }
}
