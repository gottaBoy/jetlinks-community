package org.jetlinks.community.parallel.driving.room;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@Slf4j
public class RoomCleanupScheduler {

    private final ReactiveRedisTemplate<Object, Object> redis;
    private final ParallelDrivingRoomManager roomManager;
    private final String nodeId;

    private static final String ROOM_INFO_PREFIX = "pd:room:info:";
    private static final String COCKPIT_INDEX_PREFIX = "pd:room:idx:cockpit:";
    private static final String VEHICLE_INDEX_PREFIX = "pd:room:idx:vehicle:";
    private static final Duration KEY_TTL = Duration.ofHours(12);
    private static final long SCAN_COUNT = 200;

    public RoomCleanupScheduler(ReactiveRedisTemplate<Object, Object> redis,
                                 ParallelDrivingRoomManager roomManager,
                                 @Value("${jetlinks.server-id:standalone}") String nodeId) {
        this.redis = redis;
        this.roomManager = roomManager;
        this.nodeId = nodeId;
    }

    @Scheduled(fixedDelay = 300_000, initialDelay = 60_000)
    public void cleanupOrphanedKeys() {
        log.debug("Starting room key cleanup cycle for node={}", nodeId);

        scanAndRefreshTTL()
            .then(cleanOrphanedIndexes())
            .subscribe(
                v -> log.debug("Room key cleanup completed"),
                error -> log.error("Room key cleanup failed", error)
            );
    }

    private Flux<Object> scanKeys(String pattern) {
        ScanOptions options = ScanOptions.scanOptions()
            .match(pattern)
            .count(SCAN_COUNT)
            .build();
        return redis.scan(options);
    }

    private Mono<Void> scanAndRefreshTTL() {
        return scanKeys(ROOM_INFO_PREFIX + "*")
            .flatMap(key -> redis.getExpire(key)
                .flatMap(ttl -> {
                    if (ttl.getSeconds() < 0) {
                        return redis.expire(key, KEY_TTL).then();
                    }
                    return Mono.empty();
                })
            )
            .then();
    }

    private Mono<Void> cleanOrphanedIndexes() {
        return Flux.merge(
            cleanOrphanedIndexesForPrefix(COCKPIT_INDEX_PREFIX),
            cleanOrphanedIndexesForPrefix(VEHICLE_INDEX_PREFIX)
        ).then();
    }

    private Flux<Void> cleanOrphanedIndexesForPrefix(String prefix) {
        return scanKeys(prefix + "*")
            .flatMap(indexKey -> redis.opsForValue().get(indexKey)
                .cast(String.class)
                .flatMap(roomKey -> redis.hasKey(ROOM_INFO_PREFIX + roomKey)
                    .flatMap(exists -> {
                        if (!Boolean.TRUE.equals(exists)) {
                            log.info("Removing orphaned index: {}", indexKey);
                            return redis.delete(indexKey).then();
                        }
                        return Mono.<Void>empty();
                    })
                )
            );
    }
}
