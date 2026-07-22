package org.jetlinks.community.parallel.driving.service;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.ezorm.rdb.mapping.ReactiveRepository;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.jetlinks.community.parallel.driving.entity.ParallelDrivingControlLog;
import org.jetlinks.community.parallel.driving.message.ParallelDrivingControlMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ParallelDrivingControlLogService {

    private final Sinks.Many<ParallelDrivingControlLog> logSink =
        Sinks.many().multicast().onBackpressureBuffer(10000);
    private Disposable batchSubscription;

    private final ReactiveRepository<ParallelDrivingControlLog, String> logRepository;
    private final Map<String, ControlStatistics> statistics = new ConcurrentHashMap<>();

    @Autowired
    public ParallelDrivingControlLogService(
            ReactiveRepository<ParallelDrivingControlLog, String> logRepository) {
        this.logRepository = logRepository;
    }

    @PostConstruct
    public void init() {
        batchSubscription = logSink.asFlux()
            .bufferTimeout(100, Duration.ofSeconds(5))
            .filter(batch -> !batch.isEmpty())
            .concatMap(this::persistBatch)
            .subscribe(
                count -> {},
                error -> log.error("Control log batch processing failed", error)
            );
        log.info("ControlLogService initialized with async batch persistence (bufferTimeout 100/5s)");
    }

    @PreDestroy
    public void shutdown() {
        logSink.tryEmitComplete();
        if (batchSubscription != null) {
            batchSubscription.dispose();
        }
    }

    public void logControlCommand(String cockpitDeviceId,
                                 String vehicleDeviceId,
                                 ParallelDrivingControlMessage controlMessage,
                                 boolean success,
                                 String errorMessage) {
        ParallelDrivingControlLog controlLog = new ParallelDrivingControlLog();
        controlLog.setCockpitDeviceId(cockpitDeviceId);
        controlLog.setVehicleDeviceId(vehicleDeviceId);
        controlLog.setControlType(controlMessage.getControlType() != null
            ? controlMessage.getControlType().getValue()
            : "unknown");
        controlLog.setControlParams(controlMessage.getControlParams());
        controlLog.setSuccess(success);
        controlLog.setErrorMessage(errorMessage);
        controlLog.setTimestamp(System.currentTimeMillis());

        Sinks.EmitResult result = logSink.tryEmitNext(controlLog);
        if (result.isFailure()) {
            log.warn("Failed to emit control log: {}", result);
        }

        String key = cockpitDeviceId + "-" + vehicleDeviceId;
        statistics.computeIfAbsent(key, k -> new ControlStatistics())
            .increment(controlLog.getControlType(), success);

        if (success) {
            log.info("控制指令发送成功: cockpit={}, vehicle={}, type={}",
                cockpitDeviceId, vehicleDeviceId, controlLog.getControlType());
        } else {
            log.warn("控制指令发送失败: cockpit={}, vehicle={}, type={}, error={}",
                cockpitDeviceId, vehicleDeviceId, controlLog.getControlType(), errorMessage);
        }
    }

    private Mono<Integer> persistBatch(List<ParallelDrivingControlLog> batch) {
        log.debug("Persisting control log batch: size={}", batch.size());
        return Flux.fromIterable(batch)
            .as(logRepository::insert)
            .doOnSuccess(count -> log.debug("Persisted {} control logs", count))
            .onErrorResume(err -> {
                log.error("Failed to persist control log batch (size={}): {}",
                    batch.size(), err.getMessage());
                return Mono.just(0);
            });
    }

    public Mono<PagerResult<ParallelDrivingControlLog>> getControlLogs(QueryParamEntity query) {
        return logRepository.createQuery()
            .setParam(query)
            .count()
            .flatMap(total -> {
                if (total == 0) {
                    return Mono.just(PagerResult.empty());
                }
                return logRepository.createQuery()
                    .setParam(query)
                    .fetch()
                    .collectList()
                    .map(list -> PagerResult.of(total, list, query));
            });
    }

    public Mono<List<ParallelDrivingControlLog>> getControlLogs(String cockpitDeviceId,
                                                                 String vehicleDeviceId,
                                                                 int limit) {
        QueryParamEntity query = new QueryParamEntity();
        query.setPageSize(limit);
        query.setPaging(false);
        if (cockpitDeviceId != null) {
            query.and("cockpitDeviceId", "eq", cockpitDeviceId);
        }
        if (vehicleDeviceId != null) {
            query.and("vehicleDeviceId", "eq", vehicleDeviceId);
        }
        query.orderBy("timestamp").desc();
        return logRepository.createQuery()
            .setParam(query)
            .fetch()
            .take(limit)
            .collectList();
    }

    public ControlStatistics getStatistics(String cockpitDeviceId, String vehicleDeviceId) {
        String key = cockpitDeviceId + "-" + vehicleDeviceId;
        return statistics.getOrDefault(key, new ControlStatistics());
    }

    @lombok.Data
    public static class ControlStatistics {
        private final Map<String, Long> successCount = new HashMap<>();
        private final Map<String, Long> failureCount = new HashMap<>();
        private long totalSuccess = 0;
        private long totalFailure = 0;

        public void increment(String controlType, boolean success) {
            if (success) {
                successCount.merge(controlType, 1L, Long::sum);
                totalSuccess++;
            } else {
                failureCount.merge(controlType, 1L, Long::sum);
                totalFailure++;
            }
        }

        public long getTotal() {
            return totalSuccess + totalFailure;
        }

        public double getSuccessRate() {
            long total = getTotal();
            return total > 0 ? (double) totalSuccess / total : 0.0;
        }
    }
}
