package org.jetlinks.community.zota.rule;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.zota.mgmt.ZotaMgmtClient;
import org.jetlinks.rule.engine.api.RuleData;
import org.jetlinks.rule.engine.api.task.ExecutionContext;
import org.jetlinks.rule.engine.api.task.TaskExecutor;
import org.jetlinks.rule.engine.api.task.TaskExecutorProvider;
import org.jetlinks.rule.engine.defaults.AbstractTaskExecutor;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * Rule engine action: triggers zota-server rollback when sensor anomalies are detected.
 *
 * Usage in JetLinks rule engine:
 * 1. Create a rule: "sensor.topic.frequency" property < threshold → trigger this action
 * 2. This action calls zota-server MGMT API to cancel the active deployment
 *
 * Compatible with existing JetLinks rule engine infrastructure.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ZotaRollbackAction implements TaskExecutorProvider {

    private final ZotaMgmtClient mgmtClient;

    @Override
    public String getExecutor() {
        return "zota-rollback";
    }

    @Override
    public Mono<TaskExecutor> createTask(ExecutionContext context) {
        return Mono.just(new RollbackTaskExecutor(context, mgmtClient));
    }

    static class RollbackTaskExecutor extends AbstractTaskExecutor {

        private final ZotaMgmtClient mgmtClient;

        RollbackTaskExecutor(ExecutionContext context, ZotaMgmtClient mgmtClient) {
            super(context);
            this.mgmtClient = mgmtClient;
        }

        @Override
        public String getName() {
            return "ZOTA Rollback";
        }

        @Override
        protected Disposable doStart() {
            return context
                .getInput()
                .accept()
                .concatMap(RuleData::dataToMap)
                .flatMap(data -> {
                    String controllerId = (String) data.getOrDefault("vin",
                        data.getOrDefault("deviceId",
                            data.get("controllerId")));

                    if (controllerId == null) {
                        log.warn("ZOTA rollback action: no VIN in rule data, skipping");
                        return Mono.empty();
                    }

                    String reason = (String) data.getOrDefault("reason",
                        "Sensor anomaly detected — automatic rollback triggered by JetLinks rule engine");

                    log.warn("ZOTA rollback triggered: vin={}, reason={}", controllerId, reason);

                    return mgmtClient.triggerRollback(controllerId, reason)
                        .doOnSuccess(result -> {
                            boolean ok = "ok".equals(result.get("status"));
                            if (ok) {
                                context.logger().info("Rollback triggered for {}: {}", controllerId, reason);
                            } else {
                                context.logger().warn("Rollback failed for {}: {}", controllerId, result);
                            }
                        })
                        .then();
                })
                .subscribe();
        }

        @Override
        public void reload() {
            if (disposable != null) {
                disposable.dispose();
            }
            disposable = doStart();
        }
    }
}
