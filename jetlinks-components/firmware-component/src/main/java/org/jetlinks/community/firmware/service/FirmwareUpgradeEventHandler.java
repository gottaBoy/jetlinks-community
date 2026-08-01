package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.event.EventMessage;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;

/**
 * 监听设备 OTA 状态上报事件，更新升级历史记录和任务完成状态。
 *
 * <p>两个数据源：
 * <ol>
 *   <li><b>MQTT 设备事件</b> — 设备通过 MQTT 上报 ota_status 事件，
 *       协议包解码为 {@link EventMessage}，EventBus topic:
 *       {@code /device/{productId}/{deviceId}/message/event/ota_status}</li>
 *   <li><b>DB 保存事件</b> — {@link FirmwareUpgradeHistoryEntity} 保存后，
 *       自动检查对应任务是否所有设备已完成</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FirmwareUpgradeEventHandler {

    private final FirmwareUpgradeHistoryService historyService;
    private final FirmwareUpgradeTaskService taskService;
    private final EventBus eventBus;

    private Disposable otaStatusSubscription;

    /**
     * 订阅设备 OTA 状态上报事件（MQTT → 协议包 → EventBus）。
     */
    @PostConstruct
    public void init() {
        Subscription subscription = Subscription.builder()
            .subscriberId("firmware-ota-status-handler")
            .topics("/device/*/+/message/event/ota_status")
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        otaStatusSubscription = eventBus
            .subscribe(subscription, EventMessage.class)
            .publishOn(Schedulers.boundedElastic())
            .flatMap(this::handleOtaStatus, 8)
            .onErrorContinue((err, obj) ->
                log.error("OTA status handler error, skipped: {}", err.getMessage(), err))
            .subscribe();

        log.info("Firmware OTA status handler subscribed: /device/*/+/message/event/ota_status");
    }

    @PreDestroy
    public void destroy() {
        if (otaStatusSubscription != null && !otaStatusSubscription.isDisposed()) {
            otaStatusSubscription.dispose();
            log.info("Firmware OTA status handler unsubscribed");
        }
    }

    /**
     * 处理设备上报的 OTA 状态事件。
     */
    @SuppressWarnings("unchecked")
    Mono<Void> handleOtaStatus(EventMessage msg) {
        String deviceId = msg.getDeviceId();
        if (deviceId == null || deviceId.isEmpty()) {
            return Mono.empty();
        }

        Object rawData = msg.getData();
        if (!(rawData instanceof Map)) {
            log.debug("OTA status event data is not Map, ignoring: deviceId={}", deviceId);
            return Mono.empty();
        }

        Map<String, Object> data = (Map<String, Object>) rawData;
        String state = data.get("state") != null ? data.get("state").toString() : null;
        if (state == null) {
            return Mono.empty();
        }

        Object progressObj = data.get("progress");
        int progress = progressObj instanceof Number ? ((Number) progressObj).intValue() : 0;

        log.info("OTA status: device={}, state={}, progress={}%", deviceId, state, progress);

        return historyService.createQuery()
            .where(FirmwareUpgradeHistoryEntity::getDeviceId, deviceId)
            .and(FirmwareUpgradeHistoryEntity::getStatus, "pending")
            .or(FirmwareUpgradeHistoryEntity::getStatus, "downloading")
            .or(FirmwareUpgradeHistoryEntity::getStatus, "installing")
            .fetchOne()
            .flatMap(history -> updateHistory(history, state, progress))
            .switchIfEmpty(Mono.fromRunnable(() ->
                log.debug("No active upgrade history for device={}, ignoring ota_status", deviceId)));
    }

    private Mono<Void> updateHistory(FirmwareUpgradeHistoryEntity history, String state, int progress) {
        history.setProgress(progress);

        switch (state) {
            case "downloading":
            case "installing":
                history.setStatus(state);
                break;
            case "success":
                history.setStatus("success");
                history.setProgress(100);
                history.setCompleteTime(System.currentTimeMillis());
                break;
            case "failed":
                history.setStatus("failed");
                history.setCompleteTime(System.currentTimeMillis());
                break;
            default:
                break;
        }

        return historyService.updateById(history.getId(), Mono.just(history))
            .then(Mono.defer(() -> {
                if ("success".equals(state) || "failed".equals(state)) {
                    return checkTaskCompletion(history.getTaskId());
                }
                return Mono.empty();
            }))
            .doOnSuccess(v -> log.info("OTA history updated: device={}, state={}, progress={}%",
                history.getDeviceId(), state, progress))
            .doOnError(err -> log.error("Failed to update OTA history: device={}", history.getDeviceId(), err));
    }

    /**
     * 监听升级历史保存事件，自动检查对应任务是否完成。
     */
    @EventListener
    public void onHistorySaved(EntitySavedEvent<FirmwareUpgradeHistoryEntity> event) {
        event.getEntity().stream()
            .map(FirmwareUpgradeHistoryEntity::getTaskId)
            .filter(taskId -> taskId != null && !taskId.isEmpty())
            .distinct()
            .forEach(taskId -> checkTaskCompletion(taskId).subscribe(
                v -> log.debug("Task completion check done: taskId={}", taskId),
                err -> log.error("Task completion check failed: taskId={}", taskId, err)
            ));
    }

    /**
     * 检查任务是否所有设备都已完成，若是则标记任务完成并更新计数器。
     */
    public reactor.core.publisher.Mono<Void> checkTaskCompletion(String taskId) {
        if (taskId == null) return reactor.core.publisher.Mono.empty();

        return historyService.createQuery()
            .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
            .count()
            .flatMap(total -> {
                if (total == 0) return reactor.core.publisher.Mono.empty();
                return historyService.createQuery()
                    .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
                    .and(FirmwareUpgradeHistoryEntity::getStatus, "success")
                    .or(FirmwareUpgradeHistoryEntity::getStatus, "failed")
                    .or(FirmwareUpgradeHistoryEntity::getStatus, "cancelled")
                    .count()
                    .flatMap(completed -> {
                        if (completed.longValue() >= total.longValue()) {
                            return taskService.findById(taskId)
                                .flatMap(task -> historyService.createQuery()
                                    .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
                                    .and(FirmwareUpgradeHistoryEntity::getStatus, "success")
                                    .count()
                                    .flatMap(success -> historyService.createQuery()
                                        .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
                                        .and(FirmwareUpgradeHistoryEntity::getStatus, "failed")
                                        .count()
                                        .flatMap(failed -> {
                                            task.setSuccessCount(success.intValue());
                                            task.setFailCount(failed.intValue());
                                            task.setStatus("completed");
                                            return taskService.updateById(task.getId(), reactor.core.publisher.Mono.just(task));
                                        })
                                    )
                                )
                                .then();
                        }
                        return reactor.core.publisher.Mono.empty();
                    });
            });
    }
}
