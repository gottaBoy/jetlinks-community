package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 监听设备 OTA 状态上报事件,更新升级历史记录和任务完成状态。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FirmwareUpgradeEventHandler {

    private final FirmwareUpgradeHistoryService historyService;
    private final FirmwareUpgradeTaskService taskService;

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
                                .flatMap(task -> {
                                    return historyService.createQuery()
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
                                        );
                                })
                                .then();
                        }
                        return reactor.core.publisher.Mono.empty();
                    });
            });
    }
}
