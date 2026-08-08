package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeStatus;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Database-backed timeout recovery. It remains effective after a service restart.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FirmwareUpgradeTimeoutScheduler {

    private final FirmwareUpgradeHistoryService historyService;
    private final FirmwareUpgradeTaskService taskService;

    @Scheduled(fixedDelayString = "${jetlinks.firmware.ota.timeout-scan-interval:30000}")
    public void scan() {
        long now = System.currentTimeMillis();
        historyService
            .createQuery()
            .in(FirmwareUpgradeHistoryEntity::getStatus, FirmwareUpgradeStatus.activeValues())
            .fetch()
            .flatMap(history -> taskService
                .findById(history.getTaskId())
                .flatMap(task -> expireIfNeeded(history, task, now)), 8)
            .subscribe(null, error -> log.error("Firmware OTA timeout scan failed", error));
    }

    @Scheduled(fixedDelayString = "${jetlinks.firmware.ota.dispatch-scan-interval:3000}")
    public void dispatchQueued() {
        taskService
            .dispatchQueuedHistories()
            .subscribe(null, error -> log.error("Firmware OTA queued dispatch scan failed", error));
    }

    private Mono<Void> expireIfNeeded(FirmwareUpgradeHistoryEntity history,
                                      FirmwareUpgradeTaskEntity task,
                                      long now) {
        String status = FirmwareUpgradeStatus.normalize(history.getStatus());
        String timeoutStatus = null;
        String errorCode = null;
        String errorMessage = null;

        if (FirmwareUpgradeStatus.QUEUED.getValue().equals(status)) {
            return Mono.empty();
        }
        if (expired(history.getStartTime(), task.getTimeoutSeconds(), now)) {
            timeoutStatus = FirmwareUpgradeStatus.EXECUTION_TIMEOUT.getValue();
            errorCode = "EXECUTION_TIMEOUT";
            errorMessage = "设备升级超过任务总超时时间";
        } else if (FirmwareUpgradeStatus.DISPATCHING.getValue().equals(status)
            && expired(history.getDispatchTime(), task.getResponseTimeoutSeconds(), now)) {
            timeoutStatus = FirmwareUpgradeStatus.DISPATCH_FAILED.getValue();
            errorCode = "DISPATCH_STALLED";
            errorMessage = "服务端下发升级指令超时";
        } else if (FirmwareUpgradeStatus.DISPATCHED.getValue().equals(status)
            && expired(history.getDispatchTime(), task.getResponseTimeoutSeconds(), now)) {
            timeoutStatus = FirmwareUpgradeStatus.ACK_TIMEOUT.getValue();
            errorCode = "ACK_TIMEOUT";
            errorMessage = "客户端未在响应超时时间内接受升级任务";
        } else if (requiresStatusReport(status)
            && expired(lastActivityTime(history), task.getStatusTimeoutSeconds(), now)) {
            timeoutStatus = FirmwareUpgradeStatus.STATUS_TIMEOUT.getValue();
            errorCode = "STATUS_TIMEOUT";
            errorMessage = "客户端升级状态上报超时";
        }

        if (timeoutStatus == null) {
            return Mono.empty();
        }
        String finalTimeoutStatus = timeoutStatus;
        var update = historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus, timeoutStatus)
            .set(FirmwareUpgradeHistoryEntity::getErrorCode, errorCode)
            .set(FirmwareUpgradeHistoryEntity::getErrorMessage, errorMessage)
            .set(FirmwareUpgradeHistoryEntity::getCompleteTime, now)
            .setNull(FirmwareUpgradeHistoryEntity::getActiveKey)
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus, history.getStatus());
        if (requiresStatusReport(status)) {
            update
                .when(history.getLastReportTime() == null,
                      condition -> condition.isNull(FirmwareUpgradeHistoryEntity::getLastReportTime))
                .when(history.getLastReportTime() != null,
                      condition -> condition.and(
                          FirmwareUpgradeHistoryEntity::getLastReportTime,
                          history.getLastReportTime()));
        }
        return update
            .execute()
            .flatMap(updated -> {
                if (updated <= 0) {
                    return Mono.empty();
                }
                log.warn(
                    "Firmware OTA timed out: task={}, upgrade={}, device={}, status={}",
                    history.getTaskId(), history.getUpgradeId(), history.getDeviceId(), finalTimeoutStatus);
                return taskService.refreshTaskStatus(history.getTaskId());
            });
    }

    private boolean requiresStatusReport(String status) {
        return FirmwareUpgradeStatus.ACCEPTED.getValue().equals(status)
            || FirmwareUpgradeStatus.PREPARING.getValue().equals(status)
            || FirmwareUpgradeStatus.DOWNLOADING.getValue().equals(status)
            || FirmwareUpgradeStatus.DOWNLOADED.getValue().equals(status)
            || FirmwareUpgradeStatus.VERIFYING.getValue().equals(status)
            || FirmwareUpgradeStatus.VERIFIED.getValue().equals(status)
            || FirmwareUpgradeStatus.INSTALLING.getValue().equals(status)
            || FirmwareUpgradeStatus.REBOOTING.getValue().equals(status)
            || FirmwareUpgradeStatus.POST_CHECKING.getValue().equals(status);
    }

    private Long lastActivityTime(FirmwareUpgradeHistoryEntity history) {
        if (history.getLastReportTime() != null) {
            return history.getLastReportTime();
        }
        if (history.getAckTime() != null) {
            return history.getAckTime();
        }
        return history.getDispatchTime();
    }

    private boolean expired(Long since, Integer timeoutSeconds, long now) {
        return since != null
            && timeoutSeconds != null
            && timeoutSeconds > 0
            && now - since >= timeoutSeconds * 1000L;
    }
}
