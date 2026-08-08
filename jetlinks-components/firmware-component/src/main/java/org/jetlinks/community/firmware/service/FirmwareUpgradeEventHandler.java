package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeStatus;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.DeviceOnlineMessage;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.firmware.ReportFirmwareMessage;
import org.jetlinks.core.message.firmware.RequestFirmwareMessage;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Objects;

/**
 * Consumes transport-neutral ota_status events and updates the device upgrade state machine.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FirmwareUpgradeEventHandler {

    static final String OTA_STATUS_TOPIC = "/device/*/*/message/event/ota_status";
    static final String FIRMWARE_PULL_TOPIC = "/device/*/*/firmware/pull";
    static final String FIRMWARE_REPORT_TOPIC = "/device/*/*/firmware/report";
    static final String DEVICE_ONLINE_TOPIC = "/device/*/*/online";

    private final FirmwareUpgradeHistoryService historyService;
    private final FirmwareUpgradeTaskService taskService;
    private final DeviceFirmwareVersionService firmwareVersionService;
    private final EventBus eventBus;

    private Disposable otaStatusSubscription;
    private Disposable firmwarePullSubscription;
    private Disposable firmwareReportSubscription;
    private Disposable deviceOnlineSubscription;

    @PostConstruct
    public void init() {
        Subscription subscription = Subscription
            .builder()
            .subscriberId("firmware-ota-status-handler")
            .topics(OTA_STATUS_TOPIC)
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        otaStatusSubscription = eventBus
            .subscribe(subscription, EventMessage.class)
            .publishOn(Schedulers.boundedElastic())
            .flatMap(this::handleOtaStatus, 8)
            .onErrorContinue((error, event) ->
                log.error("OTA status event processing failed: event={}", event, error))
            .subscribe();

        Subscription pullSubscription = Subscription
            .builder()
            .subscriberId("firmware-pull-request-handler")
            .topics(FIRMWARE_PULL_TOPIC)
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        firmwarePullSubscription = eventBus
            .subscribe(pullSubscription, RequestFirmwareMessage.class)
            .flatMap(taskService::handlePullRequest, 8)
            .onErrorContinue((error, request) ->
                log.error("OTA pull request processing failed: request={}", request, error))
            .subscribe();

        Subscription reportSubscription = Subscription
            .builder()
            .subscriberId("device-firmware-report-handler")
            .topics(FIRMWARE_REPORT_TOPIC)
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        firmwareReportSubscription = eventBus
            .subscribe(reportSubscription, ReportFirmwareMessage.class)
            .flatMap(this::handleFirmwareReport, 8)
            .onErrorContinue((error, report) ->
                log.error("Firmware report processing failed: report={}", report, error))
            .subscribe();

        Subscription onlineSubscription = Subscription
            .builder()
            .subscriberId("device-online-firmware-version-handler")
            .topics(DEVICE_ONLINE_TOPIC)
            .features(Subscription.Feature.local, Subscription.Feature.broker)
            .build();

        deviceOnlineSubscription = eventBus
            .subscribe(onlineSubscription, DeviceOnlineMessage.class)
            .flatMap(this::handleDeviceOnline, 8)
            .onErrorContinue((error, message) ->
                log.error("Online firmware version processing failed: message={}", message, error))
            .subscribe();
    }

    @PreDestroy
    public void destroy() {
        if (otaStatusSubscription != null) {
            otaStatusSubscription.dispose();
        }
        if (firmwarePullSubscription != null) {
            firmwarePullSubscription.dispose();
        }
        if (firmwareReportSubscription != null) {
            firmwareReportSubscription.dispose();
        }
        if (deviceOnlineSubscription != null) {
            deviceOnlineSubscription.dispose();
        }
    }

    Mono<Void> handleFirmwareReport(ReportFirmwareMessage message) {
        return firmwareVersionService.updateCurrentVersion(
            message.getDeviceId(),
            message.getVersion());
    }

    Mono<Void> handleDeviceOnline(DeviceOnlineMessage message) {
        return firmwareVersionService.updateCurrentVersion(
            message.getDeviceId(),
            firstValue(
                message.getHeader("firmwareVersion").orElse(null),
                message.getHeader("fwVersion").orElse(null)));
    }

    @SuppressWarnings("unchecked")
    Mono<Void> handleOtaStatus(EventMessage message) {
        if (!StringUtils.hasText(message.getDeviceId()) || !(message.getData() instanceof Map<?, ?>)) {
            return Mono.empty();
        }
        Map<String, Object> data = (Map<String, Object>) message.getData();
        String nextStatus = FirmwareUpgradeStatus.normalize(stringValue(data.get("state")));
        if (nextStatus == null) {
            log.warn("Ignoring unknown OTA state: device={}, state={}",
                message.getDeviceId(), data.get("state"));
            return Mono.empty();
        }
        if (!FirmwareUpgradeStatus.isClientReportable(nextStatus)) {
            log.warn("Ignoring server-owned OTA state from device: device={}, state={}",
                message.getDeviceId(), nextStatus);
            return Mono.empty();
        }

        String upgradeId = firstText(data.get("upgradeId"), message.getHeader("upgradeId").orElse(null));
        if (!StringUtils.hasText(upgradeId)) {
            log.warn("Ignoring OTA state without upgradeId: device={}, state={}",
                message.getDeviceId(), nextStatus);
            return Mono.empty();
        }
        long receivedAt = System.currentTimeMillis();
        long eventTime = Math.min(longValue(
            firstValue(data.get("eventTime"), data.get("ts"), message.getTimestamp()),
            receivedAt), receivedAt);

        return findHistory(message.getDeviceId(), upgradeId)
            .map(java.util.Optional::of)
            .defaultIfEmpty(java.util.Optional.empty())
            .flatMap(history -> {
                if (history.isEmpty() || !correlationMatches(history.get(), data)) {
                    log.warn("No OTA history matched correlation: device={}, upgrade={}",
                        message.getDeviceId(), upgradeId);
                    return Mono.empty();
                }
                return applyEvent(history.get(), data, nextStatus, eventTime);
            });
    }

    private Mono<FirmwareUpgradeHistoryEntity> findHistory(String deviceId, String upgradeId) {
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getUpgradeId, upgradeId)
            .fetchOne()
            .filter(history -> Objects.equals(deviceId, history.getDeviceId()));
    }

    private boolean correlationMatches(FirmwareUpgradeHistoryEntity history,
                                       Map<String, Object> data) {
        return matchesText(data.get("taskId"), history.getTaskId())
            && matchesText(data.get("historyId"), history.getId())
            && matchesInteger(data.get("attempt"), history.getAttempt());
    }

    private boolean matchesText(Object reported, String expected) {
        return reported == null || Objects.equals(String.valueOf(reported), expected);
    }

    private boolean matchesInteger(Object reported, Integer expected) {
        if (reported == null) {
            return true;
        }
        try {
            return Objects.equals(Integer.parseInt(String.valueOf(reported)), expected);
        } catch (NumberFormatException ignore) {
            return false;
        }
    }

    private Mono<Void> applyEvent(FirmwareUpgradeHistoryEntity history,
                                  Map<String, Object> data,
                                  String nextStatus,
                                  long eventTime) {
        String currentStatus = FirmwareUpgradeStatus.normalize(history.getStatus());
        if (FirmwareUpgradeStatus.isTerminal(currentStatus)) {
            return Mono.empty();
        }
        if (history.getLastEventTime() != null && eventTime < history.getLastEventTime()) {
            log.debug("Ignoring out-of-order OTA event: upgrade={}, eventTime={}, lastEventTime={}",
                history.getUpgradeId(), eventTime, history.getLastEventTime());
            return Mono.empty();
        }
        if (!FirmwareUpgradeStatus.canTransition(currentStatus, nextStatus)) {
            log.debug("Ignoring OTA state regression: upgrade={}, current={}, next={}",
                history.getUpgradeId(), currentStatus, nextStatus);
            return Mono.empty();
        }

        String expectedStatus = history.getStatus();
        Long expectedLastEventTime = history.getLastEventTime();
        long now = System.currentTimeMillis();
        history.setStatus(nextStatus);
        history.setLastEventTime(eventTime);
        history.setLastReportTime(now);
        history.setProgress(resolveProgress(history.getProgress(), data.get("progress"), nextStatus));
        history.setReportedVersion(firstText(
            data.get("reportedVersion"),
            data.get("fwVersion"),
            history.getReportedVersion()));

        if (FirmwareUpgradeStatus.ACCEPTED.getValue().equals(nextStatus) && history.getAckTime() == null) {
            history.setAckTime(now);
        }
        if (FirmwareUpgradeStatus.isFailure(nextStatus)) {
            history.setErrorCode(firstText(data.get("errorCode"), nextStatus.toUpperCase()));
            history.setErrorMessage(firstText(data.get("errorMessage"), data.get("message")));
        }
        if (FirmwareUpgradeStatus.isTerminal(nextStatus)) {
            history.setCompleteTime(now);
            history.setActiveKey(null);
            if (FirmwareUpgradeStatus.SUCCESS.getValue().equals(nextStatus)) {
                history.setProgress(100);
            }
        }

        var update = historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus, nextStatus)
            .set(FirmwareUpgradeHistoryEntity::getLastEventTime, eventTime)
            .set(FirmwareUpgradeHistoryEntity::getLastReportTime, now)
            .set(FirmwareUpgradeHistoryEntity::getProgress, history.getProgress());
        if (StringUtils.hasText(history.getReportedVersion())) {
            update.set(FirmwareUpgradeHistoryEntity::getReportedVersion, history.getReportedVersion());
        }
        if (history.getAckTime() != null) {
            update.set(FirmwareUpgradeHistoryEntity::getAckTime, history.getAckTime());
        }
        if (StringUtils.hasText(history.getErrorCode())) {
            update.set(FirmwareUpgradeHistoryEntity::getErrorCode, history.getErrorCode());
        }
        if (StringUtils.hasText(history.getErrorMessage())) {
            update.set(FirmwareUpgradeHistoryEntity::getErrorMessage, history.getErrorMessage());
        }
        if (history.getCompleteTime() != null) {
            update
                .set(FirmwareUpgradeHistoryEntity::getCompleteTime, history.getCompleteTime())
                .setNull(FirmwareUpgradeHistoryEntity::getActiveKey);
        }

        var where = update
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus, expectedStatus)
            .when(expectedLastEventTime == null,
                  condition -> condition.isNull(FirmwareUpgradeHistoryEntity::getLastEventTime))
            .when(expectedLastEventTime != null,
                  condition -> condition.and(
                      FirmwareUpgradeHistoryEntity::getLastEventTime,
                      expectedLastEventTime));

        return where
            .execute()
            .flatMap(updated -> {
                if (updated > 0) {
                    Mono<Void> refreshTask = taskService.refreshTaskStatus(history.getTaskId());
                    if (FirmwareUpgradeStatus.SUCCESS.getValue().equals(nextStatus)) {
                        return firmwareVersionService
                            .updateCurrentVersion(history.getDeviceId(), history.getReportedVersion())
                            .then(refreshTask);
                    }
                    return refreshTask;
                }
                return historyService
                    .findById(history.getId())
                    .flatMap(latest -> applyEvent(latest, data, nextStatus, eventTime));
            });
    }

    private int resolveProgress(Integer current, Object reported, String status) {
        int oldValue = current == null ? 0 : current;
        int newValue = reported instanceof Number
            ? ((Number) reported).intValue()
            : oldValue;
        if (FirmwareUpgradeStatus.SUCCESS.getValue().equals(status)) {
            return 100;
        }
        return Math.max(oldValue, Math.max(0, Math.min(100, newValue)));
    }

    @EventListener
    public void onHistorySaved(EntitySavedEvent<FirmwareUpgradeHistoryEntity> event) {
        Flux
            .fromIterable(event.getEntity())
            .map(FirmwareUpgradeHistoryEntity::getTaskId)
            .filter(StringUtils::hasText)
            .distinct()
            .flatMap(taskService::refreshTaskStatus)
            .subscribe(null, error -> log.error("OTA task aggregation failed", error));
    }

    public Mono<Void> checkTaskCompletion(String taskId) {
        return taskService.refreshTaskStatus(taskId);
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String firstText(Object... values) {
        for (Object value : values) {
            if (value != null && StringUtils.hasText(String.valueOf(value))) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private static Object firstValue(Object... values) {
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static long longValue(Object value, long defaultValue) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value != null) {
            try {
                return Long.parseLong(String.valueOf(value));
            } catch (NumberFormatException ignore) {
                // use server time
            }
        }
        return defaultValue;
    }
}
