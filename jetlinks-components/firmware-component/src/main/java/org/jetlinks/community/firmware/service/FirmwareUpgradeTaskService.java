package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.ezorm.rdb.mapping.defaults.SaveResult;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.jetlinks.community.firmware.entity.FirmwareEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeStatus;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.exception.ProductNotActivatedException;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.firmware.RequestFirmwareMessage;
import org.jetlinks.core.message.firmware.RequestFirmwareMessageReply;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionParameter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 固件升级任务服务 — 任务创建、启动（下发 OTA 指令）、停止。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FirmwareUpgradeTaskService extends GenericReactiveCrudService<FirmwareUpgradeTaskEntity, String> {

    private static final int DEFAULT_RESPONSE_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_STATUS_TIMEOUT_SECONDS = 300;
    private static final int DEFAULT_EXECUTION_TIMEOUT_SECONDS = 3600;

    private final FirmwareService firmwareService;
    private final FirmwareUpgradeHistoryService historyService;
    private final DeviceRegistry deviceRegistry;
    private final LocalDeviceInstanceService deviceService;
    private final TransactionalOperator transactionalOperator;

    /**
     * 从任务 terms 中递归提取设备 ID。
     */
    public List<String> extractDeviceIds(FirmwareUpgradeTaskEntity task) {
        Set<String> ids = new LinkedHashSet<>();
        collectDeviceIds(task.getTerms(), ids);
        return new ArrayList<>(ids);
    }

    @SuppressWarnings("unchecked")
    private void collectDeviceIds(Object node, Set<String> ids) {
        if (node instanceof Collection<?>) {
            for (Object child : (Collection<?>) node) {
                collectDeviceIds(child, ids);
            }
            return;
        }
        if (!(node instanceof Map<?, ?>)) {
            if (node != null && StringUtils.hasText(node.toString())) {
                ids.add(node.toString());
            }
            return;
        }
        Map<String, Object> term = (Map<String, Object>) node;
        if ("id".equals(term.get("column"))) {
            collectDeviceIds(term.get("value"), ids);
        }
        collectDeviceIds(term.get("terms"), ids);
    }

    /**
     * 创建任务及固定设备快照，持久化成功后自动开始下发。
     */
    public Mono<SaveResult> createTask(FirmwareUpgradeTaskEntity task) {
        task.setId(FirmwareUpgradeIdGenerator.taskId());
        normalizeTaskOptions(task);
        task.setStatus("pending");
        task.setSuccessCount(0);
        task.setFailCount(0);
        task.setQueuedCount(0);
        task.setRunningCount(0);
        task.setCancelledCount(0);

        return ensureProductActivated(task.getProductId())
            .then(Mono.zip(firmwareService.findById(task.getFirmwareId())
                                          .switchIfEmpty(Mono.error(
                                              new IllegalArgumentException("固件不存在: " + task.getFirmwareId()))),
                           resolveTargetDevices(task)))
            .flatMap(tuple -> {
                FirmwareEntity firmware = tuple.getT1();
                validateFirmware(task, firmware);
                List<DeviceInstanceEntity> devices = tuple.getT2();
                task.setDeviceCount(devices.size());
                task.setQueuedCount(devices.size());
                List<FirmwareUpgradeHistoryEntity> histories = devices
                    .stream()
                    .map(device -> createHistorySnapshot(task, firmware, device))
                    .collect(Collectors.toList());

                return save(Mono.just(task))
                    .flatMap(result -> historyService
                        .save(Flux.fromIterable(histories))
                        .thenReturn(result))
                    .as(transactionalOperator::transactional)
                    .flatMap(result -> startTask(task.getId(), Collections.emptyList()).thenReturn(result));
            });
    }

    private void normalizeTaskOptions(FirmwareUpgradeTaskEntity task) {
        if (!StringUtils.hasText(task.getMode())) {
            task.setMode("push");
        }
        String mode = task.getMode().trim().toLowerCase(Locale.ROOT);
        if (!"push".equals(mode) && !"pull".equals(mode)) {
            throw new IllegalArgumentException("升级模式仅支持push或pull");
        }
        task.setMode(mode);
        task.setReleaseType(StringUtils.hasText(task.getReleaseType()) ? task.getReleaseType() : "all");
        task.setResponseTimeoutSeconds(positiveOrDefault(
            task.getResponseTimeoutSeconds(), DEFAULT_RESPONSE_TIMEOUT_SECONDS));
        task.setStatusTimeoutSeconds(positiveOrDefault(
            task.getStatusTimeoutSeconds(), DEFAULT_STATUS_TIMEOUT_SECONDS));
        task.setTimeoutSeconds(positiveOrDefault(
            task.getTimeoutSeconds(), DEFAULT_EXECUTION_TIMEOUT_SECONDS));
        if (task.getTimeoutSeconds() < task.getResponseTimeoutSeconds()
            || task.getTimeoutSeconds() < task.getStatusTimeoutSeconds()) {
            throw new IllegalArgumentException("任务总超时不能小于响应超时或状态上报超时");
        }
    }

    private int positiveOrDefault(Integer value, int defaultValue) {
        return value == null || value <= 0 ? defaultValue : value;
    }

    private void validateFirmware(FirmwareUpgradeTaskEntity task, FirmwareEntity firmware) {
        if (StringUtils.hasText(firmware.getProductId())
            && !Objects.equals(task.getProductId(), firmware.getProductId())) {
            throw new IllegalArgumentException("固件与升级任务产品不一致");
        }
        if (!StringUtils.hasText(firmware.getVersion())) {
            throw new IllegalArgumentException("固件版本不能为空");
        }
        if (!StringUtils.hasText(firmware.getUrl())) {
            throw new IllegalArgumentException("固件下载地址不能为空");
        }
        if (firmware.getSize() == null || firmware.getSize() <= 0) {
            throw new IllegalArgumentException("固件文件大小必须大于0");
        }
        if (!"sha256".equalsIgnoreCase(firmware.getSignMethod())
            && !"md5".equalsIgnoreCase(firmware.getSignMethod())) {
            throw new IllegalArgumentException("固件校验方式仅支持sha256或md5");
        }
        if (!StringUtils.hasText(firmware.getSign())) {
            throw new IllegalArgumentException("固件校验值不能为空");
        }
    }

    private Mono<List<DeviceInstanceEntity>> resolveTargetDevices(FirmwareUpgradeTaskEntity task) {
        if (!StringUtils.hasText(task.getProductId())) {
            return Mono.error(new IllegalArgumentException("productId不能为空"));
        }

        List<String> selectedIds = extractDeviceIds(task);
        Flux<DeviceInstanceEntity> source;
        if ("all".equalsIgnoreCase(task.getReleaseType())) {
            source = deviceService
                .createQuery()
                .where(DeviceInstanceEntity::getProductId, task.getProductId())
                .fetch();
        } else {
            if (selectedIds.isEmpty()) {
                return Mono.error(new IllegalArgumentException("请选择升级设备"));
            }
            source = deviceService
                .createQuery()
                .in(DeviceInstanceEntity::getId, selectedIds)
                .fetch();
        }

        return source
            .filter(device -> task.getProductId().equals(device.getProductId()))
            .distinct(DeviceInstanceEntity::getId)
            .collectList()
            .flatMap(devices -> {
                if (devices.isEmpty()) {
                    return Mono.error(new IllegalArgumentException("没有符合条件的升级设备"));
                }
                if (!"all".equalsIgnoreCase(task.getReleaseType())
                    && devices.size() != new LinkedHashSet<>(selectedIds).size()) {
                    return Mono.error(new IllegalArgumentException("部分设备不存在或不属于任务产品"));
                }
                return Mono.just(devices);
            });
    }

    private Mono<Void> ensureProductActivated(String productId) {
        if (!StringUtils.hasText(productId)) {
            return Mono.error(new IllegalArgumentException("productId不能为空"));
        }
        return deviceRegistry
            .getProduct(productId)
            .switchIfEmpty(Mono.error(new ProductNotActivatedException(productId)))
            .then();
    }

    private FirmwareUpgradeHistoryEntity createHistorySnapshot(FirmwareUpgradeTaskEntity task,
                                                                FirmwareEntity firmware,
                                                                DeviceInstanceEntity device) {
        FirmwareUpgradeHistoryEntity history = new FirmwareUpgradeHistoryEntity();
        history.setId(IDGenerator.SNOW_FLAKE_STRING.generate());
        history.setUpgradeId(FirmwareUpgradeIdGenerator.upgradeId());
        history.setTaskId(task.getId());
        history.setTaskName(task.getName());
        history.setDeviceId(device.getId());
        history.setDeviceName(device.getName());
        history.setProductId(device.getProductId());
        history.setProductName(device.getProductName());
        history.setFirmwareId(firmware.getId());
        history.setFirmwareName(firmware.getName());
        history.setFromVersion(resolveCurrentFirmwareVersion(device));
        history.setToVersion(firmware.getVersion());
        history.setStatus(FirmwareUpgradeStatus.QUEUED.getValue());
        history.setAttempt(1);
        history.setProgress(0);
        history.setStartTime(System.currentTimeMillis());
        return history;
    }

    private String resolveCurrentFirmwareVersion(DeviceInstanceEntity device) {
        return device
            .getConfiguration("firmwareVersion")
            .or(() -> device.getConfiguration("fwVersion"))
            .map(String::valueOf)
            .orElse(null);
    }

    public Mono<Void> startTask(String taskId, List<String> deviceIds) {
        return findById(taskId)
            .flatMap(task -> ensureProductActivated(task.getProductId())
                .then(firmwareService.findById(task.getFirmwareId())
                    .switchIfEmpty(Mono.error(
                        new IllegalArgumentException("固件不存在: " + task.getFirmwareId())))
                    .flatMap(firmware -> selectStartableHistories(taskId, deviceIds)
                        .collectList()
                        .flatMap(histories -> {
                            if (histories.isEmpty()) {
                                return Mono.error(new IllegalStateException("没有可启动或重试的设备"));
                            }
                            task.setStatus("running");
                            return updateById(taskId, Mono.just(task))
                                .thenMany(Flux.fromIterable(histories))
                                .concatMap(history -> prepareAttempt(history)
                                    .flatMap(prepared -> isPullMode(task)
                                        ? Mono.empty()
                                        : dispatchOne(task, firmware, prepared)))
                                .then();
                        }))))
            .then();
    }

    private Flux<FirmwareUpgradeHistoryEntity> selectStartableHistories(String taskId,
                                                                         List<String> deviceIds) {
        Set<String> selected = deviceIds == null
            ? Collections.emptySet()
            : deviceIds.stream().filter(StringUtils::hasText).collect(Collectors.toSet());
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
            .fetch()
            .filter(history -> selected.isEmpty() || selected.contains(history.getDeviceId()))
            .filter(history -> FirmwareUpgradeStatus.QUEUED.getValue().equals(
                FirmwareUpgradeStatus.normalize(history.getStatus()))
                || FirmwareUpgradeStatus.isRetryable(history.getStatus()));
    }

    private Mono<FirmwareUpgradeHistoryEntity> prepareAttempt(FirmwareUpgradeHistoryEntity history) {
        String normalized = FirmwareUpgradeStatus.normalize(history.getStatus());
        if (FirmwareUpgradeStatus.QUEUED.getValue().equals(normalized)) {
            return Mono.just(history);
        }
        int nextAttempt = (history.getAttempt() == null ? 1 : history.getAttempt()) + 1;
        String nextUpgradeId = FirmwareUpgradeIdGenerator.upgradeId();
        long startTime = System.currentTimeMillis();
        return historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus, FirmwareUpgradeStatus.QUEUED.getValue())
            .set(FirmwareUpgradeHistoryEntity::getAttempt, nextAttempt)
            .set(FirmwareUpgradeHistoryEntity::getUpgradeId, nextUpgradeId)
            .setNull(FirmwareUpgradeHistoryEntity::getActiveKey)
            .set(FirmwareUpgradeHistoryEntity::getProgress, 0)
            .set(FirmwareUpgradeHistoryEntity::getStartTime, startTime)
            .setNull(FirmwareUpgradeHistoryEntity::getMessageId)
            .setNull(FirmwareUpgradeHistoryEntity::getDispatchTime)
            .setNull(FirmwareUpgradeHistoryEntity::getAckTime)
            .setNull(FirmwareUpgradeHistoryEntity::getLastReportTime)
            .setNull(FirmwareUpgradeHistoryEntity::getLastEventTime)
            .setNull(FirmwareUpgradeHistoryEntity::getReportedVersion)
            .setNull(FirmwareUpgradeHistoryEntity::getCompleteTime)
            .setNull(FirmwareUpgradeHistoryEntity::getErrorCode)
            .setNull(FirmwareUpgradeHistoryEntity::getErrorMessage)
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus, history.getStatus())
            .execute()
            .filter(updated -> updated > 0)
            .flatMap(ignore -> historyService.findById(history.getId()));
    }

    public Mono<Void> stopTask(String taskId, List<String> deviceIds) {
        return findById(taskId)
            .flatMap(task -> {
                Set<String> selected = deviceIds == null
                    ? Collections.emptySet()
                    : deviceIds.stream().filter(StringUtils::hasText).collect(Collectors.toSet());
                return historyService
                        .createQuery()
                        .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
                        .fetch()
                    .filter(history -> selected.isEmpty() || selected.contains(history.getDeviceId()))
                    .filter(history -> FirmwareUpgradeStatus.QUEUED.getValue().equals(
                        FirmwareUpgradeStatus.normalize(history.getStatus())))
                    .collectList()
                    .flatMap(histories -> {
                        if (histories.isEmpty()) {
                            return Mono.error(new IllegalStateException("没有可取消的待下发设备"));
                        }
                        long completeTime = System.currentTimeMillis();
                        return Flux.fromIterable(histories)
                            .flatMap(history -> historyService
                                .createUpdate()
                                .set(FirmwareUpgradeHistoryEntity::getStatus,
                                     FirmwareUpgradeStatus.CANCELLED.getValue())
                                .setNull(FirmwareUpgradeHistoryEntity::getActiveKey)
                                .set(FirmwareUpgradeHistoryEntity::getErrorCode, "USER_CANCELLED")
                                .set(FirmwareUpgradeHistoryEntity::getErrorMessage, "升级在下发前被用户取消")
                                .set(FirmwareUpgradeHistoryEntity::getCompleteTime, completeTime)
                                .where()
                                .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
                                .and(FirmwareUpgradeHistoryEntity::getStatus,
                                     FirmwareUpgradeStatus.QUEUED.getValue())
                                .execute())
                            .reduce(0, Integer::sum)
                            .flatMap(updated -> updated > 0
                                ? Mono.empty()
                                : Mono.error(new IllegalStateException("待下发设备状态已变化，未执行取消")));
                    });
            })
            .then(refreshTaskStatus(taskId))
            .then();
    }

    public Mono<Void> refreshTaskStatus(String taskId) {
        if (!StringUtils.hasText(taskId)) {
            return Mono.empty();
        }
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
            .fetch()
            .collectList()
            .flatMap(histories -> aggregateTask(taskId, histories));
    }

    private Mono<Void> aggregateTask(String taskId, List<FirmwareUpgradeHistoryEntity> histories) {
        if (histories.isEmpty()) {
            return Mono.empty();
        }
        return findById(taskId)
            .flatMap(task -> {
                applyAggregate(task, histories);
                return updateById(taskId, Mono.just(task)).then();
            });
    }

    static void applyAggregate(FirmwareUpgradeTaskEntity task,
                               List<FirmwareUpgradeHistoryEntity> histories) {
        int success = 0;
        int failed = 0;
        int cancelled = 0;
        int queued = 0;
        int running = 0;
        for (FirmwareUpgradeHistoryEntity history : histories) {
            String status = FirmwareUpgradeStatus.normalize(history.getStatus());
            if (FirmwareUpgradeStatus.SUCCESS.getValue().equals(status)) {
                success++;
            } else if (FirmwareUpgradeStatus.CANCELLED.getValue().equals(status)) {
                cancelled++;
            } else if (FirmwareUpgradeStatus.isFailure(status)) {
                failed++;
            } else if (FirmwareUpgradeStatus.QUEUED.getValue().equals(status)) {
                queued++;
            } else {
                running++;
            }
        }

        task.setDeviceCount(histories.size());
        task.setSuccessCount(success);
        task.setFailCount(failed);
        task.setQueuedCount(queued);
        task.setRunningCount(running);
        task.setCancelledCount(cancelled);
        if (queued + running > 0) {
            task.setStatus("running");
        } else if (success == histories.size()) {
            task.setStatus("completed");
        } else if (success > 0) {
            task.setStatus("partial_failed");
        } else if (failed > 0) {
            task.setStatus("failed");
        } else {
            task.setStatus("stopped");
        }
    }

    public Mono<Void> retryHistory(String historyId) {
        return historyService
            .findById(historyId)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("升级记录不存在: " + historyId)))
            .flatMap(history -> startTask(history.getTaskId(), Collections.singletonList(history.getDeviceId())));
    }

    public Mono<Void> cancelHistory(String historyId) {
        return historyService
            .findById(historyId)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("升级记录不存在: " + historyId)))
            .flatMap(history -> stopTask(history.getTaskId(), Collections.singletonList(history.getDeviceId())));
    }

    private Mono<Void> dispatchOne(FirmwareUpgradeTaskEntity task,
                                   FirmwareEntity firmware,
                                   FirmwareUpgradeHistoryEntity history) {
        return hasCompetingActiveUpgrade(history)
            .flatMap(busy -> {
                if (busy) {
                    return markDispatchFailed(
                        history,
                        FirmwareUpgradeStatus.QUEUED.getValue(),
                        null,
                        "DEVICE_UPGRADE_BUSY",
                        "设备存在其他进行中的升级任务");
                }

                String deviceId = history.getDeviceId();
                String messageId = history.getUpgradeId() + "-a" + history.getAttempt();
                long dispatchTime = System.currentTimeMillis();
                return historyService
                    .createUpdate()
                    .set(FirmwareUpgradeHistoryEntity::getStatus,
                         FirmwareUpgradeStatus.DISPATCHING.getValue())
                    .set(FirmwareUpgradeHistoryEntity::getActiveKey, deviceId)
                    .set(FirmwareUpgradeHistoryEntity::getMessageId, messageId)
                    .set(FirmwareUpgradeHistoryEntity::getDispatchTime, dispatchTime)
                    .set(FirmwareUpgradeHistoryEntity::getStartTime, dispatchTime)
                    .where()
                    .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
                    .and(FirmwareUpgradeHistoryEntity::getStatus,
                         FirmwareUpgradeStatus.QUEUED.getValue())
                    .execute()
                    .filter(updated -> updated > 0)
                    .flatMap(ignore -> dispatchClaimed(
                        task,
                        firmware,
                        history,
                        deviceId,
                        messageId,
                        dispatchTime));
            });
    }

    private Mono<Void> dispatchClaimed(FirmwareUpgradeTaskEntity task,
                                       FirmwareEntity firmware,
                                       FirmwareUpgradeHistoryEntity history,
                                       String deviceId,
                                       String messageId,
                                       long dispatchTime) {
        return deviceRegistry
            .getDevice(deviceId)
            .switchIfEmpty(Mono.error(
                new IllegalStateException("设备不存在或未注册: " + deviceId)))
            .flatMap(device -> {
                Map<String, Object> otaData = new LinkedHashMap<>();
                otaData.put("upgradeId", history.getUpgradeId());
                otaData.put("taskId", task.getId());
                otaData.put("historyId", history.getId());
                otaData.put("firmwareId", firmware.getId());
                otaData.put("attempt", history.getAttempt());
                otaData.put("fwVersion", firmware.getVersion());
                otaData.put("fwName", firmware.getName() != null ? firmware.getName() : "");
                otaData.put("fwUrl", firmwareService.resolveDownloadUrl(firmware.getUrl()));
                otaData.put("fwSize", firmware.getSize() != null ? firmware.getSize() : 0);
                if ("sha256".equalsIgnoreCase(firmware.getSignMethod())) {
                    otaData.put("fwSha256", firmware.getSign() != null ? firmware.getSign() : "");
                } else {
                    otaData.put("fwMd5", firmware.getSign() != null ? firmware.getSign() : "");
                }
                otaData.put("force", false);
                otaData.put("deadlineS", task.getTimeoutSeconds() != null ? task.getTimeoutSeconds() : 3600);

                List<FunctionParameter> params = otaData.entrySet().stream()
                    .map(e -> new FunctionParameter(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());

                FunctionInvokeMessage msg = new FunctionInvokeMessage();
                msg.setDeviceId(deviceId);
                msg.setFunctionId("ota_upgrade");
                msg.setInputs(params);
                msg.setMessageId(messageId);
                msg.addHeader(Headers.async, true);
                msg.addHeader(Headers.timeout, 10_000L);
                msg.addHeader("upgradeId", history.getUpgradeId());
                msg.addHeader("taskId", task.getId());

                return device.messageSender()
                    .send(msg)
                    .cast(DeviceMessageReply.class)
                    .next()
                    .switchIfEmpty(Mono.error(new IllegalStateException("协议层未返回发送结果")))
                    .flatMap(reply -> {
                        if (!reply.isSuccess()) {
                            return Mono.error(new IllegalStateException(
                                StringUtils.hasText(reply.getMessage())
                                    ? reply.getMessage()
                                    : "协议层拒绝发送"));
                        }
                        return historyService
                            .createUpdate()
                            .set(FirmwareUpgradeHistoryEntity::getStatus,
                                 FirmwareUpgradeStatus.DISPATCHED.getValue())
                            .where()
                            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
                            .and(FirmwareUpgradeHistoryEntity::getStatus,
                                 FirmwareUpgradeStatus.DISPATCHING.getValue())
                            .and(FirmwareUpgradeHistoryEntity::getMessageId, messageId)
                            .execute()
                            .flatMap(updated -> {
                                if (updated <= 0) {
                                    return Mono.empty();
                                }
                                log.info(
                                    "OTA dispatched: task={}, upgrade={}, device={}, firmware={}",
                                    task.getId(), history.getUpgradeId(), deviceId, firmware.getVersion());
                                return refreshTaskStatus(task.getId());
                            });
                    });
            })
            .onErrorResume(err -> {
                String errorCode;
                if (err instanceof DeviceOperationException) {
                    errorCode = ((DeviceOperationException) err).getCode().name();
                } else if (err instanceof ProductNotActivatedException) {
                    errorCode = "PRODUCT_NOT_ACTIVATED";
                } else {
                    errorCode = "DISPATCH_ERROR";
                }
                log.warn("OTA dispatch failed: task={}, upgrade={}, device={}",
                    task.getId(), history.getUpgradeId(), history.getDeviceId(), err);
                return markDispatchFailed(
                    history,
                    FirmwareUpgradeStatus.DISPATCHING.getValue(),
                    messageId,
                    errorCode,
                    err.getMessage());
            });
    }

    private Mono<Boolean> hasCompetingActiveUpgrade(FirmwareUpgradeHistoryEntity current) {
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getActiveKey, current.getDeviceId())
            .fetch()
            .filter(history -> !Objects.equals(history.getId(), current.getId()))
            .hasElements();
    }

    private Mono<Void> markDispatchFailed(FirmwareUpgradeHistoryEntity history,
                                          String expectedStatus,
                                          String expectedMessageId,
                                          String errorCode,
                                          String errorMessage) {
        var update = historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus,
                 FirmwareUpgradeStatus.DISPATCH_FAILED.getValue())
            .setNull(FirmwareUpgradeHistoryEntity::getActiveKey)
            .set(FirmwareUpgradeHistoryEntity::getErrorCode, errorCode)
            .set(FirmwareUpgradeHistoryEntity::getErrorMessage,
                 StringUtils.hasText(errorMessage) ? errorMessage : "固件升级指令下发失败")
            .set(FirmwareUpgradeHistoryEntity::getCompleteTime, System.currentTimeMillis())
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus, expectedStatus);
        if (StringUtils.hasText(expectedMessageId)) {
            update.and(FirmwareUpgradeHistoryEntity::getMessageId, expectedMessageId);
        }
        return update
            .execute()
            .flatMap(updated -> updated > 0
                ? refreshTaskStatus(history.getTaskId())
                : Mono.empty());
    }

    public Mono<Void> dispatchQueuedHistories() {
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getStatus, FirmwareUpgradeStatus.QUEUED.getValue())
            .fetch()
            .take(100)
            .flatMap(history -> findById(history.getTaskId())
                .filter(task -> !isPullMode(task))
                .flatMap(task -> firmwareService
                    .findById(task.getFirmwareId())
                    .flatMap(firmware -> dispatchOne(task, firmware, history))), 8)
            .then();
    }

    /**
     * 处理设备主动拉取固件。协议层只需将私有报文解码为 RequestFirmwareMessage。
     */
    public Mono<Void> handlePullRequest(RequestFirmwareMessage request) {
        if (!StringUtils.hasText(request.getDeviceId())) {
            return Mono.empty();
        }
        if (!StringUtils.hasText(request.getMessageId())) {
            request.setMessageId(IDGenerator.RANDOM.generate());
        }

        return findExistingPullAssignment(request)
            .switchIfEmpty(findAndClaimPullAssignment(request))
            .switchIfEmpty(Mono.defer(() ->
                sendNoPullAssignment(request).then(Mono.empty())))
            .flatMap(assignment -> sendPullAssignment(request, assignment))
            .onErrorResume(error -> {
                log.warn("OTA pull request failed: device={}, message={}",
                    request.getDeviceId(), request.getMessageId(), error);
                return sendPullError(request, "PULL_REQUEST_FAILED", error.getMessage());
            });
    }

    private Mono<PullAssignment> findExistingPullAssignment(RequestFirmwareMessage request) {
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getDeviceId, request.getDeviceId())
            .and(FirmwareUpgradeHistoryEntity::getMessageId, request.getMessageId())
            .in(FirmwareUpgradeHistoryEntity::getStatus,
                Arrays.asList(
                    FirmwareUpgradeStatus.DISPATCHING.getValue(),
                    FirmwareUpgradeStatus.DISPATCHED.getValue()))
            .fetch()
            .concatMap(history -> loadPullAssignment(history, request))
            .next();
    }

    private Mono<PullAssignment> findAndClaimPullAssignment(RequestFirmwareMessage request) {
        return historyService
            .createQuery()
            .where(FirmwareUpgradeHistoryEntity::getDeviceId, request.getDeviceId())
            .and(FirmwareUpgradeHistoryEntity::getStatus, FirmwareUpgradeStatus.QUEUED.getValue())
            .fetch()
            .sort(Comparator
                .comparing(FirmwareUpgradeHistoryEntity::getStartTime,
                    Comparator.nullsLast(Long::compareTo))
                .thenComparing(FirmwareUpgradeHistoryEntity::getId))
            .concatMap(history -> loadPullAssignment(history, request)
                .flatMap(assignment -> {
                    if (Objects.equals(
                        request.getCurrentVersion(),
                        assignment.firmware.getVersion())) {
                        return markAlreadyCurrent(history, request.getCurrentVersion())
                            .then(Mono.empty());
                    }
                    return hasCompetingActiveUpgrade(history)
                        .filter(busy -> !busy)
                        .flatMap(ignore -> claimPullAssignment(
                            assignment,
                            request.getMessageId(),
                            request.getCurrentVersion()));
                }))
            .next();
    }

    private Mono<PullAssignment> loadPullAssignment(FirmwareUpgradeHistoryEntity history,
                                                     RequestFirmwareMessage request) {
        return findById(history.getTaskId())
            .filter(this::isPullMode)
            .filter(task -> Objects.equals(task.getProductId(), history.getProductId()))
            .flatMap(task -> firmwareService
                .findById(task.getFirmwareId())
                .filter(firmware -> !StringUtils.hasText(request.getRequestVersion())
                    || Objects.equals(request.getRequestVersion(), firmware.getVersion()))
                .map(firmware -> new PullAssignment(task, firmware, history)));
    }

    private Mono<PullAssignment> claimPullAssignment(PullAssignment assignment,
                                                      String messageId,
                                                      String currentVersion) {
        long dispatchTime = System.currentTimeMillis();
        var update = historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus,
                 FirmwareUpgradeStatus.DISPATCHING.getValue())
            .set(FirmwareUpgradeHistoryEntity::getActiveKey, assignment.history.getDeviceId())
            .set(FirmwareUpgradeHistoryEntity::getMessageId, messageId)
            .set(FirmwareUpgradeHistoryEntity::getDispatchTime, dispatchTime)
            .set(FirmwareUpgradeHistoryEntity::getStartTime, dispatchTime)
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, assignment.history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus,
                 FirmwareUpgradeStatus.QUEUED.getValue());
        if (StringUtils.hasText(currentVersion)) {
            update.set(FirmwareUpgradeHistoryEntity::getFromVersion, currentVersion);
        }
        return update
            .execute()
            .filter(updated -> updated > 0)
            .map(ignore -> {
                assignment.history.setStatus(FirmwareUpgradeStatus.DISPATCHING.getValue());
                assignment.history.setActiveKey(assignment.history.getDeviceId());
                assignment.history.setMessageId(messageId);
                assignment.history.setDispatchTime(dispatchTime);
                assignment.history.setStartTime(dispatchTime);
                if (StringUtils.hasText(currentVersion)) {
                    assignment.history.setFromVersion(currentVersion);
                }
                return assignment;
            });
    }

    private Mono<Void> markAlreadyCurrent(FirmwareUpgradeHistoryEntity history,
                                          String currentVersion) {
        long now = System.currentTimeMillis();
        return historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus, FirmwareUpgradeStatus.SUCCESS.getValue())
            .set(FirmwareUpgradeHistoryEntity::getProgress, 100)
            .set(FirmwareUpgradeHistoryEntity::getReportedVersion, currentVersion)
            .set(FirmwareUpgradeHistoryEntity::getCompleteTime, now)
            .setNull(FirmwareUpgradeHistoryEntity::getActiveKey)
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus,
                 FirmwareUpgradeStatus.QUEUED.getValue())
            .execute()
            .flatMap(updated -> updated > 0
                ? refreshTaskStatus(history.getTaskId())
                : Mono.empty());
    }

    private Mono<Void> sendPullAssignment(RequestFirmwareMessage request,
                                          PullAssignment assignment) {
        RequestFirmwareMessageReply reply = request.newReply();
        reply.success();
        reply.setUrl(firmwareService.resolveDownloadUrl(assignment.firmware.getUrl()));
        reply.setVersion(assignment.firmware.getVersion());
        reply.setSign(assignment.firmware.getSign());
        reply.setSignMethod(assignment.firmware.getSignMethod());
        reply.setFirmwareId(assignment.firmware.getId());
        reply.setSize(assignment.firmware.getSize() == null ? 0 : assignment.firmware.getSize());

        Map<String, Object> parameters = new LinkedHashMap<>();
        if (assignment.firmware.getProperties() != null) {
            parameters.putAll(assignment.firmware.getProperties());
        }
        parameters.put("upgradeId", assignment.history.getUpgradeId());
        parameters.put("taskId", assignment.task.getId());
        parameters.put("historyId", assignment.history.getId());
        parameters.put("firmwareId", assignment.firmware.getId());
        parameters.put("fwName", assignment.firmware.getName() != null ? assignment.firmware.getName() : "");
        parameters.put("attempt", assignment.history.getAttempt());
        parameters.put("force", false);
        parameters.put(
            "deadlineS",
            positiveOrDefault(
                assignment.task.getTimeoutSeconds(),
                DEFAULT_EXECUTION_TIMEOUT_SECONDS));
        reply.setParameters(parameters);
        reply.addHeader("upgradeId", assignment.history.getUpgradeId());
        reply.addHeader("taskId", assignment.task.getId());

        return sendPullReply(reply)
            .then(markPullDispatched(assignment.history, request.getMessageId()))
            .onErrorResume(error -> markDispatchFailed(
                assignment.history,
                FirmwareUpgradeStatus.DISPATCHING.getValue(),
                request.getMessageId(),
                "PULL_REPLY_ERROR",
                error.getMessage()));
    }

    private Mono<Void> markPullDispatched(FirmwareUpgradeHistoryEntity history,
                                          String messageId) {
        return historyService
            .createUpdate()
            .set(FirmwareUpgradeHistoryEntity::getStatus,
                 FirmwareUpgradeStatus.DISPATCHED.getValue())
            .where()
            .and(FirmwareUpgradeHistoryEntity::getId, history.getId())
            .and(FirmwareUpgradeHistoryEntity::getStatus,
                 FirmwareUpgradeStatus.DISPATCHING.getValue())
            .and(FirmwareUpgradeHistoryEntity::getMessageId, messageId)
            .execute()
            .flatMap(updated -> updated > 0
                ? refreshTaskStatus(history.getTaskId())
                : Mono.empty());
    }

    private Mono<Void> sendNoPullAssignment(RequestFirmwareMessage request) {
        return sendPullError(request, "NO_PENDING_UPGRADE", "当前设备没有可领取的固件升级任务");
    }

    private Mono<Void> sendPullError(RequestFirmwareMessage request,
                                     String code,
                                     String message) {
        RequestFirmwareMessageReply reply = request.newReply();
        reply.error(code, StringUtils.hasText(message) ? message : code);
        return sendPullReply(reply);
    }

    private Mono<Void> sendPullReply(RequestFirmwareMessageReply reply) {
        return deviceRegistry
            .getDevice(reply.getDeviceId())
            .switchIfEmpty(Mono.error(
                new IllegalStateException("设备不存在或未注册: " + reply.getDeviceId())))
            .flatMap(device -> device.messageSender().sendAndForget(reply));
    }

    private boolean isPullMode(FirmwareUpgradeTaskEntity task) {
        return "pull".equalsIgnoreCase(task.getMode());
    }

    private static final class PullAssignment {
        private final FirmwareUpgradeTaskEntity task;
        private final FirmwareEntity firmware;
        private final FirmwareUpgradeHistoryEntity history;

        private PullAssignment(FirmwareUpgradeTaskEntity task,
                               FirmwareEntity firmware,
                               FirmwareUpgradeHistoryEntity history) {
            this.task = task;
            this.firmware = firmware;
            this.history = history;
        }
    }
}
