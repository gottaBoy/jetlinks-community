package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.jetlinks.community.firmware.entity.FirmwareEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionParameter;
import org.springframework.stereotype.Service;
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

    private final FirmwareService firmwareService;
    private final FirmwareUpgradeHistoryService historyService;
    private final DeviceRegistry deviceRegistry;

    /**
     * 从任务 terms 中提取设备 ID 列表（仅支持 "自定义设备" 模式）。
     * terms 格式: [{"column":"id","termType":"in","value":["FDC001"]}]
     */
    public List<String> extractDeviceIds(FirmwareUpgradeTaskEntity task) {
        if (task.getTerms() == null || task.getTerms().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> ids = new ArrayList<>();
        for (Map<String, Object> term : task.getTerms()) {
            String column = (String) term.get("column");
            String termType = (String) term.get("termType");
            if ("id".equals(column) && "in".equals(termType)) {
                Object value = term.get("value");
                if (value instanceof List) {
                    for (Object v : (List<?>) value) {
                        if (v != null) ids.add(v.toString());
                    }
                }
            }
        }
        return ids;
    }

    public Mono<Void> startTask(String taskId, List<String> deviceIds) {
        return findById(taskId)
            .flatMap(task -> {
                if (!"pending".equals(task.getStatus())) {
                    return Mono.error(new IllegalStateException("任务状态不允许启动: " + task.getStatus()));
                }
                return firmwareService.findById(task.getFirmwareId())
                    .flatMap(firmware -> {
                        task.setStatus("running");
                        return updateById(taskId, Mono.just(task))
                            .then(dispatchToDevices(task, firmware, deviceIds));
                    });
            })
            .then();
    }

    public Mono<Void> stopTask(String taskId, List<String> deviceIds) {
        return findById(taskId)
            .flatMap(task -> {
                task.setStatus("stopped");
                return updateById(taskId, Mono.just(task))
                    .then(Flux.fromIterable(deviceIds)
                        .flatMap(deviceId ->
                            historyService.createQuery()
                                .where(FirmwareUpgradeHistoryEntity::getTaskId, taskId)
                                .and(FirmwareUpgradeHistoryEntity::getDeviceId, deviceId)
                                .fetchOne()
                                .flatMap(history -> {
                                    if ("pending".equals(history.getStatus())) {
                                        history.setStatus("cancelled");
                                        return historyService.updateById(history.getId(), Mono.just(history));
                                    }
                                    return Mono.empty();
                                })
                        ).then());
            })
            .then();
    }

    private Mono<Void> dispatchToDevices(FirmwareUpgradeTaskEntity task,
                                          FirmwareEntity firmware,
                                          List<String> deviceIds) {
        return Flux.fromIterable(deviceIds)
            .flatMap(deviceId -> dispatchOne(task, firmware, deviceId), 8)
            .then();
    }

    private Mono<Void> dispatchOne(FirmwareUpgradeTaskEntity task,
                                     FirmwareEntity firmware,
                                     String deviceId) {
        return deviceRegistry.getDevice(deviceId)
            .flatMap(device ->
                device.getMetadata()
                    .flatMap(devMeta -> {
                        FirmwareUpgradeHistoryEntity history = new FirmwareUpgradeHistoryEntity();
                        history.setTaskId(task.getId());
                        history.setTaskName(task.getName());
                        history.setDeviceId(deviceId);
                        history.setDeviceName(devMeta.getName());
                        history.setFirmwareId(firmware.getId());
                        history.setFirmwareName(firmware.getName());
                        history.setToVersion(firmware.getVersion());
                        history.setProductId(task.getProductId());
                        history.setStatus("pending");
                        history.setProgress(0);
                        history.setStartTime(System.currentTimeMillis());

                        return device.getProduct()
                            .flatMap(product -> product.getMetadata()
                                .doOnNext(meta -> history.setProductName(meta.getName()))
                                .then())
                            .switchIfEmpty(Mono.empty())
                            .then(historyService.save(Mono.just(history)))
                            .then(deviceRegistry.getDevice(deviceId));
                    })
            )
            .switchIfEmpty(Mono.defer(() -> {
                FirmwareUpgradeHistoryEntity history = new FirmwareUpgradeHistoryEntity();
                history.setTaskId(task.getId());
                history.setTaskName(task.getName());
                history.setDeviceId(deviceId);
                history.setDeviceName(deviceId);
                history.setFirmwareId(firmware.getId());
                history.setFirmwareName(firmware.getName());
                history.setToVersion(firmware.getVersion());
                history.setProductId(task.getProductId());
                history.setStatus("pending");
                history.setProgress(0);
                history.setStartTime(System.currentTimeMillis());

                return historyService.save(Mono.just(history))
                    .then(deviceRegistry.getDevice(deviceId));
            }))
            .flatMap(h -> deviceRegistry.getDevice(deviceId))
            .flatMap(device -> {
                Map<String, Object> otaData = new LinkedHashMap<>();
                otaData.put("fwVersion", firmware.getVersion());
                otaData.put("fwUrl", firmware.getUrl());
                otaData.put("fwSize", firmware.getSize() != null ? firmware.getSize() : 0);
                // 按签名方式填入对应字段
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
                msg.setMessageId("ota-" + deviceId + "-" + System.currentTimeMillis());

                return device.messageSender()
                    .send(msg)
                    .doOnNext(ok -> log.info("OTA dispatched: device={}, fw={}", deviceId, firmware.getVersion()))
                    .doOnError(err -> log.error("OTA dispatch failed: device={}", deviceId, err))
                    .then();
            })
            .onErrorResume(err -> {
                log.warn("Device {} dispatch error", deviceId, err);
                return Mono.empty();
            });
    }
}
