package org.jetlinks.community.zota.fdc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionParameter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

/**
 * FDC 固件升级服务 — 通过 MQTT 下发 OTA 指令到 FDC 设备。
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "zota", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class FdcOtaService {

    private final DeviceRegistry deviceRegistry;

    /**
     * 向单个 FDC 设备下发 OTA 升级指令。
     *
     * @param deviceId   FDC 设备 ID
     * @param firmware   固件信息（版本/URL/SHA256/大小）
     * @return 发送结果
     */
    public Mono<Void> dispatchOtaCommand(String deviceId, FdcFirmware firmware) {
        return deviceRegistry.getDevice(deviceId)
            .flatMap(device -> {
                Map<String, Object> otaData = new LinkedHashMap<>();
                otaData.put("fwVersion", firmware.getVersion());
                otaData.put("fwUrl", firmware.getFileUrl());
                otaData.put("fwSize", firmware.getFileSize());
                otaData.put("fwSha256", firmware.getSha256());
                otaData.put("force", false);
                otaData.put("deadlineS", 3600);

                // Map → List<FunctionParameter>
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
                    .doOnNext(ok -> log.info("OTA command sent: device={}, fw={}",
                        deviceId, firmware.getVersion()))
                    .doOnError(err -> log.error("OTA command failed: device={}", deviceId, err))
                    .then();
            })
            .switchIfEmpty(Mono.fromRunnable(() ->
                log.warn("Device {} not found, OTA dispatch skipped", deviceId)));
    }

    /**
     * 批量下发 OTA 指令。
     *
     * @param deviceIds FDC 设备 ID 列表
     * @param firmware  固件信息
     * @return 发送结果统计
     */
    public Mono<Map<String, Object>> dispatchBatch(List<String> deviceIds, FdcFirmware firmware) {
        return Flux.fromIterable(deviceIds)
            .flatMap(deviceId -> dispatchOtaCommand(deviceId, firmware)
                .thenReturn(Map.<String, Object>of("deviceId", deviceId, "status", "sent"))
                .onErrorResume(err -> Mono.just(Map.<String, Object>of(
                    "deviceId", deviceId, "status", "failed", "error", err.getMessage())))
            )
            .collectList()
            .map(results -> {
                long success = results.stream().filter(r -> "sent".equals(r.get("status"))).count();
                long failed = results.size() - success;
                return Map.<String, Object>of(
                    "total", results.size(),
                    "success", success,
                    "failed", failed,
                    "details", results
                );
            });
    }
}
