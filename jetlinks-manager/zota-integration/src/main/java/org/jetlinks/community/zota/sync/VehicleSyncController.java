package org.jetlinks.community.zota.sync;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * 手动补偿同步：当自动同步失败或 zota-repo / zota-server 恢复后，手动触发重新同步。
 *
 * <pre>
 *   POST /api/zota/sync/resend?productId=K_DC_L2        → 重扫该产品下所有设备
 *   POST /api/zota/sync/resend?productId=K_DC_L2&ids=.. → 重扫指定设备（逗号分隔）
 * </pre>
 */
@Slf4j
@RestController
@ConditionalOnProperty(prefix = "zota", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequestMapping("/api/zota/sync")
@RequiredArgsConstructor
public class VehicleSyncController {

    private final LocalDeviceInstanceService deviceInstanceService;
    private final VehicleSyncListener syncListener;

    /**
     * 手动触发重新同步。
     */
    @PostMapping("/resend")
    public Mono<Map<String, Object>> resend(
            @RequestParam("productId") String productId,
            @RequestParam(value = "ids", required = false) List<String> ids) {

        if (ids != null && !ids.isEmpty()) {
            log.info("[VehicleSync] Manual resend: productId={}, ids={}", productId, ids);
            return deviceInstanceService.createQuery()
                .where()
                .and(DeviceInstanceEntity::getProductId, productId)
                .in(DeviceInstanceEntity::getId, ids)
                .fetch()
                .collectList()
                .doOnNext(devices -> {
                    log.info("[VehicleSync] Resending {} devices", devices.size());
                    syncListener.syncDevices(devices);
                })
                .thenReturn(Map.of("success", true, "message", "syncing " + ids.size() + " devices"));
        }

        log.info("[VehicleSync] Manual resend: productId={}, all devices", productId);
        return deviceInstanceService.createQuery()
            .where()
            .and(DeviceInstanceEntity::getProductId, productId)
            .fetch()
            .collectList()
            .doOnNext(devices -> {
                log.info("[VehicleSync] Resending {} devices", devices.size());
                syncListener.syncDevices(devices);
            })
            .thenReturn(Map.of("success", true, "message", "syncing all devices for " + productId));
    }
}
