package org.jetlinks.community.zota.sync;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.events.EntityCreatedEvent;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.zota.config.ZotaProperties;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 监听设备创建事件，将符合条件的车辆同步到 zota-repo 和 zota-server。
 * 白名单由 zota.sync.product-ids 配置控制。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleSyncListener {

    private final ZotaProperties properties;

    /**
     * 手动新增设备时触发（POST /api/device-instance）
     */
    @EventListener
    public void onDeviceCreated(EntityCreatedEvent<DeviceInstanceEntity> event) {
        syncDevices(event.getEntity());
    }

    /**
     * Excel 导入 / PATCH save 时触发（save() → EntitySavedEvent）
     * 仅同步未激活的新设备（registryTime == null），避免重复同步已有设备。
     */
    @EventListener
    public void onDeviceSaved(EntitySavedEvent<DeviceInstanceEntity> event) {
        event.getEntity().stream()
            .filter(d -> d.getRegistryTime() == null)
            .forEach(device -> {
                log.info("[VehicleSync] New device via save: id={}, productId={}, name={}",
                    device.getId(), device.getProductId(), device.getName());
                syncDevices(java.util.Collections.singletonList(device));
            });
    }

    void syncDevices(List<DeviceInstanceEntity> devices) {
        devices.stream()
            .filter(this::shouldSync)
            .forEach(device -> {
                log.info("[VehicleSync] Syncing vehicle: id={}, productId={}, name={}",
                    device.getId(), device.getProductId(), device.getName());
                syncToZotaRepo(device).subscribe(
                    v -> log.info("[VehicleSync] zota-repo sync OK: {}", device.getId()),
                    err -> log.error("[VehicleSync] zota-repo sync failed: {} — {}", device.getId(), err.getMessage())
                );
                syncToZotaServer(device).subscribe(
                    v -> log.info("[VehicleSync] zota-server sync OK: {}", device.getId()),
                    err -> log.error("[VehicleSync] zota-server sync failed: {} — {}", device.getId(), err.getMessage())
                );
            });
    }

    private boolean shouldSync(DeviceInstanceEntity device) {
        return properties.getSync().getProductIds().contains(device.getProductId());
    }

    /**
     * 同步到 zota-repo：创建车辆记录（VIN + 内部编码 + 车型）
     */
    private Mono<Void> syncToZotaRepo(DeviceInstanceEntity device) {
        String repoUrl = properties.getRepoApiUrl();
        if (repoUrl == null || repoUrl.isEmpty()) {
            log.debug("[VehicleSync] zota-repo URL not configured, skip");
            return Mono.empty();
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("vin", device.getId());
        payload.put("internal_code", device.getInternalCode());
        payload.put("name", device.getName());
        payload.put("product_id", device.getProductId());
        payload.put("product_name", device.getProductName());
        payload.put("vehicle_type", device.getProductId());

        return WebClient.create(repoUrl)
            .post()
            .uri("/api/v1/inventory/vehicles")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(payload)
            .retrieve()
            .toBodilessEntity()
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(30))
                .doBeforeRetry(rs -> log.warn("[VehicleSync] zota-repo retry {} for {}: {}",
                    rs.totalRetries() + 1, device.getId(), rs.failure().getMessage())))
            .then();
    }

    /**
     * 同步到 zota-server：创建 Target（controllerId = VIN）+ 写入属性（productId/internalCode/vehicleType）
     */
    private Mono<Void> syncToZotaServer(DeviceInstanceEntity device) {
        String mgmtUrl = properties.getMgmtUrl();
        if (mgmtUrl == null || mgmtUrl.isEmpty()) {
            log.debug("[VehicleSync] zota-server URL not configured, skip");
            return Mono.empty();
        }

        String auth = "Basic " + Base64.getEncoder().encodeToString(
            (properties.getMgmtUsername() + ":" + properties.getMgmtPassword()).getBytes());

        Map<String, Object> target = new LinkedHashMap<>();
        target.put("controllerId", device.getId());
        target.put("name", device.getInternalCode() != null ? device.getInternalCode() : device.getName());
        target.put("description", (device.getProductName() != null ? device.getProductName() : ""));

        WebClient client = WebClient.create(mgmtUrl);

        // Step 1: create target (with retry)
        return client.post()
            .uri("/rest/v1/targets")
            .header("Authorization", auth)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of(target))
            .retrieve()
            .toBodilessEntity()
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(30))
                .doBeforeRetry(rs -> log.warn("[VehicleSync] zota-server target retry {} for {}: {}",
                    rs.totalRetries() + 1, device.getId(), rs.failure().getMessage())))
            // Step 2: write attributes (with retry)
            .then(writeTargetAttributes(client, auth, device))
            .then();
    }

    /**
     * 写入 target attributes：productId、internalCode、vehicleType。
     * <p>
     * 写入属性而非元数据，因为 Rollout 的 targetFilterQuery 通过 RSQL
     * {@code attribute.productId==K_DC_L2} 过滤目标——元数据不支持 RSQL 查询。
     */
    private Mono<Void> writeTargetAttributes(WebClient client, String auth, DeviceInstanceEntity device) {
        // attributes format: {"productId": "K_DC_L2", "internalCode": "ZSD-K001", ...}
        Map<String, String> attributes = new LinkedHashMap<>();

        if (device.getProductId() != null) {
            attributes.put("productId", device.getProductId());
        }
        if (device.getInternalCode() != null) {
            attributes.put("internalCode", device.getInternalCode());
        }
        if (device.getProductId() != null) {
            attributes.put("vehicleType", device.getProductId());
        }

        if (attributes.isEmpty()) {
            log.debug("[VehicleSync] No attributes to write for {}", device.getId());
            return Mono.empty();
        }

        log.info("[VehicleSync] Writing attributes for {}: productId={}, internalCode={}, vehicleType={}",
            device.getId(), device.getProductId(), device.getInternalCode(), device.getProductId());

        return client.put()
            .uri("/rest/v1/targets/{controllerId}/attributes", device.getId())
            .header("Authorization", auth)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(attributes)
            .retrieve()
            .toBodilessEntity()
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(30))
                .doBeforeRetry(rs -> log.warn("[VehicleSync] zota-server attributes retry {} for {}: {}",
                    rs.totalRetries() + 1, device.getId(), rs.failure().getMessage())))
            .then();
    }
}
