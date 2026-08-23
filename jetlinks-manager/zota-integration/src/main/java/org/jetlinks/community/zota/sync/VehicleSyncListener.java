package org.jetlinks.community.zota.sync;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.events.EntityCreatedEvent;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.zota.config.ZotaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
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
@ConditionalOnProperty(prefix = "zota", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class VehicleSyncListener {

    /** 未获取到 TargetType 时创建 target 不带 type 的哨兵值 */
    private static final long NO_TARGET_TYPE = -1L;

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
     * 同步到 zota-server：尽力确保产品 ID 对应 TargetType 存在（best-effort，失败降级为不带 type）
     * → 创建 Target（controllerId = VIN）→ 写入属性（productId/internalCode/vehicleType）。
     */
    private Mono<Void> syncToZotaServer(DeviceInstanceEntity device) {
        String mgmtUrl = properties.getMgmtUrl();
        if (mgmtUrl == null || mgmtUrl.isEmpty()) {
            log.debug("[VehicleSync] zota-server URL not configured, skip");
            return Mono.empty();
        }

        String auth = "Basic " + Base64.getEncoder().encodeToString(
            (properties.getMgmtUsername() + ":" + properties.getMgmtPassword()).getBytes());

        WebClient client = WebClient.create(mgmtUrl);

        // Step 1: 尽力确保 TargetType（name = productId）存在；失败则降级为不带 type
        return ensureTargetType(client, auth, device.getProductId())
            .onErrorResume(err -> {
                log.warn("[VehicleSync] TargetType 获取失败，降级为不带 type 创建 target: productId={}, error={}",
                    device.getProductId(), err.getMessage());
                return Mono.empty();
            })
            .defaultIfEmpty(NO_TARGET_TYPE)
            // Step 2: create target（type 可用则带，不可用则不带）
            .flatMap(targetTypeId -> createTarget(client, auth, device,
                targetTypeId != null && targetTypeId > 0 ? targetTypeId : null))
            // Step 3: write attributes (with retry)
            .then(writeTargetAttributes(client, auth, device));
    }

    /** 创建 Target；targetTypeId 为 null 时不设置 type。 */
    private Mono<Void> createTarget(WebClient client, String auth, DeviceInstanceEntity device, Long targetTypeId) {
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("controllerId", device.getId());
        target.put("name", device.getInternalCode() != null ? device.getInternalCode() : device.getName());
        target.put("description", (device.getProductName() != null ? device.getProductName() : ""));
        if (targetTypeId != null) {
            target.put("targetType", targetTypeId);
        }

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
            .then();
    }

    /**
     * 幂等获取（必要时创建）产品 ID 对应的 TargetType，返回其数字 ID。已存在同名类型则不重复创建。
     */
    private Mono<Long> ensureTargetType(WebClient client, String auth, String productId) {
        return findTargetTypeId(client, auth, productId)
            .switchIfEmpty(Mono.defer(() -> createTargetType(client, auth, productId)))
            // 并发下可能已被其他请求创建（409），降级为再查一次
            .onErrorResume(err -> findTargetTypeId(client, auth, productId)
                .switchIfEmpty(Mono.error(err)));
    }

    /** GET /rest/v1/targettypes?q=name=={productId}，命中返回 ID，未命中返回 empty。 */
    private Mono<Long> findTargetTypeId(WebClient client, String auth, String productId) {
        return client.get()
            .uri(uriBuilder -> uriBuilder.path("/rest/v1/targettypes")
                .queryParam("q", "name==" + productId)
                .build())
            .header("Authorization", auth)
            .accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .bodyToMono(TargetTypeListResponse.class)
            .flatMap(resp -> {
                if (resp.getContent() != null && !resp.getContent().isEmpty()
                    && resp.getContent().get(0).getId() != null) {
                    Long id = resp.getContent().get(0).getId();
                    log.info("[VehicleSync] TargetType 已存在: name={}, id={}", productId, id);
                    return Mono.just(id);
                }
                return Mono.empty();
            });
    }

    /** POST /rest/v1/targettypes 创建 TargetType（name = productId），返回新 ID。 */
    private Mono<Long> createTargetType(WebClient client, String auth, String productId) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", productId);
        body.put("description", productId);

        log.info("[VehicleSync] 创建 TargetType: name={}", productId);

        return client.post()
            .uri("/rest/v1/targettypes")
            .header("Authorization", auth)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(List.of(body))
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<List<TargetTypeItem>>() {})
            .flatMap(list -> {
                if (list != null && !list.isEmpty() && list.get(0).getId() != null) {
                    Long id = list.get(0).getId();
                    log.info("[VehicleSync] TargetType 创建成功: name={}, id={}", productId, id);
                    return Mono.just(id);
                }
                return Mono.error(new IllegalStateException("创建 TargetType 未返回 id: name=" + productId));
            });
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class TargetTypeListResponse {
        private List<TargetTypeItem> content;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class TargetTypeItem {
        private Long id;
        private String name;
    }

    /**
     * 写入 target attributes：productId、internalCode、vehicleType（作为展示/兼容数据）。
     * <p>
     * 注意：Rollout 按产品过滤已统一改为 {@code targettype.name==productId}（车端 configData
     * replace 会清空 attributes，attribute.productId 不可靠）；这些属性仅作展示用途。
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
