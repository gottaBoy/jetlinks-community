package org.jetlinks.community.zota.sync;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.events.EntityCreatedEvent;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 设备创建时自动同步 MQTT 凭证到 Redis，供 EMQX Redis 认证使用。
 * 白名单由 {@code ziot.emqx.sync.product-ids} 配置控制。
 *
 * <p>Redis 格式：
 * <pre>
 *   Key:   mqtt_user:{deviceId}
 *   Value: {"password":"xxx"}
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CredentialSyncListener {

    private final CredentialSyncProperties properties;

    private final ReactiveRedisTemplate<String, String> redis;

    private static final String KEY_PREFIX = "mqtt_user:";
    private static final String ACL_PREFIX = "mqtt_acl:";

    @EventListener
    public void onDeviceCreated(EntityCreatedEvent<DeviceInstanceEntity> event) {
        syncDevices(event.getEntity());
    }

    @EventListener
    public void onDeviceSaved(EntitySavedEvent<DeviceInstanceEntity> event) {
        event.getEntity().stream()
            .filter(d -> d.getRegistryTime() == null)
            .forEach(device -> syncDevices(List.of(device)));
    }

    void syncDevices(List<DeviceInstanceEntity> devices) {
        devices.stream()
            .filter(this::shouldSync)
            .forEach(device -> {
                String key = KEY_PREFIX + device.getId();
                String aclKey = ACL_PREFIX + device.getId();
                String value = buildRedisValue();
                log.info("[CredentialSync] Writing Redis {} → device id={}, productId={}",
                    key, device.getId(), device.getProductId());

                redis.opsForValue().set(key, value).subscribe(
                    ok -> log.info("[CredentialSync] Redis auth OK: device={}", device.getId()),
                    err -> log.error("[CredentialSync] Redis auth FAILED: device={} — {}",
                        device.getId(), err.getMessage())
                );

                // ACL: 限制设备只能 pub/sub 自己的 topic（productId/deviceId/#）
                String topicPrefix = device.getProductId() + "/" + device.getId() + "/#";
                redis.opsForHash().put(aclKey, topicPrefix, "pubsub").subscribe(
                    ok -> log.info("[CredentialSync] Redis ACL OK: device={}", device.getId()),
                    err -> log.error("[CredentialSync] Redis ACL FAILED: device={} — {}",
                        device.getId(), err.getMessage())
                );
            });
    }

    private boolean shouldSync(DeviceInstanceEntity device) {
        return properties.getProductIds().contains(device.getProductId());
    }

    private String buildRedisValue() {
        return "{\"password\":\"" + properties.getDevicePassword() + "\"}";
    }
}
