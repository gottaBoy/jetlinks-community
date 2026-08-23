package org.jetlinks.community.zota.sync;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashSet;
import java.util.Set;

/**
 * Optional MQTT device credential sync configuration.
 * Binds to {@code ziot.emqx.sync} in application.yml.
 * Registered via {@code @EnableConfigurationProperties} in ZotaAutoConfiguration.
 */
@Data
@ConfigurationProperties(prefix = "ziot.emqx.sync")
public class CredentialSyncProperties {

    /**
     * Enables ZIOT-managed MQTT credentials.
     * Keep disabled when an external MQTT platform owns users, passwords and ACLs.
     */
    private boolean enabled;

    /** Product IDs whose devices should be synced to Redis for EMQX auth. */
    private Set<String> productIds = new HashSet<>();

    /** Shared password for all synced devices (EMQX Redis auth). */
    private String devicePassword = "";
}
