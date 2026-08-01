package org.jetlinks.community.zota;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.zota.fdc.FdcFirmwareProperties;
import org.jetlinks.community.zota.sync.CredentialSyncProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * Auto-configuration for ZOTA integration module.
 * Enabled by default — disable with zota.enabled=false.
 *
 * Compatible with existing JetLinks modules:
 * - Uses shared RabbitMQ connection (no separate broker)
 * - Uses WebClient (non-blocking, compatible with WebFlux)
 * - Rule engine action registered as standard TaskExecutorProvider
 */
@Slf4j
@Configuration
@ComponentScan(basePackages = "org.jetlinks.community.zota")
@EnableConfigurationProperties({FdcFirmwareProperties.class, CredentialSyncProperties.class})
@ConditionalOnProperty(prefix = "zota", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ZotaAutoConfiguration {

    @PostConstruct
    public void init() {
        log.info("ZOTA integration module initialized — DMF consumer + MGMT API client + rule engine action");
    }
}
