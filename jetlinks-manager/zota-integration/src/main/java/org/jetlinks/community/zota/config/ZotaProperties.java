package org.jetlinks.community.zota.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration for zota-server integration.
 * DMF RabbitMQ properties come from spring.rabbitmq.* (shared with zota-server).
 * MGMT API properties define the zota-server admin endpoint.
 */
@Data
@Component
@ConfigurationProperties(prefix = "zota")
public class ZotaProperties {

    /** zota-server MGMT API base URL, e.g. http://zota-server:8090 */
    private String mgmtUrl = "http://localhost:8090";

    /** MGMT API credentials */
    private String mgmtUsername = "admin";
    private String mgmtPassword = "admin";

    /** DMF RabbitMQ queue name — must match HawkBit DMF configuration */
    private String dmfQueue = "hawkbit.dmf.queue";

    /** DMF exchange name */
    private String dmfExchange = "hawkbit.dmf.exchange";

    /** DMF routing key for OTA events */
    private String dmfRoutingKey = "hawkbit.dmf.#";
}
