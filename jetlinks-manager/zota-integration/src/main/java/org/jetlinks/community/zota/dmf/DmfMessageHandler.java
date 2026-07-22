package org.jetlinks.community.zota.dmf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.community.zota.mgmt.ZotaMgmtClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles DMF (Device Management Federation) messages from zota-server (HawkBit).
 * Messages are delivered via RabbitMQ when OTA events occur:
 * - TARGET_POLL (vehicle checked for updates)
 * - UPDATE_STARTED (deployment began)
 * - UPDATE_FINISHED (deployment completed successfully)
 * - UPDATE_ERROR (deployment failed)
 * - ROLLOUT_CREATED / ROLLOUT_FINISHED (rollout lifecycle)
 */
@Slf4j
@Component
public class DmfMessageHandler {

    private final ObjectMapper objectMapper;
    private final ZotaMgmtClient mgmtClient;
    private final DeviceRegistry deviceRegistry;

    public DmfMessageHandler(ObjectMapper objectMapper, ZotaMgmtClient mgmtClient,
                             DeviceRegistry deviceRegistry) {
        this.objectMapper = objectMapper;
        this.mgmtClient = mgmtClient;
        this.deviceRegistry = deviceRegistry;
    }

    /**
     * Entry point for RabbitMQ listener adapter.
     * Compatible with existing JetLinks event-driven architecture.
     */
    public void handleDmfMessage(DmfEvent event) {
        if (event == null || event.getType() == null) {
            log.warn("Received null or empty DMF event, ignoring");
            return;
        }
        log.info("DMF event received: type={}, target={}, timestamp={}",
                event.getType(), event.getTarget(), event.getTimestamp());

        switch (event.getType()) {
            case "UPDATE_FINISHED":
                handleUpdateFinished(event);
                break;
            case "UPDATE_ERROR":
                handleUpdateError(event);
                break;
            case "ROLLOUT_CREATED":
                handleRolloutCreated(event);
                break;
            case "ROLLOUT_FINISHED":
                handleRolloutFinished(event);
                break;
            default:
                log.debug("DMF event type {} logged (no handler)", event.getType());
        }
    }

    private void handleUpdateFinished(DmfEvent event) {
        log.info("OTA update finished: target={}, module={}@{}",
                event.getTarget(), event.getModuleName(), event.getModuleVersion());
        updateDeviceVersion(event.getTarget(), event.getModuleName(), event.getModuleVersion());
    }

    private void handleUpdateError(DmfEvent event) {
        log.warn("OTA update error: target={}, module={}, error={}",
                event.getTarget(), event.getModuleName(), event.getErrorMessage());
        updateDeviceError(event.getTarget(), event.getModuleName(), event.getErrorMessage());
    }

    private void handleRolloutCreated(DmfEvent event) {
        log.info("Rollout created: name={}, dsId={}, totalTargets={}",
                event.getRolloutName(), event.getDistributionSetId(), event.getTotalTargets());
    }

    private void handleRolloutFinished(DmfEvent event) {
        log.info("Rollout finished: name={}, status={}",
                event.getRolloutName(), event.getRolloutStatus());
    }

    /**
     * Updates the JetLinks device config with the new OTA version,
     * storing it as device metadata for visibility in the platform.
     */
    private void updateDeviceVersion(String targetId, String moduleName, String moduleVersion) {
        if (targetId == null || targetId.isEmpty()) {
            log.warn("DMF UPDATE_FINISHED with empty target, skipping device update");
            return;
        }
        Map<String, Object> configs = new HashMap<>();
        configs.put("zota.firmware.version", moduleVersion);
        if (moduleName != null && !moduleName.isEmpty()) {
            configs.put("zota.module." + moduleName + ".version", moduleVersion);
        }
        configs.put("zota.last_update_time", System.currentTimeMillis());

        deviceRegistry.getDevice(targetId)
                .flatMap(device -> device.setConfigs(configs))
                .doOnSuccess(v -> log.info("Device {} updated with OTA version: {}@{}",
                        targetId, moduleName, moduleVersion))
                .doOnError(e -> log.error("Failed to update device {} after OTA: {}", targetId, e.getMessage()))
                .subscribe();
    }

    /**
     * Records an OTA error in device config for diagnostics.
     */
    private void updateDeviceError(String targetId, String moduleName, String errorMessage) {
        if (targetId == null || targetId.isEmpty()) {
            return;
        }
        Map<String, Object> configs = new HashMap<>();
        configs.put("zota.last_error", errorMessage != null ? errorMessage : "unknown");
        configs.put("zota.last_error_time", System.currentTimeMillis());
        if (moduleName != null && !moduleName.isEmpty()) {
            configs.put("zota.last_error_module", moduleName);
        }

        deviceRegistry.getDevice(targetId)
                .flatMap(device -> device.setConfigs(configs))
                .doOnSuccess(v -> log.info("Device {} OTA error recorded", targetId))
                .doOnError(e -> log.error("Failed to record OTA error for device {}: {}", targetId, e.getMessage()))
                .subscribe();
    }

    /**
     * DMF event structure compatible with HawkBit DMF API v1.
     * Maps to HawkBit's DmfUpdateMessage and DmfActionStatusMessage.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DmfEvent {
        private String type;              // UPDATE_STARTED, UPDATE_FINISHED, UPDATE_ERROR, etc.
        private String target;            // Controller ID (VIN)
        private String moduleName;        // Software module name
        private String moduleVersion;     // Software module version
        private String errorMessage;      // Error details for UPDATE_ERROR
        private String rolloutName;       // Rollout campaign name
        private Long rolloutId;           // Rollout ID
        private String rolloutStatus;     // Rollout status
        private Long distributionSetId;   // DS ID
        private Integer totalTargets;     // Total targets in rollout
        private Long timestamp;           // Event timestamp
        private Map<String, Object> metadata; // Additional metadata
    }
}
