package org.jetlinks.community.zota.mgmt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.zota.config.ZotaProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Minimal client for zota-server (HawkBit) MGMT REST API.
 * Used for operational actions: triggering rollback, querying rollout status.
 * Read-only operations (actual/expected versions) are handled by zota-repo.
 *
 * Compatible with HawkBit MGMT API v1.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ZotaMgmtClient {

    private final ZotaProperties properties;

    private WebClient client() {
        String auth = properties.getMgmtUsername() + ":" + properties.getMgmtPassword();
        String encoded = Base64.getEncoder().encodeToString(auth.getBytes());
        return WebClient.builder()
                .baseUrl(properties.getMgmtUrl())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    /**
     * Trigger a rollback for a specific vehicle (target).
     * POST /rest/v1/rollouts/{rolloutId}/pause — pause active rollout
     * POST /rest/v1/targets/{targetId}/actions/rollback — rollback target
     */
    public Mono<Map<String, Object>> triggerRollback(String controllerId, String reason) {
        log.info("Triggering rollback for controller={}, reason={}", controllerId, reason);

        // Step 1: Find target ID by controller ID
        return client().get()
                .uri("/rest/v1/targets?q=controllerId==" + controllerId)
                .retrieve()
                .bodyToMono(TargetListResponse.class)
                .flatMap(response -> {
                    if (response.getContent().isEmpty()) {
                        log.warn("Target not found: {}", controllerId);
                        return Mono.empty();
                    }
                    String targetId = response.getContent().get(0).getControllerId();

                    // Step 2: Force update cancel + rollback
                    // HawkBit supports force cancel via DS assignment
                    return client().delete()
                            .uri("/rest/v1/targets/{targetId}/assignedDS", targetId)
                            .retrieve()
                            .bodyToMono(Void.class)
                            .thenReturn(Map.<String, Object>of(
                                    "controllerId", controllerId,
                                    "action", "rollback_triggered",
                                    "reason", reason,
                                    "status", "ok"
                            ));
                })
                .doOnError(e -> log.error("Rollback failed for {}: {}", controllerId, e.getMessage()))
                .onErrorReturn(Map.<String, Object>of(
                        "controllerId", controllerId,
                        "action", "rollback_failed",
                        "reason", reason,
                        "status", "error"
                ));
    }

    /**
     * Pause an active rollout.
     * POST /rest/v1/rollouts/{rolloutId}/pause
     */
    public Mono<Map<String, Object>> pauseRollout(long rolloutId, String reason) {
        return client().post()
                .uri("/rest/v1/rollouts/{rolloutId}/pause", rolloutId)
                .bodyValue(Map.of("action", "pause", "reason", reason))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .doOnSuccess(r -> log.info("Rollout {} paused: reason={}", rolloutId, reason))
                .doOnError(e -> log.error("Pause rollout {} failed: {}", rolloutId, e.getMessage()));
    }

    /**
     * Create a new software module assignment for emergency fix.
     * POST /rest/v1/softwaremodules
     */
    public Mono<Map<String, Object>> createEmergencyFix(
            String moduleName, String version, String type, String description) {
        var body = List.of(Map.of(
                "name", moduleName,
                "version", version,
                "type", type,
                "description", description != null ? description : "Emergency fix triggered by sensor anomaly"
        ));
        return client().post()
                .uri("/rest/v1/softwaremodules")
                .bodyValue(body)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .doOnSuccess(r -> log.info("Emergency fix SM created: {}@{}", moduleName, version));
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class TargetListResponse {
        private List<TargetItem> content;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class TargetItem {
        private String controllerId;
        private String name;
        private String updateStatus;
    }
}
