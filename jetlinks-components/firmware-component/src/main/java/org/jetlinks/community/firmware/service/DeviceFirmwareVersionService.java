package org.jetlinks.community.firmware.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

/**
 * Persists the device's currently running firmware version.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceFirmwareVersionService {

    static final String FIRMWARE_VERSION_CONFIG = "firmwareVersion";

    private final LocalDeviceInstanceService deviceService;

    public Mono<Void> updateCurrentVersion(String deviceId, Object reportedVersion) {
        String version = normalizeVersion(reportedVersion);
        if (!StringUtils.hasText(deviceId) || version == null) {
            return Mono.empty();
        }
        return deviceService
            .mergeConfiguration(
                deviceId,
                Map.of(FIRMWARE_VERSION_CONFIG, version),
                Function.identity())
            .doOnSuccess(ignore ->
                log.debug("Device firmware version updated: device={}, version={}", deviceId, version));
    }

    static String normalizeVersion(Object value) {
        if (value == null) {
            return null;
        }
        String version = String.valueOf(value).trim();
        if (!StringUtils.hasText(version)
            || "unknown".equalsIgnoreCase(version)
            || "--".equals(version)) {
            return null;
        }
        return version;
    }
}
