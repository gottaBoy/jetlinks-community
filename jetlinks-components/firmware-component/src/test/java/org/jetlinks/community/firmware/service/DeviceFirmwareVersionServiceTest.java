package org.jetlinks.community.firmware.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DeviceFirmwareVersionServiceTest {

    @Test
    void shouldNormalizeReportedVersion() {
        assertEquals("1.2.3", DeviceFirmwareVersionService.normalizeVersion(" 1.2.3 "));
    }

    @Test
    void shouldIgnoreMissingOrPlaceholderVersion() {
        assertNull(DeviceFirmwareVersionService.normalizeVersion(null));
        assertNull(DeviceFirmwareVersionService.normalizeVersion(""));
        assertNull(DeviceFirmwareVersionService.normalizeVersion("unknown"));
        assertNull(DeviceFirmwareVersionService.normalizeVersion("--"));
    }
}
