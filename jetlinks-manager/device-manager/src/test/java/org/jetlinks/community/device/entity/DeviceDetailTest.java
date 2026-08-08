package org.jetlinks.community.device.entity;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeviceDetailTest {

    @Test
    void shouldExposeCanonicalFirmwareVersion() {
        DeviceInstanceEntity device = DeviceInstanceEntity.of();
        device.setConfiguration(Map.of("firmwareVersion", "1.2.3"));

        DeviceDetail detail = new DeviceDetail().with(device);

        assertEquals("1.2.3", detail.getFirmwareInfo().getVersion());
    }

    @Test
    void shouldExposeLegacyFirmwareVersion() {
        DeviceInstanceEntity device = DeviceInstanceEntity.of();
        device.setConfiguration(Map.of("fwVersion", "1.2.2"));

        DeviceDetail detail = new DeviceDetail().with(device);

        assertEquals("1.2.2", detail.getFirmwareInfo().getVersion());
    }
}
