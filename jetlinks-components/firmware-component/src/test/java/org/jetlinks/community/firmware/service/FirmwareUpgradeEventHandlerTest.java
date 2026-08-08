package org.jetlinks.community.firmware.service;

import org.jetlinks.core.utils.TopicUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FirmwareUpgradeEventHandlerTest {

    @Test
    void shouldMatchTransportNeutralDeviceTopics() {
        assertTrue(matches(
            FirmwareUpgradeEventHandler.OTA_STATUS_TOPIC,
            "/device/hc_fdc/FDC001/message/event/ota_status"));
        assertTrue(matches(
            FirmwareUpgradeEventHandler.FIRMWARE_PULL_TOPIC,
            "/device/hc_fdc/FDC001/firmware/pull"));
        assertTrue(matches(
            FirmwareUpgradeEventHandler.FIRMWARE_REPORT_TOPIC,
            "/device/hc_fdc/FDC001/firmware/report"));
        assertTrue(matches(
            FirmwareUpgradeEventHandler.DEVICE_ONLINE_TOPIC,
            "/device/hc_fdc/FDC001/online"));
    }

    private boolean matches(String pattern, String topic) {
        return TopicUtils.match(TopicUtils.split(pattern), TopicUtils.split(topic));
    }
}
