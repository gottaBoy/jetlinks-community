package org.jetlinks.community.firmware.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FirmwareUpgradeStatusTest {

    @Test
    void shouldNormalizeLegacyAliases() {
        assertEquals("queued", FirmwareUpgradeStatus.normalize(" pending "));
        assertEquals("queued", FirmwareUpgradeStatus.normalize("WAITING"));
        assertEquals("downloading", FirmwareUpgradeStatus.normalize("processing"));
        assertEquals("downloading", FirmwareUpgradeStatus.normalize("running"));
        assertEquals("cancelled", FirmwareUpgradeStatus.normalize("canceled"));
        assertNull(FirmwareUpgradeStatus.normalize("unknown"));
    }

    @Test
    void shouldProtectTerminalStates() {
        assertTrue(FirmwareUpgradeStatus.canTransition("success", "success"));
        assertFalse(FirmwareUpgradeStatus.canTransition("success", "downloading"));
        assertFalse(FirmwareUpgradeStatus.canTransition("download_failed", "downloading"));
        assertFalse(FirmwareUpgradeStatus.canTransition("cancelled", "queued"));
    }

    @Test
    void shouldRejectBackwardAndInvalidSourceTransitions() {
        assertTrue(FirmwareUpgradeStatus.canTransition(null, "queued"));
        assertFalse(FirmwareUpgradeStatus.canTransition(null, "success"));
        assertTrue(FirmwareUpgradeStatus.canTransition("queued", "dispatching"));
        assertTrue(FirmwareUpgradeStatus.canTransition("queued", "cancelled"));
        assertFalse(FirmwareUpgradeStatus.canTransition("queued", "success"));
        assertTrue(FirmwareUpgradeStatus.canTransition("dispatching", "accepted"));
        assertTrue(FirmwareUpgradeStatus.canTransition("accepted", "downloading"));
        assertTrue(FirmwareUpgradeStatus.canTransition("accepted", "verify_failed"));
        assertFalse(FirmwareUpgradeStatus.canTransition("accepted", "rejected"));
        assertFalse(FirmwareUpgradeStatus.canTransition("installing", "downloading"));
        assertFalse(FirmwareUpgradeStatus.canTransition("queued", "not_a_state"));
    }

    @Test
    void shouldSeparateClientAndServerOwnedStates() {
        assertTrue(FirmwareUpgradeStatus.isClientReportable("accepted"));
        assertTrue(FirmwareUpgradeStatus.isClientReportable("download_failed"));
        assertTrue(FirmwareUpgradeStatus.isClientReportable("success"));
        assertFalse(FirmwareUpgradeStatus.isClientReportable("queued"));
        assertFalse(FirmwareUpgradeStatus.isClientReportable("dispatch_failed"));
        assertFalse(FirmwareUpgradeStatus.isClientReportable("ack_timeout"));
        assertFalse(FirmwareUpgradeStatus.isClientReportable("cancelled"));
    }

    @Test
    void shouldExposeRetryableTerminalStates() {
        assertTrue(FirmwareUpgradeStatus.isRetryable("dispatch_failed"));
        assertTrue(FirmwareUpgradeStatus.isRetryable("verify_failed"));
        assertTrue(FirmwareUpgradeStatus.isRetryable("cancelled"));
        assertFalse(FirmwareUpgradeStatus.isRetryable("success"));
        assertFalse(FirmwareUpgradeStatus.isRetryable("downloading"));
    }
}
