package org.jetlinks.community.firmware.service;

import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeTaskEntity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FirmwareUpgradeTaskServiceTest {

    @Test
    void shouldAggregateEveryDeviceStateWithoutMixingCancelledIntoFailures() {
        FirmwareUpgradeTaskEntity task = new FirmwareUpgradeTaskEntity();

        FirmwareUpgradeTaskService.applyAggregate(task, List.of(
            history("queued"),
            history("accepted"),
            history("success"),
            history("download_failed"),
            history("cancelled")
        ));

        assertEquals(5, task.getDeviceCount());
        assertEquals(1, task.getQueuedCount());
        assertEquals(1, task.getRunningCount());
        assertEquals(1, task.getSuccessCount());
        assertEquals(1, task.getFailCount());
        assertEquals(1, task.getCancelledCount());
        assertEquals("running", task.getStatus());
    }

    @Test
    void shouldMarkAllCancelledTaskAsStopped() {
        FirmwareUpgradeTaskEntity task = new FirmwareUpgradeTaskEntity();

        FirmwareUpgradeTaskService.applyAggregate(task, List.of(
            history("cancelled"),
            history("cancelled")
        ));

        assertEquals(0, task.getFailCount());
        assertEquals(2, task.getCancelledCount());
        assertEquals("stopped", task.getStatus());
    }

    private FirmwareUpgradeHistoryEntity history(String status) {
        FirmwareUpgradeHistoryEntity history = new FirmwareUpgradeHistoryEntity();
        history.setStatus(status);
        return history;
    }
}
