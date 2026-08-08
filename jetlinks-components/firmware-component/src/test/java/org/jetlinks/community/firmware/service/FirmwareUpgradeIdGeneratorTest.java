package org.jetlinks.community.firmware.service;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirmwareUpgradeIdGeneratorTest {

    @Test
    void shouldGenerateReadableTaskAndUpgradeIds() {
        assertTrue(FirmwareUpgradeIdGenerator.taskId().matches("^TASK\\d{17}-\\d{3}$"));
        assertTrue(FirmwareUpgradeIdGenerator.upgradeId().matches("^UPGRADE\\d{17}-\\d{3}$"));
    }

    @Test
    void shouldFormatTimestampAndIncrementSequence() {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        long timestamp = ZonedDateTime
            .of(2026, 8, 8, 18, 25, 34, 123_000_000, zoneId)
            .toInstant()
            .toEpochMilli();
        FirmwareUpgradeIdGenerator.TimeSequenceGenerator generator =
            new FirmwareUpgradeIdGenerator.TimeSequenceGenerator("TASK", () -> timestamp, zoneId);

        assertEquals("TASK20260808182534123-001", generator.generate());
        assertEquals("TASK20260808182534123-002", generator.generate());
    }

    @Test
    void shouldUseLogicalNextMillisecondAfterSequenceIsExhausted() {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        long timestamp = ZonedDateTime
            .of(2026, 8, 8, 18, 25, 34, 123_000_000, zoneId)
            .toInstant()
            .toEpochMilli();
        AtomicLong currentTime = new AtomicLong(timestamp);
        FirmwareUpgradeIdGenerator.TimeSequenceGenerator generator =
            new FirmwareUpgradeIdGenerator.TimeSequenceGenerator("TASK", currentTime::get, zoneId);

        String id = null;
        for (int i = 0; i < 1_000; i++) {
            id = generator.generate();
        }

        assertEquals("TASK20260808182534124-001", id);
        currentTime.set(timestamp - 1_000);
        assertEquals("TASK20260808182534124-002", generator.generate());
    }

    @Test
    void shouldGenerateUniqueIdsUnderBurstLoad() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            ids.add(FirmwareUpgradeIdGenerator.upgradeId());
        }
        assertEquals(1_000, ids.size());
    }
}
