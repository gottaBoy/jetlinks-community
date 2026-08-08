package org.jetlinks.community.firmware.service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.LongSupplier;

final class FirmwareUpgradeIdGenerator {

    private static final TimeSequenceGenerator TASK_GENERATOR =
        new TimeSequenceGenerator("TASK", System::currentTimeMillis, ZoneId.systemDefault());

    private static final TimeSequenceGenerator UPGRADE_GENERATOR =
        new TimeSequenceGenerator("UPGRADE", System::currentTimeMillis, ZoneId.systemDefault());

    private FirmwareUpgradeIdGenerator() {
    }

    static String taskId() {
        return TASK_GENERATOR.generate();
    }

    static String upgradeId() {
        return UPGRADE_GENERATOR.generate();
    }

    static final class TimeSequenceGenerator {

        private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS", Locale.ROOT);

        private final String prefix;
        private final LongSupplier currentTimeMillis;
        private final ZoneId zoneId;

        private long lastTimestamp = -1;
        private int sequence;

        TimeSequenceGenerator(String prefix, LongSupplier currentTimeMillis, ZoneId zoneId) {
            this.prefix = prefix;
            this.currentTimeMillis = currentTimeMillis;
            this.zoneId = zoneId;
        }

        synchronized String generate() {
            long currentTimestamp = currentTimeMillis.getAsLong();
            if (currentTimestamp > lastTimestamp) {
                lastTimestamp = currentTimestamp;
                sequence = 1;
            } else if (sequence < 999) {
                sequence++;
            } else {
                // Keep IDs monotonic if one millisecond is exhausted or the clock moves backwards.
                lastTimestamp++;
                sequence = 1;
            }

            return prefix
                + TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(lastTimestamp).atZone(zoneId))
                + "-"
                + String.format(Locale.ROOT, "%03d", sequence);
        }
    }
}
