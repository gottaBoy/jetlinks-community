package org.jetlinks.community.firmware.entity;

import java.util.*;

/**
 * Transport-neutral device OTA state.
 */
public enum FirmwareUpgradeStatus {
    QUEUED("queued", 0, false, false),
    DISPATCHING("dispatching", 10, false, false),
    DISPATCHED("dispatched", 20, false, false),
    ACCEPTED("accepted", 30, false, false),
    PREPARING("preparing", 40, false, false),
    DOWNLOADING("downloading", 50, false, false),
    DOWNLOADED("downloaded", 60, false, false),
    VERIFYING("verifying", 70, false, false),
    VERIFIED("verified", 80, false, false),
    INSTALLING("installing", 90, false, false),
    REBOOTING("rebooting", 100, false, false),
    POST_CHECKING("post_checking", 110, false, false),
    SUCCESS("success", 120, true, false),

    DISPATCH_FAILED("dispatch_failed", 120, true, true),
    REJECTED("rejected", 120, true, true),
    ACK_TIMEOUT("ack_timeout", 120, true, true),
    STATUS_TIMEOUT("status_timeout", 120, true, true),
    EXECUTION_TIMEOUT("execution_timeout", 120, true, true),
    DOWNLOAD_FAILED("download_failed", 120, true, true),
    VERIFY_FAILED("verify_failed", 120, true, true),
    INSTALL_FAILED("install_failed", 120, true, true),
    REBOOT_FAILED("reboot_failed", 120, true, true),
    POST_CHECK_FAILED("post_check_failed", 120, true, true),
    FAILED("failed", 120, true, true),
    CANCELLED("cancelled", 120, true, true);

    private static final Map<String, FirmwareUpgradeStatus> LOOKUP;
    private static final Set<String> TERMINAL_VALUES;
    private static final Set<String> FAILURE_VALUES;
    private static final Set<String> RETRYABLE_VALUES;
    private static final Set<String> ACTIVE_VALUES;

    static {
        Map<String, FirmwareUpgradeStatus> lookup = new HashMap<>();
        Set<String> terminal = new LinkedHashSet<>();
        Set<String> failure = new LinkedHashSet<>();
        Set<String> active = new LinkedHashSet<>();
        for (FirmwareUpgradeStatus status : values()) {
            lookup.put(status.value, status);
            if (status.terminal) {
                terminal.add(status.value);
            } else {
                active.add(status.value);
            }
            if (status.failure) {
                failure.add(status.value);
            }
        }
        lookup.put("pending", QUEUED);
        lookup.put("waiting", QUEUED);
        lookup.put("processing", DOWNLOADING);
        lookup.put("running", DOWNLOADING);
        lookup.put("canceled", CANCELLED);
        LOOKUP = Collections.unmodifiableMap(lookup);
        TERMINAL_VALUES = Collections.unmodifiableSet(terminal);
        FAILURE_VALUES = Collections.unmodifiableSet(failure);
        ACTIVE_VALUES = Collections.unmodifiableSet(active);

        Set<String> retryable = new LinkedHashSet<>(failure);
        retryable.add(CANCELLED.value);
        RETRYABLE_VALUES = Collections.unmodifiableSet(retryable);
    }

    private final String value;
    private final int order;
    private final boolean terminal;
    private final boolean failure;

    FirmwareUpgradeStatus(String value, int order, boolean terminal, boolean failure) {
        this.value = value;
        this.order = order;
        this.terminal = terminal;
        this.failure = failure;
    }

    public String getValue() {
        return value;
    }

    public int getOrder() {
        return order;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isFailure() {
        return failure;
    }

    public static Optional<FirmwareUpgradeStatus> from(String value) {
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(LOOKUP.get(value.trim().toLowerCase(Locale.ROOT)));
    }

    public static String normalize(String value) {
        return from(value).map(FirmwareUpgradeStatus::getValue).orElse(null);
    }

    public static boolean canTransition(String currentValue, String nextValue) {
        Optional<FirmwareUpgradeStatus> current = from(currentValue);
        Optional<FirmwareUpgradeStatus> next = from(nextValue);
        if (next.isEmpty()) {
            return false;
        }
        if (current.isEmpty()) {
            return next.get() == QUEUED;
        }
        if (current.get() == next.get()) {
            return true;
        }
        if (current.get().terminal) {
            return false;
        }
        if (current.get() == QUEUED) {
            return next.get() == DISPATCHING || next.get() == CANCELLED;
        }
        if (next.get() == CANCELLED) {
            return false;
        }
        if (next.get() == DISPATCH_FAILED) {
            return current.get() == DISPATCHING;
        }
        if (next.get() == REJECTED) {
            return current.get() == DISPATCHING || current.get() == DISPATCHED;
        }
        if (next.get() == ACK_TIMEOUT) {
            return current.get() == DISPATCHED;
        }
        if (next.get() == STATUS_TIMEOUT) {
            return current.get().order >= DISPATCHED.order;
        }
        if (next.get() == EXECUTION_TIMEOUT) {
            return current.get().order >= DISPATCHING.order;
        }
        if (next.get().terminal) {
            return current.get().order >= DISPATCHING.order;
        }
        return next.get().order >= current.get().order;
    }

    public static boolean isClientReportable(String value) {
        return from(value)
            .map(status -> status == ACCEPTED
                || status == PREPARING
                || status == DOWNLOADING
                || status == DOWNLOADED
                || status == VERIFYING
                || status == VERIFIED
                || status == INSTALLING
                || status == REBOOTING
                || status == POST_CHECKING
                || status == SUCCESS
                || status == REJECTED
                || status == DOWNLOAD_FAILED
                || status == VERIFY_FAILED
                || status == INSTALL_FAILED
                || status == REBOOT_FAILED
                || status == POST_CHECK_FAILED
                || status == FAILED)
            .orElse(false);
    }

    public static boolean isTerminal(String value) {
        return from(value).map(FirmwareUpgradeStatus::isTerminal).orElse(false);
    }

    public static boolean isFailure(String value) {
        return from(value).map(FirmwareUpgradeStatus::isFailure).orElse(false);
    }

    public static boolean isRetryable(String value) {
        String normalized = normalize(value);
        return normalized != null && RETRYABLE_VALUES.contains(normalized);
    }

    public static Set<String> terminalValues() {
        return TERMINAL_VALUES;
    }

    public static Set<String> failureValues() {
        return FAILURE_VALUES;
    }

    public static Set<String> activeValues() {
        return ACTIVE_VALUES;
    }
}
