package org.jetlinks.community.parallel.driving.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 车端消息转发至驾驶舱 TCP 的策略：仅白名单内的功能调用回复转舱，避免高频 reply 灌满驾驶舱。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "parallel-driving.vehicle-to-cockpit")
public class ParallelDrivingVehicleToCockpitProperties {

    /**
     * 允许转发到驾驶舱的 {@link org.jetlinks.core.message.function.FunctionInvokeMessageReply#getFunctionId()}（小写比对）。
     * 默认仅 {@code emergencystop}；{@code remotejoystick} 等不应出现在此列表（除非明确需要舱端逐条回调）。
     */
    private Set<String> forwardReplyFunctionIds = new LinkedHashSet<>(Collections.singletonList("emergencystop"));

    /**
     * 是否将车端上行的 remotejoystick 镜像（telemetry）转发给驾驶舱。默认关闭以降低舱 TCP 负载；
     * 若产品需要车端摇杆回显，在配置中打开。
     */
    private boolean forwardVehicleRemoteJoystickMirror = false;

    public boolean shouldForwardStandardFunctionReplyToCockpit(String functionId) {
        if (!StringUtils.hasText(functionId)) {
            return false;
        }
        String key = functionId.trim().toLowerCase(Locale.ROOT);
        return normalizeIds(forwardReplyFunctionIds).contains(key);
    }

    private static Set<String> normalizeIds(Set<String> raw) {
        if (raw == null || raw.isEmpty()) {
            return Collections.emptySet();
        }
        return raw.stream()
            .filter(StringUtils::hasText)
            .map(s -> s.trim().toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
