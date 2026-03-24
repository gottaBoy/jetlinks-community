package org.jetlinks.community.parallel.driving.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hswebframework.web.dict.I18nEnumDict;

/**
 * 平行驾驶会话状态枚举
 *
 * @author JetLinks
 */
@Getter
@AllArgsConstructor
public enum ParallelDrivingSessionState implements I18nEnumDict<String> {
    
    BINDING("binding", "绑定中"),      // 正在建立绑定
    ACTIVE("active", "已接管"),        // 已接管，可以控制
    RELEASING("releasing", "释放中"),  // 正在释放
    RELEASED("released", "已释放");    // 已释放
    
    private final String value;
    private final String text;
    
    /**
     * 根据值获取枚举
     */
    public static ParallelDrivingSessionState of(String value) {
        if (value == null) {
            return null;
        }
        for (ParallelDrivingSessionState state : values()) {
            if (state.value.equals(value)) {
                return state;
            }
        }
        return null;
    }
    
    @Override
    public String getValue() {
        return value;
    }
}
