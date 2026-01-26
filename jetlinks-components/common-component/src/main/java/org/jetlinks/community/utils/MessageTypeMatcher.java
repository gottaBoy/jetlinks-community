/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.utils;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.jetlinks.core.message.MessageType;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MessageTypeMatcher {

    @Getter
    private Set<String> excludes;

    @Getter
    private Set<String> includes = new HashSet<>(Collections.singleton("*"));

    /**
     * 设置为true时, 优选判断excludes
     */
    @Setter
    @Getter
    private boolean excludeFirst = true;

    private long excludesMask;

    private long includesMask;

    public void setExcludes(Set<String> excludes) {
        this.excludes = excludes;
        init();
    }

    public void setIncludes(Set<String> includes) {
        this.includes = includes;
        init();
    }

    private long createMask(Collection<MessageType> messageTypes) {
        long mask = 0;

        for (MessageType messageType : messageTypes) {
            mask |= 1L << messageType.ordinal();
        }
        return mask;
    }

    protected void init() {
        if (!CollectionUtils.isEmpty(excludes)) {
            if (excludes.contains("*")) {
                excludesMask = createMask(Arrays.asList(MessageType.values()));
            } else {
                excludesMask = createMask(excludes.stream()
                    .map(String::toUpperCase)
                    .map(MessageType::valueOf)
                    .collect(Collectors.toList()));
            }
        }
        if (!CollectionUtils.isEmpty(includes)) {
            if (includes.contains("*")) {
                includesMask = createMask(Arrays.asList(MessageType.values()));
            } else {
                includesMask = createMask(includes.stream()
                    .map(String::toUpperCase)
                    .map(MessageType::valueOf)
                    .collect(Collectors.toList()));
            }
        }
    }

    public boolean match(MessageType type) {
        // 延迟初始化：如果 includesMask 和 excludesMask 都为 0，且 includes 或 excludes 不为空，则初始化
        // 这可以处理 Spring Boot 配置绑定直接设置字段而不调用 setter 的情况
        if (includesMask == 0 && excludesMask == 0 && 
            (!CollectionUtils.isEmpty(includes) || !CollectionUtils.isEmpty(excludes))) {
            init();
        }
        
        long mask = 1L << type.ordinal();
        boolean result;
        if (includesMask != 0) {
            boolean include = (includesMask & mask) != 0;

            if (excludeFirst && excludesMask != 0) {
                result = include && (excludesMask & mask) == 0;
            } else {
                result = include;
            }
        } else if (excludesMask != 0) {
            result = (excludesMask & mask) == 0;
        } else {
            result = true;
        }
        
        // 调试日志：特别记录 EVENT、ONLINE、OFFLINE 消息类型的匹配信息
        if (type == org.jetlinks.core.message.MessageType.EVENT || 
            type == org.jetlinks.core.message.MessageType.ONLINE ||
            type == org.jetlinks.core.message.MessageType.OFFLINE) {
            log.info("[MessageTypeMatcher] match: type={}, includes={}, excludes={}, includesMask={}, excludesMask={}, result={}", 
                type, includes, excludes, includesMask, excludesMask, result);
        }
        
        return result;
    }
}