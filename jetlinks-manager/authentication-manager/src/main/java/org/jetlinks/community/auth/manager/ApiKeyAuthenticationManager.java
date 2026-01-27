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
package org.jetlinks.community.auth.manager;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.Authentication;
import org.hswebframework.web.authorization.ReactiveAuthenticationManager;
import org.jetlinks.community.auth.service.UserApiKeyService;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.publisher.Mono;

/**
 * API Key 认证管理器
 * 实现框架要求的 getAuthenticationByApiKey 方法
 * 
 * 流程：
 * 1. ApiKeyReactiveUserTokenParser 解析 X-API-Key 头
 * 2. 设置 ParsedToken("api-key", "xxx") 到 Context
 * 3. ApiKeyReactiveAuthenticationSupplier 读取
 * 4. 调用 ApiKeyAuthenticationManager.getAuthenticationByApiKey("xxx")
 * 5. 返回 Authentication
 * 6. AopAuthorizingController 权限检查通过
 *
 * @author JetLinks
 * @since 2.10.0
 * 
 * 注意：此类通过 CustomAuthenticationConfiguration 显式注册为 Bean，
 * 以确保框架的 ApiKeyReactiveAuthenticationSupplier 能够正确检测到它
 */
@Slf4j
@AllArgsConstructor
public class ApiKeyAuthenticationManager implements InitializingBean {

    private final UserApiKeyService apiKeyService;
    private final ReactiveAuthenticationManager authenticationManager;

    @Override
    public void afterPropertiesSet() {
        log.info("ApiKeyAuthenticationManager Bean 已创建并初始化");
    }

    /**
     * 根据 API Key 获取认证信息
     * 框架的 ApiKeyReactiveAuthenticationSupplier 会调用此方法
     *
     * @param apiKey API Key
     * @return Authentication
     */
    public Mono<Authentication> getAuthenticationByApiKey(String apiKey) {
        log.info("ApiKeyAuthenticationManager.getAuthenticationByApiKey 被调用: apiKey={}", maskApiKey(apiKey));
        
        if (apiKey == null || apiKey.isEmpty()) {
            log.warn("API Key 为空");
            return Mono.empty();
        }
        
        return apiKeyService.findByApiKey(apiKey)
            .doOnNext(entity -> log.debug("找到 API Key 实体: userId={}, app={}, enable={}, expired={}", 
                entity.getUserId(), entity.getApp(), entity.getEnable(), entity.isExpired()))
            .filter(entity -> {
                boolean valid = entity.isValid();
                log.debug("API Key 有效性检查: userId={}, valid={}, enable={}, expired={}", 
                    entity.getUserId(), valid, entity.getEnable(), entity.isExpired());
                return valid;
            })
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("API Key 无效或已过期: apiKey={}", maskApiKey(apiKey));
                return Mono.empty();
            }))
            .flatMap(entity -> {
                log.debug("找到有效的 API Key: userId={}, app={}", entity.getUserId(), entity.getApp());
                
                // 使用用户ID获取认证信息
                return authenticationManager.getByUserId(entity.getUserId())
                    .doOnNext(auth -> log.debug("API Key 认证成功: userId={}, apiKey={}, permissions={}", 
                        entity.getUserId(), maskApiKey(apiKey), auth.getPermissions()))
                    .doOnError(err -> log.error("获取用户认证信息失败: userId={}, error={}", 
                        entity.getUserId(), err.getMessage(), err));
            })
            .doOnError(err -> log.error("根据 API Key 获取认证信息失败: apiKey={}, error={}", 
                maskApiKey(apiKey), err.getMessage(), err))
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("API Key 认证失败: apiKey={}", maskApiKey(apiKey));
                return Mono.empty();
            }));
    }

    /**
     * 掩码 API Key（用于日志）
     */
    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() <= 12) {
            return "****";
        }
        return apiKey.substring(0, 8) + "****" + apiKey.substring(apiKey.length() - 4);
    }
}
