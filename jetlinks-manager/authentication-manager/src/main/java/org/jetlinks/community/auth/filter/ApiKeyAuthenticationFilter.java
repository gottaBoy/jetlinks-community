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
package org.jetlinks.community.auth.filter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.Authentication;
import org.hswebframework.web.authorization.ReactiveAuthenticationManager;
import org.jetlinks.community.auth.service.UserApiKeyService;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import java.util.List;

/**
 * API Key 认证过滤器
 * 支持从请求头 X-API-Key 或 Authorization: Bearer {apiKey} 中获取 API Key
 *
 * @author my
 * @since 2.10.0
 */
/**
 * @deprecated 已改用 ApiKeyReactiveUserTokenParser，此类已禁用
 * 请使用 {@link org.jetlinks.community.auth.parser.ApiKeyReactiveUserTokenParser}
 */
@Deprecated
@Slf4j
// @Component  // 已禁用，改用 ApiKeyReactiveUserTokenParser
@AllArgsConstructor
public class ApiKeyAuthenticationFilter implements WebFilter, Ordered {

    private final UserApiKeyService apiKeyService;
    private final ReactiveAuthenticationManager authenticationManager;

    private static final String API_KEY_HEADER = "X-API-Key";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();

        // 尝试从请求头获取 API Key
        String apiKey = extractApiKey(request);
        log.debug("提取 API Key: apiKey={}, path={}", maskApiKey(apiKey), request.getPath().value());
        if (!StringUtils.hasText(apiKey)) {
            // 没有 API Key，继续执行 filter 链（让其他认证机制处理）
            log.debug("未找到 API Key，继续执行 filter 链");
            return chain.filter(exchange);
        }

        // 验证 API Key 并设置认证信息
        log.debug("开始验证 API Key: apiKey={}", maskApiKey(apiKey));
        return authenticateWithApiKey(exchange, apiKey)
            .flatMap(auth -> {
                log.debug("API Key 验证成功: userId={}, 准备设置认证信息到 Context", auth.getUser().getId());
                // 将认证信息设置到 Reactor Context，供后续使用
                // 关键：contextWrite 必须在 chain.filter 之前调用，这样才能在 filter 链执行时使用 Context
                // 权限拦截器（AOP）会在 Controller 方法执行前检查认证，所以 Context 必须在 filter 链订阅时就已经设置好
                // 使用 Mono.defer 确保 Context 在订阅时就已经设置好
                return Mono.defer(() -> {
                    log.debug("在 Mono.defer 中准备执行 filter 链: userId={}", auth.getUser().getId());
                    return chain.filter(exchange)
                        .contextWrite(ctx -> {
                            log.debug("设置认证信息到 Context: userId={}, Context key={}",
                                auth.getUser().getId(), Authentication.class.getName());
                            // 验证 Context 中是否已经有认证信息
                            if (ctx.hasKey(Authentication.class)) {
                                log.warn("Context 中已存在认证信息，将被覆盖: userId={}", auth.getUser().getId());
                            }
                            Context newCtx = ctx.put(Authentication.class, auth);
                            // 验证设置是否成功
                            if (newCtx.hasKey(Authentication.class)) {
                                log.debug("认证信息已成功设置到 Context: userId={}", auth.getUser().getId());
                            } else {
                                log.error("认证信息设置失败！Context key 可能不正确: userId={}", auth.getUser().getId());
                            }
                            return newCtx;
                        });
                })
                .doOnSuccess(v -> {
                    log.debug("Filter 链执行完成: userId={}", auth.getUser().getId());
                })
                .doOnError(err -> {
                    log.error("Filter 链执行失败: userId={}, error={}", auth.getUser().getId(), err.getMessage(), err);
                });
            })
            .onErrorResume(err -> {
                log.warn("API Key 认证失败: apiKey={}, error={}", maskApiKey(apiKey), err.getMessage());
                // 认证失败时，继续执行，让其他认证机制处理
                return chain.filter(exchange);
            });
    }

    /**
     * 从请求头中提取 API Key
     * 支持两种方式：
     * 1. X-API-Key: {apiKey}
     * 2. Authorization: Bearer {apiKey}
     */
    private String extractApiKey(ServerHttpRequest request) {
        HttpHeaders headers = request.getHeaders();

        // 方式1: X-API-Key 头
        List<String> apiKeyHeaders = headers.get(API_KEY_HEADER);
        if (apiKeyHeaders != null && !apiKeyHeaders.isEmpty()) {
            String apiKey = apiKeyHeaders.get(0);
            log.debug("从 X-API-Key 头提取 API Key: apiKey={}", maskApiKey(apiKey));
            if (StringUtils.hasText(apiKey)) {
                return apiKey.trim();
            }
        }

        // 方式2: Authorization: Bearer {apiKey}
        List<String> authHeaders = headers.get(AUTHORIZATION_HEADER);
        if (authHeaders != null && !authHeaders.isEmpty()) {
            String authHeader = authHeaders.get(0);
            if (authHeader != null && authHeader.startsWith(BEARER_PREFIX)) {
                String token = authHeader.substring(BEARER_PREFIX.length()).trim();
                log.debug("从 Authorization 头提取 token: token={}", maskApiKey(token));
                // 如果 token 以 ziot_ 开头，认为是 API Key
                if (token.startsWith("ziot_")) {
                    return token;
                }
            }
        }

        log.debug("未找到有效的 API Key");
        return null;
    }

    /**
     * 使用 API Key 进行认证
     */
    private Mono<Authentication> authenticateWithApiKey(ServerWebExchange exchange, String apiKey) {
        log.debug("开始查询 API Key: apiKey={}", maskApiKey(apiKey));
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
                return Mono.error(new IllegalArgumentException("API Key 无效或已过期"));
            }))
            .flatMap(entity -> {
                log.debug("找到有效的 API Key: userId={}, app={}", entity.getUserId(), entity.getApp());

                // 直接使用用户ID获取认证信息
                return authenticationManager.getByUserId(entity.getUserId())
                    .doOnNext(auth -> log.debug("API Key 认证成功: userId={}, apiKey={}, permissions={}",
                        entity.getUserId(), maskApiKey(apiKey), auth.getPermissions()))
                    .doOnError(err -> log.error("获取用户认证信息失败: userId={}, error={}",
                        entity.getUserId(), err.getMessage(), err));
            })
            .doOnError(err -> log.error("API Key 认证失败: apiKey={}, error={}", maskApiKey(apiKey), err.getMessage(), err));
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

    @Override
    public int getOrder() {
        // 在标准认证过滤器之前执行，使用更高的优先级确保在其他认证过滤器之前执行
        // 使用 HIGHEST_PRECEDENCE 确保在所有其他过滤器之前执行
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
