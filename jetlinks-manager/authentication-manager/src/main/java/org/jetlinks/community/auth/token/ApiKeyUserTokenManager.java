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
package org.jetlinks.community.auth.token;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.authorization.token.UserToken;
import org.hswebframework.web.authorization.token.UserTokenManager;
import org.jetlinks.community.auth.entity.UserApiKeyEntity;
import org.jetlinks.community.auth.service.UserApiKeyService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * API Key UserToken 管理器
 * 包装原有的 UserTokenManager，增加对 API Key 的支持
 * 当 token 以 ziot_ 或 ziot_ 开头时，识别为 API Key 并查询对应的用户信息
 *
 * @author JetLinks
 * @since 2.10.0
 */
/**
 * API Key UserToken 管理器
 * 包装原有的 UserTokenManager，增加对 API Key 的支持
 * 
 * 工作流程：
 * 1. ApiKeyReactiveUserTokenParser 解析 X-API-Key 头，创建 ParsedToken("api-key", "xxx")
 * 2. UserTokenReactiveAuthenticationSupplier 读取 ParsedToken，调用 UserTokenManager.getByToken("xxx")
 * 3. ApiKeyUserTokenManager.getByToken() 识别为 API Key，调用 getByApiKey()
 * 4. 查询数据库获取 UserApiKeyEntity，创建 SimpleApiKeyUserToken
 * 5. UserTokenReactiveAuthenticationSupplier 从 UserToken 获取用户ID，调用 ReactiveAuthenticationManager
 * 6. 返回 Authentication，权限检查通过
 */
@Slf4j
@AllArgsConstructor
public class ApiKeyUserTokenManager implements UserTokenManager {

    private final UserTokenManager delegate;
    private final UserApiKeyService apiKeyService;

    // API Key 到 UserToken 的缓存映射
    private final ConcurrentMap<String, UserToken> apiKeyTokenCache = new ConcurrentHashMap<>();

    @Override
    public Mono<UserToken> getByToken(String token) {
        // 检查是否是 API Key
        if (isApiKey(token)) {
            log.info("ApiKeyUserTokenManager: 识别为 API Key: token={}", maskApiKey(token));
            return getByApiKey(token)
                .doOnNext(userToken -> log.info("ApiKeyUserTokenManager: API Key 转换为 UserToken 成功: userId={}, token={}", 
                    userToken.getUserId(), maskApiKey(token)))
                .doOnError(err -> log.error("ApiKeyUserTokenManager: API Key 转换为 UserToken 失败: token={}, error={}", 
                    maskApiKey(token), err.getMessage(), err));
        }
        
        // 否则使用原有的 UserTokenManager
        return delegate.getByToken(token);
    }

    @Override
    public Flux<UserToken> getByUserId(String userId) {
        return delegate.getByUserId(userId);
    }

    @Override
    public Mono<UserToken> signIn(String token, String userId, String type, long maxInactiveInterval) {
        return delegate.signIn(token, userId, type, maxInactiveInterval);
    }

    @Override
    public Mono<Void> signOutByToken(String token) {
        // 如果是 API Key，清除缓存
        if (isApiKey(token)) {
            apiKeyTokenCache.remove(token);
        }
        return delegate.signOutByToken(token);
    }

    @Override
    public Mono<Void> signOutByUserId(String userId) {
        // 清除所有相关的 API Key 缓存
        apiKeyTokenCache.entrySet().removeIf(entry -> entry.getValue().getUserId().equals(userId));
        return delegate.signOutByUserId(userId);
    }


    // 以下方法委托给 delegate
    @Override
    public Flux<UserToken> allLoggedUser() {
        return delegate.allLoggedUser();
    }

    @Override
    public Mono<Boolean> userIsLoggedIn(String userId) {
        return delegate.userIsLoggedIn(userId);
    }

    @Override
    public Mono<Boolean> tokenIsLoggedIn(String token) {
        if (isApiKey(token)) {
            return apiKeyService.findByApiKey(token)
                .filter(UserApiKeyEntity::isValid)
                .map(entity -> true)
                .defaultIfEmpty(false);
        }
        return delegate.tokenIsLoggedIn(token);
    }

    @Override
    public Mono<Void> touch(String token) {
        return delegate.touch(token);
    }

    @Override
    public Mono<Void> checkExpiredToken() {
        return delegate.checkExpiredToken();
    }

    @Override
    public Mono<Integer> totalToken() {
        return delegate.totalToken();
    }

    @Override
    public Mono<Integer> totalUser() {
        return delegate.totalUser();
    }

    @Override
    public Mono<Void> changeUserState(String userId, org.hswebframework.web.authorization.token.TokenState state) {
        // 清除所有相关的 API Key 缓存
        if (state == org.hswebframework.web.authorization.token.TokenState.expired || 
            state == org.hswebframework.web.authorization.token.TokenState.offline) {
            apiKeyTokenCache.entrySet().removeIf(entry -> entry.getValue().getUserId().equals(userId));
        }
        return delegate.changeUserState(userId, state);
    }

    @Override
    public Mono<Void> changeTokenState(String token, org.hswebframework.web.authorization.token.TokenState state) {
        // 如果是 API Key，清除缓存
        if (isApiKey(token) && 
            (state == org.hswebframework.web.authorization.token.TokenState.expired || 
             state == org.hswebframework.web.authorization.token.TokenState.offline)) {
            apiKeyTokenCache.remove(token);
        }
        return delegate.changeTokenState(token, state);
    }

    /**
     * 检查是否是 API Key
     * API Key 格式：ziot_开头
     */
    private boolean isApiKey(String token) {
        return token != null && (token.startsWith("ziot_"));
    }

    /**
     * 根据 API Key 获取 UserToken
     */
    private Mono<UserToken> getByApiKey(String apiKey) {
        // 先从缓存中查找
        UserToken cachedToken = apiKeyTokenCache.get(apiKey);
        if (cachedToken != null) {
            // 验证 token 是否仍然有效
            return apiKeyService.findByApiKey(apiKey)
                .filter(UserApiKeyEntity::isValid)
                .map(entity -> cachedToken)
                .switchIfEmpty(Mono.defer(() -> {
                    // API Key 已失效，清除缓存
                    apiKeyTokenCache.remove(apiKey);
                    return Mono.empty();
                }));
        }
        
        // 缓存中没有，查询数据库
        return apiKeyService.findByApiKey(apiKey)
            .filter(UserApiKeyEntity::isValid)
            .map(entity -> {
                // 创建 UserToken
                UserToken token = new SimpleApiKeyUserToken(apiKey, entity.getUserId(), entity.getApp());
                // 缓存 token
                apiKeyTokenCache.put(apiKey, token);
                log.debug("API Key 转换为 UserToken 成功: userId={}, apiKey={}", 
                    entity.getUserId(), maskApiKey(apiKey));
                return token;
            })
            .doOnError(err -> log.error("根据 API Key 获取 UserToken 失败: apiKey={}, error={}", 
                maskApiKey(apiKey), err.getMessage()));
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

    /**
     * 简单的 API Key UserToken 实现
     */
    private static class SimpleApiKeyUserToken implements UserToken {
        private final String token;
        private final String userId;
        private final String type;

        public SimpleApiKeyUserToken(String token, String userId, String type) {
            this.token = token;
            this.userId = userId;
            this.type = type != null ? type : "api-key";
        }

        @Override
        public String getToken() {
            return token;
        }

        @Override
        public String getUserId() {
            return userId;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public long getMaxInactiveInterval() {
            // API Key 的过期时间由 UserApiKeyEntity 管理
            return -1; // 表示不过期（由实体管理）
        }

        @Override
        public long getLastRequestTime() {
            return System.currentTimeMillis();
        }

        @Override
        public org.hswebframework.web.authorization.token.TokenState getState() {
            return org.hswebframework.web.authorization.token.TokenState.normal;
        }

        @Override
        public long getRequestTimes() {
            return 1;
        }

        @Override
        public long getSignInTime() {
            return System.currentTimeMillis();
        }
    }
}
