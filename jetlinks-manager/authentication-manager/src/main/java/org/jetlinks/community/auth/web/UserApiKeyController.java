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
package org.jetlinks.community.auth.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.authorization.Authentication;
import org.hswebframework.web.authorization.annotation.QueryAction;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.authorization.annotation.SaveAction;
import org.hswebframework.web.authorization.exception.AccessDenyException;
import org.hswebframework.web.authorization.exception.UnAuthorizedException;
import org.jetlinks.community.auth.entity.UserApiKeyEntity;
import org.jetlinks.community.auth.service.UserApiKeyService;
import org.jetlinks.community.auth.service.request.CreateApiKeyRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 用户 API Key 管理控制器
 *
 * @author JetLinks
 * @since 2.10.0
 */
@Slf4j
@RestController
@RequestMapping("/user/api-key")
@AllArgsConstructor
@Tag(name = "用户API Key管理接口")
@Resource(id = "user-api-key", name = "用户API Key", group = "system")
public class UserApiKeyController {

    private final UserApiKeyService apiKeyService;

    @PostMapping("/_create")
    @SaveAction
    @Operation(summary = "创建API Key")
    public Mono<Map<String, String>> createApiKey(@RequestBody Mono<CreateApiKeyRequest> request) {
        return Authentication.currentReactive()
            .switchIfEmpty(Mono.error(UnAuthorizedException.NoStackTrace::new))
            .zipWith(request)
            .flatMap(tuple -> {
                Authentication authentication = tuple.getT1();
                CreateApiKeyRequest req = tuple.getT2();
                
                String userId = req.getUserId();
                // 如果未指定用户ID，使用当前登录用户
                if (userId == null || userId.isEmpty()) {
                    userId = authentication.getUser().getId();
                }
                // 权限检查：只能为自己创建，或需要管理员权限
                if (!userId.equals(authentication.getUser().getId()) && 
                    !authentication.hasPermission("user-api-key", "save")) {
                    return Mono.error(new AccessDenyException("无权限创建其他用户的API Key"));
                }
                
                return apiKeyService.createApiKey(userId, req.getApp(), req.getRemark(), req.getExpiredAt())
                    .map(apiKey -> Map.of("apiKey", apiKey, "message", "API Key创建成功，请妥善保管"));
            });
    }

    @PostMapping("/_query")
    @QueryAction
    @Operation(summary = "查询API Key列表")
    public Mono<PagerResult<UserApiKeyEntity>> queryApiKeys(@RequestBody Mono<QueryParamEntity> queryParamMono) {
        return Authentication.currentReactive()
            .doOnNext(auth -> log.debug("Controller 中读取到认证信息: userId={}", auth.getUser().getId()))
            .doOnError(err -> log.error("Controller 中读取认证信息失败", err))
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("Controller 中未读取到认证信息！可能 Context 未正确设置");
                return Mono.error(UnAuthorizedException.NoStackTrace::new);
            }))
            .zipWith(queryParamMono)
            .flatMap(tuple -> {
                Authentication authentication = tuple.getT1();
                QueryParamEntity queryParam = tuple.getT2();
                
                String userId = queryParam.getTerms()
                    .stream()
                    .filter(term -> "userId".equals(term.getColumn()))
                    .findFirst()
                    .map(term -> String.valueOf(term.getValue()))
                    .orElse(null);
                
                // 如果未指定用户ID，查询当前用户的
                if (userId == null || userId.isEmpty()) {
                    userId = authentication.getUser().getId();
                }
                // 权限检查：只能查询自己的，或需要管理员权限
                if (!userId.equals(authentication.getUser().getId()) && 
                    !authentication.hasPermission("user-api-key", "query")) {
                    return Mono.error(new AccessDenyException("无权限查询其他用户的API Key"));
                }
                
                return apiKeyService.queryUserApiKeys(userId, queryParam);
            });
    }

    @GetMapping("/{id}")
    @QueryAction
    @Operation(summary = "获取API Key详情")
    public Mono<UserApiKeyEntity> getApiKey(@PathVariable @Parameter(description = "API Key ID") Long id) {
        return Authentication.currentReactive()
            .switchIfEmpty(Mono.error(UnAuthorizedException.NoStackTrace::new))
            .flatMap(authentication -> apiKeyService.findById(id)
                .switchIfEmpty(Mono.error(() -> new IllegalArgumentException("API Key不存在")))
                .filter(entity -> {
                    // 权限检查：只能查看自己的，或需要管理员权限
                    String userId = authentication.getUser().getId();
                    return entity.getUserId().equals(userId) || 
                           authentication.hasPermission("user-api-key", "query");
                })
                .switchIfEmpty(Mono.error(new AccessDenyException("无权限查看该API Key")))
                .map(entity -> {
                    // 返回时隐藏真实的 API Key，只显示前8位和后4位
                    String apiKey = entity.getApiKey();
                    if (apiKey != null && apiKey.length() > 12) {
                        String masked = apiKey.substring(0, 8) + "****" + apiKey.substring(apiKey.length() - 4);
                        entity.setApiKey(masked);
                    }
                    return entity;
                }));
    }

    @PutMapping("/{id}/_enable")
    @SaveAction
    @Operation(summary = "启用API Key")
    public Mono<Void> enableApiKey(@PathVariable @Parameter(description = "API Key ID") Long id) {
        return checkPermission(id)
            .then(apiKeyService.setEnable(id, true));
    }

    @PutMapping("/{id}/_disable")
    @SaveAction
    @Operation(summary = "禁用API Key")
    public Mono<Void> disableApiKey(@PathVariable @Parameter(description = "API Key ID") Long id) {
        return checkPermission(id)
            .then(apiKeyService.setEnable(id, false));
    }

    @DeleteMapping("/{id}")
    @SaveAction
    @Operation(summary = "删除API Key")
    public Mono<Void> deleteApiKey(@PathVariable @Parameter(description = "API Key ID") Long id) {
        return checkPermission(id)
            .then(apiKeyService.deleteApiKey(id));
    }

    /**
     * 检查权限：只能操作自己的 API Key，或需要管理员权限
     */
    private Mono<Void> checkPermission(Long id) {
        return Authentication.currentReactive()
            .switchIfEmpty(Mono.error(UnAuthorizedException.NoStackTrace::new))
            .flatMap(authentication -> apiKeyService.findById(id)
                .switchIfEmpty(Mono.error(() -> new IllegalArgumentException("API Key不存在")))
                .filter(entity -> {
                    String userId = authentication.getUser().getId();
                    return entity.getUserId().equals(userId) || 
                           authentication.hasPermission("user-api-key", "save");
                })
                .switchIfEmpty(Mono.error(new AccessDenyException("无权限操作该API Key")))
                .then());
    }
}
