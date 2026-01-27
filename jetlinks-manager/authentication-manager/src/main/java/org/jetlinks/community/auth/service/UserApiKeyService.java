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
package org.jetlinks.community.auth.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.api.crud.entity.PagerResult;
import org.hswebframework.web.api.crud.entity.QueryParamEntity;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.hswebframework.web.system.authorization.api.event.UserDeletedEvent;
import org.jetlinks.community.auth.entity.UserApiKeyEntity;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * 用户 API Key 服务
 *
 * @author JetLinks
 * @since 2.10.0
 */
@Slf4j
@Service
@AllArgsConstructor
public class UserApiKeyService extends GenericReactiveCrudService<UserApiKeyEntity, Long> {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    /**
     * 生成 API Key
     * 格式: ziot_前缀 + Base64(随机32字节) = 约44字符
     */
    public String generateApiKey() {
        byte[] randomBytes = new byte[32];
        SECURE_RANDOM.nextBytes(randomBytes);
        String base64Key = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
        return "ziot_" + base64Key;
    }

    /**
     * 创建 API Key
     *
     * @param userId   用户ID
     * @param app      应用名称
     * @param remark   备注
     * @param expiredAt 过期时间（时间戳，毫秒），null表示永不过期
     * @return API Key 字符串（仅返回一次，需要保存）
     */
    public Mono<String> createApiKey(String userId, String app, String remark, Long expiredAt) {
        String apiKey = generateApiKey();
        
        UserApiKeyEntity entity = UserApiKeyEntity.of();
        entity.setUserId(userId);
        entity.setApp(app);
        entity.setApiKey(apiKey); // 存储明文，用于验证
        entity.setRemark(remark);
        entity.setExpiredAt(expiredAt);
        entity.setEnable((byte) 1);
        entity.setCreateTime(System.currentTimeMillis());
        entity.setModifyTime(System.currentTimeMillis());
        
        return save(entity)
            .thenReturn(apiKey)
            .doOnSuccess(key -> log.info("创建 API Key 成功: userId={}, app={}, apiKey={}", userId, app, key));
    }

    /**
     * 根据 API Key 查找有效的实体
     *
     * @param apiKey API Key
     * @return 实体，如果不存在或已过期则返回空
     */
    public Mono<UserApiKeyEntity> findByApiKey(String apiKey) {
        if (!StringUtils.hasText(apiKey)) {
            return Mono.empty();
        }
        
        return createQuery()
            .where(UserApiKeyEntity::getApiKey, apiKey)
            .fetchOne()
            .filter(UserApiKeyEntity::isValid)
            .doOnNext(entity -> log.debug("找到有效的 API Key: userId={}, app={}", entity.getUserId(), entity.getApp()))
            .doOnError(err -> log.error("查询 API Key 失败: apiKey={}", apiKey, err));
    }

    /**
     * 启用/禁用 API Key
     *
     * @param id     API Key ID
     * @param enable 是否启用
     * @return void
     */
    public Mono<Void> setEnable(Long id, boolean enable) {
        return findById(id)
            .switchIfEmpty(Mono.error(() -> new IllegalArgumentException("API Key 不存在")))
            .flatMap(entity -> {
                entity.setEnable(enable ? (byte) 1 : (byte) 0);
                entity.setModifyTime(System.currentTimeMillis());
                return updateById(id, entity);
            })
            .then()
            .doOnSuccess(v -> log.info("{} API Key: id={}", enable ? "启用" : "禁用", id));
    }

    /**
     * 删除 API Key
     *
     * @param id API Key ID
     * @return void
     */
    public Mono<Void> deleteApiKey(Long id) {
        return deleteById(id)
            .doOnSuccess(count -> log.info("删除 API Key: id={}, count={}", id, count))
            .then();
    }

    /**
     * 查询用户的 API Key 列表
     *
     * @param userId 用户ID
     * @param queryParam 查询参数
     * @return 分页结果
     */
    public Mono<PagerResult<UserApiKeyEntity>> queryUserApiKeys(String userId, QueryParamEntity queryParam) {
        QueryParamEntity nestedQuery = queryParam.clone();
        // 添加用户ID过滤条件
        nestedQuery.and("userId", "eq", userId);
        return queryPager(nestedQuery);
    }

    /**
     * 订阅用户删除事件，删除用户的 API Key
     */
    @EventListener
    public void handleUserDelete(UserDeletedEvent event) {
        event.async(
            createDelete()
                .where(UserApiKeyEntity::getUserId, event.getUser().getId())
                .execute()
                .doOnSuccess(count -> log.info("删除用户 API Key: userId={}, count={}", event.getUser().getId(), count))
                .then()
        );
    }
}
