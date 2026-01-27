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
package org.jetlinks.community.auth.configuration;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.hswebframework.web.authorization.token.UserTokenManager;
import org.hswebframework.web.authorization.token.redis.RedisUserTokenManager;
import org.hswebframework.web.authorization.token.redis.SimpleUserToken;
import org.jetlinks.community.auth.dimension.UserAuthenticationEventPublisher;
import org.jetlinks.community.auth.enums.UserEntityType;
import org.jetlinks.community.auth.manager.ApiKeyAuthenticationManager;
import org.jetlinks.community.auth.web.WebFluxUserController;
import org.jetlinks.core.event.EventBus;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({
    MenuProperties.class,
    AuthorizationProperties.class
})
public class CustomAuthenticationConfiguration {

    static final String CONDITION_CLASS_NAME = "org.jetlinks.community.microservice.configuration.CloudServicesConfiguration";

    @Bean
    @Primary
    public WebFluxUserController webFluxUserController() {
        return new WebFluxUserController();
    }

    @Bean
    @ConfigurationProperties(prefix = "hsweb.user-token")
    public UserTokenManager userTokenManager(ReactiveRedisOperations<Object, Object> template,
                                             ApplicationEventPublisher eventPublisher) {
        RedisUserTokenManager userTokenManager = new RedisUserTokenManager(template);
        userTokenManager.setLocalCache(Caffeine
                                           .newBuilder()
                                           .expireAfterAccess(Duration.ofMinutes(10))
                                           .expireAfterWrite(Duration.ofHours(2))
                                           .<String, SimpleUserToken>build()
                                           .asMap());
        userTokenManager.setEventPublisher(eventPublisher);
        return userTokenManager;
    }

    /**
     * 包装 UserTokenManager，添加对 API Key 的支持
     * 框架的 UserTokenReactiveAuthenticationSupplier 会调用此 UserTokenManager 来处理所有 token（包括 API Key）
     * 
     * 工作流程：
     * 1. ApiKeyReactiveUserTokenParser 解析 X-API-Key 头，创建 ParsedToken("api-key", "xxx")
     * 2. UserTokenReactiveAuthenticationSupplier 读取 ParsedToken，调用 UserTokenManager.getByToken("xxx")
     * 3. ApiKeyUserTokenManager.getByToken() 识别为 API Key，调用 getByApiKey()
     * 4. 查询数据库获取 UserApiKeyEntity，创建 SimpleApiKeyUserToken
     * 5. UserTokenReactiveAuthenticationSupplier 从 UserToken 获取用户ID，调用 ReactiveAuthenticationManager
     * 6. 返回 Authentication，权限检查通过
     * 
     * 注意：使用 @Primary 确保框架优先使用此 UserTokenManager，但保留原始的 userTokenManager Bean
     * 
     * @param delegate 原始的 UserTokenManager（通过方法名注入）
     * @param apiKeyService API Key 服务
     * @return 包装后的 UserTokenManager
     */
    @Bean
    @Primary
    public UserTokenManager apiKeyUserTokenManager(
        @org.springframework.beans.factory.annotation.Qualifier("userTokenManager") UserTokenManager delegate,
        org.jetlinks.community.auth.service.UserApiKeyService apiKeyService) {
        log.info("正在创建 ApiKeyUserTokenManager，包装原有的 UserTokenManager 以支持 API Key");
        org.jetlinks.community.auth.token.ApiKeyUserTokenManager wrapper = 
            new org.jetlinks.community.auth.token.ApiKeyUserTokenManager(delegate, apiKeyService);
        log.info("ApiKeyUserTokenManager 创建成功，现在 UserTokenReactiveAuthenticationSupplier 可以处理 API Key 了");
        return wrapper;
    }

    // 框架已提供 org.hswebframework.web.authorization.basic.web.ApiKeyReactiveUserTokenParser
    // 它会自动被 Spring 扫描并注册，无需额外配置
    // 如果需要自定义配置，可以在这里添加

    /**
     * 注册 ApiKeyAuthenticationManager Bean
     * 框架的 ApiKeyReactiveAuthenticationSupplier 需要此 Bean 才能被创建
     * 
     * @param apiKeyService API Key 服务
     * @param authenticationManager 认证管理器
     * @return ApiKeyAuthenticationManager
     */
    @Bean
    public ApiKeyAuthenticationManager apiKeyAuthenticationManager(
        org.jetlinks.community.auth.service.UserApiKeyService apiKeyService,
        org.hswebframework.web.authorization.ReactiveAuthenticationManager authenticationManager) {
        log.info("正在创建 ApiKeyAuthenticationManager Bean...");
        ApiKeyAuthenticationManager manager = new ApiKeyAuthenticationManager(apiKeyService, authenticationManager);
        log.info("ApiKeyAuthenticationManager Bean 创建成功，框架的 ApiKeyReactiveAuthenticationSupplier 应该能够检测到此 Bean");
        return manager;
    }

    @Bean(destroyMethod = "shutdown")
    public UserAuthenticationEventPublisher userDimensionEventPublisher(EventBus eventBus) {
        return new UserAuthenticationEventPublisher(eventBus);
    }

    @Bean
    public AuthorizationPermissionInitializeService authorizationPermissionInitializeService(AuthorizationProperties properties){
        return new AuthorizationPermissionInitializeService(properties);
    }

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jackson2ObjectMapperBuilderAuthCustomizer() {
        return builder -> {
            builder.deserializerByType(UserEntityType.class, new UserEntityTypeJSONDeserializer());
        };
    }

    /**
     * 监听 Spring 上下文刷新事件，检查 ApiKeyReactiveAuthenticationSupplier 是否被创建
     */
    @Bean
    public ApplicationListener<ContextRefreshedEvent> apiKeyAuthenticationSupplierChecker(ApplicationContext context) {
        return event -> {
            try {
                log.info("========== 检查 API Key 认证相关 Bean ==========");
                
                // 检查 ApiKeyAuthenticationManager 是否存在
                if (context.containsBean("apiKeyAuthenticationManager")) {
                    Object manager = context.getBean("apiKeyAuthenticationManager");
                    log.info("✅ ApiKeyAuthenticationManager Bean 存在: type={}", manager.getClass().getName());
                } else {
                    log.warn("❌ ApiKeyAuthenticationManager Bean 不存在！");
                }
                
                // 注意：框架使用 UserTokenReactiveAuthenticationSupplier 处理所有 token（包括 API Key）
                // 不需要单独的 ApiKeyReactiveAuthenticationSupplier
                
                // 列出所有 ReactiveAuthenticationSupplier
                try {
                    Class<?> supplierInterface = Class.forName("org.hswebframework.web.authorization.ReactiveAuthenticationSupplier");
                    String[] allSuppliers = context.getBeanNamesForType(supplierInterface);
                    log.info("找到 {} 个 ReactiveAuthenticationSupplier Bean:", allSuppliers.length);
                    for (String beanName : allSuppliers) {
                        Object bean = context.getBean(beanName);
                        log.info("  - {}: {}", beanName, bean.getClass().getName());
                    }
                } catch (ClassNotFoundException e) {
                    log.warn("⚠️  无法找到 ReactiveAuthenticationSupplier 接口");
                }
                
                log.info("================================================");
            } catch (Exception e) {
                log.error("检查 API Key 认证相关 Bean 时出错", e);
            }
        };
    }
}
