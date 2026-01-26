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
package org.jetlinks.community.network.manager.service;

import lombok.AllArgsConstructor;
import org.hswebframework.web.crud.events.EntityBeforeDeleteEvent;
import org.hswebframework.web.crud.events.EntityCreatedEvent;
import org.hswebframework.web.crud.events.EntityModifyEvent;
import org.hswebframework.web.crud.events.EntitySavedEvent;
import org.hswebframework.web.exception.BusinessException;
import org.jetlinks.community.network.NetworkManager;
import org.jetlinks.community.network.NetworkProperties;
import org.jetlinks.community.network.manager.entity.CertificateEntity;
import org.jetlinks.community.network.manager.entity.NetworkConfigEntity;
import org.jetlinks.community.network.manager.enums.NetworkConfigState;
import org.jetlinks.community.reference.DataReferenceManager;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

@Component
@AllArgsConstructor
public class NetworkEntityEventHandler {

    private final NetworkConfigService networkService;

    private final DataReferenceManager referenceManager;

    private final NetworkManager networkManager;

    //禁止删除已有网络组件使用的证书
    @EventListener
    public void handleCertificateDelete(EntityBeforeDeleteEvent<CertificateEntity> event) {
        event.async(
            Flux.fromIterable(event.getEntity())
                .flatMap(e -> networkService
                    .createQuery()
                    // FIXME: 2021/9/13 由于网络组件没有直接记录证书，还有更好的处理办法？
                    .$like$(NetworkConfigEntity::getConfiguration, e.getId())
                    .or()
                    .$like$(NetworkConfigEntity::getCluster, e.getId())
                    .count()
                    .doOnNext(i -> {
                        if (i > 0) {
                            throw new BusinessException("error.certificate_has_bean_use_by_network");
                        }
                    })
                )
        );
    }

    //禁止删除已有网关使用的网络组件
    @EventListener
    public void handleNetworkDelete(EntityBeforeDeleteEvent<NetworkConfigEntity> event) {
        event.async(
            Flux.fromIterable(event.getEntity())
                .flatMap(e -> referenceManager
                    .assertNotReferenced(DataReferenceManager.TYPE_NETWORK, e.getId(), "error.network_referenced"))
        );

    }


    @EventListener
    public void handleNetworkCreated(EntityCreatedEvent<NetworkConfigEntity> event) {
        event.async(
            Flux.fromIterable(event.getEntity())
                .flatMapIterable(NetworkConfigEntity::toNetworkPropertiesList)
                .flatMap(this::checkPortConflict)
                .flatMap(this::networkConfigValidate)
                .then(handleEvent(event.getEntity()))
        );
    }

    @EventListener
    public void handleNetworkSaved(EntitySavedEvent<NetworkConfigEntity> event) {
        event.async(
            Flux.fromIterable(event.getEntity())
                .filter(conf -> conf.getConfiguration() != null || conf.getCluster() != null)
                .flatMapIterable(NetworkConfigEntity::toNetworkPropertiesList)
                .flatMap(this::checkPortConflict)
                .flatMap(this::networkConfigValidate)
                .then(handleEvent(event.getEntity()))
        );
    }

    @EventListener
    public void handleNetworkModify(EntityModifyEvent<NetworkConfigEntity> event) {
        event.async(
            Flux.fromIterable(event.getAfter())
                .filter(conf -> conf.getConfiguration() != null || conf.getCluster() != null)
                .flatMapIterable(NetworkConfigEntity::toNetworkPropertiesList)
                .flatMap(properties -> checkPortConflict(properties, event.getAfter().stream()
                    .map(NetworkConfigEntity::getId)
                    .collect(java.util.stream.Collectors.toSet())))
                .flatMap(this::networkConfigValidate)
                .then(handleEvent(event.getAfter()))
        );
    }


    //检查端口冲突
    private Mono<NetworkProperties> checkPortConflict(NetworkProperties properties) {
        return checkPortConflict(properties, java.util.Collections.emptySet());
    }

    //检查端口冲突
    private Mono<NetworkProperties> checkPortConflict(NetworkProperties properties, java.util.Set<String> excludeIds) {
        // 只检查服务器类型的网络配置（TCP Server, HTTP Server等）
        if (properties.getConfigurations() == null) {
            return Mono.just(properties);
        }

        Object portObj = properties.getConfigurations().get("port");
        Object hostObj = properties.getConfigurations().get("host");
        
        if (portObj == null) {
            return Mono.just(properties);
        }

        int port;
        try {
            port = portObj instanceof Number ? ((Number) portObj).intValue() : Integer.parseInt(portObj.toString());
        } catch (Exception e) {
            return Mono.just(properties);
        }

        String host = hostObj != null ? hostObj.toString() : "0.0.0.0";
        
        // 检查是否有相同类型、相同host和相同port的网络配置
        return networkService
            .createQuery()
            .where(NetworkConfigEntity::getType, properties.getType())
            .and(NetworkConfigEntity::getState, NetworkConfigState.enabled)
            .fetch()
            .filter(conf -> {
                // 排除当前配置
                if (excludeIds.contains(conf.getId()) || 
                    (properties.getId() != null && conf.getId().equals(properties.getId()))) {
                    return false;
                }
                
                // 检查端口和host是否冲突
                return conf.toNetworkPropertiesList()
                    .stream()
                    .anyMatch(props -> {
                        Map<String, Object> config = props.getConfigurations();
                        if (config == null) {
                            return false;
                        }
                        Object existingPort = config.get("port");
                        Object existingHost = config.get("host");
                        
                        if (existingPort == null) {
                            return false;
                        }
                        
                        int existingPortInt;
                        try {
                            existingPortInt = existingPort instanceof Number 
                                ? ((Number) existingPort).intValue() 
                                : Integer.parseInt(existingPort.toString());
                        } catch (Exception e) {
                            return false;
                        }
                        
                        String existingHostStr = existingHost != null ? existingHost.toString() : "0.0.0.0";
                        
                        // 检查端口是否相同
                        if (existingPortInt != port) {
                            return false;
                        }
                        
                        // 检查host是否冲突（0.0.0.0 与任何host都冲突，相同host也冲突）
                        return "0.0.0.0".equals(host) || "0.0.0.0".equals(existingHostStr) || host.equals(existingHostStr);
                    });
            })
            .hasElements()
            .flatMap(hasConflict -> {
                if (hasConflict) {
                    return Mono.error(new BusinessException("error.network_port_conflict", 
                        String.format("网络配置端口冲突: 类型=%s, host=%s, port=%d 已被其他网络配置使用", 
                            properties.getType(), host, port)));
                }
                return Mono.just(properties);
            });
    }

    //网络组件配置验证
    private Mono<Void> networkConfigValidate(NetworkProperties properties) {
        return Mono.justOrEmpty(networkManager.getProvider(properties.getType()))
                   .flatMap(networkProvider -> networkProvider.createConfig(properties))
                   .then();
    }

    private Mono<Void> handleEvent(Collection<NetworkConfigEntity> entities) {
        return Flux
            .fromIterable(entities)
            .filter(conf -> conf.getState() == NetworkConfigState.enabled)
            .flatMap(conf -> networkManager.reload(conf.lookupNetworkType(), conf.getId()))
            .then();
    }


}
