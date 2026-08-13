package org.jetlinks.community.firmware.service;

import org.hswebframework.ezorm.rdb.mapping.defaults.SaveResult;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.jetlinks.community.firmware.entity.FirmwareEntity;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

@Slf4j
@Service
public class FirmwareService extends GenericReactiveCrudService<FirmwareEntity, String> {

    @Autowired
    private FirmwareUrlResolver urlResolver;

    @Override
    public Mono<SaveResult> save(Publisher<FirmwareEntity> entityPublisher) {
        return Flux
            .from(entityPublisher)
            .doOnNext(this::normalizeUrl)
            .as(super::save);
    }

    @Override
    public Mono<Integer> insert(Publisher<FirmwareEntity> entityPublisher) {
        return Flux
            .from(entityPublisher)
            .doOnNext(this::normalizeUrl)
            .as(super::insert);
    }

    @Override
    public Mono<Integer> insertBatch(Publisher<? extends Collection<FirmwareEntity>> entityPublisher) {
        return Flux
            .from(entityPublisher)
            .doOnNext(entities -> entities.forEach(this::normalizeUrl))
            .as(super::insertBatch);
    }

    @Override
    public Mono<Integer> updateById(String id, Mono<FirmwareEntity> entityPublisher) {
        return super.updateById(id, entityPublisher.doOnNext(this::normalizeUrl));
    }

    public String resolveDownloadUrl(String url) {
        return urlResolver.resolveDownloadUrl(url);
    }

    private void normalizeUrl(FirmwareEntity firmware) {
        firmware.setUrl(urlResolver.normalizeForStorage(firmware.getUrl()));
    }
}
