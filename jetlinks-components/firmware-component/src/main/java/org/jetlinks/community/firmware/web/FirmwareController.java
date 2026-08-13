package org.jetlinks.community.firmware.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Generated;
import lombok.Getter;
import org.hswebframework.web.authorization.annotation.Authorize;
import org.hswebframework.web.authorization.annotation.Resource;
import org.hswebframework.web.crud.web.reactive.ReactiveServiceCrudController;
import org.jetlinks.community.firmware.entity.FirmwareEntity;
import org.jetlinks.community.firmware.service.FirmwareService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/firmware")
@Authorize
@Resource(id = "firmware-manager", name = "固件管理")
@Tag(name = "固件管理")
public class FirmwareController implements ReactiveServiceCrudController<FirmwareEntity, String> {

    @Autowired
    @Getter
    @Generated
    private FirmwareService service;

    @GetMapping("/{productId}/{versionOrder}/exists")
    @Operation(summary = "校验固件版本是否存在")
    public Mono<Boolean> versionExists(@PathVariable String productId,
                                        @PathVariable Integer versionOrder) {
        return service.createQuery()
            .where(FirmwareEntity::getProductId, productId)
            .and(FirmwareEntity::getVersionOrder, versionOrder)
            .count()
            .map(count -> count > 0);
    }

    @GetMapping("/{id}/download")
    @Operation(summary = "下载固件文件（返回文件URL，由前端/设备直接下载）")
    public Mono<String> download(@PathVariable String id) {
        return service.findById(id)
            .map(FirmwareEntity::getUrl)
            .map(service::resolveDownloadUrl);
    }
}
