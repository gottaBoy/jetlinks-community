package org.jetlinks.community.firmware.service;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.jetlinks.community.firmware.entity.FirmwareEntity;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FirmwareService extends GenericReactiveCrudService<FirmwareEntity, String> {
}
