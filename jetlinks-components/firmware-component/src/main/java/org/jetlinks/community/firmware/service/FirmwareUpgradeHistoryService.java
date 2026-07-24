package org.jetlinks.community.firmware.service;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.crud.service.GenericReactiveCrudService;
import org.jetlinks.community.firmware.entity.FirmwareUpgradeHistoryEntity;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FirmwareUpgradeHistoryService extends GenericReactiveCrudService<FirmwareUpgradeHistoryEntity, String> {
}
