package org.jetlinks.community.zota.fdc;

import lombok.Data;
import java.time.LocalDateTime;

/**
 * FDC 固件升级任务实体（映射 fdc_upgrade_task 表）
 */
@Data
public class FdcUpgradeTask {
    private Long id;
    private Long firmwareId;
    private int deviceCount;
    private int successCount;
    private int failCount;
    private String status;      // pending / running / completed / cancelled
    private String createdBy;
    private LocalDateTime createdAt;
    private LocalDateTime finishedAt;
}
