package org.jetlinks.community.zota.fdc;

import lombok.Data;
import java.time.LocalDateTime;

/**
 * FDC 设备升级记录（映射 fdc_device_upgrade 表）
 */
@Data
public class FdcDeviceUpgrade {
    private Long id;
    private Long taskId;
    private String deviceId;
    private String fromVersion;
    private String toVersion;
    private String status;      // pending / downloading / installing / success / failed
    private int progress;
    private LocalDateTime startedAt;
    private LocalDateTime finishedAt;
}
