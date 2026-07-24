package org.jetlinks.community.zota.fdc;

import lombok.Data;
import java.time.LocalDateTime;

/**
 * FDC 固件包实体（映射 fdc_firmware 表）
 */
@Data
public class FdcFirmware {
    private Long id;
    private String version;
    private String filename;
    private String fileUrl;
    private Long fileSize;
    private String sha256;
    private String description;
    private LocalDateTime createdAt;
}
