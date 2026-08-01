package org.jetlinks.community.firmware.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.annotation.Comment;
import org.hswebframework.ezorm.rdb.mapping.annotation.DefaultValue;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.crud.annotation.EnableEntityEvent;
import org.hswebframework.web.crud.generator.Generators;

import javax.persistence.Column;
import javax.persistence.Table;

@Getter
@Setter
@Table(name = "dev_firmware_upgrade_history")
@Comment("固件升级历史表")
@EnableEntityEvent
public class FirmwareUpgradeHistoryEntity extends GenericEntity<String> {

    @Column(length = 64, nullable = false)
    @Schema(description = "任务ID")
    private String taskId;

    @Column(length = 128)
    @Schema(description = "任务名称")
    private String taskName;

    @Column(length = 64, nullable = false)
    @Schema(description = "设备ID")
    private String deviceId;

    @Column(length = 128)
    @Schema(description = "设备名称")
    private String deviceName;

    @Column(length = 64)
    @Schema(description = "产品ID")
    private String productId;

    @Column(length = 128)
    @Schema(description = "产品名称")
    private String productName;

    @Column(length = 64)
    @Schema(description = "固件ID")
    private String firmwareId;

    @Column(length = 64)
    @Schema(description = "固件名称")
    private String firmwareName;

    @Column(length = 64)
    @Schema(description = "源版本")
    private String fromVersion;

    @Column(length = 64)
    @Schema(description = "目标版本")
    private String toVersion;

    @Column(length = 32)
    @DefaultValue("pending")
    @Schema(description = "状态: pending/downloading/installing/success/failed")
    private String status;

    @Column(length = 1024)
    @Schema(description = "失败原因")
    private String errorMessage;

    @Column
    @DefaultValue("0")
    @Schema(description = "进度 0-100")
    private Integer progress;

    @Column
    @DefaultValue(generator = Generators.CURRENT_TIME)
    @Schema(description = "开始时间")
    private Long startTime;

    @Column
    @Schema(description = "完成时间")
    private Long completeTime;
}
