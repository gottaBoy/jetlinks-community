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
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;

@Getter
@Setter
@Table(name = "dev_firmware_upgrade_history", indexes = {
    @Index(name = "idx_fw_upgrade_history_task", columnList = "task_id"),
    @Index(name = "idx_fw_upgrade_history_device", columnList = "device_id"),
    @Index(name = "idx_fw_upgrade_history_upgrade", columnList = "upgrade_id", unique = true),
    @Index(name = "idx_fw_upgrade_history_active", columnList = "active_key", unique = true)
})
@Comment("固件升级历史表")
@EnableEntityEvent
public class FirmwareUpgradeHistoryEntity extends GenericEntity<String> {

    @Override
    @GeneratedValue(generator = Generators.SNOW_FLAKE)
    public String getId() {
        return super.getId();
    }

    @Column(length = 64, nullable = false)
    @Schema(description = "单设备单次升级关联ID，格式: UPGRADEyyyyMMddHHmmssSSS-NNN")
    private String upgradeId;

    @Column(length = 64)
    @Schema(description = "设备活动升级唯一键，终态时置空")
    private String activeKey;

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
    @DefaultValue("queued")
    @Schema(description = "通用OTA设备状态")
    private String status;

    @Column(length = 128)
    @Schema(description = "最近一次下发消息ID")
    private String messageId;

    @Column
    @DefaultValue("1")
    @Schema(description = "当前尝试次数")
    private Integer attempt;

    @Column(length = 64)
    @Schema(description = "失败错误码")
    private String errorCode;

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
    @Schema(description = "协议发送完成时间")
    private Long dispatchTime;

    @Column
    @Schema(description = "客户端接受时间")
    private Long ackTime;

    @Column
    @Schema(description = "服务端最近收到有效状态的时间")
    private Long lastReportTime;

    @Column
    @Schema(description = "客户端事件时间")
    private Long lastEventTime;

    @Column(length = 64)
    @Schema(description = "客户端最近上报的固件版本")
    private String reportedVersion;

    @Column
    @Schema(description = "完成时间")
    private Long completeTime;
}
