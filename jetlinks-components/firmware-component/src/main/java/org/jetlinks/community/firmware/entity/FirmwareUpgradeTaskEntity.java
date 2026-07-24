package org.jetlinks.community.firmware.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.annotation.ColumnType;
import org.hswebframework.ezorm.rdb.mapping.annotation.Comment;
import org.hswebframework.ezorm.rdb.mapping.annotation.DefaultValue;
import org.hswebframework.ezorm.rdb.mapping.annotation.JsonCodec;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.api.crud.entity.RecordCreationEntity;
import org.hswebframework.web.crud.annotation.EnableEntityEvent;
import org.hswebframework.web.crud.generator.Generators;

import javax.persistence.Column;
import javax.persistence.Table;
import java.sql.JDBCType;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Table(name = "dev_firmware_upgrade_task")
@Comment("固件升级任务表")
@EnableEntityEvent
public class FirmwareUpgradeTaskEntity extends GenericEntity<String> implements RecordCreationEntity {

    @Column(length = 128, nullable = false)
    @Schema(description = "任务名称")
    private String name;

    @Column(length = 64, nullable = false)
    @Schema(description = "固件ID")
    private String firmwareId;

    @Column(length = 64)
    @Schema(description = "产品ID")
    private String productId;

    @Column(length = 32)
    @Schema(description = "升级模式: push/pull")
    private String mode;

    @Column(length = 32)
    @DefaultValue("all")
    @Schema(description = "发布类型: all/part")
    private String releaseType;

    @Column
    @Schema(description = "超时时间(秒)")
    private Integer timeoutSeconds;

    @Column
    @Schema(description = "响应超时时间(秒)")
    private Integer responseTimeoutSeconds;

    @Column(length = 512)
    @Schema(description = "任务说明")
    private String description;

    @Column
    @ColumnType(jdbcType = JDBCType.LONGVARCHAR)
    @JsonCodec
    @Schema(description = "设备选择条件(JSON)")
    private List<Map<String, Object>> terms;

    @Column
    @Schema(description = "目标设备数量")
    private Integer deviceCount;

    @Column
    @DefaultValue("0")
    @Schema(description = "成功数量")
    private Integer successCount;

    @Column
    @DefaultValue("0")
    @Schema(description = "失败数量")
    private Integer failCount;

    @Column(length = 32)
    @DefaultValue("pending")
    @Schema(description = "任务状态: pending/running/completed/stopped")
    private String status;

    @Column(updatable = false)
    @Schema(description = "创建者ID")
    private String creatorId;

    @Column(updatable = false)
    @DefaultValue(generator = Generators.CURRENT_TIME)
    @Schema(description = "创建时间")
    private Long createTime;

    @Column(name = "creator_name", updatable = false)
    @Schema(description = "创建者名称")
    private String creatorName;
}
