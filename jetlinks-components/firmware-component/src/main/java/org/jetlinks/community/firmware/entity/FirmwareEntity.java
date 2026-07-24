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
import java.util.Map;

@Getter
@Setter
@Table(name = "dev_firmware")
@Comment("固件信息表")
@EnableEntityEvent
public class FirmwareEntity extends GenericEntity<String> implements RecordCreationEntity {

    @Column(length = 128, nullable = false)
    @Schema(description = "固件名称")
    private String name;

    @Column(length = 64)
    @Schema(description = "产品ID")
    private String productId;

    @Column(length = 64)
    @Schema(description = "版本号")
    private String version;

    @Column
    @Schema(description = "版本序号")
    private Integer versionOrder;

    @Column(length = 32)
    @Schema(description = "签名方式")
    private String signMethod;

    @Column(length = 128)
    @Schema(description = "签名")
    private String sign;

    @Column(length = 512)
    @Schema(description = "固件文件URL")
    private String url;

    @Column
    @Schema(description = "文件大小(bytes)")
    private Long size;

    @Column(length = 512)
    @Schema(description = "说明")
    private String description;

    @Column
    @ColumnType(jdbcType = JDBCType.CLOB)
    @JsonCodec
    @Schema(description = "扩展属性")
    private Map<String, Object> properties;

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
