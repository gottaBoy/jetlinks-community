package org.jetlinks.community.parallel.driving.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.annotation.ColumnType;
import org.hswebframework.ezorm.rdb.mapping.annotation.DefaultValue;
import org.hswebframework.ezorm.rdb.mapping.annotation.JsonCodec;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.crud.generator.Generators;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;
import java.sql.JDBCType;
import java.util.Map;

@Getter
@Setter
@Table(name = "parallel_driving_control_log", indexes = {
    @Index(name = "idx_pdcl_cockpit", columnList = "cockpit_device_id"),
    @Index(name = "idx_pdcl_vehicle", columnList = "vehicle_device_id"),
    @Index(name = "idx_pdcl_type", columnList = "control_type"),
    @Index(name = "idx_pdcl_time", columnList = "timestamp")
})
public class ParallelDrivingControlLog extends GenericEntity<String> {

    @Override
    @GeneratedValue(generator = Generators.SNOW_FLAKE)
    @Schema(description = "ID")
    public String getId() {
        return super.getId();
    }

    @Column(name = "cockpit_device_id", length = 64, nullable = false)
    @Schema(description = "驾驶舱设备ID")
    private String cockpitDeviceId;

    @Column(name = "vehicle_device_id", length = 64, nullable = false)
    @Schema(description = "车辆设备ID")
    private String vehicleDeviceId;

    @Column(name = "control_type", length = 64, nullable = false)
    @Schema(description = "控制类型")
    private String controlType;

    @Column(name = "control_params")
    @ColumnType(jdbcType = JDBCType.CLOB)
    @JsonCodec
    @Schema(description = "控制参数")
    private Map<String, Object> controlParams;

    @Column(name = "success", nullable = false)
    @Schema(description = "是否成功")
    private boolean success;

    @Column(name = "error_message", length = 1024)
    @Schema(description = "错误信息")
    private String errorMessage;

    @Column(name = "timestamp", nullable = false)
    @DefaultValue(generator = Generators.CURRENT_TIME)
    @Schema(description = "时间戳")
    private Long timestamp;
}
