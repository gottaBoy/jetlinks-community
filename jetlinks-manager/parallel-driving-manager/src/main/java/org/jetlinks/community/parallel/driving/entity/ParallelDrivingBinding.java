package org.jetlinks.community.parallel.driving.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.api.crud.entity.RecordCreationEntity;
import org.hswebframework.web.crud.annotation.EnableEntityEvent;
import org.hswebframework.web.crud.generator.Generators;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import java.sql.JDBCType;

/**
 * 平行驾驶绑定关系实体
 * 管理驾驶舱-车辆之间的授权关系（多对多）
 * 一个驾驶舱可以绑定多个车辆，一个车辆可以被多个驾驶舱绑定
 * 但只有绑定的车辆才能被对应的驾驶舱接管
 *
 * @author JetLinks
 */
@Getter
@Setter
@Table(name = "parallel_driving_binding", indexes = {
    @Index(name = "idx_pdb_cockpit", columnList = "cockpit_device_id"),
    @Index(name = "idx_pdb_vehicle", columnList = "vehicle_device_id"),
    @Index(name = "idx_pdb_cockpit_vehicle", columnList = "cockpit_device_id,vehicle_device_id")
})
@EnableEntityEvent
public class ParallelDrivingBinding extends GenericEntity<String> implements RecordCreationEntity {

    @Override
    @GeneratedValue(generator = Generators.SNOW_FLAKE)
    @Schema(description = "ID")
    public String getId() {
        return super.getId();
    }

    @Column(name = "cockpit_device_id", length = 64, nullable = false, updatable = false)
    @Schema(description = "驾驶舱设备ID")
    @NotBlank
    private String cockpitDeviceId;

    @Column(name = "cockpit_device_name", length = 255)
    @Schema(description = "驾驶舱设备名称")
    private String cockpitDeviceName;

    @Column(name = "vehicle_device_id", length = 64, nullable = false, updatable = false)
    @Schema(description = "车辆设备ID（VIN号）")
    @NotBlank
    private String vehicleDeviceId;

    @Column(name = "vehicle_device_name", length = 255)
    @Schema(description = "车辆设备名称")
    private String vehicleDeviceName;

    @Column(name = "bind_time", nullable = false, updatable = false)
    @Schema(description = "绑定时间")
    @org.hswebframework.ezorm.rdb.mapping.annotation.DefaultValue(generator = Generators.CURRENT_TIME)
    private Long bindTime;

    @Column(name = "remark", length = 500)
    @Schema(description = "备注")
    private String remark;

    @Column(name = "create_time", updatable = false)
    @org.hswebframework.ezorm.rdb.mapping.annotation.DefaultValue(generator = Generators.CURRENT_TIME)
    @Schema(description = "创建时间", accessMode = Schema.AccessMode.READ_ONLY)
    private Long createTime;

    @Column(name = "creator_id", updatable = false)
    @Schema(description = "创建者ID", accessMode = Schema.AccessMode.READ_ONLY)
    private String creatorId;
}
