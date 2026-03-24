package org.jetlinks.community.parallel.driving.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.annotation.ColumnType;
import org.hswebframework.ezorm.rdb.mapping.annotation.DefaultValue;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.api.crud.entity.RecordCreationEntity;
import org.hswebframework.web.crud.annotation.EnableEntityEvent;
import org.hswebframework.web.crud.generator.Generators;
import org.jetlinks.community.parallel.driving.enums.ParallelDrivingSessionState;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.sql.JDBCType;

/**
 * 平行驾驶会话实体
 * 管理驾驶舱-车辆之间的绑定关系和会话状态
 *
 * @author JetLinks
 */
@Getter
@Setter
@Table(name = "parallel_driving_session", indexes = {
    @Index(name = "idx_pds_cockpit", columnList = "cockpit_device_id"),
    @Index(name = "idx_pds_vehicle", columnList = "vehicle_device_id"),
    @Index(name = "idx_pds_state", columnList = "state"),
    @Index(name = "idx_pds_cockpit_vehicle", columnList = "cockpit_device_id,vehicle_device_id")
})
@EnableEntityEvent
public class ParallelDrivingSession extends GenericEntity<String> implements RecordCreationEntity {
    
    @Override
    @GeneratedValue(generator = Generators.SNOW_FLAKE)
    @Pattern(regexp = "^[0-9a-zA-Z_\\-]+$", message = "ID只能由数字,字母,下划线和中划线组成")
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
    @Schema(description = "车辆设备ID")
    @NotBlank
    private String vehicleDeviceId;
    
    @Column(name = "vehicle_device_name", length = 255)
    @Schema(description = "车辆设备名称")
    private String vehicleDeviceName;
    
    @Column(name = "state", length = 32, nullable = false)
    @ColumnType(jdbcType = JDBCType.VARCHAR)
    @Schema(description = "会话状态")
    @DefaultValue("binding")
    private String state;
    
    @Column(name = "bind_time", nullable = false, updatable = false)
    @Schema(description = "绑定时间")
    @DefaultValue(generator = Generators.CURRENT_TIME)
    private Long bindTime;
    
    @Column(name = "last_active_time")
    @Schema(description = "最后活动时间")
    @DefaultValue(generator = Generators.CURRENT_TIME)
    private Long lastActiveTime;
    
    @Column(name = "operator_id", length = 64)
    @Schema(description = "操作员ID")
    private String operatorId;
    
    @Column(name = "operator_name", length = 128)
    @Schema(description = "操作员名称")
    private String operatorName;
    
    @Column(name = "create_time", updatable = false)
    @DefaultValue(generator = Generators.CURRENT_TIME)
    @Schema(description = "创建时间", accessMode = Schema.AccessMode.READ_ONLY)
    private Long createTime;
    
    @Column(name = "creator_id", updatable = false)
    @Schema(description = "创建者ID", accessMode = Schema.AccessMode.READ_ONLY)
    private String creatorId;
    
    /**
     * 获取会话状态枚举
     */
    public ParallelDrivingSessionState getSessionState() {
        return ParallelDrivingSessionState.of(state);
    }
    
    /**
     * 设置会话状态
     */
    public void setSessionState(ParallelDrivingSessionState sessionState) {
        if (sessionState != null) {
            this.state = sessionState.getValue();
        }
    }
    
    /**
     * 检查会话是否活跃
     */
    public boolean isActive() {
        return ParallelDrivingSessionState.ACTIVE.getValue().equals(state);
    }
}
