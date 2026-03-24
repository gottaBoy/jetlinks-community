-- 平行驾驶会话表（接管会话，一对一）(MySQL)
-- 一个驾驶舱同时只能接管一个车辆，一个车辆同时只能被一个驾驶舱接管
-- 注意：唯一性约束在应用层验证（通过查询活跃会话）
CREATE TABLE IF NOT EXISTS parallel_driving_session (
    id VARCHAR(64) NOT NULL COMMENT '主键ID',
    cockpit_device_id VARCHAR(64) NOT NULL COMMENT '驾驶舱设备ID',
    cockpit_device_name VARCHAR(255) COMMENT '驾驶舱设备名称',
    vehicle_device_id VARCHAR(64) NOT NULL COMMENT '车辆设备ID（VIN号）',
    vehicle_device_name VARCHAR(255) COMMENT '车辆设备名称',
    state VARCHAR(32) NOT NULL DEFAULT 'binding' COMMENT '会话状态: binding-绑定中, active-已接管, releasing-释放中, released-已释放',
    bind_time BIGINT NOT NULL COMMENT '接管时间',
    last_active_time BIGINT COMMENT '最后活动时间',
    operator_id VARCHAR(64) COMMENT '操作员ID',
    operator_name VARCHAR(128) COMMENT '操作员名称',
    create_time BIGINT COMMENT '创建时间',
    creator_id VARCHAR(64) COMMENT '创建者ID',
    PRIMARY KEY (id),
    INDEX idx_pds_cockpit (cockpit_device_id),
    INDEX idx_pds_vehicle (vehicle_device_id),
    INDEX idx_pds_state (state),
    INDEX idx_pds_cockpit_state (cockpit_device_id, state),
    INDEX idx_pds_vehicle_state (vehicle_device_id, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平行驾驶会话表（接管会话）';
