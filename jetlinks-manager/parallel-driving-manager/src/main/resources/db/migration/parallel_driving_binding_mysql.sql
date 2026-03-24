-- 平行驾驶绑定关系表（多对多授权关系）
-- 一个驾驶舱可以绑定多个车辆，一个车辆可以被多个驾驶舱绑定
CREATE TABLE IF NOT EXISTS parallel_driving_binding (
    id VARCHAR(64) NOT NULL COMMENT '主键ID',
    cockpit_device_id VARCHAR(64) NOT NULL COMMENT '驾驶舱设备ID',
    cockpit_device_name VARCHAR(255) COMMENT '驾驶舱设备名称',
    vehicle_device_id VARCHAR(64) NOT NULL COMMENT '车辆设备ID（VIN号）',
    vehicle_device_name VARCHAR(255) COMMENT '车辆设备名称',
    bind_time BIGINT NOT NULL COMMENT '绑定时间',
    remark VARCHAR(500) COMMENT '备注',
    create_time BIGINT COMMENT '创建时间',
    creator_id VARCHAR(64) COMMENT '创建者ID',
    PRIMARY KEY (id),
    UNIQUE KEY uk_pdb_cockpit_vehicle (cockpit_device_id, vehicle_device_id),
    INDEX idx_pdb_cockpit (cockpit_device_id),
    INDEX idx_pdb_vehicle (vehicle_device_id),
    INDEX idx_pdb_cockpit_vehicle (cockpit_device_id, vehicle_device_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin COMMENT '平行驾驶绑定关系表（授权关系）';
