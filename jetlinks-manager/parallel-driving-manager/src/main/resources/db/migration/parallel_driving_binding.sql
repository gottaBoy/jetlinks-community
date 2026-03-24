-- 平行驾驶绑定关系表（多对多授权关系）
-- 一个驾驶舱可以绑定多个车辆，一个车辆可以被多个驾驶舱绑定
CREATE TABLE IF NOT EXISTS parallel_driving_binding (
    id VARCHAR(64) PRIMARY KEY COMMENT '主键ID',
    cockpit_device_id VARCHAR(64) NOT NULL COMMENT '驾驶舱设备ID',
    cockpit_device_name VARCHAR(255) COMMENT '驾驶舱设备名称',
    vehicle_device_id VARCHAR(64) NOT NULL COMMENT '车辆设备ID（VIN号）',
    vehicle_device_name VARCHAR(255) COMMENT '车辆设备名称',
    bind_time BIGINT NOT NULL COMMENT '绑定时间',
    remark VARCHAR(500) COMMENT '备注',
    create_time BIGINT COMMENT '创建时间',
    creator_id VARCHAR(64) COMMENT '创建者ID',
    CONSTRAINT uk_pdb_cockpit_vehicle UNIQUE (cockpit_device_id, vehicle_device_id)
);

CREATE INDEX IF NOT EXISTS idx_pdb_cockpit ON parallel_driving_binding (cockpit_device_id);
CREATE INDEX IF NOT EXISTS idx_pdb_vehicle ON parallel_driving_binding (vehicle_device_id);
CREATE INDEX IF NOT EXISTS idx_pdb_cockpit_vehicle ON parallel_driving_binding (cockpit_device_id, vehicle_device_id);

COMMENT ON TABLE parallel_driving_binding IS '平行驾驶绑定关系表（授权关系）';
COMMENT ON COLUMN parallel_driving_binding.id IS '主键ID';
COMMENT ON COLUMN parallel_driving_binding.cockpit_device_id IS '驾驶舱设备ID';
COMMENT ON COLUMN parallel_driving_binding.cockpit_device_name IS '驾驶舱设备名称';
COMMENT ON COLUMN parallel_driving_binding.vehicle_device_id IS '车辆设备ID（VIN号）';
COMMENT ON COLUMN parallel_driving_binding.vehicle_device_name IS '车辆设备名称';
COMMENT ON COLUMN parallel_driving_binding.bind_time IS '绑定时间';
COMMENT ON COLUMN parallel_driving_binding.remark IS '备注';
COMMENT ON COLUMN parallel_driving_binding.create_time IS '创建时间';
COMMENT ON COLUMN parallel_driving_binding.creator_id IS '创建者ID';
