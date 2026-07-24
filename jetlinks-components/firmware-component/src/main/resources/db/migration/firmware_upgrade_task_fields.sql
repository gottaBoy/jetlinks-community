-- 固件升级任务表：补全升级模式、超时、发布类型、说明、设备选择条件字段
ALTER TABLE dev_firmware_upgrade_task
    ADD COLUMN IF NOT EXISTS mode VARCHAR(32) COMMENT '升级模式: push/pull',
    ADD COLUMN IF NOT EXISTS release_type VARCHAR(32) DEFAULT 'all' COMMENT '发布类型: all/part',
    ADD COLUMN IF NOT EXISTS timeout_seconds INT COMMENT '超时时间(秒)',
    ADD COLUMN IF NOT EXISTS response_timeout_seconds INT COMMENT '响应超时时间(秒)',
    ADD COLUMN IF NOT EXISTS description VARCHAR(512) COMMENT '任务说明',
    ADD COLUMN IF NOT EXISTS terms TEXT COMMENT '设备选择条件(JSON)';
