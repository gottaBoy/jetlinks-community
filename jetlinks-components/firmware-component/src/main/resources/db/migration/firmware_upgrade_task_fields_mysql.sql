-- MySQL one-time migration for the generic OTA task state machine.
-- Stop OTA task creation and dispatch before running this script.
-- Before running the ALTER statements, this query must return no rows:
--
-- SELECT device_id, COUNT(*)
-- FROM dev_firmware_upgrade_history
-- WHERE LOWER(status) IN (
--   'dispatching', 'dispatched', 'accepted',
--   'preparing', 'processing', 'running', 'downloading', 'downloaded',
--   'verifying', 'verified', 'installing', 'rebooting', 'post_checking'
-- )
-- GROUP BY device_id
-- HAVING COUNT(*) > 1;
--
-- MySQL DDL auto-commits. Back up the tables and run this script once.

ALTER TABLE dev_firmware_upgrade_task
    ADD COLUMN mode VARCHAR(32) COMMENT '升级模式: push/pull',
    ADD COLUMN release_type VARCHAR(32) COMMENT '发布类型: all/part',
    ADD COLUMN timeout_seconds INT COMMENT '任务总超时时间(秒)',
    ADD COLUMN response_timeout_seconds INT COMMENT '响应超时时间(秒)',
    ADD COLUMN status_timeout_seconds INT COMMENT '状态上报超时时间(秒)',
    ADD COLUMN description VARCHAR(512) COMMENT '任务说明',
    ADD COLUMN terms LONGTEXT COMMENT '设备选择条件(JSON)',
    ADD COLUMN queued_count INT COMMENT '等待下发设备数量',
    ADD COLUMN running_count INT COMMENT '升级中设备数量',
    ADD COLUMN cancelled_count INT COMMENT '已取消设备数量';

UPDATE dev_firmware_upgrade_task
SET mode = COALESCE(NULLIF(mode, ''), 'push'),
    release_type = COALESCE(NULLIF(release_type, ''), 'all'),
    timeout_seconds = COALESCE(timeout_seconds, 3600),
    response_timeout_seconds = COALESCE(response_timeout_seconds, 30),
    status_timeout_seconds = COALESCE(status_timeout_seconds, 300),
    queued_count = COALESCE(queued_count, 0),
    running_count = COALESCE(running_count, 0),
    cancelled_count = COALESCE(cancelled_count, 0);

ALTER TABLE dev_firmware_upgrade_task
    MODIFY COLUMN mode VARCHAR(32) DEFAULT 'push' COMMENT '升级模式: push/pull',
    MODIFY COLUMN release_type VARCHAR(32) DEFAULT 'all' COMMENT '发布类型: all/part',
    MODIFY COLUMN timeout_seconds INT DEFAULT 3600 COMMENT '任务总超时时间(秒)',
    MODIFY COLUMN response_timeout_seconds INT DEFAULT 30 COMMENT '响应超时时间(秒)',
    MODIFY COLUMN status_timeout_seconds INT DEFAULT 300 COMMENT '状态上报超时时间(秒)',
    MODIFY COLUMN queued_count INT DEFAULT 0 COMMENT '等待下发设备数量',
    MODIFY COLUMN running_count INT DEFAULT 0 COMMENT '升级中设备数量',
    MODIFY COLUMN cancelled_count INT DEFAULT 0 COMMENT '已取消设备数量';

ALTER TABLE dev_firmware_upgrade_history
    ADD COLUMN upgrade_id VARCHAR(64) COMMENT '单设备单次升级全局关联ID',
    ADD COLUMN active_key VARCHAR(64) COMMENT '设备活动升级唯一键，终态时置空',
    ADD COLUMN message_id VARCHAR(128) COMMENT '最近一次下发消息ID',
    ADD COLUMN attempt INT COMMENT '当前尝试次数',
    ADD COLUMN error_code VARCHAR(64) COMMENT '失败错误码',
    ADD COLUMN dispatch_time BIGINT COMMENT '下发尝试时间',
    ADD COLUMN ack_time BIGINT COMMENT '客户端接受时间',
    ADD COLUMN last_report_time BIGINT COMMENT '最近收到有效状态的时间',
    ADD COLUMN last_event_time BIGINT COMMENT '客户端事件时间',
    ADD COLUMN reported_version VARCHAR(64) COMMENT '客户端上报固件版本';

UPDATE dev_firmware_upgrade_history
SET status = CASE LOWER(status)
                 WHEN 'pending' THEN 'queued'
                 WHEN 'waiting' THEN 'queued'
                 WHEN 'processing' THEN 'downloading'
                 WHEN 'running' THEN 'downloading'
                 WHEN 'canceled' THEN 'cancelled'
                 ELSE LOWER(status)
             END,
    upgrade_id = COALESCE(NULLIF(upgrade_id, ''), id),
    attempt = COALESCE(attempt, 1);

UPDATE dev_firmware_upgrade_history
SET active_key = CASE
    WHEN status IN (
        'dispatching', 'dispatched', 'accepted', 'preparing',
        'downloading', 'downloaded', 'verifying', 'verified', 'installing',
        'rebooting', 'post_checking'
    ) THEN device_id
    ELSE NULL
END;

ALTER TABLE dev_firmware_upgrade_history
    MODIFY COLUMN upgrade_id VARCHAR(64) NOT NULL COMMENT '单设备单次升级全局关联ID',
    MODIFY COLUMN attempt INT NOT NULL DEFAULT 1 COMMENT '当前尝试次数',
    ADD UNIQUE KEY idx_fw_upgrade_history_upgrade (upgrade_id),
    ADD UNIQUE KEY idx_fw_upgrade_history_active (active_key);
