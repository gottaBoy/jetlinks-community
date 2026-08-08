-- PostgreSQL one-time migration for the generic OTA task state machine.
-- Stop OTA task creation and dispatch before running this script.

BEGIN;

ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS mode VARCHAR(32);
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS release_type VARCHAR(32);
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS timeout_seconds INT;
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS response_timeout_seconds INT;
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS status_timeout_seconds INT;
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS description VARCHAR(512);
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS terms TEXT;
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS queued_count INT;
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS running_count INT;
ALTER TABLE dev_firmware_upgrade_task ADD COLUMN IF NOT EXISTS cancelled_count INT;

UPDATE dev_firmware_upgrade_task
SET mode = COALESCE(NULLIF(mode, ''), 'push'),
    release_type = COALESCE(NULLIF(release_type, ''), 'all'),
    timeout_seconds = COALESCE(timeout_seconds, 3600),
    response_timeout_seconds = COALESCE(response_timeout_seconds, 30),
    status_timeout_seconds = COALESCE(status_timeout_seconds, 300),
    queued_count = COALESCE(queued_count, 0),
    running_count = COALESCE(running_count, 0),
    cancelled_count = COALESCE(cancelled_count, 0);

ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN mode SET DEFAULT 'push';
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN release_type SET DEFAULT 'all';
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN timeout_seconds SET DEFAULT 3600;
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN response_timeout_seconds SET DEFAULT 30;
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN status_timeout_seconds SET DEFAULT 300;
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN queued_count SET DEFAULT 0;
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN running_count SET DEFAULT 0;
ALTER TABLE dev_firmware_upgrade_task ALTER COLUMN cancelled_count SET DEFAULT 0;

ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS upgrade_id VARCHAR(64);
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS active_key VARCHAR(64);
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS message_id VARCHAR(128);
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS attempt INT;
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS error_code VARCHAR(64);
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS dispatch_time BIGINT;
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS ack_time BIGINT;
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS last_report_time BIGINT;
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS last_event_time BIGINT;
ALTER TABLE dev_firmware_upgrade_history ADD COLUMN IF NOT EXISTS reported_version VARCHAR(64);

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

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM dev_firmware_upgrade_history
        WHERE status IN (
            'dispatching', 'dispatched', 'accepted', 'preparing',
            'downloading', 'downloaded', 'verifying', 'verified', 'installing',
            'rebooting', 'post_checking'
        )
        GROUP BY device_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Multiple active OTA histories exist for one device. Resolve conflicts before migration.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM dev_firmware_upgrade_history
        GROUP BY upgrade_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Duplicate OTA upgrade_id values exist. Resolve conflicts before migration.';
    END IF;
END $$;

UPDATE dev_firmware_upgrade_history
SET active_key = CASE
    WHEN status IN (
        'dispatching', 'dispatched', 'accepted', 'preparing',
        'downloading', 'downloaded', 'verifying', 'verified', 'installing',
        'rebooting', 'post_checking'
    ) THEN device_id
    ELSE NULL
END;

ALTER TABLE dev_firmware_upgrade_history ALTER COLUMN upgrade_id SET NOT NULL;
ALTER TABLE dev_firmware_upgrade_history ALTER COLUMN attempt SET DEFAULT 1;
ALTER TABLE dev_firmware_upgrade_history ALTER COLUMN attempt SET NOT NULL;

COMMENT ON COLUMN dev_firmware_upgrade_task.mode IS '升级模式: push/pull';
COMMENT ON COLUMN dev_firmware_upgrade_task.release_type IS '发布类型: all/part';
COMMENT ON COLUMN dev_firmware_upgrade_task.timeout_seconds IS '任务总超时时间(秒)';
COMMENT ON COLUMN dev_firmware_upgrade_task.response_timeout_seconds IS '响应超时时间(秒)';
COMMENT ON COLUMN dev_firmware_upgrade_task.status_timeout_seconds IS '状态上报超时时间(秒)';
COMMENT ON COLUMN dev_firmware_upgrade_task.description IS '任务说明';
COMMENT ON COLUMN dev_firmware_upgrade_task.terms IS '设备选择条件(JSON)';
COMMENT ON COLUMN dev_firmware_upgrade_task.queued_count IS '等待下发设备数量';
COMMENT ON COLUMN dev_firmware_upgrade_task.running_count IS '升级中设备数量';
COMMENT ON COLUMN dev_firmware_upgrade_task.cancelled_count IS '已取消设备数量';
COMMENT ON COLUMN dev_firmware_upgrade_history.upgrade_id IS '单设备单次升级全局关联ID';
COMMENT ON COLUMN dev_firmware_upgrade_history.active_key IS '设备活动升级唯一键，终态时置空';
COMMENT ON COLUMN dev_firmware_upgrade_history.message_id IS '最近一次下发消息ID';
COMMENT ON COLUMN dev_firmware_upgrade_history.attempt IS '当前尝试次数';
COMMENT ON COLUMN dev_firmware_upgrade_history.error_code IS '失败错误码';
COMMENT ON COLUMN dev_firmware_upgrade_history.dispatch_time IS '下发尝试时间';
COMMENT ON COLUMN dev_firmware_upgrade_history.ack_time IS '客户端接受时间';
COMMENT ON COLUMN dev_firmware_upgrade_history.last_report_time IS '最近收到有效状态的时间';
COMMENT ON COLUMN dev_firmware_upgrade_history.last_event_time IS '客户端事件时间';
COMMENT ON COLUMN dev_firmware_upgrade_history.reported_version IS '客户端上报固件版本';

CREATE UNIQUE INDEX IF NOT EXISTS idx_fw_upgrade_history_upgrade
    ON dev_firmware_upgrade_history (upgrade_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fw_upgrade_history_active
    ON dev_firmware_upgrade_history (active_key);

COMMIT;
