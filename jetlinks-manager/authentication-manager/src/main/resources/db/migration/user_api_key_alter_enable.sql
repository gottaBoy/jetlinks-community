-- -- =============================================
-- -- 修改 user_api_key 表的 enable 列类型
-- -- 从 BOOLEAN 改为 SMALLINT (PostgreSQL)
-- -- =============================================

-- -- 步骤 1: 删除默认值
-- ALTER TABLE user_api_key ALTER COLUMN enable DROP DEFAULT;

-- -- 步骤 2: 转换列类型（将 TRUE 转为 1，FALSE 转为 0）
-- ALTER TABLE user_api_key 
--     ALTER COLUMN enable TYPE SMALLINT 
--     USING CASE WHEN enable THEN 1 ELSE 0 END;

-- -- 步骤 3: 重新设置默认值为 1
-- ALTER TABLE user_api_key ALTER COLUMN enable SET DEFAULT 1;

-- -- 步骤 4: 更新现有数据（将 NULL 值设为 1）
-- UPDATE user_api_key SET enable = 1 WHERE enable IS NULL;

-- -- 步骤 5: 确保列不允许 NULL（如果需要）
-- -- ALTER TABLE user_api_key ALTER COLUMN enable SET NOT NULL;

-- -- 验证：检查列类型和默认值
-- -- SELECT column_name, data_type, column_default, is_nullable 
-- -- FROM information_schema.columns 
-- -- WHERE table_name = 'user_api_key' AND column_name = 'enable';
