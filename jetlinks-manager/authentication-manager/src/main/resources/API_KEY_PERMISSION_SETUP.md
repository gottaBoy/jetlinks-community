# API Key 权限配置指南

## 权限标识

- **权限ID**: `user-api-key`
- **权限名称**: `用户API Key`
- **权限组**: `system`

## 权限操作类型

- `query`: 查询 API Key 列表
- `save`: 创建/编辑 API Key
- `delete`: 删除 API Key

## 配置方法

### 方法一：通过前端界面配置（推荐）

#### 步骤 1：确认权限资源已创建

1. 登录系统（使用 admin 账号）
2. 进入 **系统管理** → **权限管理**
3. 在搜索框中输入：`user-api-key`
4. 如果找不到，说明权限资源还未创建，需要：
   - 重启系统（权限资源会在启动时通过 `@Resource` 注解自动创建）
   - 或者等待系统自动扫描并创建

#### 步骤 2：为角色分配权限

1. 进入 **系统管理** → **角色管理**
2. 找到你的角色（通常是"管理员"或"administrator"）
3. 点击角色名称进入详情页
4. 点击 **权限配置** 标签
5. 在权限树中找到 **系统管理** → **用户API Key**
6. 勾选需要的操作权限：
   - ✅ **查询**（query）- 查看 API Key 列表
   - ✅ **保存**（save）- 创建/编辑 API Key
   - ✅ **删除**（delete）- 删除 API Key
7. 点击 **保存** 按钮

#### 步骤 3：刷新权限缓存

配置完成后：
1. 退出登录
2. 重新登录
3. 或者刷新页面（F5）

### 方法二：通过 SQL 快速配置（适用于开发环境）

如果权限资源已创建，可以通过 SQL 直接为角色分配权限。

#### 步骤 1：查找权限资源ID

```sql
-- PostgreSQL
SELECT id, name FROM s_permission WHERE id = 'user-api-key';

-- 如果不存在，说明权限资源还未创建，需要重启系统
```

#### 步骤 2：查找角色ID

```sql
-- 查找管理员角色
SELECT id, name, code FROM s_role WHERE name LIKE '%管理员%' OR code = 'administrator' OR code = 'admin';
```

#### 步骤 3：为角色分配权限

假设：
- 权限ID: `user-api-key`
- 角色ID: `your-role-id`（从步骤2获取）

```sql
-- PostgreSQL: 为角色分配 user-api-key 权限（查询、保存、删除）
INSERT INTO s_role_permission (role_id, permission_id, actions)
VALUES ('your-role-id', 'user-api-key', '["query","save","delete"]')
ON CONFLICT (role_id, permission_id) 
DO UPDATE SET actions = '["query","save","delete"]';

-- 如果表结构不同，可能需要使用以下方式：
-- 先删除旧权限
DELETE FROM s_role_permission WHERE role_id = 'your-role-id' AND permission_id = 'user-api-key';

-- 再插入新权限
INSERT INTO s_role_permission (role_id, permission_id, actions)
VALUES ('your-role-id', 'user-api-key', '["query","save","delete"]');
```

#### 步骤 4：清除权限缓存

配置完成后，需要清除权限缓存：

```sql
-- 清除用户权限缓存（可选，系统会自动刷新）
-- 或者重启系统
```

### 方法三：通过 API 配置（适用于自动化）

如果需要通过 API 配置权限，可以使用角色管理 API：

```bash
# 1. 获取角色详情
curl -X POST "http://iot.intra.zeron.ai:8848/jetlinks/role/detail/_query" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"paging": false, "terms": [{"column": "code", "value": "administrator"}]}'

# 2. 更新角色权限（需要根据实际 API 文档调整）
curl -X PUT "http://iot.intra.zeron.ai:8848/jetlinks/role/detail/{roleId}/permission" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "permissions": [
      {
        "permission": "user-api-key",
        "actions": ["query", "save", "delete"]
      }
    ]
  }'
```

## 验证权限配置

### 方法一：浏览器控制台

打开 API Key 页面，在浏览器控制台（F12）中应该看到：

```javascript
user-api-key 权限: ["query", "save", "delete"]
是否有查询权限: true
是否有新增权限: true
是否有删除权限: true
```

### 方法二：通过 API 检查

```bash
curl -X GET "http://iot.intra.zeron.ai:8848/jetlinks/authorize/me" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

在返回的 JSON 中查找 `permissions` 字段，应该包含：

```json
{
  "permissions": [
    {
      "id": "user-api-key",
      "actions": ["query", "save", "delete"]
    }
  ]
}
```

## 常见问题

### 1. 权限资源不存在

**问题**：在权限管理中搜索不到 `user-api-key`

**原因**：权限资源还未创建

**解决方法**：
1. 重启系统（权限资源会在启动时自动创建）
2. 确认 `UserApiKeyController` 类已正确加载
3. 检查后端日志，确认 `@Resource` 注解是否生效

### 2. 权限已配置但仍无权限

**问题**：已为角色分配权限，但用户仍无权限

**解决方法**：
1. 确认用户已绑定到该角色
2. 退出登录并重新登录
3. 清除浏览器缓存
4. 检查权限缓存是否已刷新

### 3. 菜单不显示

**问题**：权限已配置，但菜单不显示

**解决方法**：
1. 检查菜单配置中的 `permissions` 字段
2. 确认用户有 `user-api-key:query` 权限
3. 检查菜单的 `showPage` 配置

## 快速配置脚本（PostgreSQL）

```sql
-- =============================================
-- API Key 权限快速配置脚本
-- =============================================

-- 1. 确认权限资源存在（如果不存在，需要重启系统）
SELECT id, name FROM s_permission WHERE id = 'user-api-key';

-- 2. 查找管理员角色ID（根据实际情况修改）
DO $$
DECLARE
    role_id_var VARCHAR(64);
BEGIN
    -- 查找管理员角色
    SELECT id INTO role_id_var 
    FROM s_role 
    WHERE code = 'administrator' OR code = 'admin' OR name LIKE '%管理员%'
    LIMIT 1;
    
    IF role_id_var IS NULL THEN
        RAISE NOTICE '未找到管理员角色，请手动指定角色ID';
    ELSE
        RAISE NOTICE '找到角色ID: %', role_id_var;
        
        -- 删除旧权限（如果存在）
        DELETE FROM s_role_permission 
        WHERE role_id = role_id_var AND permission_id = 'user-api-key';
        
        -- 插入新权限
        INSERT INTO s_role_permission (id, role_id, permission_id, actions, create_time)
        VALUES (
            gen_random_uuid()::VARCHAR,
            role_id_var,
            'user-api-key',
            '["query","save","delete"]'::jsonb,
            EXTRACT(EPOCH FROM NOW()) * 1000
        );
        
        RAISE NOTICE '权限配置成功！角色ID: %, 权限: user-api-key', role_id_var;
    END IF;
END $$;

-- 3. 验证配置
SELECT 
    r.name AS role_name,
    rp.permission_id,
    rp.actions
FROM s_role_permission rp
JOIN s_role r ON r.id = rp.role_id
WHERE rp.permission_id = 'user-api-key';
```

## 注意事项

1. **权限资源自动创建**：权限资源会在系统启动时通过 `@Resource` 注解自动创建，无需手动创建
2. **权限缓存**：配置权限后，需要重新登录或等待缓存刷新
3. **菜单权限**：菜单显示需要 `user-api-key:query` 权限
4. **操作权限**：
   - 创建 API Key：需要 `user-api-key:save` 权限
   - 删除 API Key：需要 `user-api-key:delete` 权限
   - 启用/禁用：需要 `user-api-key:save` 权限

## 推荐配置

对于管理员角色，建议配置所有权限：

```json
{
  "permission": "user-api-key",
  "actions": ["query", "save", "delete"]
}
```

对于普通用户，可以只配置查询权限：

```json
{
  "permission": "user-api-key",
  "actions": ["query"]
}
```
