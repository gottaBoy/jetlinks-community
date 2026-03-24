# 并行驾驶管理模块 (Parallel Driving Manager)

## 概述

并行驾驶管理模块实现了驾驶舱-云端-车端的端到端远控通信能力，支持：
- 驾驶舱和车端的一对一绑定关系管理
- 驾驶舱到车端的控制指令路由
- 车端到驾驶舱的状态上报转发
- 权限验证和控制

## 后端模块结构

```
parallel-driving-manager/
├── pom.xml
└── src/main/java/org/jetlinks/community/parallel/driving/
    ├── configuration/
    │   └── ParallelDrivingConfiguration.java    # Spring配置类
    ├── message/
    │   └── ParallelDrivingMessageRouter.java    # 消息路由器
    ├── service/
    │   └── ParallelDrivingRelationService.java  # 关系服务
    └── web/
        └── ParallelDrivingController.java       # REST API控制器
```

## API接口

### 1. 绑定驾驶舱到车端
```
POST /api/parallel-driving/bind
参数:
  - cockpitDeviceId: 驾驶舱设备ID
  - vehicleDeviceId: 车端设备ID
```

### 2. 解绑
```
DELETE /api/parallel-driving/unbind
参数:
  - cockpitDeviceId: 驾驶舱设备ID
  - vehicleDeviceId: 车端设备ID
```

### 3. 查询驾驶舱绑定的车
```
GET /api/parallel-driving/cockpit/{cockpitId}/vehicle
返回: 已绑定的车辆ID
```

### 4. 查询车被哪个驾驶舱绑定
```
GET /api/parallel-driving/vehicle/{vehicleId}/cockpit
返回: 已绑定的驾驶舱ID
```

### 5. 检查控制权限
```
GET /api/parallel-driving/permission/check
参数:
  - cockpitDeviceId: 驾驶舱设备ID
  - vehicleDeviceId: 车端设备ID
返回: 是否有权限 (true/false)
```

## 消息路由

### 驾驶舱发送控制指令
1. 驾驶舱设备发送消息，消息头包含 `targetDeviceId`
2. `ParallelDrivingMessageRouter` 订阅 `/device/*/*/message/downstream`
3. 验证权限后，转发到目标车端

### 车端上报状态
1. 车端设备上报状态消息
2. `ParallelDrivingMessageRouter` 订阅 `/device/*/*/message/upstream`
3. 查找绑定的驾驶舱，转发状态消息

## 前端模块创建指南

前端模块应创建在：`C:\d\ideaSpace\jetlinks-ui-vue\src\modules\parallel-driving-manager-ui`

### 目录结构
```
parallel-driving-manager-ui/
├── index.ts                    # 模块入口
├── index.vue                   # 主页面
├── components/                 # 组件
│   ├── BindForm.vue           # 绑定表单
│   └── RelationList.vue       # 关系列表
└── model/                      # 数据模型
    └── api.ts                  # API接口定义
```

### 参考 device-manager-ui
可以参考 `device-manager-ui` 模块的实现方式：
- 使用 JetLinks UI 框架的组件
- 使用统一的 API 调用方式
- 遵循相同的代码风格和规范

## 使用示例

### 绑定驾驶舱和车端
```javascript
// 前端调用示例
import { parallelDrivingApi } from '@/modules/parallel-driving-manager-ui/model/api';

// 绑定
await parallelDrivingApi.bind({
  cockpitDeviceId: 'cockpit-001',
  vehicleDeviceId: 'vehicle-001'
});

// 查询绑定关系
const vehicleId = await parallelDrivingApi.getBoundVehicle('cockpit-001');
```

### 发送控制指令
```javascript
// 驾驶舱发送控制指令时，需要在消息头中添加 targetDeviceId
const message = {
  messageType: 'REMOTE_CONTROL',
  deviceId: 'cockpit-001',
  headers: {
    targetDeviceId: 'vehicle-001'  // 目标车端ID
  },
  data: {
    command: 'start',
    params: {
      speed: 60
    }
  }
};
```

## 注意事项

1. **一对一约束**：每个驾驶舱只能绑定一个车，每个车只能被一个驾驶舱绑定
2. **权限验证**：每次消息路由前都会验证权限
3. **设备离线**：如果设备离线，消息会缓存，设备上线后自动接收
4. **关系存储**：使用关系配置功能（`s_object_related` 表）存储绑定关系

## 开发说明

### 编译和运行
```bash
# 编译模块
mvn clean install -pl jetlinks-manager/parallel-driving-manager

# 运行项目
mvn spring-boot:run -pl jetlinks-standalone
```

### 测试
```bash
# 运行单元测试
mvn test -pl jetlinks-manager/parallel-driving-manager
```

## 相关文档

- [远控能力设计方案](../docs/remote-control-design.md)
- [一对一关系约束实现方案](../docs/one-to-one-relation-constraint.md)
- [关系配置功能说明](../docs/relation-configuration-feature.md)

