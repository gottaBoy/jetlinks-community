# JetLinks 平行驾驶集群部署

## 快速启动

```bash
cd jetlinks-community/docker/cluster
docker compose -f docker-compose-cluster.yml up -d
```

## 验证集群

### 1. 检查 Scalecube gossip 互联

```bash
# 查看各节点日志中的集群成员发现
docker logs jetlinks-node-1 2>&1 | grep -i "scalecube\|cluster\|member"
docker logs jetlinks-node-2 2>&1 | grep -i "scalecube\|cluster\|member"
docker logs jetlinks-node-3 2>&1 | grep -i "scalecube\|cluster\|member"
```

### 2. 验证 EventBus 集群订阅

现有代码使用 `.broker().local()` 构建订阅，等价于 `features(Feature.local, Feature.broker)`：
- `Feature.local`：本节点发布的消息会投递给订阅方
- `Feature.broker`：其他节点发布的消息也会通过 Scalecube RPC 路由过来

验证方法：
1. 设备 A 连接到 Node-1
2. 在 Node-2 上通过 REST API 发送控制指令
3. 确认 Node-1 的 MessageRouter 收到消息并转发给设备 A

```bash
# Node-1 API
curl http://localhost:8848/api/v1/device/test-vehicle/message -X POST -H 'Content-Type: application/json' -d '{...}'
```

### 3. 验证 DeviceOperator.messageSender() 跨节点路由

JetLinks 的 `DeviceOperator.messageSender().send()` 在集群模式下是集群感知的：
- 通过 `DeviceRegistry` 查询设备连接在哪个节点
- 自动通过 Scalecube RPC 路由消息到目标节点
- 不需要业务代码手动实现 RPC 转发

### 4. IntelliJ 2 节点本地调试

在 IntelliJ 中配置两个 Run Configuration：

**Node-1:**
```
VM Options: -Dspring.profiles.active=default,dev,local,cluster
Environment: JETLINKS_SERVER_ID=node-1;CLUSTER_HOST=127.0.0.1;CLUSTER_PORT=13800;CLUSTER_RPC_PORT=13900;CLUSTER_SEED_1=127.0.0.1:13800;CLUSTER_SEED_2=127.0.0.1:13801
Program Arguments: --server.port=8848
```

**Node-2:**
```
VM Options: -Dspring.profiles.active=default,dev,local,cluster
Environment: JETLINKS_SERVER_ID=node-2;CLUSTER_HOST=127.0.0.1;CLUSTER_PORT=13801;CLUSTER_RPC_PORT=13901;CLUSTER_SEED_1=127.0.0.1:13800;CLUSTER_SEED_2=127.0.0.1:13801
Program Arguments: --server.port=8849
```

## 关键技术发现

### EventBus `.broker().local()` 语义

```java
// 以下两种写法完全等价：
// 写法一（现有代码）
.broker().local()
// 写法二（显式）
.features(Subscription.Feature.local, Subscription.Feature.broker)
```

`Builder.broker()` 和 `Builder.local()` 都是调用 `features()` 追加，非覆盖。
因此现有平行驾驶代码的订阅在集群模式下已经具备跨节点接收能力，
只需正确配置 `application-cluster.yml` 激活 Scalecube 集群即可。

### DeviceOperator 路由行为

`DeviceOperator.messageSender().send()` 内部通过以下链路实现集群路由：
1. `DeviceRegistry.getDevice(deviceId)` → 获取 `DeviceOperator`
2. `DeviceOperator` 关联了设备当前连接的节点信息（Scalecube ServiceInfo）
3. `messageSender().send()` 自动通过 RSocket RPC 将消息路由到设备所在节点
4. 目标节点的 Vert.x TCP/MQTT Server 将消息下发给设备

**结论：不需要在业务代码中手动实现跨节点 RPC。**
