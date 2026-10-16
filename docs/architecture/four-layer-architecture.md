# DUBI 四层服务端架构

本文记录 DUBI 服务端目前确定的四层结构，作为 Runtime 底座、Engine、Module 和 Gateway 之间的共同设计约定。

## 1. 总体结构

```text
                    External Clients
                 Redis / S3 / HTTP / SDK
                              │
                          Gateway
                              │
                           Module
                              │
                           Engine
                              │
                           Runtime
```

依赖方向固定为：

```text
Gateway → Module → Engine → Runtime
```

一个 DUBI 节点仍然是一个进程。Runtime、Engine、Module 和 Gateway 都在同一个 `dubi server` 进程中运行；这四层是职责边界，不是四个独立服务。

## 2. Runtime：分布式底座

Runtime 是 DUBI 的统一运行底座，负责所有跨节点、节点生命周期和数据分布相关的基础能力。

Runtime 提供：

- Cluster、Node、Region、Store、Volume 身份与生命周期
- Root Group、Raft Group 和状态机生命周期
- Tenant、Dataset、Keyspace、Range 路由
- Owner、Range Epoch 和 Fence
- Placement、数据目标、迁移、Rebalance、Repair
- 节点成员管理、故障隔离、恢复和 Snapshot
- 本地持久化、Pack、资源预算和后台任务
- Runtime 内部 RPC 与节点通信
- 控制面通信和业务数据通道
- 面向 Engine 的稳定 Runtime Contract

Runtime 不理解 Config、Cache、Queue 或 Search 的业务语义，也不负责解释 Module 的业务载荷。Module 只需要声明自己的数据类型、Dataset 和 RoutingKey，Runtime 负责把请求送到正确的 Range、Group 和 Node。

当前仓库中的 `internal/platform` 和 `internal/platform/node.Runtime`，就是这一层的主要实现范围。

### 2.1 Runtime 的通信边界

Runtime 内部可以区分两类通信：

```text
Control Plane
  Raft、节点管理、同步、复制、成员变更

Data Plane
  Engine 和 Module 的高流量数据请求
```

两者可以使用不同的端口、连接或资源池，也可以由 Runtime 自动多路复用。端口、节点地址和连接生命周期由 Runtime 自动管理，Engine 和运维不需要配置这些细节。

Transport 是 Runtime 提供的基础能力，不单独作为顶层 Transport Engine。Engine 通过 Runtime Contract 使用通信能力，不直接依赖端口、连接池、Raft RPC 或底层网络实现。

## 3. Engine：可复用的基础机制

Engine 是构建 Module 的通用机制。Engine 直接使用 Runtime 提供的标准接口，但不关心 Runtime 内部使用单 Raft、Multi-Raft、何种存储或何种网络实现。

Engine 的共同规则：

1. 通过 Runtime Contract 初始化和运行。
2. 不直接实现节点发现、数据分布、Raft、Placement、故障恢复和节点间通信。
3. 不包含某个具体 Module 的业务语义。
4. 可以被多个 Module 复用。
5. 需要的能力由 Engine 声明，具体实现由 Runtime 提供。

可能的 Engine 包括：

| Engine | 提供的通用机制 |
| --- | --- |
| State Engine | 状态读写、事务、版本、CAS、Watch 等 |
| Message Engine | 消息封装、关联 ID、目标、流和通用消息处理 |
| Schema Engine | 结构定义、默认值、类型检查和版本校验 |
| Log Engine | 追加、Offset、保留、重放 |
| Object Engine | Blob/Object、分块、范围读写、Multipart |
| Index Engine | 倒排索引、查询、索引任务和水位 |
| TimeSeries Engine | 时间序列写入、聚合、降采样和保留 |
| Scheduler Engine | 延迟任务、定时任务和后台调度 |
| Identity Engine | 身份、签名、加密和凭据相关机制 |

### 3.1 State Engine

State Engine 不负责数据分布。它只提供 Module 使用的状态抽象，并通过 Runtime Contract 完成实际读写。

```text
Config / Cache / Session Module
  ↓
State Engine
  ↓
Runtime State API
  ↓
本地执行或远程执行
  ↓
Runtime 的 Route / Group / Raft / Placement / Recovery
```

State Engine 不需要知道：

- 数据在哪个 Node
- Owner Group 的 Leader 是谁
- 是否需要跨节点转发
- 使用哪个端口或 Transport
- 底层是 Pebble、Pack 还是其他存储

它只需要使用类似以下的 Runtime 能力：

```text
Resolve(keyspace, routingKey)
Read(routeTarget, payload)
Mutate(routeTarget, payload)
```

`OpenView`、`WatchState` 等更高阶能力暂不进入第一版 Contract，待对应 Engine 的一致性和生命周期语义确定后再分别扩展。

当前仓库的 `enginestate`、`routedstate` 和 `engineruntime` 已经体现了这一边界：Runtime 负责 Route Target、Group、Fence、ReadIndex 和状态机执行边界，能力处理自己的状态语义。

### 3.2 Runtime Contract

Runtime 对 Engine 暴露的稳定边界位于 `internal/platform/runtimecontract`。Engine 只依赖以下三类操作：

```text
Resolve(Keyspace, RoutingKey) -> RouteTarget
Mutate(RequestID, Keyspace, RoutingKey, opaque payload) -> MutationResult
Read(Keyspace, RoutingKey, opaque payload) -> ReadResult
```

Contract 的实现负责把逻辑地址转换为当前 Route Target，并在需要时执行本地处理或跨 Node 转发。Engine 不构造 Route Target，不直接编码 Routed Command，也不直接调用 Group RPC。

Contract 返回的是能力中立的结果：确定性拒绝通过 `Rejection` 表达，无法确认执行结果才返回错误；`AppliedIndex` 和 `ConfirmedIndex` 只作为 Runtime 的确认位置，不被 Engine 当作业务版本或物理持有证明。

当前 `runtimecontract.Dispatcher` 是可测试的参考实现，使用注入的 RouteResolver 和 StateExecutor 连接现有 Runtime 组件。正式 Node 的本地执行、远端转发、物理多数和 Geo 确认仍由 Runtime 适配器提供，不能由 Engine 自行实现。

## 4. Module：完整功能能力

Module 是面向用户的完整服务端功能，由一个或多个 Engine 组合而成。Module 拥有自己的数据格式、业务规则和公开 API，但不重复实现 Runtime 的分布式机制。

初步组合关系如下：

| Module | 主要组成 |
| --- | --- |
| Config | State + Schema，变更通知可使用 Message |
| Cache | State + TTL/淘汰策略 |
| Session | State + Identity |
| Token | Identity + State |
| Bus | Message |
| Queue | Message + Log + Scheduler + State |
| PubSub | Message，可选 Log 用于持久化与重放 |
| File | Object + State，可选 Index |
| Search | Index + State/Object |
| Log | Log + Index/Object |
| Metric | TimeSeries + Index |
| Trace | Log/Index + State |

### 4.2 多 Engine 组合与数据权威

一个 Module 可以同时使用多个 Engine。Module 负责组合这些 Engine，并定义它们之间的业务关系、可见性和一致性语义；Engine 只提供各自的通用机制。

例如全文搜索 Module 可以组合：

```text
原始文档 ─> Object Engine
文档元数据、版本和状态 ─> State Engine
倒排索引和查询 ─> Index Engine
```

日志 Module 可以组合：

```text
日志追加、Offset、保留和重放 ─> Log Engine
日志内容或大字段 ─> Object Engine
日志元数据和查询状态 ─> State Engine
全文检索 ─> Index Engine
```

在这类组合中，Module 必须明确：

- 哪一份数据是原始权威数据，哪一份是可重建的派生数据。
- 原始数据写入和索引更新的先后关系。
- 索引尚未完成时，查询是等待、返回不完整结果，还是报告索引延迟。
- 原始数据删除、保留期变化或覆盖后，索引如何更新和回收。
- 如何通过水位、版本或重建任务保证索引不会越过原始数据的可见边界。

通常，Index Engine 中的索引属于派生数据，不能替代原始文档或日志内容。索引损坏或丢失时，Module 应能根据原始数据重新构建索引；Runtime 只负责这些数据的路由、持有、迁移和恢复，不解释索引或日志的业务语义。

Module 负责：

- 业务数据结构和编码
- 业务语义与错误
- Dataset 和 RoutingKey 的定义
- TTL、保留、淘汰、索引和重放规则
- 对外 API 和权限语义

Runtime 负责：

- 路由和所有权
- 节点与分布
- 共识、数据持有和多数确认边界
- 迁移、修复和恢复

### 4.1 Engine 与 Module 的关系

架构、公开 API 和新文档统一使用 `Engine`，不再引入另一种独立的能力层概念。

```text
Engine  = 可复用的通用机制
Module  = 面向用户的完整功能，由一个或多个 Engine 组合而成
```

例如，`State Engine` 可以同时被 Config、Cache、Session 等 Module 使用；Queue 则可以组合 Message、Log、Scheduler 和 State 等多个 Engine。

当前代码中的 `Engine`、`EngineID`、`engineruntime` 和 `Engine Handler` 已是统一后的正式内部命名。后续新增代码、持久状态和内部协议继续使用这些名称，不再引入另一套同义词。

当前 Keyspace 中的 `EngineID` 是 Engine 的路由命名空间。Module 不作为新的 Runtime 路由维度；一个 Module 使用多个 Engine 时，由 Module 为各 Engine 选择对应 Dataset，并负责它们之间的业务关联和一致性。

当前设计中的 `Record`、`Cache`、`Message`、`Search`、`File`、`Log`、`Metric` 和 `Trace` 是现有实现单元。它们未来可以分别成为 Engine、Module，或拆分为 Module 加多个 Engine，不能简单假设一一对应。

## 5. Gateway：外部协议接入层

Gateway 是面向外部客户端的协议适配层。它把 Redis、S3、HTTP、CLI 或其他客户端协议转换成 Module 能理解的请求。

```text
Redis Client → Redis Gateway → Cache / Queue / PubSub Module
S3 Client    → S3 Gateway    → File Module
HTTP Client  → HTTP Gateway  → 对应 Module
```

Gateway 负责：

- 外部协议解析和响应编码
- 外部连接和认证入口
- 协议语义到 Module API 的转换

Gateway 不负责：

- Raft
- 数据分布
- 节点选择
- 数据持有与多数确认
- Module 内部数据格式

Gateway 不是微服务，也不是独立部署单元；它是 DUBI 节点进程内的接入层。

## 6. 依赖和边界总结

```text
Runtime
  提供分布式底座和标准运行契约

Engine
  基于 Runtime 的稳定接口，提供可复用机制

Module
  组合 Engine，提供完整功能和业务语义

Gateway
  把外部协议接入 Module
```

必须保持的边界：

- Runtime 不依赖任何 Module。
- Engine 不直接依赖 Runtime 的内部实现，只依赖 Runtime Contract。
- Module 不重复实现 Raft、Placement、数据迁移和节点恢复。
- Gateway 不绕过 Module 直接访问底层存储。
- Transport 属于 Runtime 基础能力，不作为额外的顶层 Engine。
- State Engine 不负责数据分布，只通过 Runtime 使用分布式状态能力。
- `Engine` 不是独立于 Engine 和 Module 之外的第五层；它只是在完成受控改名之前保留的历史代码术语。
- 一个 Engine 可以被多个 Module 复用，一个 Module 可以组合多个 Engine。

## 7. 一句话定义

```text
Runtime 决定数据如何在集群中运行和分布。
Engine 提供可复用的基础机制。
Module 提供具体的数据能力。
Gateway 提供外部协议接入。
```
