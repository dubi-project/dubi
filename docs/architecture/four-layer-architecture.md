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

- Cluster、Node、Region、Volume 身份与生命周期；节点元数据目录及内部 StoreID 校验
- Root Group、Raft Group 和状态机生命周期
- 可信 Tenant/Module/Engine 地址编码与自动 Range 路由（Keyspace 仅是内部键空间概念，不创建 Dataset）
- Owner、Range Epoch 和 Fence
- Placement、数据目标、迁移、Rebalance、Repair
- 节点成员管理、故障隔离、恢复和 Snapshot
- 本地持久化、Pack、资源预算和后台任务
- Runtime 内部 RPC 与节点通信
- 控制面通信和业务数据通道
- 面向 Engine 的稳定 Runtime Contract

Runtime 不理解 Config、Cache、Queue 或 Search 的业务语义，也不负责解释 Module 的业务载荷。Module 只使用服务端绑定的 Engine API，Engine 提供数据操作和键编码，Runtime 自动绑定完整地址并将请求送往正确的 Range、Group 和 Node。Module 不管理 Dataset、Keyspace 或 Range。详见 [数据寻址与隔离](data-addressing.md)。

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

优先围绕以下四种存储机制随实际 Module 需求落地；这不是要求预先实现全部 Engine，也不是当前实现完成清单：

| Engine | 提供的通用机制 |
| --- | --- |
| State Engine | 状态读写、事务、版本、CAS、Watch 等 |
| Record Engine | 追加、Offset、保留、重放 |
| Blob Engine | 大字节内容、分块、范围读写、Multipart |
| Index Engine | 倒排索引、查询、索引任务和水位 |

核心 Engine 收敛为 State、Record、Blob、Index。TimeSeries、Trace 与 Bus 均为 Module，组合既有 Engine；不设独立 TimeSeries、Trace 或 Message Engine。时序模块定义时间点、乱序/更正、窗口聚合和保留语义，Record 保存记录，State 管理元数据和必要聚合状态，Index 按需提供时间/标签访问机制。追加 Offset 不等于事件时间，时间范围查询需要明确的访问路径。当前规划没有必须增加的第五类 Engine，新增机制应先评估能否归入四类。

Schema 校验、签名/加密、业务定时调度先按普通共享库或 Module 内部逻辑使用，不预设独立 EngineID、存储范围或生命周期。节点 PKI、成员认证以及 Repair/Rebalance 后台调度始终属于 Runtime，不能依赖业务 Identity/Scheduler Engine。

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

目标是使用已绑定身份的 Runtime 接口，下例仅表示调用语义，不是已实现的 Go 签名：

```text
Read(key, payload)
Mutate(requestID, key, payload)
```

`OpenView`、`WatchState` 等更高阶能力暂不进入第一版 Contract，待对应 Engine 的一致性和生命周期语义确定后再分别扩展。

当前仓库的 `enginestate`、`routedstate` 和 `engineruntime` 已经体现了这一边界：Runtime 负责 Route Target、Group、Fence、ReadIndex 和状态机执行边界，能力处理自己的状态语义。

### 3.2 Runtime Contract

Runtime 对 Engine 的当前参考契约位于 `internal/platform/runtimecontract`，底层适配操作如下，仅供 Runtime 装配；Module 使用 Engine 的绑定接口：

```text
Resolve(Keyspace, RoutingKey) -> RouteTarget
Mutate(RequestID, Keyspace, RoutingKey, opaque payload) -> MutationResult
Read(Keyspace, RoutingKey, opaque payload) -> ReadResult
```

`runtimecontract.Bind(...).ForTenant(...)` 和 `enginedata.Service.Bind(...).ForTenant(...)` 固定调用范围，首次写入自动准备初始 Range，读取不创建资源。调用方请求 ID 也按绑定范围隔离。

Contract 的实现负责把逻辑地址转换为当前 Route Target，并在需要时执行本地处理或跨 Node 转发。Engine 不构造 Route Target，不直接编码 Routed Command，也不直接调用 Group RPC。

Contract 返回的是能力中立的结果：确定性拒绝通过 `Rejection` 表达，无法确认执行结果才返回错误；`AppliedIndex` 和 `ConfirmedIndex` 只作为 Runtime 的确认位置，不被 Engine 当作业务版本或物理持有证明。

当前 `runtimecontract.Dispatcher` 是可测试的参考实现，使用注入的 RouteResolver 和 StateExecutor 连接现有 Runtime 组件。正式 Node 的本地执行、远端转发、物理多数和 Geo 确认仍由 Runtime 适配器提供，不能由 Engine 自行实现。

State 首版实际使用已绑定的 `enginedata.Write/Read/Query`：Query 在 Owner 一致性切点读取业务元数据/回执，Write/Read 复用真实 Pack 与物理确认。Config 已通过这条链路接入 HTTP；具体实现、限制和验证见[开发规范第 13 节](../development/engine-module-gateway.md#13-state--config-首版实现)，不表示通用 Dispatcher 已实现全部执行能力。

## 4. Module：完整功能能力

Module 是面向用户的完整服务端功能，由一个或多个 Engine 组合而成。Module 拥有自己的数据格式、业务规则和公开 API，但不重复实现 Runtime 的分布式机制。

业务组合方向如下；普通库和业务逻辑不因出现在组合中就成为独立 Engine：

| Module | 主要组成 |
| --- | --- |
| Config | State + 普通 Schema 校验；通知能力按实际需求扩展 |
| Cache | State + TTL/淘汰策略 |
| Session | State + 会话认证逻辑 |
| Token | State + 签名/加密库 |
| Bus | 统一提供 req/reply、pub/sub 和 queue；需要持久化、确认及重放的部分组合 Record + State |
| File | Blob + State，可选 Index |
| Search | Index + State，小原文可随状态保存，大原文按需 Blob |
| Log | Record + State，检索用 Index，大字段按需 Blob |
| 时序数据（TimeSeries，含 Metric 场景） | 明确后续交付；Record + State，按需 Index，提供序列、标签、时间范围查询及聚合 |
| Trace | 明确后续交付；Record + State，属性检索用 Index，大载荷按需 Blob |

Bus 是 DUBI 内置的统一消息 Module。req/reply、pub/sub、queue 是 Bus 内的业务能力，不默认再注册独立的 Event、PubSub 或 Queue Module。只有将来出现 Bus 无法合理覆盖的独立需求时才考虑拆分；如果 Bus 已覆盖，就不建立重复模块，也不因此预设 Message Engine。

Bus 的节点间传输复用 Runtime 通信和资源治理，业务消息不能挤占 Raft 的控制通道；请求路由、订阅、消费组、确认及重投属于 Bus 语义。即时请求/通知不要求逐条持久化；可靠事件和队列通过 Record/State 保存消息及必要消费状态。不同能力的成功响应分别说明是已提交、已持久化还是已处理，不能混为一谈。

`dubi-ex` 中可选的 event、queue 等扩展属于应用侧外接服务适配，与 DUBI 服务端的 Module 划分不是一一对应关系。项目可按需使用外部事件/队列服务；这些扩展及外部服务不是 DUBI 内置 Bus 的部署依赖，也不要求所有项目采用同一种后端。

### 4.2 多 Engine 组合与数据权威

一个 Module 可以同时使用多个 Engine。Module 负责组合这些 Engine，并定义它们之间的业务关系、可见性和一致性语义；Engine 只提供各自的通用机制。

例如全文搜索 Module 可以组合：

```text
原始文档 ─> Blob Engine
文档元数据、版本和状态 ─> State Engine
倒排索引和查询 ─> Index Engine
```

这里 State 保存文档版本、删除状态、原文引用和必要构建状态，不承担倒排索引或全文匹配。小文档首版可直接保存在受限的 State 记录中，按需要再使用 Blob。Search Module 定义公开文档/查询语义并编排原文提交、版本对应的索引更新与结果读取；Index 提供通用文本分析、索引和查询机制。具体首版流程见[开发规划的全文搜索分工](../development/engine-module-gateway.md#127-全文搜索的-engine--module-分工)。

日志 Module 可以组合：

```text
日志追加、Offset、保留和重放 ─> Record Engine
日志内容或大字段 ─> Blob Engine
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
- 业务 Key、记录类型和 Engine 操作的组合；不管理底座路由容器
- TTL、保留、淘汰、索引和重放规则
- 对外 API 和权限语义

Runtime 负责：

- 路由和所有权
- 节点与分布
- 共识、数据持有和多数确认边界
- 迁移、修复和恢复

物理字节复制、持有校验和确认由 Runtime 完成，不能留给每个 Engine/Module 重做。Module 重建业务派生数据与 Runtime 修复丢失/损坏的持久数据是不同职责。Engine 可提供确定性的格式与语义校验，但不能自行选择来源节点、组织多数确认或驱动成员变更。

### 4.1 Engine 与 Module 的关系

架构、公开 API 和新文档统一使用 `Engine`，不再引入另一种独立的能力层概念。

```text
Engine  = 可复用的通用机制
Module  = 面向用户的完整功能，由一个或多个 Engine 组合而成
```

例如，`State Engine` 可以同时被 Config、Cache、Session 等 Module 使用；Bus 的持久消息和队列能力可组合 Record 和 State，投递与延迟调度先由业务逻辑实现；是否提取新 Engine 取决于实际复用需求。

当前代码中的 `Engine`、`EngineID`、`engineruntime` 和 `Engine Handler` 已是统一后的正式内部命名。后续新增代码、持久状态和内部协议继续使用这些名称，不再引入另一套同义词。

目标完整地址为 `Tenant + Module + Engine + Key`：不同 Module 复用同一个 Engine 仍然数据隔离。Module/Engine 身份由服务端装配固定，Tenant 来自认证授权上下文，不能由客户端覆盖。取消独立 Dataset；Module 只组合 Engine API 并定义业务关联，Runtime 自动编码地址、建立和维护 Range。代码已使用新的地址组合；旧 Dataset 持久格式没有自动转换，缺少映射时明确拒绝打开。

当前 `internal/engine/record` 是开发验证实现；cache/message/search/file/log/metric/trace 等旧占位目录不代表已实现的 Engine。业务功能按 Module 落地，复用实际需要的 Engine，不按旧目录名称建立一一对应关系。

## 5. Gateway：外部协议接入层

Gateway 是面向外部客户端的协议适配层。它把 Redis、S3、HTTP、CLI 或其他客户端协议转换成 Module 能理解的请求。

```text
Redis Client → Redis Gateway → Cache / Bus Module（按支持的命令映射）
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

内置 HTTP Gateway 统一承载管理 API、业务 API、监控与健康检查，以及嵌入服务端的 Web UI。业务请求遵守 Gateway → Module → Engine → Runtime；管理请求调用 Runtime 的受控管理服务，监控读取只读观察，静态 UI 是 API 的客户端，不增加架构层。管理入口不需要虚构业务 Module，也不得直接操作底层存储。具体边界、现状和实施顺序见 [HTTP Gateway 与 Web UI](http-gateway.md)。

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
- API 是公开契约，Web UI 是客户端，都不增加第五层服务端架构。
- 一个 Engine 可以被多个 Module 复用，一个 Module 可以组合多个 Engine。

## 7. 一句话定义

```text
Runtime 决定数据如何在集群中运行和分布。
Engine 提供可复用的基础机制。
Module 提供具体的数据能力。
Gateway 提供外部协议接入。
```
