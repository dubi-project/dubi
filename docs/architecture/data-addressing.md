# 数据寻址、Module 隔离与 Range / Pack

状态：2026-11-04 代码已迁移到 Tenant + Module + Engine；独立 Dataset 目录、命令和 DataService 入口已移除。VPS 回归证据见验收记录。此次是开发格式切换，旧集群和旧备份没有被原地转换。

## 1. 确定的概念

- **Tenant**：数据归属、授权与资源计量边界。
- **Module**：功能与数据隔离边界。同一 Tenant 的不同 Module 即使使用同一个 Engine，也不共用同名 Key 的数据。
- **Engine**：可复用的存储机制及数据语义。复用 Engine 实现不等于共享业务数据。
- **Keyspace**：完整内部键组成的逻辑地址空间，不是独立资源，没有创建命令、独立 ID 或用户配置。
- **Range**：键空间中的一个半开区间 `[start,end)` 及其对应的数据，是 Runtime 管理路由、所有权和数据分布的单位。
- **Pack**：本机聚合保存记录的物理文件，减少小文件，不承担 Range 的逻辑边界或集群所有权。
- **Volume**：节点存放 Pack 等持久资源的本地卷。

取消 Dataset 作为独立底座概念、资源和使用前置条件。不能简单把 Dataset 改名为另一个需要创建的容器。Topic、搜索 Index 等业务对象仍可由 Module 定义，它们不是通用存储管理层。

## 2. 完整地址与可信身份

逻辑地址表示为：

```text
Tenant + Module + Engine + Key
```

例如：

```text
Tenant A / Config  / State / user-001
Tenant A / Session / State / user-001
Tenant B / Config  / State / user-001
```

这三条地址相互独立。同一 Tenant 内的多个应用访问同一个 Module，使用相同 Engine 和 Key，则访问同一份数据，但仍须通过对应业务授权。不建立 Project 资源，不因应用名称自动分库。

上述斜杠仅为示意，不是文件路径或规范字节编码。实际编码必须版本化、无歧义，并保持所需的键顺序；不能裸拼接可能含分隔符的字符串。身份使用稳定内部标识，显示名称变化不能改变存储地址。

- Tenant 来自认证与授权后的可信上下文。具备跨 Tenant 数据权限的管理员仍须经过目标 Tenant 授权。
- Module 身份由 API 路由及服务端装配固定绑定。
- Engine 身份由 Module 的服务端依赖绑定确定。
- 客户端提供业务 Key、参数和载荷，不能用请求体、Header 或查询参数覆盖 ModuleID、EngineID，也不能构造内部地址。
- Runtime 可记录 Module 用于审计和计量，但不理解它的业务规则。

默认规则是不同 Module 数据隔离，不为它们提供任意选择另一个 Module 存储范围的公共 API。跨模块组合业务若确有需求，应调用明确授权的业务接口，不绕过隔离读取对方底层 Key。

## 3. 调用边界

```text
Config API
    ↓ 认证 Tenant，固定调用 Config
Config Module
    ↓ 使用已绑定的 State Engine 接口
State Engine
    ↓ 存储操作、业务 Key / 编码键、稳定 RequestID
Runtime
    ↓ 编码完整地址，定位 Range，执行路由与持久化
节点上的状态存储 / Volume 中的 Pack
```

Module 只面向 Engine API，不创建 Dataset、不拼 Keyspace、不选择 Range、Group、Node 或 Pack。Module/Engine 的稳定身份在装配时绑定，不能要求 Module 每次调用重复填写自己的身份。Runtime 负责在读取和写入路径中保留并校验完整归属，不能只在 Gateway 检查一次，内部就丢失隔离信息。

Engine 定义编码、状态版本、CAS、索引和访问模式；Runtime 管理完整地址、分布、共识和持久资源。一个 Module 内部的多种记录可使用稳定的类型标记和业务键编码，不增加一个用户要管理的容器。索引与原文等多 Engine 组合不自动获得跨 Engine 事务。

所需内部 Range 和路由由 Runtime 自动建立与维护。首次写入或模块激活按受控流程准备，支持并发、重试和崩溃恢复；普通读取不存在的数据不能偷偷创建租户或存储资源。不能为每个 Key 建一个 Range，也不能将 Module 绑定变成每次请求新增一次 Root 确认。身份隔离本身不要求每个 Module 独占 Raft Group、Pack、Volume 或节点。

## 4. Range 与 Pack 的关系

Range 是逻辑区间；Pack 存放属于这些区间的数据记录。二者没有一一对应关系：一个 Pack 可以混放多个 Range 的记录，一个 Range 的记录可以分散到多个 Pack 和 Volume。

```text
节点元数据目录（--store）：身份 / 共识状态 / 索引 / 卷登记

Volume A
├─ Pack 01：Range A 的部分记录 + Range B 的部分记录
└─ Pack 02：Range A 的其他记录 + Range C 的部分记录

Volume B
└─ Pack 03：Range B 的其他记录
```

一个 Node 管理多个 Volume；未指定初始卷时使用 `<store>/volume`，显式指定时仅登记指定卷。`StoreID` 是节点存储实例的内部校验身份，不增加独立 Store 放置层。

不同节点可以用完全不同的 Pack 布局存放同一份逻辑数据。权威数据身份和引用不能包含某节点的 Pack 路径或 Offset；本地位置由节点自己的索引管理。状态机、索引等内部状态可使用对应状态存储，不要求所有底层文件都改成 Pack。

- **Range 迁移**：按逻辑边界复制必要记录，目标节点写入自己的 Pack。经过复制、校验、所有权 Fence 和受控切换后才完成；不必搬走混有其他 Range 数据的整个源 Pack。
- **Pack Compaction / Volume 均衡**：重写记录并安全更新本地位置索引，不改变记录的逻辑归属，也不因换 Pack 改变 Range 所有权。
- **旧字节回收**：只有新位置已安全发布、相关引用与备份/恢复保留约束都允许时才回收，不能在复制一开始就删源数据。

Range 可以按需要拆分、迁移或合并是目标能力，不意味着现有实现已全部支持。当前 `rootstate.RouteKeyspace` 按 Tenant/Module/Engine 查找初始完整 Range，尚不是通用的多区间目录查找。后续实现要保留无重叠/无空洞、代际栅栏、并发更新与恢复约束，不能只替换名称或直接改描述符。

## 5. 当前实现与格式边界

- `enginedata.Service.Bind(module, engine).ForTenant(tenant)` 提供已绑定的不可变数据入口；调用方只传 Key、Body、RequestID 和 Engine 的 Publication 编码器。Module/Engine 身份存于私有字段，不由请求携带。`ForTenant` 接收已完成授权的 Tenant，本身不是认证器。
- `runtimecontract.Bind(...).ForTenant(...)` 为状态 Engine 的参考适配提供同样边界；底层 Contract 的显式 Keyspace/Route 操作保留给 Runtime 适配器，不能直接传给 Module。正式不可变数据路径已接入；通用状态 Dispatcher 仍是参考适配，具体 Engine 开发须接通真实后端。
- 首次写入由 Runtime 提交 `PrepareRange`，Root 只存 Pending Range 和反向索引，Owner 安装 Fence 后发布 Active。`RangeSeed` 仅为内部命令载荷，不是新容器。并发初始化使用确定的 RangeID 和可重试命令，失败后以持久 Pending 状态继续；读取和绑定不创建资源。首笔写入会通过与后台协调器相同的 Root 命令帮助发布缺失的初始 Region placement，并确认其持久结果，避免空等周期任务；尚未完成区域组建时仍由后台推进。已有 placement 不由此路径刷新或扩张。并发调用即使已看到 Active Range，也不能跳过该准备阶段，提交成功本身不算 placement 已生效。等待受请求取消及 30 秒上限约束。活动路径本地检查初始 Range 和 placement，不因绑定额外增加 Root 往返。
- Root 目录、Owner Fence、状态键、不可变描述、复制和 Repair 全部携带 Module。内部键前缀采用 Tenant/Module/Engine，标识符校验拒绝分隔符；业务键保留字节语义。RecordID 使用版本化域和长度前缀编码。
- 绑定入口将调用方 RequestID 与 Tenant/Module/Engine 一起派生内部请求 ID。不同范围可使用同一个请求 ID；同一范围重试必须保持业务键和载荷一致，否则仍按原去重规则拒绝。
- 开发探针改为 `dubi tenant create <tenant>`、`dubi record put <tenant> <key> <value>`、`dubi record get <tenant> <key>`。首次写入自动准备 Range；Tenant 仍须显式登记。探针固定绑定 record Module/Engine，仍是 Node 认证的内部管理入口，不是应用 Tenant API。
- DataService 删除 CreateDataset RPC，保留旧 dataset 字段号和名称为 reserved，并拒绝 Put/Get 的未知字段，防止旧客户端或伪造 Module/Engine 参数被静默忽略。

| 格式 | 当前版本 |
| --- | --- |
| 内部协议最低/当前版本 | 2 / 2 |
| Root 状态、命令和快照 | 2 |
| Routed Command | 2 |
| Engine 状态和快照 | 8 |
| RecordID / Pack 记录键域 | v2 |
| Records 本地索引 / 可移植索引快照 | 2 / 2 |
| Pack 容器 / Catalog | 2 / 2（未改变） |

备份仍复制底座的 Root/Engine 快照、Pack 和记录索引，不解释 Module 数据。备份索引工件标记为 `DUBI_RECORD_INDEX_V2`；恢复保留逻辑归属，重新映射 Node/Store/Volume 等物理身份。

旧 Dataset 不能可靠推导正式 Module。因此新版拒绝非空无版本标记的旧 Root、旧 Engine 状态、旧索引和不兼容协议；不能删除格式标记或改版本号绕过。旧数据迁移工具和旧新版本混跑均未提供。初轮回归使用 VPS 隔离目录和全新身份；随后按用户授权，九节点验收集群清空旧 Store，以全新身份部署新版。具体部署与验收结果见[迁移验证记录](../development/module-addressing-verification.md)，不将旧版六小时测试计作新版验收。

当前自动管理的是每个绑定范围的初始完整 Range。通用多区间拆分/合并、公开 Tenant 业务 API 和各业务 Engine/Module 不在这次迁移中实现。

## 6. 参考依据

CRDB 的分布层将有序键空间切为 Range，调用者不管理每个 Range 的物理位置；这不意味着没有内部键编码与逻辑归属。[CRDB Distribution Layer](https://docs.cockroachlabs.com/docs/stable/architecture/distribution-layer)

Byfile 实际代码的 `internal/rangemeta`、`internal/rangerouter` 和 `internal/nodeserver/placement_registration.go` 提供默认 Namespace 自动建立、区域 Cell 选择和 Path Range 路由的参考。Dubi 借鉴其自动管理和栅栏机制，不复制文件专用 Namespace/Cell 层级。
