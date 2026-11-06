# 仓库目录与依赖方向

```text
dubi/
├── cmd/dubi/                  # 唯一二进制入口
├── api/v1/                    # 公开契约的源文件；生成物不与手写实现混放
├── internal/
│   ├── bootstrap/             # init、bootstrap、join 等首次运行编排
│   ├── command/               # CLI 子命令适配；不放领域逻辑
│   ├── server/                # 进程装配、监听器和生命周期
│   ├── platform/              # 所有 Engine 共用的分布式底座
│   │   ├── auth/              # Tenant 身份、应用凭据、授权与审计边界
│   │   ├── cluster/           # 集群、Tenant 目录、成员与全局控制面
│   │   ├── config/            # 配置加载、校验和动态配置投影
│   │   ├── identity/          # NodeID、CSR/enrollment、证书、PKI、TLS 身份
│   │   ├── node/              # Node 加入、Drain、成员移除、退役与身份 Fence
│   │   ├── observability/     # 状态、指标、日志、诊断基础设施
│   │   ├── placement/         # 跨 Engine 的 Node 目标、预约、迁移与均衡控制
│   │   ├── raft/              # Raft Storage、Group Host、Snapshot 生命周期
│   │   ├── recovery/          # 持久资源登记、物理 Checkpoint、备份与恢复编排
│   │   ├── resource/          # 统一准入、预算、Tenant/能力计量与公平调度
│   │   ├── router/            # 完整内部键 / Range 路由、Epoch 与栅栏
│   │   ├── rpc/               # 连接、传输、重试、控制/数据通道边界
│   │   ├── runtimecontract/   # Runtime 对 Engine 暴露的稳定路由/读写契约
│   │   ├── storage/           # 节点存储身份、Volume 生命周期与空间预留
│   │   │   ├── pack/          # 节点底座提供的聚合记录容器
│   │   │   └── records/       # 不可变内容身份、本地索引和持有校验（Node 所有）
│   │   └── task/              # 有界后台任务、调度与资源预算
│   ├── engine/                # 可复用存储语义；当前有 record 验证实现
│   │   └── record/            # 开发用 Record Handler，不代表全部业务模块
│   ├── module/                # 按需落地：Config、Cache、File、Search 等业务
│   ├── gateway/               # 按需落地：HTTP 等外部协议适配
│   └── testkit/               # 故障注入、集群测试和恢复测试共用工具
├── docs/                      # 设计、运维与开发文档
├── web/console/               # 未来嵌入 dubi 二进制的控制台源代码
└── deploy/                    # 部署示例、服务单元和监控配置
```

图中的 module/gateway 是职责规划，按功能创建；当前 engine 下 cache/file/message/search 等旧占位目录不代表已实现，也不决定将来的层次。优先按需求落地 State/Record/Blob/Index，不为占位目录补造 Engine。

HTTP Gateway 与内嵌控制台的新增职责以 [HTTP Gateway 架构](http-gateway.md) 为准：`internal/gateway/http/` 负责协议适配与静态资源托管，`internal/module/` 负责业务 API，`web/console/` 负责控制台源码，`internal/server/` 保留监听器与生命周期装配。这些目录按功能落地，不要求先创建空包。业务导入/导出属于对应 Module；管理 API 可调用 Runtime 管理服务，但不能直接操作底层存储。

## 依赖规则

- `cmd/dubi`、`internal/command`、`internal/bootstrap` 和 `internal/server` 只做装配与编排，可以调用 `platform` 与 `engine`，但不保存领域状态。
- `engine/*` 可以使用 `platform/*` 的稳定接口；engine 之间不直接读写彼此持久状态。跨能力协作必须经过公开的内部接口、事件或明确的编排层。
- `platform/runtimecontract` 是 Engine 依赖的稳定 Runtime 边界；Engine 不得依赖 `platform/raft`、`platform/rpc`、`storage/pack` 或 Node 内部实现。
- 架构、新增代码、内部协议和持久状态统一使用 `Engine` / `EngineID`；不再引入另一套同义命名。
- `platform/*` 不得依赖任何 `engine/*`。节点身份、共识、物理存储与修复等 Runtime 职责直接归入 `platform`，不要求先有两个 Engine；业务工具只有存在真实复用需求时才提取，不能借共享之名把业务规则塞入 Runtime。
- 从 Byfile 采用实现时先按职责拆分：节点、共识、Volume、任务和资源等共用机制进入 `platform`；对象数据机制进入对应 Engine，文件路径、文件版本和业务 Manifest 等语义进入 File Module。KV、Bus 等模块应能接入同一平台接口，而不需要模拟 File 对象。
- `platform/placement` 按持久拓扑自动推导 Region 内与跨 Region 多数派目标，统一计划 Range 所有权、Group 成员和数据拷贝的资源预约与后台预算；不开放副本数配置，物理复制、持有校验和确认由 Runtime 执行，Engine 只提供必要的确定性格式/语义校验。
- `platform/node` 与 `platform/storage` 将生命周期意图和健康观察分开。Node/Volume 的受控退出 必须持久、可续跑；Lost 身份要先 Fence，旧盘重现不能自动恢复服务。
- `platform/placement` 同时比较期望耐久性与实际已确认存放位置；Group 多数派、数据拷贝和 Region 故障策略分别检查，不把计划或暂存拷贝计为 Ready。DUBI 不将 Replica/ReplicaSet 作为底座术语或配置项。
- `platform/resource` 对 Node/Volume 及实际文件系统设资源限额，对 Tenant/能力计量，统一前台准入和后台预算；Repair 有保底，备份、均衡、Compaction 和索引任务不能绕过限额。
- `platform/recovery` 管理 Raft 快照之外的 Cluster Backup/Restore；登记全部持久资源，统一 Checkpoint、Repository、完整性校验、维护门禁、恢复预检及演练证据，不按 Tenant 或能力筛选，也不解析业务记录格式。新节点通过普通 Join、Raft Snapshot 追赶和 Placement/Repair 恢复，不需要 Node Snapshot。Raft 快照由 `platform/raft` 管理。
- File、Log、Search 等业务数据导入/导出留在 `module/*`，通过 Engine 接口操作，不复用集群备份格式，也不能替代整集群恢复。
- `platform/auth` 与 `platform/cluster` 以 Tenant 作为数据归属、权限和计量边界；同一 Tenant 的业务应用可访问同一 Module 的数据；不同 Module 复用 Engine 仍隔离，Module/Engine 身份由服务端固定。Dataset 不再是目标资源，Keyspace 仅是内部地址概念，详见 [数据寻址与隔离](data-addressing.md)。
- `platform/storage/pack` 提供 Pack 的追加、校验、封存、恢复与 Compaction 生命周期。Engine 定义记录编码和访问语义，Module 定义保留/过期规则；Runtime 根据权威引用与备份/快照保护执行物理 GC，不解释业务载荷。
- `platform/storage` 管理节点存储身份、Volume 和通用资源计量；`StoreID` 是内部实例标识，不是多 Store 资源层。文件业务引用属于 File Module，物理持有与回收属于 Runtime。
- `api/v1` 只包含公开契约、错误码和版本协商；服务端内部模型不能被其反向依赖。
- `dubi-go` 只面向 `api/v1` 的稳定公开能力，不导入本仓库任何 `internal/` 包。

## 当前落地边界与顺序

底座已具备节点、共识、Volume/Pack、自动初始 Range 和内部 Record 验证路径；具体资格以当前验收记录为准。现有 `record` 不替代 State/Record/Blob/Index 的正式业务契约。

1. State Engine → Config Module → 最小 HTTP Gateway 首版已接通；代码、限制与 VPS 验证见[开发规范第 13 节](../development/engine-module-gateway.md#13-state--config-首版实现)。Go SDK 和更高阶 State 能力后续逐步增加。
2. 随 File、Log、Search 的具体需求复用或落地 Blob、Record、Index；不先造全部 Engine 或空包。
3. 多区间 Range 路由、拆分与合并由 Runtime 后续实现，Module 不新增存储容器或管理流程。

Runtime 调用边界、职责和测试要求见[Engine / Module / Gateway 开发规范](../development/engine-module-gateway.md)。
