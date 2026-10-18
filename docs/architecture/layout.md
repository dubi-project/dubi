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
│   │   ├── placement/         # 跨能力的 Store 选择、预约、迁移与均衡控制
│   │   ├── raft/              # Raft Storage、Group Host、Snapshot 生命周期
│   │   ├── recovery/          # 持久资源登记、物理 Checkpoint、备份与恢复编排
│   │   ├── resource/          # 统一准入、预算、Tenant/能力计量与公平调度
│   │   ├── router/            # Tenant / Dataset / Range 路由、Epoch 与栅栏
│   │   ├── rpc/               # 连接、传输、重试、控制/数据通道边界
│   │   ├── runtimecontract/   # Runtime 对 Engine 暴露的稳定路由/读写契约
│   │   ├── storage/           # Store / Volume 生命周期、故障隔离与空间预留
│   │   │   ├── pack/          # 节点底座提供的聚合记录容器
│   │   │   └── records/       # 不可变内容身份、本地索引和持有校验（Node 所有）
│   │   └── task/              # 有界后台任务、调度与资源预算
│   ├── engine/                 # Engine/Module 实现
│   │   ├── record/            # 首个正式 engine：版本化 KV / 文档记录
│   │   ├── cache/             # TTL、淘汰、可重建缓存
│   │   ├── message/           # 主题、分区、消费位点和重放
│   │   ├── search/            # 异步索引、查询与索引水位
│   │   ├── file/              # CID、Block、Object、Version 与 Pack 适配
│   │   ├── log/               # 结构化追加日志与保留
│   │   ├── metric/            # 时间序列、聚合、降采样与保留
│   │   └── trace/             # Trace/Span、采样和关联查询
│   └── testkit/               # 故障注入、集群测试和恢复测试共用工具
├── docs/                      # 设计、运维与开发文档
├── web/console/               # 未来嵌入 dubi 二进制的控制台源代码
└── deploy/                    # 部署示例、服务单元和监控配置
```

## 依赖规则

- `cmd/dubi`、`internal/command`、`internal/bootstrap` 和 `internal/server` 只做装配与编排，可以调用 `platform` 与 `engine`，但不保存领域状态。
- `engine/*` 可以使用 `platform/*` 的稳定接口；engine 之间不直接读写彼此持久状态。跨能力协作必须经过公开的内部接口、事件或明确的编排层。
- `platform/runtimecontract` 是 Engine 依赖的稳定 Runtime 边界；Engine 不得依赖 `platform/raft`、`platform/rpc`、`storage/pack` 或 Node 内部实现。
- 架构、新增代码、内部协议和持久状态统一使用 `Engine` / `EngineID`；不再引入另一套同义命名。
- `platform/*` 不得依赖任何 `engine/*`。只有真正被两个及以上 engine 共用的机制才可进入 `platform`。
- 从 Byfile 采用实现时先按职责拆分：节点、共识、Volume、任务和资源等共用机制进入 `platform`；CID、Block、Manifest、路径与文件版本等语义只进入 `engine/file`。Record、Message 等模块应能接入同一平台接口，而不需要模拟 File 对象。
- `platform/placement` 按持久拓扑自动推导 Region 内与跨 Region 多数派目标，统一计划 Range 所有权、Group 成员和数据拷贝的资源预约与后台预算；不开放副本数配置，复制、校验和确认仍由所属能力实现。
- `platform/node` 与 `platform/storage` 将生命周期意图和健康观察分开。Node/Store/Volume Drain 必须持久、可续跑；Lost 身份要先 Fence，旧盘重现不能自动恢复服务。
- `platform/placement` 同时比较期望耐久性与实际已确认存放位置；Group 多数派、数据拷贝和 Region 故障策略分别检查，不把计划或暂存拷贝计为 Ready。DUBI 不将 Replica/ReplicaSet 作为底座术语或配置项。
- `platform/resource` 对 Node/Store 设硬上限，对 Tenant/能力计量，统一前台准入和后台预算；Repair 有保底，备份、均衡、Compaction 和索引任务不能绕过限额。
- `platform/recovery` 管理 Raft 快照之外的 Cluster Backup/Restore；登记全部持久资源，统一 Checkpoint、Repository、完整性校验、维护门禁、恢复预检及演练证据，不按 Tenant 或能力筛选，也不解析业务记录格式。新节点通过普通 Join、Raft Snapshot 追赶和 Placement/Repair 恢复，不需要 Node Snapshot。Raft 快照由 `platform/raft` 管理。
- File、Log、Search 等能力自己的业务数据导入/导出留在 `engine/*`，不复用集群备份格式，也不能替代整集群恢复。
- `platform/auth` 与 `platform/cluster` 以 Tenant 作为数据归属、权限和计量边界；同一 Tenant 的业务应用可共享 Dataset，键、路径或名称前缀由业务方自行约定。
- `platform/storage/pack` 提供 Pack 的追加、校验、封存、恢复与 Compaction 生命周期。File Block 默认使用 Pack；各能力自己的记录编码、索引、保留及 GC 不进入通用 Pack 层。
- `platform/storage` 同时管理 Store/Volume 与通用资源计量；File 专属的 CID、逐 CID 副本状态和 GC 属于 `engine/file`。
- `api/v1` 只包含公开契约、错误码和版本协商；服务端内部模型不能被其反向依赖。
- `dubi-go` 只面向 `api/v1` 的稳定公开能力，不导入本仓库任何 `internal/` 包。

## 当前落地顺序

1. `cmd/dubi`、`bootstrap`、`server` 和 `platform/{config,identity,node,raft,router,placement,resource,recovery,storage,rpc,observability}`。
2. `api/v1` 与 `engine/record`，交付单节点后扩展到三节点 Range。
3. `testkit` 的强杀、网络分区、Leader 切换、Snapshot 恢复和滚动升级基线。
4. 再按规划进入 Cache、Message、File、Search、Log、Metric、Trace。

空目录目前仅用于冻结职责；实际代码进入时必须同时补充包级说明和测试。
