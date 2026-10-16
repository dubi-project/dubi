# DUBI 集群底座概念规范

本文定义 DUBI 自己的集群与存储底座。它约束后续模块的命名和职责；公开 API、磁盘编码等实现参数另行版本化。DUBI 的底座负责身份、路由、放置、均衡、资源与故障恢复，各模块负责自己的数据语义和格式。持久数据按节点与 Region 的多数派自动放置，不向用户提供副本数或最小确认数配置。所有运维入口采用可安全推导的最少参数。ClusterID、NodeID、RaftID、GroupID、内部证书名、证书路径和成员列表由系统生成、持久化和维护，不提供用户覆盖或专家模式。用户仅表达数据目录、加入凭据、网络地址、Region 等实际部署意图；新增节点不能增加手工编排内部身份的工作。启动不需要 YAML。

## 1. 总体关系

```text
Cluster
├── Region → Node → Store → Volume            物理拓扑与资源
├── Tenant → Dataset → Keyspace（内部）→ Range → Owner Group
├── Placement → Group 成员 / 数据存放位置       放置与均衡
└── Capability → 自己的数据格式                 业务语义

Volume → Pack（节点底座提供）→ 多条本地记录
```

`Range` 是 DUBI 底座的统一路由与所有权单位。`Pack` 是本地物理容器，可放多条来自不同 Range 的记录。Range 不装在 Pack 里；Pack 也不参与集群路由。底座不使用 `Partition` 作为 Range 的同义名称。某个模块可在自己的公开 API 中使用符合其领域的词，例如消息的 Topic 分区，但内部路由仍落到 Range。

## 2. 基础术语

| 概念 | DUBI 定义 | 标识及边界 |
| --- | --- | --- |
| Cluster | 独立的部署、信任和版本管理边界。 | `ClusterID` 持久不变；成员、根证书、路由目录和集群协议属于它。 |
| Tenant | 集群内的数据归属、授权与资源计量边界；一个 Cluster 可服务多个 Tenant。 | `TenantID` 持久标识租户；其 Dataset 和数据不能被其他 Tenant 默认访问。 |
| Region | 节点所在的单层地理故障域。 | 一个 Node 归属一个 Region；Region 本身不保存数据，也不是共识组。 |
| Node | 运行同一 `dubi server` 程序的成员，可接入请求、承载共识组和保存数据。 | `NodeID` 持久不变；重启不生成新身份。可选 `Name` 是集群内唯一的管理员显示标签，可为空、可修改，不参与身份、路由或授权；正式移除的旧节点名称可由新节点复用。节点不是“专用元数据机”或“专用数据机”。 |
| Store | Node 对集群暴露的可放置存储资源。 | `StoreID` 归属 `NodeID`；报告容量、预留、健康和负载。一个 Node 可管理多个 Store。 |
| Volume | Store 管理的本地磁盘或持久目录。 | `VolumeID` 只标识本地介质；多 Volume 不等于跨 Node 副本。 |
| Capability | Record、Cache、Message、Search、File、Log、Metric、Trace 等数据能力。 | 能力注册路由键、权威/可重建/易失类型、资源指标和本地存储适配；其数据格式由能力拥有，不自定义副本数。 |
| Dataset | 某 Tenant 下由某能力管理的一组逻辑数据，如 Record 集合、Message Topic 或 Search Index。 | `DatasetID` 归属 Tenant 和 Capability；一个能力可拥有多个 Dataset，各自声明路由方式。它不是物理文件、Store 或共识组。 |
| Keyspace（内部） | 某 Tenant 的某能力 Dataset 的有序路由键域，由 `(TenantID, CapabilityID, DatasetID)` 确定；一个 Tenant 可有多个 Keyspace。 | 只用于底座目录与路由实现，不是用户可创建或授权的独立资源；能力定义稳定的 `RoutingKey` 编码。当前每个 Dataset 对应一个 Keyspace。 |
| Range | 一个 Keyspace 内，按该能力定义的 `RoutingKey` 切分出的不重叠半开区间 `[start, end)`；负责路由、所有权和计量。 | `RangeID` 标识逻辑范围，`RangeEpoch` 标识当前路由代际；Range 可以拆分、迁移，支持时也可合并。 |
| Raft Group | 独立的共识日志、成员配置、状态机和 Snapshot。 | `GroupID` 标识共识组；持久 `StateKind` 决定节点运行时使用哪个已注册状态机工厂。一个 Group 可承载一个或多个 Range，也可同时承载不同 Capability 的 Range；Range 与 Group 不强制一一对应。 |
| Root Group | 保存集群成员、Group 描述符和 Range 路由目录的权威控制面 Group。 | 不保存大量逐条业务记录、文件内容或索引数据；目录自身需要容量与扩展边界。 |
| Placement | 根据 Active Node/Store 与 Region 拓扑自动确定本地和跨 Region 多数派目标，并负责补齐、迁移和均衡。 | 不接受手工副本数或逐节点拼放置策略；能力负责数据复制、校验和确认。 |
| PlacementPlan | Placement 针对当前拓扑产生的目标位置与执行步骤。 | 描述期望状态；成功写入并确认后才更新实际存放状态，拓扑变化时重新计算。 |
| Epoch / Fence | 所有权代际与拒绝旧写入的栅栏。 | Range 切流后，旧 Epoch 的持有者不得继续确认写入。 |
| Pack | DUBI 节点底座提供的物理记录容器，把大量小型或不可变条目聚合到少量本地文件。 | `PackID` 是本地物理标识；Pack 不对应 Range、Group 或跨节点副本。File 的 Block 默认使用 Pack；其他能力按格式需要选用。 |

`NodeID`、`StoreID`、`RangeID`、`GroupID` 与 `PackID` 代表不同层次，不能互相代替。Raft 通信所需的数值身份也与 NodeID 分开。

DUBI 不把 `Replica` / `ReplicaSet` 作为底座概念，也不提供副本数配置。实现仍须分别记录 Group 成员和数据的实际存放位置、确认状态与代际；它们是运行事实，不是用户选择的放置策略。Group 描述符中的 `Voter` / `Learner` 是 Raft 成员角色，不是副本策略；`GroupEpoch` 是成员描述符的 CAS 代际，也不能替代 `RangeEpoch`。

Tenant 是底座必须识别的逻辑边界，不等于独占 Node、Store 或 Volume；集群可共享物理资源，但鉴权、数据可见性、用量、配额和后台任务必须按 Tenant 归属。DUBI 不建立项目层级。一个 Tenant 内的多个业务应用可在授权范围内共享同一 File 内容、Record Dataset 或其他数据；业务方如需区分用途，可自行使用键、路径或名称前缀，这些前缀不是底座的隔离单位。需要独立的数据、权限和配额边界时，使用不同 Tenant。跨 Tenant 共享数据需要单独设计显式授权机制，默认不允许。

## 3. Range 是集群的逻辑单位

请求先确定 Tenant 身份并完成授权。需要集群所有权路由的请求，由能力转换为所属 Dataset 的 RoutingKey。底座将 `(TenantID, CapabilityID, DatasetID)` 视作内部 Keyspace，再按 `[start,end)` 定位 Range、Owner Group 与 RangeEpoch。同一 Keyspace 的活动 Range 必须无重叠、无空洞。Tenant 是用户可见的归属、授权与计量边界；Keyspace 不单独授予权限，也不对用户提供创建/删除入口。入口 Node 可接收请求，但要按目录将其送往当前拥有者。一个能力可用不同 Dataset 表达不同路由对象；文件元数据、内容放置记录、索引任务不必共享一个 Range 切分。

Range 只表示**哪一段可路由状态由谁负责**。它不要求每份物理数据都在 Range 中，也不规定持久格式、缓存规则或读写一致性。例如：

| 能力 | RoutingKey 示例 | Range 承担什么 |
| --- | --- | --- |
| Record | 编码后的记录键或其哈希 | 键区间的权威读写路由 |
| Cache | 缓存键的哈希 | 缓存桶的归属和容量计量 |
| Message | Topic 与内部流标识 | 流的归属；顺序和消费位点由 Message 定义 |
| Search | 索引分片键 | 索引任务、资源及查询路由 |
| File | 路径/对象键，或 CID 哈希形成的路由键 | 文件元数据或内容放置记录的归属 |
| Log / Metric / Trace | Stream、序列/Trace 标识与时间窗组合 | 摄取与查询的路由和资源计量 |

能力须声明合法切分点；不能因为 Range 是区间就任意拆开一条消息流或一个必须整体维护的索引单元。Range 迁移、拆分和合并时由集群控制器维护目录、Epoch 与任务状态，并通过状态复制、增量追平和校验完成迁移；这些内部复制步骤不作为另一类对外快照功能。Group 迁移是另一件事：其 Voter/Learner 成员改变，不要求同步改变 Range 的逻辑边界。

入口 Node 将一次目录解析固定为不可变的 Route Target，至少携带内部 Keyspace、RoutingKey、RangeID、Owner Group 和 RangeEpoch。Owner 端必须把“Target 与本 Group 已应用 Range Fence 完全匹配”的检查，和业务写入放在同一条 Raft 状态机原子事务中；仅在 RPC 入口或提交 Raft 前检查会留下 Fence 更新与业务提交之间的竞态，不能作为写栅栏。线性一致读必须先完成该 Owner Group 的 ReadIndex，再持有一个受保护的本地状态视图，在该视图内核对精确 Fence 并读取能力数据；不能先检查 Fence、释放状态保护后再读取。Epoch 不匹配、Owner 不匹配、RoutingKey 越界或 Keyspace 不匹配都应失败关闭并让调用方重新解析目录，不能静默转交给旧 Owner。

所有写命令必须携带稳定 RequestID。Owner Group 将命令内容摘要、结果和 AppliedIndex 与业务变更同批同步落盘；相同 RequestID 与相同命令的重试返回首次结果且不重复执行，不同命令复用 RequestID 必须报告冲突。Fence 失败、配额拒绝等业务结果也要持久化，不能让响应丢失后的重试绕过原判定。初始 Range Fence 由独立的 Raft 控制命令安装；在 Split、迁移和受控切流协议完成前，只允许 Epoch 1 的完整 Keyspace Range 激活，不能通过普通数据命令改写 Fence。

Dataset 创建采用两阶段可恢复流程：Root 先持久化 `Pending` Range，此时 Router 不得返回它；协调器以确定性 RequestID 在目标 Owner Group 安装 `Active` Fence，并线性一致确认其结果；最后向 Root 提交带 Owner Fence RequestID 和实际 AppliedIndex 的激活记录，Root 才将 Range 置为 `Active` 并开放路由。Root 的 Pending 描述符就是持久任务事实，协调器不依赖内存进度；在 Owner 提交后或 Root 发布后中断，都可通过两边的请求结果安全重试。若 Owner 上已有不匹配的 RangeID、Keyspace、Epoch、边界或 Owner Fence，流程必须停止，不得覆盖。

跨节点控制命令通过内部 Group RPC 发送，必须携带 ClusterID、来源/目标 RaftID 和 GroupID，并使用双向 TLS 将来源声明绑定到节点证书。远端 `Submit` 只有在目标节点完成该 Group 的 ReadIndex、确认命令已进入已应用状态后才能返回 confirmed index；这只证明日志应用进度，不等于业务结果成功。协调器仍须按 RequestID 查询并核对状态机的持久 Result，再推进下一阶段。命令大小、deadline、目标 Group 和目标节点都必须有界且显式，不能将 Raft peer 消息 RPC 直接当作业务控制 API。

对 Range Bootstrap，不开放任意状态键远程读取，而由专用 Range Control RPC 在 Owner 节点内完成“提交初始 Fence → ReadIndex → 按确定性 RequestID 核对持久 Result”，只返回经核对的 OwnerProof。Proof 至少包含 Owner RequestID 和实际 AppliedIndex；调用端再次核对它与目标 Range 的确定性请求身份一致后，才允许提交 Root 激活。这个接口必须保持窄化，后续 Split/Move 使用独立的带阶段与期望 Epoch 的协议，不能复用 Initial Range RPC 绕过迁移状态机。

普通能力跨节点请求使用独立的 Capability RPC，而不是暴露 Pebble key。Mutation 携带完整 Routed Command；Owner 节点严格解码 Target 与 RequestID、提交所属 Group、完成 ReadIndex，再按完整命令核对持久 Result 后返回。Read 携带 Route Target 和能力自定义的不透明 payload；Owner 节点完成 ReadIndex，在同一受保护状态视图中核对 Fence，再调用能力的只读 Handler。平台只负责身份、路由代际、一致性和结果边界，不解释 Record、Cache、Message、Search、File 等能力的业务载荷。能力拒绝作为稳定结果返回；路由/Fence/身份或状态完整性错误则失败关闭。Tenant 授权必须在进入内部转发前完成，内部 RPC 的节点身份认证不能替代租户权限判断。

不可变数据的进程内能力入口遵守同一个“任意 Node 接入、当前 Owner 执行”边界：入口从 Root 解析 Owner，本机不托管时通过节点认证的独立流式 RPC 向 Owner 成员转送逻辑请求和分帧 Body；Owner 执行物理多数、引用发布和代际确认，入口验证完整引用与确认信息后才返回 `LocalReady`。流式传输成功本身不构成数据多数或写入成功。路由代际变化或结果不确定时调用方使用同一 RequestID 重新解析和重试；此内部入口并非已授权的 Tenant 公共 API。实现边界见[能力数据入口](docs/architecture/capability-data-entry.md)。

`StateKind=capability` 使用通用 Mux 按 Route Target 中已验证的 `CapabilityID` 分派 Handler；Mux 的注册集合必须在节点启动装配期封存，运行中不能增删，避免同一节点上先后打开的 Group 得到不同实现集合。未注册 Capability、或对未实现只读接口的 Capability 发起 Read，返回可持久/可重试的稳定拒绝，不得使已提交日志毒化状态机。Handler 看到的是 Keyspace 本地逻辑键；底座将 Transaction/ReadView 绑定到 `(TenantID, CapabilityID, DatasetID)`，自动加入完整命名空间。同一 Group 中不同 Tenant、Capability 或 Dataset 使用同名逻辑键都不会碰撞，不由业务方补写隔离前缀。业务项目可在同一 Tenant/Dataset 的键内自用前缀，共享内容也不需要 Project 资源。存储隔离不替代入口授权和 Range Fence：每次读写仍须通过完整 Route Target 校验。当前 capability 状态/快照为 v7，保持完整 Keyspace 归属，包含[区域代际/成员位置栅栏](docs/architecture/region-fences.md)、持久交接门禁、[不可变记录权威引用和本地多数确认记录](docs/architecture/immutable-records.md)；旧 v6 及更早开发格式明确拒绝自动迁移，滚动协商和迁移工具未完成。

不可变记录的逻辑权威和物理持有分开：事务可原子发布/覆盖/显式删除某 Keyspace/Key 对应的完整数据描述及发布位置，与 Result/applied 同步；普通 KV 删除不隐式删除该引用。引用禁止带本地物理位置，两种 Raft 快照只恢复引用元数据、不据此宣告数据已持有。来源 Group 不等于当前 Owner，历史成功 Result 不等于当前引用，当前引用也不等于 LocalReady/GeoReady；认证持有与稳定多数确认必须独立实现。此接口不强制上层模块改变载荷格式，不增加部署参数。

内部 Node 现可直接核验指定节点是否实际持有精确不可变记录：重新读取 Pack，前后确认 Root 成员及 TLS 身份，客户端另绑定真实服务端证书，返回 Node/Store 观察。观察接口不复制载荷；另有有界主动发送和远端拉取接口，在 TLS 握手阶段绑定指定 Node 后传递精确描述和实际字节，完整 EOF/摘要校验、目标 Pack 与索引同步并重新读验后确认。拉取必须收到正常最终状态才写本地 Store，完整字节后的错误也拒绝发布；只返回来源观察与本地持有，不把大缓冲交给调用方长期保留。上述接口均不要求本机托管来源 Group，不证明当前逻辑引用、Region 代际或数据保留；Root 读屏障不能当作数据提交位置，多 Volume 不能算多个 Node。后续必须另实现数据补齐、代际绑定与稳定多数确认，不能把物理观察或传输成功当作 LocalReady/GeoReady。

封口代际已有本地固定引用清单原语：Group ReadIndex 后在同一 Pebble 快照核对未激活 seal、当前引用及 Range/Keyspace/Result 关联，流式生成与 seal 绑定的完整集合。Artifact 摘要绑定精确文件切点，集合摘要绑定有序完整引用，避免无关 applied 推进影响集合比较；末尾及整个文件校验前不能宣称扫描完成。清单无物理位置、载荷或 ConfState，不是数据覆盖、保留承诺、Raft 快照或完整备份；认证清单传输与区域增长时新增节点的一次自动补齐已接通，但持久覆盖记录和多数确认未完成。捕获不会阻止实时激活，使用时须重新核对当前代际资格。详见[不可变记录](docs/architecture/immutable-records.md)。

Owner 指拥有 Range 的 Group，不是某一台固定 Node。Root 持久保存 Group 的 `GroupEpoch` 以及由已登记 Node RaftID 组成的 `Voter` / `Learner` 成员集合；成员描述符以期望 Epoch 做 CAS 更新，必须含至少一个 Voter，且不能引用未知 Node。这条 Root 更新是对已确认 Raft `ConfState` 的**发布**，不是执行成员变更的捷径：控制器必须先通过受控的 Learner 加入、追平、提升或移除流程确认实际成员状态，再发布新描述符；失败或中途状态不能冒充当前路由事实。

成员调整先在 Root 创建唯一的持久 `GroupChange`，记录 ChangeID、期望 GroupEpoch 和最终 Voter/Learner 集合；同一 Group 同时只允许一个 Pending 任务，任务存在时禁止旁路发布成员描述符。协调器每次都从 Root 任务和目标 Group 已持久应用的 `ConfState` 重新推导下一步，按“目标节点根据 Root 持久 `StateKind` 选择已注册状态机工厂，打开空的 Joining Group → 新增节点作为 Learner → 确认成员变更 → 追平后提升 Voter → 删除多余 Learner/Voter → 实际集合完全匹配后原子发布 Root 描述符并完成任务”的顺序执行。Joining Group 不带 Bootstrap peers，不能自行形成另一套日志；只有现有 Leader 提交加入后才从合法 Group 追平。工厂选择前必须完成 Root ReadIndex，未知 StateKind、未登记 Group 或工厂构造失败都应失败关闭。工厂由节点运行时注册，可构造通用能力状态机及其 Handler/Mux；Raft 层不能猜测 Record、File、Search 等模块的持久格式。新增和提升必须先于删除，避免为了达到目标集合而提前降低当前法定人数；管理员节点移除使用显式维护任务，只允许删除现有成员并保留当前 voter 严格多数，不增加新成员。若要删除本地 Leader，先把领导权转给仍保留的 Voter，再由新 Leader 所在节点续跑任务。控制器崩溃不丢进度，重复执行只观察或推进尚未完成的一步；Voter 不能原地降级为 Learner，如确有需要必须拆成先移除、后重新加入的两个受控任务。

跨节点成员控制使用独立的内部 mTLS RPC 准备目标 Joining Group、查询线性确认后的 `ConfState`、执行单步成员操作和转移领导权；它不复用普通能力接口，也不允许客户端直接提交任意 `ConfChange`。变更只在目标 Group 当前 Leader 上接受，Follower 返回可重试的不可用状态，协调器按候选成员寻找 Leader；追平不足、状态机无法构造、非法角色转换等稳定拒绝不得换节点掩盖。任务执行期间，成员控制候选是“Root 已发布成员 ∪ Pending GroupChange 的目标成员”，因为领导权可能已经转给刚提升但尚未最终发布的新 Voter；普通数据路由仍以已发布描述符为准。

常驻成员调度器只在本节点确认自己是 Root Group Leader 时扫描 Pending GroupChange；Follower 不执行控制任务。Root Leader 切换后，新 Leader 从 Root 任务和各目标 Group 的实际 `ConfState` 继续，而不是接管旧进程的内存步骤。旧 Root Leader 在失去领导权前已经提交的单步操作可能已经生效，因此所有步骤仍必须可重入，最终发布继续受期望 GroupEpoch CAS 保护；不能把“只有一个调度循环”当作幂等保证。

Node 进程启动分成两个阶段。第一阶段只加载稳定身份、打开 Root 状态与 Root Group、封存 Capability Mux，并注册带证书身份校验的内部 Raft、Group、Range、Capability 和 Membership RPC；任一步失败都按逆序关闭已取得的状态与 Store 锁。第二阶段必须先通过 Root ReadIndex，再从已确认的 Group 描述符恢复包含本 RaftID 的 Group，并恢复 Pending GroupChange 中以本节点为目标、但尚未发布的 Joining Group。不能只扫描磁盘目录并自动打开所有旧 Group，因为已被移除的本地目录不再代表合法成员身份。RPC 监听与第二阶段恢复由 server 入口协调，使 Root 能先参与选举和收发消息；Capability Group 关闭时必须同步从本地 RPC 状态目录注销，避免请求命中已关闭状态。

内部 server 只接受 TLS 1.3 双向认证配置：服务端必须强制验证客户端证书，客户端必须验证服务端证书，且每个 RPC 仍由 `AuthorizePeer` 将已验证证书绑定到请求声明的 RaftID。TLS 握手成功不等于节点有权冒充任意 RaftID。出站 Raft Transport 和 Group Client 共用经 Root 目录确认的地址。集群采用首节点初始化、其他节点受控加入的流程，禁止要求用户配置初始 RaftID/peer 列表。底层测试与内部装配仍可传入 bootstrap peers；生产入口不能把这些参数暴露为配置。后续远端加入通道须自动取得并持久保存首次联络信息，且不能覆盖 Root 权威成员状态。server 在本地 Group 恢复完成后才发布 Ready，并启动仅限 Root Leader 执行的成员任务调度器；关闭顺序为停止 RPC/后台循环、关闭出站客户端和 Transport、最后关闭 Raft Host 与全部状态机。

Node 证书的身份 URI 固定为 `spiffe://dubi/<ClusterID>/node/<NodeID>/<RaftID>`，Subject Common Name 必须等于 NodeID，且叶证书同时具备 ServerAuth/ClientAuth。内置 PKI 的全部节点证书共享虚拟 DNS SAN `dubi.internal`；客户端连接 Root 目录给出的真实 advertise 地址，但固定用该名称验证证书，因此节点换 IP、主机名或端口不触发重签。虚拟名称不参与寻址，也不要求 DNS 可解析。进程启动时证书 URI 必须与 Store 中的持久身份完全相等，服务端处理内部 RPC 时再将对端 URI 与请求声明的 RaftID 比较。新集群初始化必须先原子持久化稳定身份，再将 Node CA、首节点私钥、叶证书和绑定该身份的配置作为一个可重复验证的 PKI 目录原子发布；Node CA 签发私钥不得以裸文件保存，当前使用节点本地 X25519 包装密钥和认证加密信封，并将 ClusterID/NodeID/RaftID/CA 指纹绑定到附加认证数据。包装密钥与信封同盘并不构成独立 HSM/KMS 安全边界，Store 目录权限和受控备份仍是当前静态保护；加入已有集群只分发节点叶证书和信任根，不能复制 CA 签发材料。`dubi init` 自动生成身份及 PKI，并在 Store 排他锁内建立首节点的持久 Root 成员状态；重复初始化保留已有成员和日志。`dubi server` 只恢复已有 Root，不会因为缺少配置、日志或尚未完成加入而自动建立新 Root。节点身份持久记录由初始化还是加入产生，加入来源在绑定 Cluster 后保持不变，不能转为新集群首节点；旧版无来源字段的身份仅在验证已有 bootstrap 签发器后允许继续初始化。进程默认从 `/var/lib/dubi` 读取 Store，可用 `DUBI_STORE` 或 `--store` 指定，监听地址默认为 `0.0.0.0:7443`，可用 `--listen` 调整。托管 PKI 路径和 `dubi.internal` 固定，CLI 不接受 YAML、ID、peer 列表或 TLS 覆盖。尚未注册具体 Capability 时允许以封存的空 Mux 启动控制面，但所有能力请求必须返回“未注册”拒绝，不能把空实现宣传为对应能力可用。当前签发器仍由 bootstrap Node 持有；在集群级 enrollment authority 完成前，移除该 Node 后只保证已有成员继续运行，不保证新 Node 可立即加入。

Enrollment 的新节点先持久生成未绑定 Cluster 的 NodeID/RaftID、Ed25519 私钥和 CSR；CSR 身份 URI 为 `spiffe://dubi/join/node/<NodeID>/<RaftID>`，内置 PKI 自动覆盖 `dubi.internal`。普通加入使用 `dubi join --ticket-file <私密文件>`；默认自动按到签发器的路由推导联络地址，Region 默认 `default`，不同故障域和特殊网络可显式提供 Region/advertise。签发协调器先验证与完整规范请求绑定的持久一次性 admission proof，再完成 Root ReadIndex，以确定性 RequestID 提交并确认 Node 描述符；只有 Root 中的 NodeID、RaftID、RegionID 和地址与申请完全一致后才能调用 Node CA 签发。返回 bundle 只含 ClusterID、集群级 `spiffe://dubi/<ClusterID>/node-ca` 信任根和叶证书，不含 CA 签发材料。安装端在绑定 ClusterID 前验证 CSR 私钥、CA 自签名与身份、完整证书链、Node/Raft 身份、虚拟 TLS 名称和有效期，原子发布 leaf-only PKI。临时加入证书仅有 enrollment 角色，不能降低或冒充 Node mTLS 授权。公有回执持久化后无需原 bearer 即可恢复；Root 接纳记录先于空 learner 启动，只有本地追赶和 ReadIndex 确认后的持久激活才允许 Ready。已接纳节点冷启动不依赖原签发器或新凭证；已有资源但缺失/损坏加入记录时拒绝重新初始化，不能静默更换 Region、地址、密钥或身份。自动 voter 晋升、跨 Region 多数策略、证书生命周期与真实环境资格仍须独立完成，加入 CLI 可运行不代表生产资格通过。

Group 路由目录直接投影 Root 的成员描述符和 Node 地址，并可缓存运行时观察到的 Leader；Leader hint 只能在当前成员中调整优先顺序，不能自行扩大成员集合。请求优先发往最近 Leader，也可发往其他成员由 Raft 转交。带稳定 RequestID 的 Mutation 和线性一致读在目标节点 `Unavailable` 或不承载该 Group 时，可在同一次调用 deadline 内换另一个已知成员重试；Mutation 的状态机去重负责处理响应丢失后的重复提交。权限、参数、资源限制、Fence/Epoch、业务拒绝和数据完整性错误不得自动换节点重试。成员被移除后，即使易失 Leader hint 尚未清理，也必须因不在当前描述符中而失效；目录为空或只剩本机而本地路径不可用时应明确失败，不能猜测节点。

上述加入后的独立控制能力已有阶段性落地：正式托管进程自动选择 Root voter，按当前已接纳节点/Region 形成目标，先验证旧/新 voter 活跃且追平，再用 ConfState CAS 和联合共识切换；角色调整不删除 Node，也不由健康观察缩小目标。普通叶证书已有本地/远端自动续期、双向热加载及连接生命周期管理；CA 轮换/吊销仍未完成。Root 票数与数据 Placement 的本地/区域多数分别证明，不能混为一谈。详细限制见[自动 Root 拓扑](docs/architecture/root-topology.md)与[节点加入](docs/architecture/node-enrollment.md)。

区域数据参与集合已有 Root 持久目录：计划与已确认代际分离，历史集合不可变，不按离线观察缩小多数分母。Root/Placement 结构和自动建组都允许单节点或双节点小 Region；至少三个 Node 的集合才提供本地 `LocalReady`，小 Region 的跨 Region 同步策略已在 Placement/Geo 层定义。初始形成会等待计划参与者的读屏障/成员检查，不能把仍在加入的 Root learner 提前写进初始集合；随后正式后台提交持久代际栅栏、逐节点核验完全相同的应用记录，再经 Root 计划/epoch/成员任务及当前成员资格复验确认集合，不增加用户配置。后继代际当前只允许同 Group 增加节点，自动增长交接已接通；节点移除统一走受控成员变更，新节点走普通加入和自动 Repair，不设置替换任务。整区域节点全部被管理员明确移除后，底层可以自动开启同名 Region 的新内部代际，隔离旧身份并让新节点重新形成集合；用户不需要执行额外的 Region 重建操作。初始确认是历史建组断言，不是永续写权限或业务数据已达到 LocalReady/GeoReady；数据耐久回执仍按本地或跨 Region 策略判断。详见[区域集合与建组](docs/architecture/region-cohorts.md)。

后继增长已具备数据 Group 内的持久交接原语：先封口旧代际业务写入，再建立精确候选，最后通过匹配 Root 确认的内部断言激活；重启和快照保留封口，成员位置失效不能继续写入。Root 增长授权将旧 seal 断言、代际计划与唯一成员任务原子绑定，禁止已确认区域通过普通任务或直接发布绕过；只有该任务完成且候选晚于封口才允许确认新代际。正式增长协调器按固定旧集合的严格多数核验精确封口，执行授权任务；新增节点可在旧成员离线时先补齐 Pack，再核验全部新参与者的候选，并对封口清单的每条记录取得新集合严格多数的认证物理持有观察，然后才发布/激活。持久状态驱动换主、丢响应和重启续跑；逐条物理复核目前仍是一次性操作，尚无可恢复的分页覆盖进度。没有自动缩容或健康过滤，新节点追赶后晋升 voter，不增加用户配置。永久离线旧节点仍会阻止候选全员确认，手动移除后的自动再平衡尚未完成。这不代表每笔数据多数确认或生产资格已完成。详见[区域代际交接](docs/architecture/region-handoff.md)。

当前签发能力已由 Root authority epoch/member 记录、稳定 Root voter 多数调度和本地密封 envelope 边界覆盖；旧 Node Fence 后立即失去签发资格。bootstrap 移除后的真实跨节点 envelope 传输与新 Node 加入闭环仍需联测，暂不宣称该场景已完成验收。

## 4. 统一放置与均衡

底座维护 Store/Volume 的容量、预留、IO、CPU、网络、热点、维修与故障状态，并将资源使用和任务归属计入 Tenant。Placement 根据持久拓扑成员、当前健康可用位置、容量和能力是否为权威持久数据自动推导目标；没有用户可设置的副本数、最小确认数、Home Region 或逐节点规则。参与集合及 Active Region 名单不得因心跳超时自动缩小多数门槛，只能经受控拓扑变更和代际切换调整。写入端 Region 用于定位本地放置，目标分为两个层次：

持久数据以空间换安全：优先满足真实故障域上的数据存放和修复余量，再优化存储利用率与均衡成本。容量成本不能成为把同一 Node 的多盘或同一 Region 的多个位置伪装成跨故障域多数的理由；纯易失或可重建数据按能力声明采用不同目标。

- **本地多数**：在一个 Region 内选定可持续维护的 Node/Store 参与集合，持久数据要在其中严格多数的位置确认。参与集合必须分散在不同 Node/故障域，不因临时节点失联就降低已承诺的多数要求；不能简单把同一 Node 的多个 Volume 当成多个节点。
- **区域多数**：当集群有多个 Active Region 时，持久数据最终还要在严格多数的 Region 中各有经确认的存放位置。不是把所有 Region 全量镜像；Region 加入、退出或长期失联后由受控拓扑代际重新计算并补齐。

本地参与集合的形成、扩缩与上限属于底座自动控制面，不能随着任意 Node 加入就让既有数据瞬间失去达标状态；参与集合的改变必须先复制、校验，再切换代际。同 Region 在持久拓扑中有至少 3 个合格独立 Node 时，写入在本地稳定参与集合的严格多数确认后可向前台返回成功，再异步补齐多数 Region；不足 3 个时（包括只有 1 个或 2 个），不能靠单 Region 确认返回成功，必须在本地留有已确认存放位置且同步得到多数 Active Region 确认。短时失联不改变当前模式和多数门槛。若集群只有一个 Region 且该 Region 不足 3 个合格 Node，只能显式降级运行或拒绝生产写入。

Placement 至少处理三种动作：

1. **Range 所有权调整**：把 Range 交给另一个 Group，或按能力允许的边界拆分，处理元数据容量和热点。
2. **Group 成员调整**：在 Node/Store 之间增减或移动共识成员，维持多数派与故障域要求。
3. **数据存放位置调整**：为能力的独立数据项选择存放位置；例如 File 可按 CID 逐个放置内容拷贝。

三种动作共享节点资源清单、空间预约、速率限制、故障域约束和持久任务记录，但分别由正确的状态机完成确认。不能把“Group 多数派提交”当作“外部数据拷贝已落盘”，也不能把可重建缓存计入权威持久数据的多数派证明。

**实际存放状态**与 PlacementPlan 不同：前者记录已经落盘、校验和确认的位置，后者描述随拓扑变化的目标与动作。例如某 Region 的参与集合为 A、B、C，A、B 已确认时达到本地多数；A 丢失后不能靠“计划把数据写到 C”维持已达标状态，只有 C 写入并校验完成才重新达到本地多数。

迁移的共同顺序是 `计划 → 预约 → 复制/重建 → 校验并追平 → 发布新代际或副本状态 → 退役旧副本`。切流必须有 Fence；任何阶段中断后，任务可恢复且不会同时产生两个合法写入拥有者。均衡综合容量、热度、故障域和前台延迟，并设置迟滞与并发上限，避免频繁搬迁。

Root Group 只保存目录与控制状态。逐项数据副本记录由相应能力拥有的 Range/Group 管理，不能随着数据量增加无限写入 Root。若目录规模超过单个 Root Group 的能力，目录本身需要分层或分片。

## 5. Store、Volume 和 Pack

Store/Volume 元数据当前为 v2，既有 Store 清单还必须登记不可变记录索引的稳定资源 ID 和创建/激活阶段。正式 Node 自动建立/恢复并拥有该索引；父级已激活后丢失子索引不能按新资源重建。v1 开发目录拒绝打开，不自动迁移。capstate 权威引用和认证逐条传输已独立接通，但引用存在或跨节点传输成功不代表 LocalReady/GeoReady。详见[不可变数据记录](docs/architecture/immutable-records.md)。

节点的 `--store` / `StoreRoot` 表示节点持久数据根目录，默认 `/var/lib/dubi`；它是本地路径，不等于集群放置资源的 `StoreID`。身份、PKI、Root/共识状态和卷登记由节点管理。首次初始化未指定数据卷时，只自动创建并登记一个默认 Volume，路径为 `<store>/volume`（默认 `/var/lib/dubi/volume`）；首次显式指定一个或多个外部卷时，仅登记指定卷，不创建、不启用 `<store>/volume`。VolumeID 始终由系统生成并持久保存，用户只提供路径。

当前已实现模块无关的 Volume 登记、状态查询和节点启动恢复，通用 Pack 的追加、读取/扫描、持久封存及崩溃恢复，以及按 Volume 统一、跨 Capability 混写的持久集合清单和自动滚动也已实现；Pack 的多卷选址和进程内字节预约已接入 Node Runtime；其它存储引擎的统一准入、节点内均衡与安全移除尚未实现。单盘部署不要求用户指定两个根目录；后续增加外部 Volume 是对持久卷集合的追加，已有 `<store>/volume` 保持 Active。后续数据层须让其与新增卷一起参与存储和节点内均衡，不能因为加盘而自动停用、替换或删除。节点身份始终保持在原 StoreRoot。

卷集合及默认卷是否已被移除由持久登记决定。只有首次未指定卷时才创建默认卷；重启未传卷路径、默认目录不存在、或曾显式移除默认卷，都不能触发重新创建。登记过的卷缺失或挂载失效时按故障处理，不能自动创建空卷或回退到节点所在盘继续写入。同一文件系统内的节点状态与 Volume 共用空间预留，不能将子目录重复计为独立磁盘容量或故障域。

当前本地登记保存在 `<store>/DUBI_VOLUMES`，独立的 `DUBI_STORAGE_ID` 绑定自动生成的 StoreID、NodeID 和 RaftID；卷内 `DUBI_VOLUME` 标记再绑定 VolumeID。登记不绑定尚未取得的 ClusterID，因此可在 join prepare 时完成并在之后绑定集群时保留。注册先同步发布 Registered 意图，再排他发布并同步介质标记，最后同步发布 Active；重复初始化和重复加卷恢复同一意图及 ID。待激活卷额外记录目录的设备号/inode，防止中断后误认被替换的目录；完成激活后不依赖跨重启可能变化的设备号作为持久身份。活动卷缺失/损坏只改变健康观察，不重建目录、不篡改生命周期。节点可保持控制面启动以便观察故障；Pack 通过 Volume 访问门禁拒绝访问缺失/被替换的卷，不自动补建；尚无具体能力写入器，不能据此宣称损坏卷仍可承载数据。

Store 运行锁、卷登记锁及介质标记锁分别保护进程、登记变更和实际卷的排他占用。路径须存在且首次登记时为空，拒绝符号链接卷、重叠路径和侵入节点元数据目录。容量查询按文件系统设备去重；它仅为物理观测值，不包含进程内预约。新增 Registry.Reserve 和 Pack Pool 负责 Pack 写入的字节准入，但尚未统一接管其它存储引擎。CLI 已提供 `init/join prepare --volume`（可重复）、`volume list`、`volume add`、`volume drain` 和 `volume remove`；Drain/Remove 都是管理员显式操作，运行中的 Node 通过本地认证 RPC 执行。底层 Registry/Pack Pool 已提供可恢复的 `BeginDrain → RemoveDrained` 写入停止边界：Drain 保留读取并拒绝新 Pack 写入；路径仍在且有 Pack 时最终移除会拒绝，管理员明确确认永久丢盘后即使旧路径不可读也可以移除其元数据，不会删除可见数据或把新盘认成旧 Volume。

默认卷和外部卷采用相同的安全移除流程：先预检本节点其他活动卷的可用容量与迁移预算，将目标卷置为 Draining 并停止向其分配新写入，再由当前节点将必需的活数据迁到本节点其他卷、同步并校验目标数据、原子更新本地位置索引。待在途写入及读取引用安全收敛，并满足快照/备份保护条件、确认源卷不再承载必需数据后，才持久标记 Removed 并注销卷。迁出任务须可中断续跑；容量不足或没有可用目标卷时不得丢弃数据或宣布移除完成。这个过程不改变 NodeID、RaftID、Group 成员或 Range 所有权，也不能因其他节点有数据拷贝而省略本节点内的迁移。

Store 是集群可以选择的资源；Volume 是本地介质；Pack 是节点底座**必须提供**的聚合存储能力。平台统一提供空间预约、磁盘安全预留、健康上报、IO 配额、故障隔离及本地文件生命周期。File BlockStore 默认把 Block 写入 Pack，避免一块一个小文件。Record、Message、Search 等能力可按自身读写模式使用 Pack、Pebble/SST、顺序日志段或索引段；不强迫每种数据都进入 Pack。

一个 Node 可以挂载多个 Volume，通过加盘在线扩容；新增 Volume 先登记介质身份、校验可用空间和健康，再加入 Store 的可放置容量，最后由限速均衡逐步利用。加盘提高该 Node 的容量和本地 IO 并行度，但不会增加独立 Node/Region 的故障容忍度，也不能替代缺失节点上的数据拷贝。均衡和修复须预留 Pack Compaction、快照及故障重建所需空间，不能把新增盘立即填满。

Pack 的初始目标大小定为 **512 MiB**，节点配置可调整；建议调节范围为 **256 MiB–1 GiB**。这些是目标值，写入完整记录时允许超过目标后封存。第一阶段内部 API 将目标硬上限设为 1 GiB，单条 Payload 上限 64 MiB、Key 上限 64 KiB、Tenant 标记上限 256 字节、Capability 标记上限 128 字节；Pack 数据文件上限为目标加一条最大编码记录，超大对象由能力切分。Pack 使用 `Active → Sealed → Compacting → Obsolete` 生命周期，并提供追加、校验、同步、封存、崩溃恢复和安全回收。

每个 Pack 归属一个 Volume，不归属某个 Capability；可以混放不同能力、Tenant 和 Range 的记录。每个 Volume 统一使用一个 `packs/` 目录、一份集合清单和一个当前写入 Pack，不按能力划分目录或 Pack。Pack 的完整本地位置由 `StoreID / VolumeID / PackID` 确定。使用 Pack 的能力维护逻辑索引 `记录键 → VolumeID / PackID / Offset / Length / 校验信息`；每条记录必须显式保存 CapabilityID 和 TenantID 归属，并纳入记录完整性校验，以支持隔离、计量和安全回收。记录载荷、索引键、压缩、保留、过期和 GC 由能力定义。Range 搬迁按活记录复制和能力校验进行，不能仅复制整个 Pack；Pack Compaction 只更新本地索引，不改变集群 Range 路由。

Pack 是底座提供的容器机制，统一外层记录封装但不要求所有模块采用相同的载荷编码。能力只是记录归属，不是物理文件边界；不同能力按写入到达顺序追加到同一个当前 Pack。保留、过期和活数据判定按记录处理；未来 Compaction/GC 必须核对所有记录的引用及备份/快照保护，不能因为某一能力的数据过期就删除整个混合 Pack。授权仍由路由与能力层实施，物理 Location 不授予对其它能力或 Tenant 的读取权限。

当前单 Pack 实现将不透明 Key/Payload 与 Capability/Tenant 归属聚合进一个数据文件，并用一个原子检查点保存已提交边界及封存状态；数据、检查点和目录同步后才返回本地追加确认。启动先逐条校验全部已提交数据，再仅截掉未封存 Pack 的未提交尾部；已提交损坏、短文件或检查点丢失均拒绝打开，不静默截断。到达目标时自动封存；单 Pack API 的后续追加返回 ErrSealed，Collection API 则通过持久创建意图自动建立并发布下一 Pack 后再接收记录。集合清单保存已发布 Pack 顺序、目标大小和 Pending ID，Volume 根目录另存身份标记；重启完整验证清单和已提交数据，恢复同 ID 的未完成初始化。清单或整个集合目录丢失不重建空集合，未列入清单的文件不自动导入/删除。当前最多 16,384 个 Pack、清单上限 1 MiB，句柄数量不随已封存 Pack 数量增长。集合管理和带预约的多卷 Pool 已由 Node Runtime 持有；通用 capstate 已接入大值 Raft Apply：先同步 Pack，再同批发布本地引用、Result 和 applied；快照传递逻辑值，接收节点重建位置。64 KiB 为内部外置阈值，节点不增加配置；具体业务索引、能力写入器和物理回收仍未实现。提交、崩溃及快照兼容边界详见 [状态机与 Pack](docs/architecture/pack-state.md)。此次实现不提供多数派耐久确认或完整快照/备份。格式、恢复边界和限制详见 [Pack 与集合管理实现](docs/architecture/pack.md)。

`Chunk` 是 File 对原始文件切片的名称；`Block` 和 `CID` 是 File 内容寻址模型的名称。它们不是 DUBI 集群底座要求其他能力采用的最小存储单位。File 的 Block 默认存入 Pack；Message 的追加记录或 Search 的索引段不因共用集群就改叫 Chunk 或 Block。

## 6. 快照、备份与恢复

DUBI 底座只定义 Raft 快照、整节点快照和整集群备份/恢复三类功能，不能让任一类替代另一类。备份面向整个集群的物理持久状态，不按 Tenant、Dataset 或能力筛选；底座只处理已注册存储资源的不透明字节与一致性切点，不解析各能力的记录格式。

| 机制 | 目的与边界 | 不承诺什么 |
| --- | --- | --- |
| Raft Group Snapshot | 截断/压缩共识日志，为同 Group 副本追平和恢复状态机服务；绑定 Group 成员与日志代际。 | 不是可移植备份，不能当作跨集群恢复点。 |
| Node Snapshot | 在线固定整个节点的本地持久视图，包含该节点所有已注册 Store、Pack、索引、配置和受保护恢复材料；用于节点盘损坏后的替换与预热。 | 只覆盖该节点，不能替代集群灾备；不能以旧 RaftID 回滚旧日志再入群。 |
| Cluster Backup/Restore | 协调所有权威 Group 和全部节点的持久存储资源，将可验证的物理 Checkpoint 归档到独立 Backup Repository，并从完整 Recovery Point 恢复整个集群。 | 不提供单 Tenant、单 Dataset 或单模块的数据导入/导出；不自动承诺任意拓扑或跨不兼容格式版本恢复。 |

每种持久资源必须由 Store/Group 底座登记其所有者、文件范围、格式版本、Checkpoint/恢复操作和垃圾回收保护；能力若只使用底座支持的存储引擎，不需要实现单独的备份编码器。Pack、Pebble/SST、顺序日志段和索引段都可作为不透明物理资源截取。未登记的外部文件不能悄悄遗漏；系统必须拒绝将这样的任务标记为完整 Cluster Backup。内存 Cache 等明确可丢弃的状态可以排除，并在恢复后按其能力语义重建。

Backup Repository 是独立于源集群故障域的备份目标；Recovery Point 是已封存并完成完整性校验的不可变清单，而不是“某个任务显示上传完成”。清单记录源 Cluster、各 Group/Store 的切点、资源清单、格式版本、对象摘要和恢复所需的集群配置。底座统一提供任务持久化、Lease 接管、外部目标适配、流式传输、限速、容量预约、完整性校验、加密与密钥恢复边界、审计及状态查询。备份使用临时对象和最后发布的完成清单；未完成、校验失败或遭隔离的清单不得作为恢复点。Repository 的保留、校验、清理与修复必须有互斥门禁，不能与正在运行的备份或恢复交叉破坏对象。

“不解析数据格式”不等于可以随时复制文件。底座必须协调 Group 提交点、各存储引擎的 Checkpoint/WAL、Pack 封存和 GC/Compaction 保护，确保每个 Recovery Point 包含其切点引用的全部数据。若没有统一写入/提交屏障，多个资源的切点可能不同，清单必须准确记录并标注一致性级别，不能宣称全局原子恢复点。新能力若使用底座以外的存储路径，必须先接入通用资源登记与 Checkpoint 协议；不能因为底座不解析格式，就让部分持久数据遗漏于整集群备份。

导入/导出是另一条业务数据路径，由各能力按自身格式定义：File 可导入/导出文件或对象，Log 可导入/导出日志，Search 可导入/导出索引数据。它们可按业务对象或范围选择，不属于 Cluster Backup/Restore，不得以一次业务导出冒充可恢复的集群备份。

Node Snapshot 在本地固定视图时要协调 Checkpoint、WAL、Pack、GC、Compaction 和密钥轮换；完成后才发布清单，不能按 Tenant 或能力裁剪。正常重启可继续使用原节点稳定存储；从快照重建丢失节点时则生成新节点/共识身份，经认证加入并追平，不让旧副本与新副本同时合法服务。Cluster Restore 先执行只读 Plan、格式/版本/密钥/容量预检与完整校验，再进入明确支持的整集群恢复模式；不能把旧 Raft 日志直接作为新集群的共识历史，也不能默认复制旧 CA 或应用私钥。恢复后需由 Placement/Repair 补齐耐久副本，并验证各注册存储资源可打开、权威 Group 可服务；业务级校验可作为更高层验收，但不要求底座解析每种记录。

备份、节点快照与普通 GC、Compaction、迁移、升级共享资源预算和维护门禁，避免长任务饿死前台流量或在保留保护解除前删除所需数据。生产验收必须定期从真实目标恢复到隔离环境，覆盖损坏对象、Leader/Worker 中断、容量不足、节点移除/重新加入与版本升级，并记录 RPO、RTO、恢复吞吐及各能力的可服务水位。

## 7. Node、Store 与 Volume 生命周期

生命周期状态是控制面的**持久意图**，健康状态是监测得到的**当前观察**；二者不能混用。Node 心跳超时只表示 `Unavailable`，不能自动移除或复用其身份。Node 正常重启保留 NodeID、RaftID 和受管 Store 身份并追赶数据；管理员移除后重新加入的机器即使地址和路径相同，也必须取得全新的 NodeID、RaftID、StoreID 和 VolumeID。StoreID 与 VolumeID 也不得因路径重用而指向不同物理介质。

当前内部 Root `FenceNode` 只允许没有 Root/Group 当前或待执行责任的已登记身份；它持久拒绝该身份再加入或被选为新成员。Group 层已支持显式维护移除并保留当前 voter 严格多数；跨 Root/多 Group 的管理员入口、最终 Fence 编排和新节点加入后的物理再平衡仍未完成，不能靠健康超时强行缩小多数派。见[节点故障、手动移除与自动再平衡](docs/architecture/node-replacement.md)。

| 对象 | 控制面生命周期 | 关键规则 |
| --- | --- | --- |
| Node | `Joining → Active → Draining → Retired`；不可恢复时由授权操作进入 `Lost` | `Joining` 不承载生产责任；`Draining` 不接收新的 Group 成员或数据存放任务；`Retired`/`Lost` 身份不得重新入群。失联、网络分区和进程重启是健康观察，不直接改变持久生命周期。 |
| Store | `Joining → Active → Draining → Retired`；介质不可恢复时进入 `Lost` | Store 是 Placement 的放置目标；`Draining`/`Lost` 不得分配新副本。Store 故障不自动意味着同 Node 的其他 Store 或 Volume 故障。 |
| Volume | `Registered → Active → Draining → Removed`；介质损坏是 `Failed` 健康观察 | Volume 是本地持久介质，不是独立的集群成员；迁移或删除前须证明其上已无必需的活数据，并保留必要的备份/快照保护。 |

Node/Store/Volume 的 Drain 都是持久、可恢复的操作：`请求与预检 → 禁止新放置/写入 → 搬迁或重建 → 校验目标和耐久性 → 按作用域更新位置或成员/所有权 → 验证无剩余责任 → 退役`。Node/Store 的跨节点退役由 Root 或所属权威 Group 记录；单个 Volume 的节点内迁出使用节点持久卷登记与任务记录，按第 5 节执行，只更新本地数据位置，不改变共识成员和 Range 所有权。操作有稳定 ID、代际、进度、失败原因和重试/取消边界；控制器换 Leader 或节点强杀后必须能续跑。未达到自动多数派安全条件时不允许仅为了完成 Drain 而删除最后一份有效数据或破坏 Raft 多数派。取消只允许在不可逆成员/身份变更之前；之后应以新的补偿操作处理。

计划内节点迁移先 Drain 再退出；突发损坏则由健康状态标记、管理员手动移除旧成员、新节点普通加入和后台自动 Repair/再平衡组成。超时不能触发移除，已标记 `Lost` 的旧盘重新出现时只能隔离检查，不能以旧身份自动恢复服务；若需取回其中数据，必须走受控导入/校验而非让其重新成为合法副本。扩容与缩容应同时检查 Root/各 Group 的多数派、Region 故障域、空闲容量、后台搬迁预算和正在运行的备份/升级任务。

## 8. 数据安全与故障策略

Region 明确退休并以同名新代际重新确认后，Range placement 只刷新同一 Range epoch、同一 Region/Group 集合的 generation；不会借刷新偷偷改变 Range Owner 或 Region 集合。

当前已落地跨 Region 数据多数的规划与显式写入原语：Root 在同一 applied cut 固定选定的 Region cohort、Node 集合及其 generation/Group epoch/fence，并持久化 Range→Region Group 映射；`rangeplacement` 后台协调器从稳定 confirmed cohort 自动生成确定性映射，不按在线数量缩小集合。Placement 再按每个 Region 的本地严格多数计算同一 RecordID 的物理覆盖，并按 Active Region 的严格多数判断 `GeoDataCoverage`，现已覆盖 1/2 节点小 Region。`StageGeo`/`Writer.WriteGeoFromRange` 可在显式选择时从该映射先写入 Owner Region，再取得固定计划中的 Region 严格多数；目标 Region 的 `GeoMetadataMirror` 账目可保存当前引用切点和本地 fence，供来源 Region 丢失后的恢复判断；`SelectWritePolicy` 明确至少三个本地 Node 时采用本地优先，小 Region 必须等待 Region 严格多数，默认本地优先写入不被悄悄改变。覆盖可绑定到单一权威引用的 RequestID、Index、Hash、Range epoch 和路由键，防止引用替换后复用旧观察；Owner Group 还能持久保存并在两种快照中校验 `GeoCoverageRecord`，引用替换/删除会原子清除。`GeoMetadataMirror` 现在可通过经认证的 capability RPC 提交到已覆盖 Region 的任一当前 voter，并等待目标 StateMachine 的持久 Result；未覆盖 Region 留在后续修复账中。`Writer.WriteGeo` 在提供该传输器时会先完成已覆盖 Region 镜像，再发布 GeoReady 账目。该账目仍只是能力中立的物理观察，不等同于完整恢复流程；日志尾部追赶和来源 Region 丢失后的重建仍需独立协议，不能因为区域字节覆盖达到多数就提前发布 `GeoReady`。

镜像欠账重试按当前 Root 计划运行：同名 Region 明确退休后重新形成时，只要 Region、Group、Owner 路由和引用仍一致，旧 generation 的欠账会自动重绑定到当前 generation/epoch/fence，再投递给新节点；不同 Region 或过期 Owner 引用继续挂起。该重绑定不改变多数门槛，也不把旧物理观察直接当作当前确认。

底座统一维护“按持久拓扑应达到的多数派目标”和“当前已证实的存放状态”两套状态。多数门槛由稳定参与集合、Active Region 和能力的权威持久性推导，健康观察只影响可选目标，不能自动缩小分母；用户不输入副本数。Group 成员、实际数据拷贝及其校验结果描述事实；计划、暂存或未同步的拷贝不算已确认。能力可声明权威持久、可由权威来源重建或纯易失，但不得把 Cache 或派生索引当作权威数据的多数派证明。

为避免“已写入”掩盖不同故障保证，底座至少区分 `Accepted`（已受理）、`Committed`（权威状态经 Group 多数派提交）、`LocalReady`（已达到本地确认门槛）和 `GeoReady`（数据已覆盖多数 Active Region，且本地门槛已满足）。同 Region 在持久拓扑中有至少 3 个合格独立 Node 时，`LocalReady` 必须是稳定参与集合的严格多数；不足 3 个时，它只表示本地已有确认的存放位置，**不**代表本地多数，也不足以向前台返回成功。Region 模式须有持久代际，不能在故障后静默切换。Group 的多数派和数据的本地/区域多数分别证明；一方满足不等于另一方满足。计入持久数据证明的实际拷贝必须经目标 Store 写入、同步、校验并由权威状态确认；每份拷贝的 Node、Store、Region、状态与确认代际可查询。修复任务幂等、可续跑，不把同一 Node 的多个 Volume 算成多个节点，也不把一个 Region 的多份拷贝算成多个 Region。

**前台确认按本地节点数确定。** 同 Region 至少 3 个合格独立 Node 时，权威 Group 提交且数据达到 `LocalReady` 后可返回成功；跨 Region 复制在后台推进到 `GeoReady`。同 Region 不足 3 个合格 Node 时，权威 Group 提交且数据达到 `GeoReady` 后才可返回成功。多 Region 的可用数量不足多数门槛时，不得静默退化为单节点确认。前一种模式下，`LocalReady` 到 `GeoReady` 之间是明确的跨 Region 数据丢失窗口：如果整个本地 Region 此时丢失，已返回成功的写入仍可能丢失。故“半数 Region 故障不丢数据”只对已经达到 `GeoReady` 的数据成立，不是所有成功写入的统一保证。

若本地参与集合有 `N` 个独立 Node，其严格多数是 `⌊N/2⌋+1`；`R` 个 Active Region 的严格多数是 `⌊R/2⌋+1`。数据确实落在相应多数位置后，单独丢失至多 `⌊N/2⌋` 个参与 Node，或至多 `⌊R/2⌋` 个 Region，仍至少留下一个有效拷贝；两类故障预算不能简单相加。这是**不丢数据**的交集性质，不保证剩余位置还能形成 Raft 或数据写入多数。特别是两个 Region 丢失一个时，数据可以仍在，但单一跨 Region Root Group 不可能在两侧都保持写入多数。

**降级可用是目标，不是“在线数量过半”的直接推论。** 例如 3 个 Region 中数据已落在 A、B，B 失联后 A、C 虽然仍是在线多数，但这份数据暂时只在 A；需要从 A 向 C 修复并完成确认，才能重新达到区域多数。期间是否可读、可写，还要分别检查数据源、Owner/Root Group 的法定多数、请求路由和当前确认规则；不满足某一条件的 Range 或能力须暂停相应操作，不能让整个集群或所有数据一概宣称可用。控制面应保留不受影响的 Range 继续服务，并在资源允许时优先修复受影响数据。

| 故障 | 允许继续服务的前提 | 必须拒绝或等待的情况 |
| --- | --- | --- |
| 单节点或 Store 失联 | 仍有合法 Owner/Group 多数派和足够的实际有效拷贝时，按当前确认模式继续服务并限速修复。 | 不能把超时拷贝或计划目标计为 Ready，也不得让旧 Owner 在新 Epoch 后确认写入。 |
| Volume 损坏或数据校验失败 | 只从已校验的其它拷贝读取并修复；损坏拷贝隔离。 | 无有效拷贝时拒绝依赖该数据的操作，不得返回未经确认的内容。 |
| Group 失去多数派 | 其他不受影响的 Group 可以继续服务。 | 该 Group 不得确认新的权威写入；需要线性一致性的读取也不得伪装成功。 |
| 整个 Region 失联 | 已达到 `GeoReady` 的数据在丢失少于区域严格多数的 Region 后仍至少保有一份有效拷贝；能否继续读写还取决于 Group/Root 多数派与当前确认模式。 | `LocalReady` 但尚未 `GeoReady` 不具备同等跨 Region 不丢保证；故障降级不得静默放宽已承诺的确认级别。 |

Root Group 负责集群目录和成员变更，必须有独立的多数派与恢复预案；Root 不可用时，不得靠本地缓存目录执行新的所有权切换或放置变更。已有数据面能否短时继续服务须由路由缓存期限、Epoch 和能力一致性要求显式定义，不能一概视为可写。任何永久失联判定、降低耐久策略、强制移除最后有效副本或跨 Region 失败转移都要有明确的授权、预检和审计路径。

`GeoReady` 的区域覆盖必须同时保护可恢复的权威元数据切点/日志尾部与其引用的数据，不能仅证明各区存有不可变字节。整个来源 Region 丢失后，远端仅有多个物理版本不足以判断当前逻辑引用；恢复与路由重新建立还须遵守新身份和 Fence 边界。当前清单、持有观察和逐条传输均不提供这一保证。

首个验收场景以单 Region、三节点为起点，覆盖任意单节点失效、一个 Store/Volume 损坏、Leader 切换、旧节点重新出现和恢复中的再次故障。多 Region 还须覆盖 `LocalReady → GeoReady` 补齐窗口、Writer Region 整体失联和 Root 多数派丢失。用户不设置副本数；本地参与集合的形成/上限、降级读写边界及目标修复时间在故障域与性能测试后定稿。在此之前不得把实验拓扑的可用性写成产品承诺。

## 9. 统一资源治理

当前首先落地 Registry 的进程内磁盘字节预约与 Node Runtime 持有的 Pack Pool：按文件系统设备共享预算，默认保留 `max(1 GiB, TotalBytes / 20)`，从健康 Active 卷中选可预约字节最多者，并列轮转。Pool 在写入前预约、完成/失败/取消后释放；读取不要求新增预约，也不为读取完成 Pending Pack 创建。写入错误不自动跨卷重试，避免不确定成功后的重复写入。该能力不是物理预分配或持久 Placement 任务；其它引擎尚未接入，外部占用与底层存储限制仍可能导致 IO 失败。CLI 容量仍是观察值，未包含预约明细；下述统一多资源治理仍是后续目标。

所有能力共享一套资源准入与计量路径，而不是各自建立互不知情的限速器。底座以 `Cluster → Node → Store/Volume` 为物理资源层，以 `Tenant → Capability` 为归属层，再按工作类别分配预算。每个请求和后台任务在消耗大量内存、磁盘、IO 或网络之前先取得预算；完成、取消或失败都必须释放。无法精确预估的流式工作按有界批次续租，禁止先无限缓存再事后计量。

| 维度 | 底座统一处理 | 上层可声明什么 |
| --- | --- | --- |
| 磁盘与容量 | Store/Volume 真实空间、安全预留、已用/预约量、Pack/索引/WAL/快照及修复临时空间；到保留线前拒绝新放置。 | 每类数据的大小预估、保留和可重建性。 |
| 内存、CPU 与并发 | 进程总预算、请求/任务内存令牌、并发槽、队列上限及超时取消。 | 单次操作成本估计和可拆分批次。 |
| 磁盘 IO 与网络 | 按 Node/Store 限制前台与后台吞吐，控制复制、备份、Compaction 和索引构建的并发。 | 工作的读写特征、预计字节数和中断/续跑能力。 |
| Tenant 与能力公平性 | Tenant 用量和上限、能力级资源池、热点监测及公平调度；一个高吞吐能力不能占满共享节点。 | 业务优先级和其自身 API 的超限提示，不得绕过底座硬上限。 |

工作类别至少区分控制面必要任务、前台读写、维持数据安全的 Repair，以及可延后的 Backup/Rebalance/Compaction/Index 等后台工作。控制面和 Repair 要有受保护的最低预算，但也有硬上限；前台优先并不意味着 Repair 可以无限期饥饿。容量不足时先暂停或退避可延后工作，再拒绝新的大写入，同时保留元数据、故障修复和安全关停所需空间。任何自动均衡都要有迟滞、每节点并发上限和跨 Region 带宽预算，避免因负载抖动反复迁移。

资源拒绝必须给出稳定的原因与可重试性，例如 Tenant 配额、节点过载、磁盘保留线、后台预算或存储不可用；客户端重试应有退避，不能把集群推入重试风暴。状态和指标至少展示按 Node、Store、Tenant、Capability、工作类别的已用/预约量、等待、拒绝、排队时长和 Repair 欠账。配额与调度参数可以渐进开放，但缺省值也必须有边界，不能以“未配置”为无限制。

## 10. 各层代码职责

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| `platform/cluster`、`platform/router` | Cluster、Tenant 目录、内部 Keyspace/Range 路由、Epoch 和 Fence | 业务值编码与索引 |
| `platform/node` | Node 进程底座装配与关闭、Root 先行启动、已确认本地 Group 恢复，以及后续加入、Drain、成员移除、身份隔离和健康观察 | 仅凭旧磁盘目录恢复已被移除的 Group，或将短暂失联直接判为永久丢失 |
| `platform/auth` | Tenant 身份、应用凭据、授权与审计归因 | 将业务方的键或路径前缀当作底座隔离层 |
| `platform/raft` | Group 生命周期、成员、日志、Snapshot | 将所有能力强制写入同一 Raft Group |
| `platform/placement` | 本地/区域多数派目标、安全预检、Repair、迁移任务与均衡 | 把尚未校验的实际拷贝当作已达到多数派 |
| `platform/resource`、`platform/task` | 统一准入、预算、公平调度、后台任务限额及可恢复进度 | 允许能力绕过物理或 Tenant 硬上限 |
| `platform/storage` | Store/Volume 的生命周期、故障隔离、空间安全预留及 Pack 通用容器生命周期 | 决定能力的载荷编码、索引和保留规则 |
| `platform/recovery` | 注册持久资源、协调 Checkpoint、整节点 Snapshot、整集群 Backup/Restore、Recovery Point、任务门禁、恢复预检和演练证据 | 按 Tenant/能力筛选备份或解析业务记录格式 |
| `capability/*` | RoutingKey、数据格式、复制/保留语义和业务数据导入/导出；持久资源接入底座登记和 Checkpoint 协议 | 自建另一套节点身份、集群目录或整集群备份系统 |

## 11. 需要在实现前定稿的参数

- Range 描述符的具体字段、各能力 RoutingKey 的编码版本、拆分规则和目录扩展方式。
- Tenant 的授权、配额、用量与删除流程；跨 Tenant 物理 Pack 混写和加密边界需在磁盘格式确定前明确。
- Group 与 Range 的映射策略，以及不同能力的数据确认级别。
- Store/Volume 数量和身份、单盘到多盘迁移与磁盘失效处理。
- Placement 的容量预留、故障域、热度指标和后台任务预算。
- Pack 的记录头、索引接口、磁盘格式、硬大小上限，以及 512 MiB 默认目标在目标机器上的性能验证。
- 注册资源的 Checkpoint/WAL 一致性协议、Cluster Restore 所支持的拓扑与格式版本、Repository 格式及密钥恢复边界。
- Node/Store/Volume 状态描述符、失联与 Lost 判定、Drain 任务状态及旧身份重现时的隔离流程。
- 本地多数参与集合的形成、扩缩与上限；Region 故障模式、Root 失联期间的服务边界及耐久性 SLO。
- 磁盘安全预留、内存/IO/网络/并发预算、Tenant/能力公平性、Repair 保底和各类过载错误的默认阈值。

这些是实现参数；Cluster、Tenant、Node、Region、Store、Volume、Range、Group、Placement、Pack 的概念边界由本文确定。
