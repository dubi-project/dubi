# Engine、Module 与 Gateway 开发规范

适用范围：DUBI 的新 Engine、新 Module、外部协议 Gateway，以及为它们接入 Runtime 所需的最小适配。新开发线程必须先读本文、相关架构和实际代码，再开始实现。

目标是交付业务方真正能使用的功能，同时复用同一个集群底座。Engine/Module 开发不以“底座已通过全部生产验收”为前提；涉及数据正确性、隔离或必要接口的阻塞必须先处理。功能开发完成、VPS 验证通过和取得生产资格是不同结论，必须分别记录。

## 1. 开始工作前

先确认当前目录、分支/worktree、工作区修改，以及本线程的任务范围。不要默认 Git HEAD 就包含其他线程已落地但尚未提交的代码；不要覆盖、清理、stash 或 reset 别人的修改。

按以下顺序阅读，相关实现和测试必须一起看：

| 阅读入口 | 需要明确的问题 |
| --- | --- |
| [项目说明](../../README.md)、[总体规划](../../../dubi.md)、[底座规格](../../spec.md) | DUBI 的目标、命名、隔离和运维约定 |
| [四层架构](../architecture/four-layer-architecture.md)、[目录职责](../architecture/layout.md) | 依赖方向、共享机制、模块组合、装配位置 |
| [底座路线图](../foundation-roadmap.md)、[验收记录](../production-readiness.md) | 已实现、已验证、待实现分别是什么；是否有正在进行的测试 |
| [Engine 数据入口](../architecture/engine-data-entry.md)、[不可变记录](../architecture/immutable-records.md) | 真实读写、引用、确认、重试和跨节点转发边界 |
| [Tenant 对外入口](../architecture/tenant-entry.md) | 应用认证、租户绑定、内部与公开接口隔离 |
| 本任务涉及的代码、测试和文档 | 当前调用链、错误模型、持久格式、缺失的接入环节 |

优先核对这些源码入口；只深读与任务相关的实现，不要求先研究全部底座：

- `internal/platform/runtimecontract/contract.go` 及其测试：Engine 面向 Runtime 的契约。
- `internal/platform/cluster/engineruntime/`、`enginestate/`、`routedstate/`：注册、确定性执行、状态事务和路由适配。
- `internal/platform/cluster/enginedata/`：已装配的通用不可变数据读写路径。
- `internal/engine/record/`：最小 Engine Handler 和编码示例；它是验证探针。
- `internal/platform/auth/`、`internal/server/public.go` 及其测试：Tenant 凭据、公开入口与协议隔离。
- `internal/server/server.go`、`internal/platform/node/runtime.go`、`internal/command/`：实际装配和调用入口。

文档可能落后于代码。用户已确定的设计约束决定目标，代码和测试证明当前实际行为；发现矛盾时说明差异并修正本任务相关内容，不能把代码缺陷视为新的设计约定，也不能把文档中的规划写成已经实现。

开始实现前，在线程中简短说明：已读入口、当前可复用能力、第一版闭环、接口缺口、准备修改的目录和验证方式。普通实现选择可直接推进；确有跨线程接口冲突或业务语义缺失时再协调，不需要每一步都等待确认。

## 2. 分层与依赖

```text
外部客户端 → Gateway → Module → Engine → Runtime
```

四层运行在同一个 `dubi server` 进程内，不拆成四个服务，不新增必须独立部署的中间件。

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| Runtime | 路由、共识、节点与区域、持久资源、数据放置、复制、修复、均衡、集群备份恢复、资源预算 | Config、Cache、Search 等业务规则 |
| Engine | 可被复用的状态、对象、日志、索引等机制，以及对应编码和执行语义 | 节点发现、Raft 调度、选主、连接池、复制和恢复编排 |
| Module | 面向业务的操作、数据模型、Engine 组合、权限范围及业务一致性 | 重复实现底座；把通用机制绑定到单一外部协议 |
| Gateway | 协议解析、认证入口、请求限制、调用 Module、响应和错误映射 | 直接访问 Pack/Pebble、绕过 Module 调用 Engine、构造 Raft 命令 |

一个 Engine 可以供多个 Module 使用，一个 Module 也可以组合多个 Engine。一个 Gateway 可适配多个 Module，同一个 Module 可以被不同 Gateway 调用。

禁止为某个 Module 复制一套 Runtime。Engine 之间不形成循环依赖；Module 组合通过明确的 Engine 接口实现，不通过互相访问内部存储完成。

### 现有代码与目标边界

`runtimecontract.Contract` 当前提供 `Resolve`、`Mutate`、`Read`，`Dispatcher` 是可注入的参考实现。不能因为接口存在，就声称真实节点的物理确认、远端转发或状态执行已经通过该接口完整接入。

新 Engine 的调用侧以逻辑 Keyspace、RoutingKey、RequestID 和业务载荷访问 Runtime；不手工构造 Owner、Fence、RouteTarget，不直接发 Group RPC。契约已有类型引用不等于授权 Engine 自行操作 Root、Raft 或 Router 内部状态。

现有 `record.Handler` 使用 `enginestate.Transaction` 等类型，这是理解实际执行边界的参考，不是鼓励所有业务层依赖集群内部实现。注册和 Handler 接入确需这些类型时，把依赖限制在窄的适配层；缺少通用 Handler/持久数据契约时，列出缺口并补最小公共接口，不能把底座实现泄漏到 Module/Gateway。

`enginedata.Service` 是受信任进程内入口，不是公开 Tenant API。不得把它或 `record` 探针包装成“任意 Tenant、任意内部命令”的公共接口。

## 3. 目录与装配

按职责放置新增代码；以下路径是约定，不表示这些包已经存在，也不要求提前创建空目录：

```text
internal/engine/<name>/        # 可复用机制、编码、确定性处理、Engine API
internal/module/<name>/        # 业务模型、操作、Engine 组合
internal/gateway/<protocol>/   # HTTP、Redis、S3 等协议适配
internal/server/              # 注册、依赖注入、监听器和进程生命周期
internal/command/             # CLI 参数与结果适配
internal/platform/            # 必要的公共 Runtime 能力
api/v1/                       # 已明确的公开协议契约，遵循现有生成流程
```

测试就近放置。按需要补充 Engine/Module 的设计说明和业务使用文档，避免为小功能生成空的接口、目录或插件框架。

首次接入应完成真实装配：构造 Engine → 构造 Module → 注册 Gateway → 接入服务启动与关闭。优先采用显式构造和注册；没有实际需求时不引入动态插件加载、全局服务定位器或反射注册系统。

包名不得和层次混淆，例如 Config Module 不等于 `platform/config` 的节点启动配置。

## 4. 公共概念与简化运维

- 统一使用 **Engine**，不重新引入 capability 或第五层同义概念。
- 对用户使用 Tenant；内部 Keyspace 保留 `(TenantID, EngineID, DatasetID)`。Module 不是新的 Runtime 路由维度。
- 不增加 Project 资源。同一 Tenant 的多个业务项目可以共享内容；业务方可自行使用前缀，或使用不同 Tenant 完全隔离。
- Module 明确自己的 Dataset、RoutingKey 编码和命名规则。多个 Module 使用同一个 Engine 时，要说明哪些 Dataset 独立、哪些数据明确共享，避免同名键意外互相覆盖。
- 任意健康节点都是业务访问入口，路由和转发由 Runtime 处理。用户不需要找 Owner、Leader 或某个特定节点。
- Pack 和 Volume 由 Runtime 统一管理，不按 Engine/Module 分目录、分 Pack 或绑定 Volume；不把同一节点的多个 Volume 当成多个故障域。
- 不引入副本数设置，不自动按在线节点缩小多数派分母。成功写入和后台跨区域复制的关系沿用底座策略，Engine/Module 不另定一套确认门槛。
- 不新增 Node Snapshot、Node Replacement、Region Rebuild 流程。上层接入正常 Join、Remove/Fence、Raft Snapshot、Repair/Rebalance 和 Cluster Backup/Restore。
- 默认运维保持简单：一个二进制、一台机器通常一个节点、身份自动生成、Store 默认 `/var/lib/dubi`。不新增逐节点手填 ID、必需 YAML 或重复传 `--store`/`--server` 的日常操作。
- 新增配置必须有实际业务或资源约束用途、合理默认值和校验；内部实现参数不得变成用户使用功能的前置条件。

## 5. Engine 开发要求

### 5.1 先明确语义，再定义接口

每项操作说明：输入与大小限制、读一致性、写入成功条件、确定性拒绝、结果不明、取消/超时、重试、版本与删除语义。小而具体的接口优先，不预先为未来全部模块设计万能 API。

以 State Engine 为例，第一版可围绕 Get、Put、Delete、单 Key 版本和 CAS 建立闭环。必须明确不存在、创建、覆盖、删除后重建时的版本规则，以及如何防止旧版本误匹配。不能把一次普通 Put 加读取包装成原子 CAS。

Watch、多 Key 事务、跨 Range 事务、TTL 等仅在任务需要时加入。声明单 Key 或同一事务边界的能力，不暗示跨 Group 原子性已经存在。

### 5.2 确定性与提交

- 同一已提交命令在所有执行节点产生相同结果；Apply 中不读取本机时钟、随机数、网络响应或可变环境作为业务决策。
- 时间、ID 等需要影响持久结果时，先定义由可信路径确定、校验并随命令提交的值；不能让客户端任意自报时间绕过过期或授权规则。
- CAS 的比较、状态变更、业务版本和幂等结果在同一个受支持的状态事务里完成。
- 稳定 RequestID 随同一逻辑请求重试。结果不明时不能生成新 ID 自动重做；相同 ID、不同内容必须遵循底座冲突规则，不能当成功。
- `AppliedIndex`、`ConfirmedIndex`、Raft term 不是业务版本。`Applied=true` 或 RPC 返回成功也不自动等于不可变数据已经满足物理多数。
- 使用实际接入路径的确认语义。涉及 Pack 引用时，遵守引用发布、真实物理持有与持久确认顺序；返回错误或 `unknown` / `published_unconfirmed` 时不能向业务宣告成功，也不能宣告“肯定未写入”。

### 5.3 数据、资源与恢复

Engine 可以定义数据编码、索引和文件格式，但物理资源必须通过 Runtime 管理的契约登记和访问。不直接在节点目录打开另一套数据库、维护另一套复制日志，或创建未纳入恢复的权威文件。

命令、状态和公开协议分别考虑版本；不要用 Go 内存布局作为持久格式。大载荷遵守现有数据通道和存储契约，不能无上限塞入 Raft 命令。格式调整要说明已有开发数据的读取、迁移或重建方式；不能静默丢弃其他线程或 VPS 上的测试数据。

后台扫描、TTL 清理、索引构建等必须有界、可取消、可恢复，并复用统一资源预算。恢复后不能凭本地空目录或空 Pack 就宣告数据就绪。

需要持久化的权威状态和资源必须进入底座恢复登记；派生数据要有明确的重建来源和水位。Engine 提供必要的恢复适配，集群备份编排不能因此开始解析业务格式。导入/导出属于对应 Module 的业务功能。

## 6. Module 开发要求

Module 暴露独立于 HTTP/Redis/S3 的进程内业务 API。它依赖所需 Engine 的明确接口，不依赖 `http.Request`、gRPC 服务对象、节点地址或底座磁盘路径。

每个 Module 至少写清楚：

1. 第一版支持的操作、限制和明确未支持的能力。
2. Tenant/Dataset/Key 的组织方式，以及共享和隔离边界。
3. 使用哪些 Engine，每份数据的权威来源。
4. 并发、版本冲突、重试和结果不明的行为。
5. 删除、过期、保留及恢复后的业务行为（任务涉及时）。

组合多个 Engine 不自动获得跨 Engine 事务。比如 Search 的原始文档是权威数据、索引是派生数据时，需要明确提交顺序、幂等更新、索引水位、部分失败恢复、查询可见性和删除传播。不能先返回成功，再仅靠一个不可恢复的内存 goroutine 完成必需步骤。

业务规则属于 Module；普适状态原语留在 Engine。只为 Config 服务的默认配置、校验规则不能塞进 State Engine；多个模块共同需要的 CAS 也不应各实现一遍。

## 7. Gateway 开发要求

- Gateway 只开放已定义的 Module 操作，负责解析、认证、授权上下文传递、大小/并发/时限限制和响应映射。
- Tenant 来自验证过的应用凭据；客户端路径、Header、请求体不能改变凭据绑定的 Tenant。任何内部路由、转发或数据访问前完成授权。
- Module 接收可信的调用身份和受约束的资源范围，不把一个客户端任填的 TenantID 参数当作已授权身份。
- 复用已有 Token 验证、撤销和授权不可确认时失败关闭的行为；Node 证书不作为普通应用 Token。
- 公开 listener 与内部 Node mTLS/Raft/管理 RPC 保持隔离，不为新协议放宽内部认证。
- 网络断开和 deadline 传递到后续调用；跨层重试不能各自重新获得完整超时时间。
- 明确映射未找到、参数错误、版本冲突、未认证、无权限、资源不足、暂不可用和结果不明。不要将它们全部映射成 500，也不要把未知结果返回成业务成功。
- 持续保留请求关联 ID 和必要的可重试信息；日志不记录 Token、密钥或完整敏感载荷，指标不使用无界业务 Key 作为标签。

先交付最小协议范围。HTTP 入口不需要先造一套通用 API 框架；实现部分 Redis 命令或 S3 操作时，明确兼容范围，不宣称完整兼容。Gateway 线程不能自行改 Module 的存储语义来适配协议。

## 8. 多线程协作

第一条 State Engine → Config Module → 最小 HTTP Gateway 可以在一个功能线程中打通，以真实接入校正公共接口。接口稳定后，其他 Engine/Module 可分线程开发；当前底座线程继续负责 Runtime 和共享集群验收。

并行开发使用独立 worktree，开始前核实它包含本任务所需的代码基线。不要把正在验证的共享工作目录当作多个线程的并行编辑区。新线程不能以“独立 worktree 的 HEAD 没有”为由再实现一套已存在但未提交的功能。

共享接口变更应记录：变更目的、调用方、输入/结果/错误语义、受影响文件、兼容方式和测试。优先增加小接口并保持现有调用方可用；涉及确认策略、持久格式、Root/Group 协议或正在验收的行为时，由底座线程统一集成。需要协调不等于停止所有独立开发。

不要同时改别的线程负责的实现。明确约定接口后，可以用测试替身独立实现调用方，但交付时必须有真实适配验证；只有 mock 通过不能算集成完成。

未经当前任务授权，不自行提交推送、部署覆盖共享 VPS、启动新一轮故障注入或创建定时任务。已有授权继续有效，不重复索要已经给出的许可。

## 9. 测试与验收

当前项目约定：运行测试使用 VPS，开发机仅做阅读、静态检查、编译/交叉编译和证据分析。新线程先确认当前约定和可用测试环境，不能直接照旧 README 的本地集群脚本启动测试。

共享 VPS 可能正在固定版本长测。先从最新验收记录和实际进程确认占用；不得覆盖运行中的二进制、重启节点、注入故障、改成员或清理测试目录。单元测试也要在独立目录和端口中执行，限制 CPU、内存、磁盘和时长，不干扰正在运行的负载。没有合适环境时，明确说明未执行项，不能把交叉编译成功写成测试通过。

按实际变更选择测试，不要求每个小改动都重新跑六小时，也不为可逆的文档/外观修改建立复杂测试体系：

| 范围 | 最小有效验证 |
| --- | --- |
| Engine | 确定性、编码边界、版本/CAS 竞争、重复请求和不同内容冲突、删除/不存在、取消与错误传播 |
| 持久状态 | 真实重启/恢复后的数据、版本和幂等行为；涉及格式时验证旧数据处理 |
| Module | 业务规则、Tenant/Dataset 隔离、共享数据约定、多 Engine 部分失败（若涉及） |
| Gateway | 公开协议、凭据撤销/伪造 Tenant、响应语义、输入限制、取消、不能调用内部 RPC |
| 完整链路 | 真实 Gateway → Module → Engine → Runtime；从不同健康节点访问同一业务数据 |
| 故障相关改动 | 获得测试环境协调后验证丢响应、Owner/节点故障与安全重试，不重复发明故障编排 |

如果依赖异步复制或索引，不把“后台队列瞬时非零”直接等同于失败，也不能无限放行。预先定义可观测的完成条件、等待上限和硬故障条件；保留初始失败/降级样本，不为了通过而删证据、延长原轮次时限或修改原结果。

## 10. 交付标准

一个功能闭环完成必须同时具备：

- 实现与进程装配真实存在，能经已定义入口使用；不是只有类型、mock、未注册 Handler 或占位路由。
- 核心业务语义、Tenant 隔离、必要的并发/幂等和错误处理经过对应验证。
- 所有权威数据有明确持久化与恢复归属；没有游离于底座之外的存储或后台队列。
- 有可执行的使用示例，标明需要的最少初始化步骤、默认行为、限制和兼容范围。
- 文档与实际代码一致，说明改动、验证环境/结果、未验证内容、接口变更和后续事项。

不要把“State/Config 首版可开发、可运行”升级为“底座或模块已经生产可用”。性能报告写清楚负载、入口、样本量和环境；当前秒级跨区域读写结果不能作为高性能缓存已经达标的证据。

当前建议的首个闭环：State Engine 的 Get/Put/Delete/版本/单 Key CAS → Config Module → 带 Tenant Token 的最小 HTTP Gateway。目标是任意节点访问、跨 Tenant 拒绝、并发修改不静默覆盖、重启后数据和版本仍可读取。Watch、Schema Engine、跨 Key 事务、Redis/S3 兼容等不自动成为这一版的前置任务。

## 11. 新线程启动说明模板

将下面的任务目标替换为本线程实际范围即可：

> 本线程负责：填写 Engine / Module / Gateway 名称和第一版目标。先阅读 `docs/development/engine-module-gateway.md`，再阅读其中要求的架构、相关源码和测试。核对工作分支/worktree、未提交改动和代码基线，区分已实现与规划能力。简短列出复用路径、接口缺口、最小闭环及验证计划，然后推进实现。遵守四层边界、Tenant 隔离、简化运维和 VPS 测试约定；不要重建底座，不要覆盖共享长测环境。与其他线程协调公共接口，普通实现不需要逐步等待确认。完成后提供改动与测试清单、使用方式、未验证项及下一步。
