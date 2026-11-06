# Engine、Module 与 Gateway 开发规范

适用范围：DUBI 的新 Engine、新 Module、外部协议 Gateway，以及为它们接入 Runtime 所需的最小适配。新开发线程必须先读本文、相关架构和实际代码，再开始实现。

目标是交付业务方真正能使用的功能，同时复用同一个集群底座。Engine/Module 开发不以“底座已通过全部生产验收”为前提；涉及数据正确性、隔离或必要接口的阻塞必须先处理。功能开发完成、VPS 验证通过和取得生产资格是不同结论，必须分别记录。

## 1. 开始工作前

先确认当前目录、分支/worktree、工作区修改，以及本线程的任务范围。不要默认 Git HEAD 就包含其他线程已落地但尚未提交的代码；不要覆盖、清理、stash 或 reset 别人的修改。

按以下顺序阅读，相关实现和测试必须一起看：

| 阅读入口 | 需要明确的问题 |
| --- | --- |
| [项目说明](../../README.md)、[总体规划](../../../dubi.md)、[底座规格](../../spec.md) | DUBI 的目标、命名、隔离和运维约定 |
| [数据寻址与隔离](../architecture/data-addressing.md) | Dataset 取消、Tenant/Module/Engine 身份绑定、Range 与 Pack 的边界及代码迁移现状 |
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

服务端使用 `enginedata.Service.Bind(module, engine)` 或 `runtimecontract.Bind(...)` 固定身份，再用已授权 Tenant 取得绑定接口。Engine 提交 Key、RequestID 和载荷，Runtime 自动准备初始 Range；Module 不接触 Dataset/Keyspace/Range。底层显式 Keyspace 的 Contract 仅供 Runtime 适配。通用状态 Dispatcher 仍是参考实现，具体 Engine 必须接通真实后端；不可变数据入口已有 Node 适配。Owner、Fence、RouteTarget 和 Group RPC 留在 Runtime 内部。

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
- 取消 Dataset 作为独立资源和使用前置步骤。Keyspace 只是内部地址空间；Runtime 按 `Tenant + Module + Engine + Key` 编码地址。旧 Dataset 格式明确拒绝自动打开；详见数据寻址文档中的兼容边界。
- 不增加 Project 资源。同一 Tenant 的多个应用可在授权范围内共享同一 Module 的数据；不同 Tenant 或不同 Module 的数据独立，即使使用相同 Engine。
- Module 只使用已绑定的 Engine API；Module 身份由路由/装配确定，Engine 身份由服务端依赖确定，客户端不能覆盖。Module 内部类型可采用稳定业务键编码，不新增可管理存储容器。
- 任意健康节点都是业务访问入口，路由和转发由 Runtime 处理。用户不需要找 Owner、Leader 或某个特定节点。
- Range 是逻辑区间；Pack 是本地记录聚合文件，一个 Pack 可混写多个 Range，一个 Range 可跨多个 Pack/Volume。Runtime 统一管理位置，不按 Engine/Module 分目录、分 Pack 或绑定 Volume；多卷不增加故障域。
- 不引入副本数设置，不自动按在线节点缩小多数派分母。成功写入和后台跨区域复制的关系沿用底座策略，Engine/Module 不另定一套确认门槛。
- 不新增 Node Snapshot、Node Replacement、Region Rebuild 流程。上层接入正常 Join、Remove/Fence、Raft Snapshot、Repair/Rebalance 和 Cluster Backup/Restore。
- 默认运维保持简单：一个二进制、一台机器通常一个节点、身份自动生成、节点元数据目录 `--store` 默认 `/var/lib/dubi`。一个 Node 管理多个 Volume；未显式指定初始卷时登记 `<store>/volume`，显式指定时仅登记指定卷。后加卷保留原卷，重启不自动补建丢失卷；StoreID 仅作内部存储实例校验，不新增 Store 管理资源。不新增逐节点手填 ID、必需 YAML 或重复传 `--store`/`--server` 的日常操作。
- 新增配置必须有实际业务或资源约束用途、合理默认值和校验；内部实现参数不得变成用户使用功能的前置条件。

## 5. Engine 开发要求

### 5.1 先明确语义，再定义接口

优先围绕 State/Record/Blob/Index 随业务落地；TimeSeries、Trace、Bus 均为 Module，组合这些 Engine，不为业务模块一一增加 Engine。Schema、签名/加密和业务调度先用普通库或 Module 逻辑；不能因规划表或旧占位目录就提前创建 EngineID、专用存储和生命周期。节点 PKI、后台修复调度保持 Runtime 所有。

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

物理复制、持有校验、丢失/损坏数据修复和再平衡全部属于 Runtime；Module 根据权威数据重建业务派生结果属于业务流程，两者不能混用。Engine 可以定义数据编码、索引和文件格式，但物理资源必须通过 Runtime 管理的契约登记和访问。不直接在节点目录打开另一套数据库、维护另一套复制日志，或创建未纳入恢复的权威文件。

命令、状态和公开协议分别考虑版本；不要用 Go 内存布局作为持久格式。大载荷遵守现有数据通道和存储契约，不能无上限塞入 Raft 命令。格式调整要说明已有开发数据的读取、迁移或重建方式；不能静默丢弃其他线程或 VPS 上的测试数据。

后台扫描、TTL 清理、索引构建等必须有界、可取消、可恢复，并复用统一资源预算。恢复后不能凭本地空目录或空 Pack 就宣告数据就绪。

需要持久化的权威状态和资源必须进入底座恢复登记；派生数据要有明确的重建来源和水位。Engine 提供必要的恢复适配，集群备份编排不能因此开始解析业务格式。导入/导出属于对应 Module 的业务功能。

## 6. Module 开发要求

Module 暴露独立于 HTTP/Redis/S3 的进程内业务 API。它依赖所需 Engine 的明确接口，不依赖 `http.Request`、gRPC 服务对象、节点地址或底座磁盘路径。

每个 Module 至少写清楚：

1. 第一版支持的操作、限制和明确未支持的能力。
2. Tenant + Module 的隔离、业务 Key/记录类型及所用 Engine；Module 不负责 Dataset、Keyspace 或 Range。
3. 使用哪些 Engine，每份数据的权威来源。
4. 并发、版本冲突、重试和结果不明的行为。
5. 删除、过期、保留及恢复后的业务行为（任务涉及时）。

组合多个 Engine 不自动获得跨 Engine 事务。比如 Search 的原始文档是权威数据、索引是派生数据时，需要明确提交顺序、幂等更新、索引水位、部分失败恢复、查询可见性和删除传播。不能先返回成功，再仅靠一个不可恢复的内存 goroutine 完成必需步骤。

业务规则属于 Module；普适状态原语留在 Engine。只为 Config 服务的默认配置、校验规则不能塞进 State Engine；多个模块共同需要的 CAS 也不应各实现一遍。

## 7. Gateway 开发要求

HTTP Gateway 同时承载管理 API、业务 API、监控入口和内嵌 Web UI，具体约定见 [HTTP Gateway 架构](../architecture/http-gateway.md)。业务请求沿 Module → Engine → Runtime；管理 API 调用经过授权的 Runtime 管理服务，监控使用只读接口，均不得直接操作底层存储。

公共入口应按架构文档第 8 节的工作包交付：入口配置与证书生命周期、管理登录与监控权限、只读管理 API、内嵌 UI、Go SDK、管理写操作。证书热加载不等于自动续期，探针 200 不等于控制台或 SDK 可用。浏览器、SDK 和监控共用公共 HTTPS，不要求它们取得内部 Node 证书；每一类客户端按自身契约完成真实联调。

- 业务 Gateway 只开放已定义的 Module 操作，负责解析、认证、授权上下文传递、大小/并发/时限限制和响应映射。
- Tenant 来自验证过的应用凭据；客户端路径、Header、请求体不能改变凭据绑定的 Tenant。任何内部路由、转发或数据访问前完成授权。
- Module 接收可信的调用身份和受约束的资源范围，不把一个客户端任填的 TenantID 参数当作已授权身份。
- 复用已有 Token 验证、撤销和授权不可确认时失败关闭的行为；Node 证书不作为普通应用 Token。
- 公开 listener 与内部 Node mTLS/Raft/管理 RPC 保持隔离，不为新协议放宽内部认证。
- 网络断开和 deadline 传递到后续调用；跨层重试不能各自重新获得完整超时时间。
- 明确映射未找到、参数错误、版本冲突、未认证、无权限、资源不足、暂不可用和结果不明。不要将它们全部映射成 500，也不要把未知结果返回成业务成功。
- 持续保留请求关联 ID 和必要的可重试信息；日志不记录 Token、密钥或完整敏感载荷，指标不使用无界业务 Key 作为标签。

Go SDK 与 Web UI 共用 Module 的公开 HTTP 契约，SDK 接入和全局数据权限约定见 [HTTP Gateway 架构](../architecture/http-gateway.md)。新增业务 API 应同时考虑 SDK 调用与 UI 所需的列表、分页和错误语义；管理权限不自动授予 Tenant 数据权限，跨 Tenant 访问须经显式授权。

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
| Module | 业务规则、Tenant/Module/Engine 隔离、同一 Module 内应用的数据访问约定、多 Engine 部分失败（若涉及） |
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

> 本线程负责：填写 Engine / Module / Gateway 名称和第一版目标。先阅读 `docs/development/engine-module-gateway.md`，再阅读其中要求的架构、相关源码和测试。核对工作分支/worktree、未提交改动和代码基线，区分已实现与规划能力。简短列出复用路径、接口缺口、最小闭环及验证计划，然后推进实现。遵守四层边界、Tenant/Module 隔离、服务端身份绑定、简化运维和 VPS 测试约定；不要重建底座，不要覆盖共享长测环境。与其他线程协调公共接口，普通实现不需要逐步等待确认。完成后提供改动与测试清单、使用方式、未验证项及下一步。

## 12. Engine 与 Module 分阶段规划（2026-11-06）

本节是下一阶段的实施建议，不表示下列业务功能已经实现，也不是一次性批准开发全部模块。基础稳定性已取得一轮三区域九节点六小时验证，见[验收记录](read-discovery-soak.md)。现在按业务闭环推进；内存趋势等待查项单独跟踪，不自动成为每个模块开发前必须解决的任务。早期总规划中的完整生产资格要求，不作为首版功能交付的前置清单。

### 12.1 当前可复用能力与真实缺口

| 现状 | 下一阶段如何使用 |
| --- | --- |
| `enginestate.Transaction` 已有 Get/Set/Delete、事务提交和幂等结果；`engineruntime.Registry` 已能注册确定性 Handler | 复用执行机制；业务版本、CAS、删除后重建语义由 State Engine 定义，不能把底座已有事务类型称为业务事务已完成 |
| `enginedata.Service.Bind(...).ForTenant(...)` 已接通不可变字节、引用、物理多数与后台复制 | 复用真实持久化路径；新增状态操作不能绕过已有确认门槛 |
| `runtimecontract` 有绑定的 Read/Mutate 参考适配 | State 首版通过真实 `enginedata.Binding` 的 Write/Read/Query 接入，通用 Dispatcher 仍是参考实现 |
| `internal/engine/record` 是验证 Handler，其他旧 Engine 目录多为占位 | 不把占位目录当成果；保留探针，不将其直接改名冒充正式 State 或 Record Engine |
| 公开 HTTP 已有认证、监控及 Config 首版入口 | Config 复用 Tenant 凭据，使用 State 与真实 Runtime；接口及限制见第 13 节 |
| 邻近 `dubi-go` 仓库已有应用框架、注册和 BUS 等代码 | 不视为新的 HTTP 数据客户端已完成；客户端采用独立包增量接入，不覆盖既有框架 |

State 接入首先补齐的是**绑定身份的真实状态执行与结果读取**：比较版本、修改值或引用、记录业务版本及幂等结果在同一受支持的 Owner 状态事务内完成；读取必须返回同一权威版本的值和元数据。不得在客户端用 Get 后 Put 模拟 CAS，也不得先独立读版本再拼接另一版本的值。多区域的字节持久确认不等于允许各区域独立竞争修改同一个 Key；请求由 Runtime 路由到当前权威 Owner 进行串行判定，Engine 不新增自己的选主或多主冲突规则。

### 12.2 核心 Engine

按实际模块需要建设 State/Record/Blob/Index 四类。TimeSeries 是 Module，不设独立 TimeSeries Engine。以下均为职责规划，不预先创建空包或所有接口：

| Engine | 核心职责 | 首次交付范围 | 随调用方再增加 |
| --- | --- | --- | --- |
| State | 小型可变状态与原子条件更新 | Get、Put、Delete、单 Key 业务版本、仅不存在时创建、CAS、稳定请求 ID 重试 | 有界前缀扫描/分页、过期原语、Watch、受限多 Key 事务 |
| Blob | 大字节内容的持久读写与校验 | 有界流式写入、读取及范围读取、长度/摘要校验、内容引用生命周期 | Multipart、断点续传、按需求优化分块和去重 |
| Record | 有序追加和可恢复读取 | 按逻辑流追加、流内 Offset、从 Offset 有界读取、幂等追加、保留边界 | 批量追加、长期重放优化、更多订阅机制 |
| Index | 派生检索结构与查询 | 文档版本对应的倒排索引、基本文本查询/过滤、索引水位、可恢复构建与重建 | 排序、聚合、复杂分析、向量等有明确需求的能力 |

State 是需要 KV/可变状态的模块的基础，但不要求对象数据体、日志正文、倒排索引或时序点全部变成 State KV。各 Engine 共用 Runtime，不再建立各自的集群、目录或复制系统。Engine 负责通用数据编码与访问机制，Module 定义业务数据模型；Range/Pack/Volume 仍由 Runtime 自动管理。

Record 是有序追加机制的统一名称，Blob 是大字节内容机制的统一名称；Log 保留为日志 Module 名称。当前 `internal/engine/record` 只是验证 Handler，不代表正式 Record Engine 已实现。Trace 与 TimeSeries 是明确要做的后续 Module，组合既有 Engine，不阻塞前面的模块。Schema 校验、签名、加密、延迟调度先作为共享库或业务逻辑，不因名称增加新的底座概念。

当前规划没有必须增加的第五类 Engine。向量检索先视为 Index 的扩展，工作流、锁、计数、调度先组合现有机制；只有出现可复用且无法合理归入四类的独立存储或执行机制，才单独评估新增 Engine。业务名称本身不是新增 Engine 的依据。

### 12.3 模块与依赖

| Module | 使用的 Engine | 交付给业务方的能力与边界 |
| --- | --- | --- |
| Config | State | 按 Key 保存配置、读写删除、版本冲突检查；后续再加订阅通知和配置历史 |
| Session | State | 创建、读取、续期、撤销和过期；业务会话与节点身份/控制台登录不是同一功能 |
| Cache | State | Get/Set/Delete、TTL、条件写；容量与淘汰策略属于 Cache，不能误淘汰 Config/Session 数据 |
| KV（按需） | State | 面向应用的通用 KV API；不是 Config 的开发前置，不与 Record Engine 混名 |
| File | Blob + State；检索时再加 Index | 对象内容与元数据、业务 Key、版本和删除；目录、ACL 和 S3 兼容按实际需求扩展 |
| Bus | 请求/订阅/投递业务逻辑；持久部分使用 Record + State | 统一 req/reply、pub/sub、queue；队列支持领取、确认、可见性超时和重投，首版至少一次投递，不承诺端到端 exactly-once |
| Search | Index + State，原文较大时用 Blob | 自有文档集合的写入、查询、删除和索引重建；集合是业务模型，不转化为 Dataset 或手动 Range |
| Log | Record + State，全文检索时加 Index，大字段按需用 Blob | 日志采集、按时间/游标读取、保留和检索 |
| 时序数据（TimeSeries，含 Metric 场景） | Record + State；时间/标签检索按需使用 Index | 明确后续交付：序列/标签、时间点写入、时间范围查询、基础聚合与保留；不仅限于服务监控指标 |
| Trace | Record + State，属性检索使用 Index，大载荷按需 Blob | 明确后续交付：Span 接收、按 TraceID 查询、父子关系、时间线与属性检索；不新增 Trace Engine |
| Token 等 | 根据已确定的业务需求组合 | 保留方向，当前不启动专用 Engine、协议或预置空模块 |

Config、Session、Cache 即使共用 State 实现，数据仍按 **Tenant + Module** 独立。一个 Module 同时用多个 Engine 时，这些 Engine 都绑定该 Module；客户端不能覆盖绑定身份。

**Bus 划分已明确**：请求应答、发布订阅和队列都是同一个 Bus Module 的能力，使用 Tenant + Bus 的归属，内部以稳定记录类型编码区分消息、订阅和消费状态，避免同名业务 Key 冲突。当前不单独建设 Event/PubSub/Queue Module；只有后续独立需求确实需要时才考虑，否则保持集成。

`dubi-go` 的 Request/Broadcast/Dispatch/Enqueue 可以继续作为统一 Bus 调用入口，服务端接入仍需真实实现。`dubi-ex` 的 event、queue 扩展允许应用按需接入外部服务，属于可选应用适配，不意味着 DUBI 服务端也必须提供同名独立模块。内置 Bus 不依赖这些外部服务；外接后端的持久、确认及重试语义按相应适配契约说明。

File 自带文件检索、Log 自带日志全文检索时，直接使用各自绑定的 Index Engine，不必绕一圈调用独立 Search Module。独立 Search 不天然拥有其他模块的数据；未来统一检索必须通过明确授权的业务接口及更新/删除传播机制接入，不能读取别的 Module 的裸存储范围。

### 12.4 分阶段交付及停止条件

建议顺序如下。完成一个阶段就交付其可用功能，不等整张表完成，也不在当前阶段顺手加入后续项。

| 阶段 | 本阶段交付 | 完成后可以做什么 | 明确留到以后 |
| --- | --- | --- | --- |
| M1 | State v1 + Config v1 + 最小 HTTP 业务 API + 对应 Go 客户端调用 | 经任意健康节点读取、创建、修改和删除配置；并发修改检测版本冲突；重试及重启后结果一致 | Watch、列表扫描、TTL、跨 Key/跨 Range 事务、配置历史、完整管理 UI |
| M2 | State 过期原语 + Session；随后 Cache v1 | 会话创建/续期/撤销；缓存 TTL 与条件写；验证两个 Module 同名 Key 不串数据 | Redis 兼容、复杂数据结构、全局淘汰排名；不能把秒级路径直接称为低延迟缓存 |
| M3 | Blob v1 + File v1 | 流式上传/下载、范围读取、元数据版本和删除 | 完整目录/ACL、Multipart、断点续传、完整 S3 协议 |
| M4 | Bus v1；持久功能配套 Record v1 + State | 分步交付请求应答、发布订阅和持久队列；队列支持领取/确认、超时重投、重启后继续消费 | 全局消息顺序、任意跨流事务、复杂工作流、独立 Message/Event/PubSub/Queue 模块体系 |
| M5 | Index v1 + Search；随后接入 Log Module 的检索 | 文档/日志可检索，显示索引可见水位，可恢复更新并从原文重建 | SQL 通用查询、复杂聚合、向量检索 |
| M6 | TimeSeries Module；组合 Record + State，按需 Index | 时间点写入、序列/标签过滤、时间范围读取、基础窗口聚合和保留 | 完整监控查询语言兼容、复杂降采样、全套外部协议 |
| M7 | Trace Module | Span 接收、按 TraceID 查看链路及父子关系、基础属性查询和保留 | 全套采样系统、服务拓扑分析、所有追踪协议兼容 |

M2 完成后，M3 与 M4 在共享接口稳定时可分别开线程；默认上述顺序便于逐项交付，不代表 File 必须依赖 Session/Cache。M5 只依赖它实际选用的原文存储能力，不依赖 Bus 的业务完成。Redis、S3、日志采集等 Gateway 都随已有模块增加，不能反过来要求一次实现整个协议生态。

M6/M7 是明确的后续交付项，不再归为“有需要再考虑”。编号表示建议排期，不表示 Trace 必须等待时序模块完成；Trace 可在 Record/State 及其实际需要的 Index 接口就绪后推进。

M4 内部按请求应答 → 发布订阅 → 持久队列逐步交付，共用同一 Bus Module 和入口，不拆成三个前置模块。即时消息部分不必等待 Record 完成；可靠事件/持久队列再接入 Record + State，并分别验收持久与重试语义。节点间传输复用 Runtime 的受控业务通信能力，业务进程通过 Gateway 注册服务或订阅，不需要成为集群节点，也不能借此使用内部 Node 凭据。

M1 的值先作为有大小上限的不透明 bytes，由 Config API 定义内容类型与编码；JSON Schema 不成为前置。版本必须是 Engine 管理的不透明业务版本，重试不生成新版本，删除后重建不能让旧 CAS 重新匹配；不把 Raft Index 当业务版本。具体的删除代际保留与回收方案在 State 设计中落定，不能先把删除版本清零。

第一阶段不提供列表的限制应公开写明；最小示例按明确 Key 操作。前缀扫描/分页在配置浏览页面实际接入时补齐，必须有条数/字节上限，不能全量拉到 Gateway 做排序或模拟分页。Watch 另行定义断线续订和过期游标，不混入第一次 Get/Put 交付。

Go 客户端只覆盖实际已交付的 Config 操作和共同错误/重试约定。建议在 `dubi-go` 增加独立 `client` 包，开工时核对该仓库规范与已有命名；不改写现有应用框架。HTTP 契约可供 SDK 与未来 Web UI 共用，完整管理 API/控制台不是 M1 的前置。

### 12.5 后续阶段必须先说清的业务语义

- **过期**：Module 决定 TTL/续期策略，State 提供复用原语；过期逻辑不可见与物理清理分离。旧清理任务不能删除已续期的新版本。提交中携带可信时间依据，Apply 不自行读取本机时钟决定结果；实现时明确时钟误差与过期精度边界。
- **File**：Blob 内容与 State 元数据不是跨 Engine 原子事务。先准备可校验内容，再受条件控制地发布权威元数据/引用；中断后的暂存与孤立内容有明确生命周期，物理回收遵守 Runtime 引用与保留约束，不能靠内存异步任务或直接删除 Pack 收尾。
- **Bus**：请求应答、发布订阅和队列分别说明提交、持久化、处理完成的边界；无订阅者、离线、断连与超时的行为不得混用。队列顺序仅在约定逻辑流内成立，Offset 不当作全局序号。消息追加和消费状态组合必须能从部分失败恢复；同一入队请求不重复生成消息，过期领取的旧确认不能确认后来重新领取的任务，消费者仍要处理重复投递。
- **Search / Log 检索**：原文是权威数据，索引是派生数据。更新任务可恢复、按版本幂等，删除和保留期变化能够传播；索引落后时明确返回水位或按契约等待，不能把未索引完的查询当完整结果。

这些语义只在对应阶段实现，不提前为全部模块设计万能事务、任务调度器或存储接口。

### 12.6 M1 的有限验收清单

完成以下事项即交付 M1，后续优化另记，不自动追加长测：

1. 服务启动真实装配 State → Config → HTTP；Go 客户端可完成同一组操作，用户无需创建 Dataset、指定 Range 或选择 Owner。
2. Get/创建/覆盖/删除/未找到行为明确；两个并发相同版本 CAS 只能有一个成功；删除后重建拒绝旧版本。
3. 同一请求 ID 和内容重试返回同一结果，不同内容冲突；丢响应后用原 ID 重试，不把结果不明当作未写入。
4. 不同 Tenant 与不同 Module 的同名 Key 隔离；伪造身份、撤销凭据和越权请求拒绝。
5. 从不同健康节点访问同一数据，真实重启后值、业务版本和必要去重信息仍正确；确认路径不绕过 Runtime。
6. 明确输入/结果/并发上限，记录一次目标负载下的时延和资源数据；没有达成的性能目标如实列限制，不扩成无期限调优。
7. 给出最小使用示例、接口/格式说明及验证证据，标明未实现能力。

运行验证仍在 VPS 隔离环境进行。正常 Engine/Module 开发使用相关单元、集成及有界业务回归；重大共识、持久格式、确认或恢复路径改动才评估专项故障验证/长测。发现数据错误、隔离破坏或使本阶段不能正常使用的缺陷时修复；纯优化进入待办，不以“还能改进”为由延后交付。

### 12.7 全文搜索的 Engine / Module 分工

Search 是业务 Module；State 与 Index 是职责不同的 Engine。全文搜索不放进 State，也不要求为了搜索新增一种集群存储容器。

| 层/组件 | 搜索场景中的职责 |
| --- | --- |
| Search Module | 定义文档 ID、业务集合和字段规则、选择分析方式、写入/更新/删除与查询 API、权限、索引可见性以及返回结果；编排多个 Engine 的操作 |
| State Engine | 保存文档当前版本、删除标记、原文引用、字段配置、待索引状态及构建进度；小文档首版可将原文与元数据放在同一受限记录中 |
| Blob Engine（按需） | 保存较大原文或附件的字节内容，返回可校验的引用；不理解检索规则 |
| Index Engine | 执行选定的文本分析/分词、构建倒排及必要字段索引、匹配与评分、返回文档 ID/版本等命中信息；管理自身构建水位和索引代际 |
| Runtime | 自动寻址、节点间转发、共识、持久化、复制、修复和资源治理；不负责分词或搜索业务规则 |

Search 的索引配置选择属于 Module，通用分析与查询执行属于 Index。Index 不要求把每条倒排记录通过 State 对外 KV 接口保存，也不能自己打开脱离 Runtime 管理的权威数据库或文件目录。

建议首版流程：

1. Search 校验已授权 Tenant、文档和字段。小原文与业务版本、待索引标记在同一受支持状态记录内原子发布；大原文先通过 Blob 写入，再发布其引用及版本。不能假定跨 Engine 事务已存在。
2. 可恢复任务按文档版本更新 Index；旧任务不得覆盖新版本或撤销删除。待索引状态必须持久化，不只放在内存，也不要求先完成 Bus 才能做 Search。
3. 原文提交成功与该版本可被搜索区分表示；查询按契约返回索引可见进度，或在限定时间内等待指定版本可见。水位按约定索引/范围及代际定义，不能假装所有 Range 共用一个全局事务版本。
4. 查询命中关联文档 ID 和版本，按约定读取对应原文；不可把旧索引命中与另一版本的原文无说明地拼接。删除标记参与查询可见性判断，后台再清理索引和满足回收条件的旧数据。
5. 索引可从权威原文、版本和删除状态重建。首版只交付文本匹配、必要字段过滤和有界分页，复杂排序/聚合后续再加。

File、Log、Trace 需要自己的检索时，可复用 Index Engine 实现，并固定绑定各自 Module。独立 Search Module 管理自己的文档，不能借复用 Engine 直接访问别的 Module 的原文。

### 12.8 Trace 与时序模块的后续边界

TimeSeries 是时序数据 Module，面向“序列标识/标签 + 时间戳 + 值”，用于服务指标、业务统计和设备采样等场景。Module 定义数据模型、重复时间点处理、乱序/更正边界、窗口聚合、降采样及保留策略；Record 保存采样记录，State 管理序列配置、元数据及必要聚合状态，按需使用 Index 支持时间/标签定位，大载荷按需使用 Blob。通用排序、过滤和编码机制随实际需求补入既有 Engine，不新增 TimeSeries Engine。

Record 的追加 Offset 不等于事件时间。时序阶段必须定义有效的序列与时间范围访问路径，不能靠扫描全部记录完成每次查询；乱序、更正及过期数据的索引更新同样需要可恢复。首版明确时间单位、精度、标签基数限制和删除/保留行为，不把简单按时间拼 KV Key 当成已交付完整时序能力。

Trace Module 定义 TraceID/SpanID、父子关系、开始/结束时间、状态与属性；Record 保存权威 Span 内容，State 保存必要元数据和查询引用，Index 支持属性检索。到达顺序不等于调用顺序，查询要容忍 Span 乱序、重复和不完整链路，并明确后续补到数据的可见性。首版可先支持按 TraceID 查询，不强制所有检索/采样能力同时完成。

Trace、Log、时序数据可以用业务字段建立关联，但各自仍遵守 Tenant + Module 隔离。跨模块派生指标或关联查询通过明确授权的业务接口实现，不共享底层 Key。上述是后续产品范围，不追加为当前 State/Config 阶段的验收项。

## 13. State / Config 首版实现

实现入口：`internal/engine/state`、`internal/module/config`、`internal/gateway/httpapi`；公开 HTTP 类型位于 `api/v1/config.go`。默认命令装配同时注册开发探针和 State Handler，服务装配固定 Config Module + State Engine，Tenant 来自已验证的应用 Token。模块不接受客户端提交的 Module/Engine/Range 身份。

### 13.1 State 语义与持久化

- 支持 Get、Put、Delete，Put 支持无条件覆盖、仅不存在时创建、指定业务版本 CAS；Delete 支持无条件或指定版本。版本是不透明 SHA-256 标识，绑定规范化操作、逻辑 Key 和 RequestID，比较仅在同一 Tenant/Module/Key 中有效，不使用 Raft Index。
- Key 最大 2048 字节，Value 最大 64 KiB，空值有效。Config Key 使用非空 UTF-8 字符串。删除写入有版本的墓碑；重建使用新 RequestID，旧版本不会重新匹配。删除不存在的 Key 也持久保存该操作的墓碑与结果。
- 比较条件、更新当前版本、登记操作结果和发布 Pack 引用，在 Owner 的同一个状态事务内完成。冲突也有持久业务结果，不改变当前值；Runtime 命令已应用不代表业务成功。
- 值和版本共同编码进一份不可变 Pack 内容，经过现有 Write/Read 的物理多数、区域确认及后台复制路径；读取从同一已校验内容取得版本和值，不拼接两次读取。首版为严格校验 Publication 描述，受限的小值也随 Engine 命令编码；这不是大对象接口，后续优化不能取消物理确认。
- RequestID 在 Tenant + Module + Engine 内唯一。同 ID、同内容重试保持原结果；同 ID 修改 Key、值或条件会冲突。覆盖前保留旧操作已完成的持久确认事实，允许重试历史成功操作，**不会回写旧值**。`replayed=true` 是原操作回执，不是对旧数据当前仍存在或仍有多数持有的声明。
- 已发布但尚未确认的操作若被另一操作覆盖，不能从已应用结果推断成功，仍返回 `outcome_unknown`。客户端不能据此认定未写入或换 RequestID 自动重复执行。Get 始终按当前引用重新验证物理数据；成功操作回执不替代 Get。
- 当前版本元数据、墓碑和幂等回执进入通用 Engine 持久状态与快照，Pack 继续由 Runtime 备份恢复。首版不自动回收墓碑/操作回执；去重保留期、通用多区间拆分时关联元数据的迁移另行定义，不能提前删回执或重置版本。

### 13.2 Config HTTP 使用

按现有方式登记 Tenant、创建应用 Token，并在需要提供业务访问的节点启用 `dubi server --api-listen <地址>`。listener 继续独立于 Node mTLS RPC。客户端验证集群 CA 与服务端证书，使用 `Authorization: Bearer <Token>`，不需要 Node 客户端证书。

所有操作使用 `/api/v1/config/entry?key=<URL 编码的配置 Key>`：

| HTTP 方法 | 请求 | 成功响应 |
| --- | --- | --- |
| GET | 无请求体 | `version`、`value`；值按 JSON bytes 编码（base64，空值也可表示为 null） |
| PUT | `request_id`、base64 `value`；可选 `if_absent: true` 或 `version`，两者互斥 | `request_id`、`version`、`deleted: false`、`replayed` |
| DELETE | `request_id`；可选 `version` | `request_id`、`version`、`deleted: true`、`replayed` |

例如，仅在配置不存在时创建：

```json
{"request_id":"config-create-001","value":"ZW5hYmxlZA==","if_absent":true}
```

其中值为 `enabled`。更新时传 GET 返回的 `version` 和新的 `request_id`；网络中断后重试必须保留原请求。没有条件字段的 PUT 表示主动覆盖，不提供并发冲突保护。

错误响应使用 `code`、`message` 和可用的 `request_id`：400 `invalid_request`，401 `unauthorized`，403 `forbidden`，404 `not_found`，409 `version_conflict` / `request_id_reused`，503 `busy` / `unavailable` / `outcome_unknown`。请求体上限 96 KiB，每节点最多 16 个并发 Config 请求，操作总时限 60 秒并服从客户端更短期限。写前和响应前复核 Token；写入后授权复核失败返回结果不明，不发送数据或虚报未写入。

首版不提供列表、Watch、TTL、跨 Key 事务、配置历史浏览、Web UI 或 Go SDK；这些不是本次 State + Config 的完成条件。第 12 节完整 M1 的 Go 客户端仍待后续实现。此版需要所有参与执行的节点都注册 State Handler，不支持向混有旧版节点的 Group 开放 State 写入。

### 13.3 首版验证记录

在 TT01 VPS 的隔离目录执行，未升级或重启既有九节点集群；开发机仅交叉编译、`go vet` 和差异检查。

| 验证 | 结果 |
| --- | --- |
| State 单元测试 | 2 项通过：CAS、删除/重建与 ABA、相同请求重放、跨 Tenant/Module 隔离、状态重开及 Raft 状态快照恢复、编码与大小边界 |
| 三节点真实 HTTP 链路 | 通过，32.24 秒；真实 TLS/Token、Raft、Pack、物理确认、并发 CAS、历史请求重放、节点重启和凭据撤销 |
| 四节点补充验收 | 通过，43.02 秒；同机模拟两个 Region（三个 Owner 成员 + 一个非 Owner 入口），验证远端读取/写入/CAS、取消、64 KiB 值及超限拒绝，并重复上述核心行为 |
| 既有公开 HTTPS 回归 | 4 项通过：身份及撤销、服务端证书验证、Root 多数不可用时拒绝授权、listener 失败时终止启动 |
| 静态检查 | 相关包 Linux 交叉编译与 `go vet`、`git diff --check` 通过 |

证据在 VPS `/var/tmp/dubi-state-config-20261106/`，本机副本 `/tmp/dubi-state-config-20261106/`：`unit.log`、`acceptance.log`、`four-node.log`、`http-regression.log`。四节点验收程序 SHA-256 为 `b21e89dee43829597ffd6761823ca849e1b05a8ea48caef6c014b35238ba2bee`。测试设置 CPU/内存/时长上限，进程已结束，原 DUBI 服务仍 active。

这是 State/Config 首版功能验证，不是九台 VPS 的 WAN 性能验收或新增生产资格。未进行本次业务版本的六小时长测、整集群灾备恢复验收和所有 Owner/Region 故障组合；不把既有底座长测直接计作新模块验收。上述单元测试的快照指 Raft 状态恢复，不引入 Node Snapshot。


## 14. 公共 Gateway 骨架接入

已实现范围、启动方法、登录协议、权限与后续接入位置以 [HTTP Gateway 第 9 节](../architecture/http-gateway.md#9-首版骨架使用与后续接入) 为准。管理凭据独立于 Tenant Token；管理身份不授予业务数据权限。服务端通过 `GatewayOptions.Admin` 接入未来的具体管理路由；写操作必须复用 Runtime 受控服务并校验操作权限，不能从 Handler 直接提交任意 Root 命令。业务路由保留 Module 的 Tenant 认证与请求契约。

Stitch 负责正式页面设计，本轮内嵌页面只验证入口。不要把当前 Token 登录壳扩展成未经讨论的账号、注册或密码找回体系。原九节点底座集群不因 Gateway 骨架开发自动升级；新增 Root 凭据命令要求参与节点先升级到支持版本，再创建管理凭据，不支持混合旧版使用这些命令或降级恢复新格式的授权记录。
