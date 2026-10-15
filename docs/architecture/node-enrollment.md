# 节点加入：一次性凭证与证书边界

这是 P05 的分阶段实施记录，**不是完整远端 Join 或生产资格声明**。默认身份均自动生成；不增加 NodeID、RaftID、CA 路径或 TLS ServerName 配置。

## 已实现的授权闭环

1. 管理客户端本地生成随机 GrantID、32 字节 secret 和临时 Ed25519 密钥。创建 RPC 只发送 secret 的 SHA-256、公钥和过期时间；不发送私钥或 secret。
2. 持有既有 CA signer 的节点，使用确认后的 Root Node 地址签发 client-only enrollment 证书，并通过 Root Raft 发布 public grant。只有 ReadIndex 证明同一 grant 已持久登记且仍 Active 后才返回证书。并发同请求和提交响应丢失可重试；终态不复活。
3. 客户端将公有 bundle 与本地私有材料组成 `djt1.` 凭证。默认 15 分钟、最多 24 小时。编码有 32 KiB 上限，固定版本、规范 JSON/Base64、单证书与单私钥，拒绝重复字段、替换密钥、替换 CA、模糊 URI、尾随数据和不合法目标地址。凭证是 bearer secret，不能进入日志、工单或公开仓库；打印结构体默认脱敏，只有显式编码才输出凭证。
4. 新节点使用凭证固定的 CA、自动虚拟服务名 `dubi.internal` 和 TLS 1.3 建立双向 TLS，提交其**本地持久 CSR** 与 secret。节点自己的私钥始终留在新节点。
5. 服务端逐次检查 TLS enrollment 角色、Cluster/Grant、证书有效期、公钥摘要和 constant-time secret 摘要；Root ReadIndex 等待后重新取时间。Root 将 grant 从 Active 原子绑定到整个规范请求的 SHA-256（包含 CSR、Node/Raft、Region、地址等）。同请求重试可恢复，不同请求不能抢占。
6. 既有 Issuer 再登记 Node，确认**精确 CSR 对应的 Root 命令结果**后签发节点叶证书。客户端确认响应未替换 Cluster/CA、Node/Raft、自动服务名或 CSR 公钥，才允许后续持久化和安装。

Grant 状态只有 Active、Consumed、Revoked。撤销与消费并发时只有一个终态获胜；Consumed 不能通过“撤销凭证”变成节点吊销。已消费的凭证也不能在过期后借重试无限续签节点证书。未收到创建响应可重试同一 draft；丢弃 draft 后剩余 Active grant 等待过期或由管理端撤销，不能猜测/恢复 secret。

管理 grant 的 Node 必须在 ReadIndex 后确认仍属于当前 Root ConfState（含 learner 和联合配置过渡成员），节点登记与成员资格在同一个状态锁内读取；历史登记不是管理授权。真实 mTLS 回归覆盖已签发但尚未接纳、已接纳 learner 和移除后复用同一连接：前后两种状态均不能创建或撤销 grant，拒绝不提案、不改变 Root。这是调用时的成员资格检查，不是全局证书吊销或与随后成员变更原子提交的维护栅栏；Root 加入/发现仍需允许已签发但尚未接纳的节点进入各自受限流程。

Root 的 `data/join-grant/` 只存公有摘要、签发节点、授权时间与状态；不存 secret、临时私钥、节点私钥或 CA 私钥。Root 文件快照校验 grant 格式和签发 Node 引用，覆盖恢复与重启。Raft Apply 不读本地时钟、不取随机数。

加入端新增不可变 `DUBI_JOIN_RECEIPT`：保存匹配本地持久 CSR 的已验证公有 bundle、请求摘要、签发地址与原接收时间，不保存 ticket/secret/临时私钥。回执在绑定 Cluster/安装 PKI 前，经独占 Store 锁、文件 Sync、无覆盖 link 和目录 Sync 发布。相同请求重试保留第一份有效回执；不同请求、其他节点、符号链接、损坏或替换目录均拒绝。之后可在临时 grant 已过期、无需联网时恢复叶证书安装，但当前 Node 证书自身仍须有效。回执只证明收到过授权证书，**不证明已加入 Root**。

回执测试覆盖四个文件发布阶段的错误/子进程退出，以及身份已绑定但 PKI 尚未发布、PKI 已发布但 CSR staging 尚未清理的恢复状态。进程退出不等于物理掉电资格；崩溃残留的未知临时文件不自动删除。

叶证书安装重试必须匹配已安装 CA 的完整 DER 和节点公钥，不能仅凭相同 Cluster/Node/Raft 字符串返回成功。相同 CA/公钥的新签发证书允许作为等价安装重试，但保留原叶证书，不把加入重试变为隐式续期。拒绝不匹配的 bundle 时保留原 PKI 与 CSR staging；清理 staging 的失败必须返回，不能吞掉目录 Sync 错误。

Root 侧进一步保存 `data/root-join/<GrantID>` 待办：Begin 必须匹配已消费 grant 的完整请求摘要、Node 元数据以及精确 CSR 的成功登记结果；Complete 只有在 Root 已应用的 ConfState 包含目标时才能标记 Admitted。记录同时保存登记命令摘要与完成索引，快照检查其 Grant/Node/Result 引用。已完成记录是历史授权事实，不代表成员永远在线或仍在组内；不能重置为 Pending 来复活后来移除的成员。

`cluster/rootjoin` 协调器仅由 Root leader 推进：持久 Begin、添加 **非投票 learner**、ReadIndex 确认、持久 Complete。提交/成员变更响应丢失可由持久结果确认；后台 Reconcile 仅恢复 Pending 记录。遇到既有完成拒绝或已接纳成员被移除时停止自动重加。Begin 的幂等键绑定 GrantID 和完整 CSR 请求摘要，错误 CSR/元数据的拒绝结果不能抢占正确请求。测试使用真实单 Voter Root 和离线 learner，覆盖并发、各持久边界、重开 Raft 后恢复；它不证明 learner 已追上，更不会自动晋升 Voter。

正式 Server 已注册独立的 `RootJoinService`，无需持有 CA signer：`JoinRoot` 只允许已登记且当前有效的 Node mTLS 身份，并逐次核对证书公钥与完整 CSR。Enrollment 临时身份不能调用 Join 或 Discover。Follower 只返回经 ReadIndex 确认的路由视图，不确认接纳；Leader 推进上述持久任务，再确认当前成员资格。已移除成员返回 FailedPrecondition，旧回执不能自动复活它。

`RootRoutingView` 在同一状态锁下读取 applied index、真实 ConfState 和所有成员的 Node 描述；含联合配置的 outgoing voters/staged learners，不返回单纯登记但尚非成员的 Node。缺少成员描述、损坏、提交结果不确定或恢复中的 Root 均不提供视图。协议校验成员/地址完整对应、ID 和端点去重、可拨号地址、联合配置语义；leader hint 只能指向 voter，不能引入额外成员或替代信任锚。

路由规则已下沉到无 protobuf 依赖的 `cluster/rootjoin.View`；RPC 编解码和本地恢复记录共用它，并对数组/描述符进行独立拷贝和规范排序，避免调用方修改已验证对象。

本地 `DUBI_ROOT_JOIN` 已实现持久记录核心：绑定 Cluster/Node/Raft/Store、Grant 和完整请求摘要，保存接纳位置、首次确认视图、当前地址视图，以及 `catching_up`/`active` 阶段。接纳与查看较新远端视图都不能代替本地激活；激活要求调用方先完成本地 ReadIndex，并提供匹配的已应用 Root 任务。激活位置只记录第一次成功，旧响应不能回退视图，同索引冲突明确拒绝；地址更新可以跟随新的确认位置，但不能改节点身份。

该记录有 4 MiB 上限、0600 权限、规范 JSON 与回执关联校验；首次以无覆盖 link 发布，更新经临时文件 Sync/原子 rename/目录 Sync，Store 目录固定且要求整个生命周期持有 Store 锁。写入结果不确定后，本实例停止读写，重开以磁盘事实恢复。测试覆盖首次发布和激活更新各四阶段错误及子进程退出、旧响应、锁冲突、损坏/符号链接/外来身份、缺失回执和替换目录；未知崩溃临时文件保留。这仍是本机进程崩溃证据，不是物理掉电资格。

持久记录已接入正式 Node/Server：只有有效 `catching_up` 记录可开启空 Root learner；身份绑定的地址缓存只辅助拨号，不变成 bootstrap voters。Raft 重放后执行 `CheckRecovered`：旧 catching_up 标记不能绕过已重放的成员移除；active 节点丢失激活位置之前的共识资源必须失败，不能重新当作空 learner 初始化。记录自身不存在与记录引用的回执缺失严格区分，后者不能当作旧节点而跳过检查。

首次启动先等待本地应用达到缓存确认位置，再取得 Root ReadIndex、核对持久加入任务和当前成员，持久化 active 后才发布 Ready。心跳发现 leader 不等于已有成员配置：空 learner 在安装快照前会拒绝未知 peer 的 ReadIndexResp，该回复也不会自动重发。启动屏障会以有限单次期限重试丢失回复/暂时无 leader，不能重试掩盖存储错误；整体等待受节点取消上下文控制。ReadIndex 请求/回复虽然用 Entries 携带 token，但使用独立控制通道预算。

正式双节点 mTLS 测试已覆盖：源节点压缩 Root 后空 learner 从文件快照追平、首次追赶前源节点离线及加入进程中断、确认后激活、单节点重启与新命令复制、全部停机后不带 ticket/signer/bootstrap 参数恢复。重启保留首次激活位置。这个聚焦追赶的内部 fixture 是**同机单 voter 加 learner**，不证明多 voter/跨区域故障多数。正式托管流程现已另行接入[自动 Root 拓扑](root-topology.md)并有多 voter 测试；持续后台刷新地址、受控地址变更及真实故障域资格仍待完成。

`rootjoinrpc.Join` 使用不可变回执、已安装的 Node 私钥及与回执一致的 CA；可尝试已确认的 seeds、原 issuer 和响应内 leader 路由。同一调用总计最多 16 次 RPC、每端点最多 8 次、总计 30 秒，每次 RPC 最多 5 秒。临时 Unavailable/DeadlineExceeded 会先尝试其他已知地址，再按 100 毫秒至 1 秒退避重访原端点；重定向循环也计入这些上限。权限、参数、前提条件和资源压力不自动换节点/重试。始终复用同一回执，临时 ticket 过期不影响此步骤，但 Node 证书/CA 仍须当前有效。客户端只返回验证后的接纳结果，**不写本地加入阶段、不创建 Raft、不宣称追赶完成**。

正式 Server 的后台调度已并行执行 Root 加入待办与数据 Group 成员待办；默认每轮 30 秒协作式上下文上限，收齐两个 worker 后才开始下一轮。Root leader 门槛与每个协调器的持久事实检查均保留。客户端在 Begin 后消失，服务端可继续完成已授权的 learner 接纳；该能力不授权任何 voter 晋升、Region 策略或维护移除。

CSR 登记 RequestID 现使用完整 SHA-256，不再截断为旧开发版的 12 字节摘要。新 Root 加入流程不会以普通 Node 登记或旧短摘要结果冒充精确 CSR 授权；旧开发状态的兼容/迁移仍是 P11 未完成项，不自动改写已有 Store。

## 身份与服务隔离

- Enrollment URI 为 `spiffe://dubi/<ClusterID>/enrollment/<GrantID>`，仅 ClientAuth；不能被 Node URI 解析器接受，更不能作为服务器身份。
- `CreateGrant`、`RevokeGrant` 要求当前有效的 Node mTLS 身份，以及 Root 中匹配的已注册 Node。临时加入身份不能调用它们。
- `EnrollNode` 只接收 enrollment 身份。持有普通 Node 证书但复制了 secret，仍不能使用该 grant。
- 现有 Raft/Group RPC 的 Node 身份检查拒绝 enrollment 证书，不提供明文或跳过证书验证的引导口。
- 当前管理信任域是受信集群节点，**不是用户/Tenant 管理授权体系**。不能把 Node 证书交给业务用户；节点权限细分与凭证吊销/自动轮换仍需后续实现。
- 加入节点安装 leaf-only PKI，不接收 signer-wrap key 或 CA signer envelope。当前 Root 已记录稳定 voter 多数的 enrollment authority epoch/member cut，后台会自动推进稳定成员变化；密封 signer envelope 仍需通过受 mTLS 保护的 NodeAdmin transfer 安装到目标节点。旧 Node Fence 后立即失去签发资格，签发材料不能写入 Root 明文快照，也不能用复制旧 CA 文件的方式绕过授权。bootstrap 移除后的完整跨节点传输与新节点加入联测仍是未完成的验收项。

TLS 的 VerifiedChains 是握手时的验证结果，不是无限期授权。普通 Node RPC 每次检查其至少一条已验证链（含叶证书与 CA）当前仍在有效期内；管理/Join 在等待 Root quorum 前后都检查，临时 enrollment 同样刷新链有效期。真实 TCP/mTLS 测试只推进授权时钟、不推进 TLS 时钟，确认过期拒绝发生在同一连接、没有重新握手。

快照流在每次接收前后、EOF 和 ACK 前重新授权。文件导入保留接收错误原因，权限拒绝不会被包装成可重试 Unavailable；接收或 EOF 时失效不得发布工件。安装已持久完成后才失效的 ACK 会拒绝，但不会回滚已经接受的共识状态。此边界不等于证书吊销、撤销已有提案、主动关闭闲置连接或强制中断阻塞 IO。

## 有界处理与错误语义

正式 Enroll 客户端对临时 Unavailable/DeadlineExceeded 最多执行八次 RPC，每次不超过 5 秒，总调用不超过原有 30 秒窗口，退避从 100 毫秒倍增至 1 秒。所有尝试使用完全相同的已持久 CSR、身份和 admission proof；不因自动 Root Leader 转移而生成另一份请求。权限、无效参数、前提条件失败及资源压力直接返回。该补强由九节点自动拓扑加入中的失败触发，并有真实 mTLS 瞬时 Root 失败、重复请求一致性、次数上限和取消测试。

服务端最多同时处理 4 个 enrollment/management 调用，每调用 30 秒上下文上限；请求协议体最多 32 KiB、CSR 文档最多 16 KiB、proof 最多 4 KiB。忙时立即返回 ResourceExhausted 和 RetryInfo，取消释放槽。磁盘压力沿用底座 ResourceExhausted/RetryInfo；Root 不可用返回 Unavailable，不伪装成永久权限拒绝；无效/撤销/已被不同请求绑定的凭证返回 PermissionDenied。对外不回显 secret、CSR 或内部 Root 命令。

Root Join/Discover 共用另外 4 个处理槽，每调用 30 秒，请求最多 32 KiB/CSR 16 KiB，响应最多 4 MiB/4096 个成员。这是单次发现协议的资源上限，不是用户副本数；超限拒绝返回，不截断成员、更不缩小 quorum。客户端不把 resolver URI 或 wildcard 地址当作拨号目标。错误分类同样区分权限拒绝、待重试不可用、磁盘压力和成员已移除。

Issuer、CSR advertise 和成员发现共用可拨号地址校验：拒绝 wildcard/multicast、零端口/非规范端口、URI/path/userinfo、带 zone 的非通用地址及过长 DNS 名称。Prepare 在创建 Store/身份前拒绝；网络 Enroll 在消费 grant、登记 Node 前拒绝，失败不会花掉凭证。此检查不解析 DNS，也不保证实际路由可达；跨主机连通性仍需正式加入流程确认。

这些槽在 protobuf 解码之后计数；正式 Server 共用的 MaxRecvMsgSize 才是解码前消息上限，不能将 32 KiB 业务检查或 4 个槽宣称为全局连接/内存上限。阻塞的本地文件系统操作也不会被上下文强制打断。连接准入、解码总预算和真正饱和 IO/WAN 资格属于尚未完成的 P02/Q 项。

## 实现和验证位置

- `identity/enrollment`：本地 CSR/leaf 安装、draft/ticket、CA 固定、自动客户端 TLS 与响应验证。
- `identity/nodepki`：client-only 短期证书签发和共用 Cluster CA 验证；Node URI 同时收紧规范化检查。
- `cluster/rootstate`：grant 持久状态、消费/撤销竞争及快照验证。
- `cluster/enrollment`：GrantManager、GrantAuthorizer 与既有 Issuer；覆盖失响应、并发抢用、绑定后注册失败、过期和撤销。
- `rpc/enrollmentwire` / `rpc/enrollmentrpc`：内部 protobuf RPC、受限服务与固定 CA 客户端。测试通过真实本机 TCP/mTLS 完成凭证创建、消费、精确请求重试、leaf 安装、角色越权拒绝、CA 错误、资源限制和可恢复失败。
- `rpc/rootjoinwire` / `rpc/rootjoinrpc`：独立 Node-role 接纳/发现协议、固定回执 CA 的客户端、成员视图校验。真实 TCP/mTLS 测试覆盖并发接纳、同 NodeID 异公钥拒绝、临时角色隔离和移除后拒绝重加；单元故障注入覆盖等待 quorum 后过期、处理槽取消、Follower redirect 不提案、完整视图边界。Follower 的 redirect 单测不是跨机 leader 切换资格。
- `server.Options.EnrollmentSigner`：内部组合入口，不是部署参数。正式 CLI 自动加载 bootstrap 节点已有的托管签发器，首节点地址/Region 经本地自身登记和 Root 确认后才 Ready；加入节点仍为 leaf-only。CLI 集成测试从 init/server 进入真实 mTLS grant 创建，不手工登记 Node。底层双节点快照测试保留显式 fixture 登记。
- 共识恢复补强：历史快照可以早于本节点的 AddLearner，启动先重放已提交成员尾部，再判断本节点资格。回归覆盖快照是否已应用、已提交/未提交加入、授权等待与已提交移除；普通恢复仍拒绝非成员。Root 路由直接投影其已应用 ConfState（含联合配置），不要求第二份 Root Group 描述记录；失败/恢复中的 Root 不提供路由投影。

## 叶证书续期：本地落盘、远端签发与后台运行

普通续期固定原 CA、节点私钥、Cluster/Node/Raft 和服务名，仅替换 `node.crt`。内部提前 30 天续期策略不增加用户配置。签发原语检查旧证书当前有效，沿用原公钥；调用者仍须另行证明旧私钥持有权并经 Root 确认当前成员资格，公有证书本身不是授权。

安装端固定 Store/PKI 目录句柄，以独立短期文件锁串行化续期，校验当前本地材料和新证书，拒绝异 CA、异公钥、异身份、过期/尚未生效、有效期回退或无延长。新叶文件 Sync → 同目录原子 Rename → 目录 Sync；发布后报错可能已经可见，精确重试会重新 Sync 文件及目录后确认。不会改写 CA、私钥、配置或 signer envelope；未知崩溃临时文件不自动扫除。原证书已过期的节点恢复、CA 到期后的轮换是不同流程，不能借此跳过鉴权。

测试覆盖三个发布阶段的错误/真实子进程退出、重试、并发写互斥和 Store/PKI 目录替换；证书读取使用同一 descriptor 检查类型/权限并有界读取，拒绝 symlink、FIFO、目录及超大文件。进程退出测试不构成物理掉电验收。

正式 CLI 的服务端与出站 TLS 已共用叶证书热加载器，新握手至多每 30 秒检查本地托管文件，不增加部署参数。动态配置不保留静态 `Certificates` 回退，确保无 SNI 的服务端握手也经过回调；一次性管理/加入客户端仍一次加载当前材料。新候选须保持原 CA、节点公钥、身份和物理 Store/PKI 目录，并单调延长有效期。错误候选不会顶替仍有效的缓存，缓存叶证书或 CA 到期则拒绝；真实双向 TLS 测试核验双方实际收到的新证书，竞态测试覆盖并发回调和刷新。刷新错误可从加载器读取，但尚未接入统一诊断/告警；这不是 CA 轮换或磁盘阻塞时间上限。

签发节点已注册独立 `NodeCertificateService`。请求只带 Cluster/Raft 身份，不接受 CSR、公钥或待续叶证书；服务端使用实际已验证 mTLS 叶证书，以握手证明旧私钥持有权。签发前后分别完成 Root ReadIndex，申请节点与签发节点的成员资格及描述符来自同一个原子状态视图；等待后再次检查链有效期。临时 enrollment、未接纳/已移除节点、冒用其他 RaftID 均拒绝。未进入续期窗口则返回当前叶证书，不制造序列号变化；正常续签只延长原公钥的有效期，不改 Root。此检查不是与以后成员移除原子提交的全局吊销栅栏。

正式运行进程 Ready 后检查一次，以后默认约每 6 小时检查；距到期 30 天进入续期窗口。单次尝试上下文 1 分钟，失败从 1 分钟指数退避至 1 小时、时间带 10% 抖动，并向后台错误通道报告；不增加 CLI/YAML 参数。首节点使用原托管 signer，普通节点经持久回执中的原签发地址调用续期 RPC，并固定当前 CA/身份/公钥验证响应。客户端不写 Store；运行时重新确认本节点成员资格后才安装新叶。工作者纳入 Server 的取消/等待屏障，结束前不能释放 Host/Store；后台报告统一串行调用，避免并发写诊断输出。

即使当前叶证书尚未进入续期窗口，检查也会通过幂等安装路径重新同步该叶及目录，但不会重新签发。这使“上次已 Rename、却未确认目录 Sync”的可见证书在重试或重启后得到持久确认，而不因有效期已延长就跳过同步；测试通过占用发布锁证明该路径确实执行。

真实双节点 Server/Node 测试覆盖首节点本地自动续期、leaf-only 节点远端自动续期、CA/私钥不变及重启复用新证书；RPC 覆盖原连接移除拒绝、临时角色隔离、quorum 等待后过期、签发后移除、工作槽耗尽/取消。另测瞬时失败退避恢复和关闭等待续期清理。仍缺长期跨机运行、真实时钟偏差、CA 轮换/吊销、签发器迁移/高可用及原证书已过期后的恢复；原签发器不可用时不会偷偷换 CA 或重新加入。

## 连接更新与会话到期

正式内部 gRPC Server 约每 6 小时发出 GOAWAY（gRPC 自带 ±10% 抖动），新请求转到重新握手的连接；旧连接上的既有流最多保留 25 小时宽限期，以容纳快照最多 24 小时的总期限及建流余量。无需用户参数。宽限期只用于平滑连接更新，不能延长证书有效期；证书续期也不会改变旧会话所使用的叶证书。

服务端、Raft 出站及加入/管理/续期客户端统一使用有界 TLS transport。它保留标准 TLS 验签和 AuthInfo，只接受握手已经验证的路径；每条路径的期限取叶和全部 CA 的最早到期时间，再从当前有效路径中取最晚期限，避免短备选路径误伤另一条有效路径。到期关闭底层 socket、打断阻塞读写和空闲连接；每次读写前后重查，过期后新读出的明文不再交给 HTTP/2。已写出的字节或已提交状态不能追回，仍由上层幂等和恢复协议处理。

每连接一个定时器，正常到期按剩余有效期触发，同时最长每分钟复核一次墙上时钟；时钟跳变下的活动 IO 即时复核，空闲连接在下一次复核关闭。没有每连接常驻额外 goroutine，但这不是总连接数、RSS 或 IO 预算。角色、当前成员资格仍由 RPC 授权执行；本策略不实现 CA 轮换或吊销。

真实 TCP/mTLS 测试以可控会话校验时钟触发旧连接失效，验证两端重新握手收到新叶；另用正式 Server 构造器及缩短的内部测试时限验证 GOAWAY 后新请求重连、既有流在宽限期存活并最终关闭。单元测试覆盖叶/CA 到期、阻塞读、时钟前后跳变和读取中失效；这些不是真实 24 小时 WAN 或系统时钟故障资格。

## 下一阶段，尚未完成

Join CLI 已接通 `token/revoke` 和 `join --ticket-file` 一步加入运行；`prepare/install` 保留为内部诊断步骤。凭证仅从私密普通文件或 stdin 读取，不保存到 Store；请求→回执→接纳→激活的持久边界支持重试，回执之后不需原凭证。监听端口先于入网绑定且一直保留，独立流程锁串行化多步骤，运行 Store 锁保护本地资源。已有接纳记录先校验完整绑定，再考虑卷参数；已有 Raft 资源但丢失接纳记录时不能再远端接纳并重建 fence。正式 server 已自动接入首节点自身登记及现有签发器。

管理客户端固定既有 Node CA，拒绝返回不同 Cluster 或同 Cluster 的另一 CA；失响应最多三次重试相同公有 draft，凭证私钥/secret 不发送给签发器。已消费、已撤销、冲突或缺失的 grant 返回 FailedPrecondition；quorum/未确认结果才保留 Unavailable。自动加入测试覆盖真实 mTLS、回执后无凭证恢复、离线签发中断后保留原 CSR、双节点全停冷开/追赶、并发锁和显式多卷。还需：

正式 `server` 对 OriginJoin 节点也在打开共识资源前持 Store 锁验证完整回执/接纳记录；不能切换命令绕过缺失 fence。回归先复现了旧开发节点恢复分支接受缺失记录的情况，再收紧正式入口；内部 Runtime 仍有无记录的旧测试 fixture 恢复路径，不构成生产迁移接口。

1. 已有身份绑定的首次地址登记/加入缓存和重启复用；仍需持续缓存刷新及受控地址/Region 变更，不能生成新 Node 或要求用户配置 RaftID。默认路由推导只选择本地源地址，不替代真实跨主机可达性证明。
2. 已实现公有 bundle 持久回执、安装恢复、Root 持久加入任务、网络 learner 接纳、后台待办恢复、本地阶段/地址记录、CLI 与实际 learner 快照追赶/重启，并接入自动 Root voter 规划/联合变更。仍需持续路由刷新/地址变更、区域数据 Group 策略与真实故障域冷启动资格。**获得叶证书不等于已加入 Root 共识，远端 learner 接纳也不等于本地已追上**；leaf-only 节点仍不能直接启动为空 Root。
3. 普通叶证书自动续期、热加载、连接平滑更新及到期关闭已有闭环；仍需 CA 轮换、吊销、签发器可用性与恢复。授权依赖正确本地时钟，时钟偏差和跨机长期运行资格未完成。
4. 稳定 Region/Node 多数策略、Placement/Repair 和真正独立主机/磁盘故障验收。底层 learner 晋升已要求近期 Raft 活跃且追平当前提交位置，拒绝用历史 Match 晋升已断联节点；这只是晋升的必要条件，不替代自动拓扑策略，也不能保证晋升后不会发生故障。

本地加入阶段的实施约束：

- 先持久化经认证的远端接纳、完整请求摘要、Root 接纳索引以及 Node/Raft/Store 身份绑定，才允许首次空 learner 启动；peer 地址只帮助拨号，绝不能被转换成新 Root bootstrap voters。
- 区分等待追赶和已激活。只有本地 Root 完成 ReadIndex、应用到已确认接纳位置且当前成员/持久任务均匹配后，才可持久化已激活状态。
- 即使仍有等待追赶标记，只要重放后的本地 applied index 已到接纳位置却不含本节点，就必须视作移除/不一致，不能永久使用 `Joining=true` 绕过成员检查。
- 地址缓存独立于临时 ticket/原 signer 可用性，按确认后的 applied cut 单调更新；旧响应不回退地址/成员视图，同索引冲突需显式失败。缓存损坏和已登记资源缺失不能用重新初始化掩盖。
- 全群冷启动使用已有 Raft 状态与身份绑定的缓存恢复连通，不先依赖一个已经在线的 Join leader。已有双节点全部停启和初次追赶中断测试；多 voter/多区域拓扑、已移除节点运行/重启与维修协议仍需完整验收。

所有当前网络测试均为同机临时目录；没有执行生产部署、真实掉盘、WAN 或长期压力资格，生产结论保持 No-Go。
