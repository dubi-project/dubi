# 区域数据参与集合与自动初始建组

本阶段实现 Root 中的区域集合记录、代际发布、恢复校验，以及由持久计划驱动的**自动初始区域 Group 建立与全员应用确认**。正式后台流程会提交数据 Group 栅栏，逐节点检查实际应用后才调用 `ConfirmRegionCohort`，将初始 head 从 current=0/pending=1 转为 current=1/pending=0。`confirmed` 是内部协调器发布的历史断言，不是业务数据已经耐久、更不是生产资格；严格增加节点的自动交接已接通，数据写入确认和修复仍未实现。

后继代际由独立增长协调器消费 Root 授权：全体旧参与者的相同 seal 观察、精确计划和成员任务原子发布，旧成员角色保持不变，完成该任务后才能经全体新参与者观察、用晚于 seal 的候选确认新代际，最后精确激活。已确认区域拒绝普通成员任务/直接发布绕过；快照恢复验证完整授权链。详见[区域代际交接](region-handoff.md)。

## 与现有概念的关系

Region cohort 是一个 Region 在某一代际的数据参与 Node 集合。它不是新的用户资源、可配置副本数、Engine 或 Volume：用户不用创建它或逐节点填 ID。自动协调器从已接纳的节点和持久区域归属推导初始计划。节点有多个 Volume 仍只算一个故障域；Root learner 也可能是数据参与节点。

Root 投票角色、区域 Group 的投票角色、数据参与集合分别记录。集合包括区域 Group 的 voter 和 learner，不以 voter 数替代数据多数的分母。内部单条集合记录最多 4096 个 Node，是有界处理限制，不是运维参数，也不是已经验证的集群规模。

一个或两个节点的区域不能靠本记录启用本地确认：Placement 和自动建组都支持小区域，但小区域的前台确认仍必须等待跨 Region 门槛，只有至少三个不同 Node 的稳定集合才允许本地 `LocalReady`。不能从“未记录 cohort”推导免于确认。

跨 Region 物理覆盖现在有一个能力中立的规划和写入原语：Root 在同一 applied cut 返回明确选定的已确认 Region cohort 及其注册 Node，`placement.PlanGeo` 固定 Region、Group、generation、epoch 和 fence。Range placement 在 Region 明确退休并以同名新代际确认后，只刷新同一 Range epoch、同一 Region/Group 集合的 generation；不会借刷新偷偷改变 Range Owner 或 Region 集合。`StageGeo` 先在本地 Region 写入严格多数，再在固定计划中取得足够的 Region 严格多数；离线的非必要 Region 不会缩小分母，而是留下后续修复账。`AssessPublication` 还把覆盖绑定到单一 RecordID、引用 RequestID/Index/Hash、Range epoch 和路由键，引用替换或代际变化时不能复用旧观察。Owner Group 现在可持久保存 `GeoCoverageRecord`，`CheckCommittedGeo` 会在使用前重新核验新鲜计划、已覆盖区域证据和 Owner 当前引用；快照会校验引用回链，引用替换/删除会原子清除旧账目。`Writer.WriteGeo` 是显式 opt-in 的同步跨区写入路径，不改变默认本地多数优先行为，并要求元数据镜像传输器在已覆盖 Region 保存引用切点后才进入 coverage 确认。该账目只证明同一不可变 RecordID 的物理持有观察，不能直接发布 `GeoReady`，因为权威引用/日志切点的跨区可恢复性仍需后续接通。

Geo 元数据镜像现在支持节点本地的持久 retry ledger 和有界 worker round：临时无法联系任何目标 voter 时，确定性的 mirror 请求会保留在有界账目中；某一目标 Region 成功应用后才删除对应项，目标状态机的持久拒绝不会盲目切换节点重试。每轮重试必须由调用方用当前 Root 计划解析 Region generation/epoch/fence；同名 Region 明确退休后重新形成时，只要 Region、Group、Owner 路由和引用仍一致，欠账会自动重绑定到当前 generation/epoch/fence，再投递给新节点；不同 Region 或过期 Owner 引用则继续挂起。托管 Server 已接入该账目的周期 worker；它只处理当前 Root placement 能重新匹配的任务。该账目本身不改变 Region 多数、不发布 GeoReady；源 Region 丢失后的重建流程仍需接入。

## 持久状态与转换

- 每个 Region 有一个 head，分别指向已确认 `CurrentGeneration` 和可选的 `PendingGeneration`。
- 每一代保留自己的 Node 列表、Group、计划 Root index；确认后再记录 Group epoch、Group fence 的请求 ID/应用位置和确认 Root index。不同 Group 的 index 不做大小比较。
- 计划采用当前代际 CAS，同一 Region 只允许一个 pending。计划写入不修改当前代际，不改变已有写入所对应的多数分母。
- 确认必须指向原计划，同一个 Root 批次更新该代际和 head，并与命令结果、applied index 一起同步持久化。Group 描述符 epoch 必须相符、不能有待处理成员任务、成员集合必须与计划完全一致，且至少三个 voter；不足三成员的小 Region 要求全员为 voter。所有参与者必须仍是本 Region 的当前 Root 成员，Root joint 期间拒绝新确认；历史登记不授予当前资格。小 Region 的 Root/Placement 结构已允许单节点；整区域明确退出后，新的同名 Region 形成代际由后台自动接续，不增加用户重建步骤。不能把加入中的 Root learner 提前写进形成集合。
- 确认后的记录不可修改；相同命令请求重试返回原结果，使用新请求 ID 重复同一确认也不改变它。旧代际迟到确认不能回退 head。整区域明确移除后，后续新节点使用新的内部代际；这只是隔离旧身份的底层记录，不是用户操作。
- 后继代际目前只允许在同一 Group 内增加 Node，Group epoch 和 Group fence 必须前进，fence 请求不可复用。历史代际保留供恢复及后续数据证明引用，不进行自动删除。
- 一个 Group 持久绑定一个 Region，所有计划 Node 必须已经登记且属于该 Region。没有按健康过滤、离线超时缩容、临时降低多数门槛的入口。

删除节点和数据回收仍由明确的 `node remove` 与后台修复处理；不自动丢弃参与者以“恢复健康”。整区域全部明确移除后，底层可以自动开启同名 Region 的新代际，用户不需要单独执行重建。

## 自动初始建组

正式托管 `server` / `join` 的每个节点都有串行后台任务，不新增配置或操作步骤。Root leader 在读屏障后读取当前 ConfState 与 Node 的原子视图；joint 期间不规划。一个 Region 只要有当前 Root 成员，便自动登记确定性内部 GroupID，并保存首次完整参与集合；不把仅有历史登记的节点加入，也不按健康缩小集合。

Root 只有在第一代、Group epoch=1、尚未发布成员且没有成员变更任务时才生成 `formation` 权限。它不是调用方可传入的开关；已有 Group 的普通 cohort 不获得此权限。首次发布必须与原计划完全相同、全部为 voter，且节点仍是当前 Root 成员；单节点/双节点 Region 也走同一流程。在首次发布前不能创建另一个成员任务。之后的扩展、角色调整仍需正常成员协议，不能改写原始 bootstrap 集合。

各节点在 Root 读屏障后独立恢复自己的计划，即使不是 Root leader。仅原集合内、区域与本地身份匹配的节点使用该集合初始化；跨区域节点不创建此 Group。持久 Group/日志登记及 formation 栅栏防止丢失历史后重新 bootstrap。普通 `PrepareJoining` 只能按 Root 中明确包含本节点的待办成员任务打开 learner，不能抢先占用初始 Group。

每轮最多检查一个待发布 Group，轮转游标仅用于公平调度，丢失它不丢失建组权限。原始参与者必须逐一通过真实 ReadIndex 和稳定 ConfState 检查；本机走 Host，远端走认证 RPC。全部原始 voter 和有效 leader 匹配才发布 epoch=2 路由；失联保留 pending，其他区域可继续。单次检查窗口与后台轮次有内部超时，关闭时取消并等待退出后才释放 Store。

上述只是初始建组；后续新节点经独立的增长交接自动扩入已存在 cohort，不能修改原 bootstrap 集合。一个或两个节点的区域同样形成自己的区域 Group，但不能因此获得本地 `LocalReady`。路由发布本身不确认 cohort，随后必须完成下述全员协议；建组/增长均不承诺每笔业务数据的多数持有或故障修复。所有节点启动仍需可用 Root 多数，区域 Group 有多数不代表整套服务无 Root 也能冷启动。

## 自动初始集合确认

`RegionCohortConfirmation` 是内部受信协调器断言。Root 能验证自己的目录和 CAS，不能靠一个远端 index 数字验证另一个节点的磁盘、实际 ConfState 或状态机内容。它不对外提供为用户可提交的“证明”。

正式托管流程在同一串行后台任务中依次执行建组、初始确认和增长，不引入运维参数。初始确认器仅由 Root 实际 leader 为 `formation=true` 的第一代执行下述协议，非初始增长计划由专用增长协调器处理：

1. Root ReadIndex 后，在同一锁下读取 pending cohort、Group 描述符、成员任务和全员当前 Root 资格；未发布、成员不符、joint、成员退出或任务未完成均不进入确认。
2. 从目标参与者取得实际稳定 ConfState 与成员应用位置，构造绑定 Region、Group、原计划、代际、完整成员/角色及位置的 fence。提案前再核对 Root 切点；相同 fence 已存在时直接复用原记录，失响应/领导权变化不需要易失进度。
3. 逐一通过[指定参与者观察](region-fences.md)确认所有目标节点。Root 读屏障至少达到选中计划的 Root 切点；数据读屏障已应用，稳定配置与成员位置精确相符，Active 为真且每份完整 fence 记录一致。本机使用 Host 和状态机读屏障，不调用禁止 self-target 的 RPC；远端查询不能重定向为 leader 的状态。
4. 收齐后再次读取 Root，计划/Group 必须仍一致且没有成员任务，再提交确认。Root Apply 再检查 epoch、任务和当前成员资格，防止重验至提交之间的变更被接受。暂时拒绝会留下持久结果；下一轮确认请求使用新的 Root 应用切点派生请求 ID，避免旧拒绝结果永久阻塞重试，已成功确认的 cohort 本身仍幂等不可变。

每轮最多一个区域，轮转避免失联区域独占检查；确认上下文最多 20 秒，受后台整轮 30 秒上限约束。全员缺一则保持 pending，不按健康改变分母。观察串行且不跨轮缓存；4096 是格式上限，不是已经验证可在该时间窗口完成的生产规模。关闭时取消并等待确认任务退出后才释放 Store。

这不是跨 Group 原子事务或拜占庭证明。Group 在某份观察之后仍可能发生变化，历史 `confirmed` 不授予永续写权限。后续增长需要独立的 active/pending 数据代际交接；业务写入还必须取得针对确定代际、确定数据的本地与跨区域耐久回执，不能直接消费历史建组断言作为当前数据确认。

本地优先确认只能支持相应的 LocalReady 语义，不能冒充多数区域已确认的 GeoReady。区域失效前尚未完成异地复制的窗口需要明确暴露。

## 恢复与验证

文件和旧内联 Root 快照共用严格的记录及关联校验，恢复前检查：Node/Region、Group 归属、head 与历史连续性、只增不减的参与集合、递增 fence、Root cut 上界，以及同 epoch 的成员集合一致性。错误快照不能改变既有状态。文件格式使用同一个固定视图做交叉读取，按记录有界处理；旧内联格式仍受 64 MiB 上限约束，不代替流式格式。

测试覆盖计划/确认分离、CAS、不可变历史、重复和迟到请求、跨区/未知节点拒绝、Group 任务与 epoch 竞态、learner 计入数据集合、少于三个 voter 拒绝、条目边界、真实单 voter Root Raft 重启、Sync 不确定结果的两种持久结局，以及两种快照的正常恢复和关联损坏拒绝。

自动建组还覆盖 follower 恢复、不确认 Root 时不规划、一个区域失联不阻塞另一地区、实际成员不符拒绝发布、注册未接纳节点拒绝、成员任务互斥、formation 权限的两种快照恢复和仅保存计划时的本地冷重启；单节点 Region 也必须经过同一全员观察和 fence 确认。确认专项覆盖提案失响应、无易失进度接管、全员精确记录、成员 ABA、错误切点、观察期间退出/变更任务/epoch 变化，以及最后提交时 joint 导致持久拒绝后的成功重试。真实 CLI/TCP/mTLS 的单区域三节点、三区各一节点、3×3 九节点测试不再手工提交 fence，验证正式后台自动确认、单节点失效后的区域 quorum、全群冷启动及相同历史 fence 保留。关闭回归分别验证建组与确认任务退出前保留 Host/Store。

真实三节点测试曾捕获协调器使用远端客户端检查本机、导致成员已形成但无法发布路由的问题；现已显式区分本地/远端路径，并在单元测试中禁止 RPC self-target。失败日志保留，不能以增加超时掩盖接线错误。

这些是同机目录/恢复和初始 fence 全员观察的真实协议测试；不是业务数据耐久性或 Placement/Repair 验收，也不是独立故障域或物理掉电测试。
