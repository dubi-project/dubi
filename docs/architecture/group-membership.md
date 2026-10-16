# 数据 Group 成员任务与位置栅栏

数据 Group 的 PrepareJoining、AddLearner、PromoteLearner、RemoveMember 和 TransferLeadership 均绑定 **原始 Root 任务**；成员变更使用实际成员位置 CAS，转移还绑定 leader term。这是内部节点控制协议，不增加用户 ID、副本数或配置参数。区域自动严格增长由[交接协调器](region-handoff.md)消费这些任务；本协议不代表故障替补、Drain 或业务数据迁移已完成。

已确认的区域 Group 进一步要求增长任务来自 `AuthorizeRegionGrowth` 的原子授权，绑定当前 pending 代际与旧 seal；普通 PlanGroupMembers 和直接 Publish 不得绕过这些门禁。另有显式 `Maintenance` 成员任务，只能移除现有成员、保留当前 voter 严格多数，不添加新成员；它用于管理员确认后的普通节点退出，不是替换任务。Root 处于联合配置等临时不合格状态时，成员任务暂停；完成提交使用新 Root 切点的请求 ID 重试，避免第一次持久拒绝污染后续恢复，已完成任务则不重复执行。详见[区域交接](region-handoff.md)。

## 两个独立检查

每个成员 mutation 由协调器携带原始 `ChangeID`、GroupID、预期 Group descriptor epoch、操作和目标 RaftID。重试不能把旧操作重新解释成另一个当前任务。

Root 读屏障后，服务端在同一 Root 状态锁内验证：

- 调用者与被调用节点仍是当前 Root 成员，历史 Node 登记不能替代；Root learner 可以参与。
- Group 的 active-task 反向关联精确指向该任务，任务仍 pending，Group/任务 epoch 相符，且属于通用数据 Group。
- 被调用节点属于已发布成员或该任务的目标集合，避免新 leader 尚未发布时无法继续。
- Add 目标在期望集合，Promote 目标期望为 voter；两者还必须是当前 Root 成员。Remove 目标来自原已发布集合且不在期望集合，允许清理已经退出 Root 的旧数据参与者。

随后，Raft Group 在与提案相同的临界区里核对成员位置和完整 ConfState。`MembershipCut.Index` 是最近一次持久应用 ConfChange 的位置，不是整个状态机的 applied index：普通数据写入不会让它过期，但移除再加入、即使名单恢复原样，也会让旧位置永久失效。仍保留原有 leader、joint、磁盘准入、learner 活跃/追平和不能由底层 Raft 直接移除本地 leader/最后 voter 的限制；显式 `node remove` 对确认的单节点 Region 会先由 Root 退休该 Region Group，再完成身份移除和 Fence。

这两层不是跨 Group 原子事务，也不是全局吊销栅栏。Root 检查有时间点边界；Group CAS 防止观察后已发生的成员变化以及 ABA。业务数据授权、维护/撤销的全局序列化仍须独立协议，不能仅凭该检查宣称所有并发管理操作可线性化。

## 本地与远端路径

本地控制先确认 Root 任务，完成数据 Group ReadIndex 后读取成员切点，再次确认 Root 任务/本节点资格，最后调用同一 Raft CAS；不再直接调用无任务的 Add/Promote/Remove。

远端客户端从指定候选节点取得带 Cluster/Group/目标身份、读屏障和成员位置的 Inspect 响应，再提交任务与精确位置/ConfState。服务端在 Root 等待后重新验证当前 TLS 授权，成功提案后经数据读屏障确认，返回前再次确认 Root 成员和 TLS 有效期。权限在确认等待期间失效会拒绝响应；已经持久发生的成员变更不回滚。调用方重试应读取实际状态，而不是假定错误等于没有发生。

PrepareJoining 将原始 GroupID/ChangeID/epoch 一直传递到正式 Node 工厂；等待 JoinManager 锁后，工厂重新读 Root 并验证这个任务，不能按 GroupID 换用新任务。目标必须是被调用本机且属于期望成员。上下文取消后不再打开 Host Group；工厂已返回的状态会关闭。打开后的 Root/TLS 复验失败不回滚已登记资源，后续恢复仍受持久目录约束。

TransferLeadership 仅允许为当前任务中即将退出 voter 角色的 leader 转移，不授予任意负载均衡权限。目标必须是期望 voter、当前 Root 成员和实际数据 Group voter。调用携带精确成员位置/ConfState 与观察到的 leader term；Raft 在同一锁内验证后发起转移，转移中止后的每次内部重试也重新验证，不能跨成员变化或重新选举继续执行旧意图。完成等待后再验证 Root 任务/成员与远端 TLS，响应绑定完整任务、调用目标和新 leader；失去授权不返回成功，但已经发生的转移不回滚。

Inspect 在数据读屏障之后再次验证 Root 成员/TLS，返回 leader term 供转移使用。候选节点为 follower 时，客户端先根据观察继续寻找 leader，避免将不适用的转移请求发给仍保留 voter 角色的节点。Root 的加入/投票拓扑使用各自工作流，这些数据任务入口不允许操作 Root。整个维护协议仍需容量/故障域与全局授权序列化，不能由任务信封替代。

Raft 保留供受信 Root 加入/拓扑内部调用的本地原语；普通数据成员协调路径和 RPC 已使用受约束入口。其他通用内部提案服务、公开 Tenant 授权、运维策略和所有 RPC 的统一治理仍有独立未完成项。

## 限制、格式和恢复

远端 Change、PrepareJoining、TransferLeadership 共用最多 8 个活动请求，过载不排队；内部上下文 10 秒。ConfState 最大 256 KiB，Change/Transfer 请求最大 256 KiB + 4 KiB；准备请求及准备/转移响应最大 4 KiB。Inspect/Change 响应上限为 256 KiB + 4 KiB；客户端设置单次发送/接收上限，服务端也检查解码后大小。Inspect 尚未使用这个并发准入；该限制不是连接/握手/所有 RPC 解码的总内存预算，也不能强制中断磁盘或已有临界区。

新 Change 请求必须含任务、epoch、成员位置和配置；Prepare 必须含任务/epoch，Transfer 还必须含成员切点与 term，旧空字段请求明确拒绝。新客户端拒绝缺少身份/成员位置/读屏障/term 的旧 Inspect 响应，以及没有完整任务绑定的准备/转移确认，不降级为无栅栏控制。协议协商和滚动升级仍未完成，当前不支持混用旧二进制。

成员控制本身沿用 enginestate v4 引入的成员位置键，与 ConfState/applied 原子提交，并随文件/内联快照恢复；v5 另增加[区域交接](region-handoff.md)语义，v6 增加权威记录引用，当前 v7 增加[本地多数确认记录](immutable-records.md)。状态适配器将成员切点暴露给 Raft；不支持它的状态机不得以普通 applied index 代替。失去既有日志的资源保护不变。

## 验证边界

- 真实 Raft + Pebble 状态验证普通写入不使成员 CAS 过期、成员 ABA 的拒绝、关闭重开后旧请求仍拒绝，以及新切点可继续操作。
- Root 专项覆盖任务/epoch/目标角色/服务节点归属、已退出调用者或目标、任务完成和关闭后的拒绝。
- 准备与转移专项覆盖已完成/被后续任务替代、等待后资格变化、工厂取消清理、陈旧 leader term、真实 Raft 转移重试再次读取成员切点、旧确认与错任务响应、三类操作共享准入及取消回收。
- RPC 与本地控制覆盖过期/错配置/错位置、等待后的成员或授权变化、旧请求、超限、槽耗尽、取消归还、响应身份绑定和同一 mTLS 连接移除后复用拒绝。
- 三节点正式 Server 测试仅手工初始化首个数据节点，后续两节点由持久任务自动准备、加入和晋升；空 Group 再通过任务转移当前 leader、移除并重新加入，然后执行 80 MiB 文件快照追赶及工件恢复重启，核验成员位置保留。这个空 Group 往返不证明业务 Drain 或区域自动缩容。另保留远端 leader 转移后、Root 尚未发布新 leader 时继续完成任务的测试。

上述为同机真实协议与持久状态证据，不是独立主机/WAN、物理掉电或生产发布批准。
