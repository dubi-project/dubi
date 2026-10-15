# 集群级 enrollment authority

## 目标

新节点加入不能依赖某一台 bootstrap Node 永久在线。任一仍在役、已通过 Root 多数确认的 authority Node 都可以完成 enrollment；Node 被手动移除或 Fence 后，立即失去签发资格。

Node CA signer 不写入 Root 状态，而是以绑定本机身份的密封 envelope 分发到 authority 成员自己的 Store。首节点仍负责初始 bootstrap；稳定 Root 多数形成后，其他成员可以自动获得 envelope，bootstrap 被手动移除后，幸存 authority 仍可继续签发新的 Node bundle。

## Root 持久状态

Root 保存以下公开元数据（实现键为 `data/enrollment-authority`）：

- `AuthorityEpoch`：单调递增的 signer 代际；
- `AuthorityMembers`：当前允许持有 signer 能力的 NodeID/RaftID 集合；
- `ReadyMembers`：已经完成本地 envelope 校验、原子安装和目录 Sync 的成员；只有达到 Root voter 严格多数才允许 authority epoch 生效；
- `AuthorityCut`：该集合由哪个 Root applied index 确认；
- `AuthorityState`：`forming`、`active`、`degraded`。

Root 不保存 CA 私钥、signer envelope、临时密钥或可重放的传输凭证。Authority 成员必须是稳定 Root voter，并且至少达到 Root voter 严格多数；Root learner 不能成为唯一 authority。

## 能力密封与迁移

每个 authority Node 在本地 Store 保存自己的 signer envelope。envelope 使用节点本地独立 wrapping key 加密，并绑定 ClusterID、authority epoch、NodeID、RaftID 和 CA 指纹。网络传输使用当前 Node mTLS，并且同时校验：

1. 发送方仍属于 Root 当前 authority 集合，或是当前 Root voter 中已经预热下一 epoch envelope 的成员；
2. 接收方仍属于 Root 当前 authority 目标集合；
3. 双方看到当前 epoch 或紧邻的下一 epoch；
4. 传输请求只允许一次性、固定目标和固定 envelope 摘要；
5. Root 读屏障和本地 Store Sync 都成功后，接收方才报告完成。

签发服务每次请求都读取 Root 当前 authority epoch 和本机资格。已过期多代、已 Fence Node、已移除 Node、非 authority Node 或 Root 读屏障失败都拒绝签发。成员缩小但 CA 未变化时，仍在新集合中的节点最多可用上一代 envelope 完成这一轮切换；首节点在 authority 初次扇出期间也保留一个兼容例外（仅限 bootstrap 身份且仍在当前 authority 集合）。一旦被移除/Fence，这些例外立即消失。已发出的证书不会因后续 Fence 回收，但旧 Node 不能继续创建新的 grant 或 bundle。

## 迁移顺序

authority 迁移不建立用户可见的替换流程，后台按以下持久步骤推进：

1. Root Leader 观察当前 authority 集合和 Root voter 多数；
2. 选择仍在役的目标 Node，发布下一 `AuthorityEpoch` 和完整目标集合；
3. 现有 authority 成员把密封能力发送给所有目标成员；
4. 每个目标 Node 本地 Sync 并返回固定 envelope 摘要；
5. Root 收到目标集合的严格多数确认后，将新 epoch 标记为 `active`；
6. 旧 epoch 只保留历史记录，不再接受签发请求。

在新 epoch 激活前，旧 authority 仍可继续签发；迁移失败时不缩小旧集合，也不伪造完成。若旧 authority 只剩不足多数，集群进入 `degraded`，已有 Node 继续运行，新增 Node 接纳暂停，等待管理员恢复至少一个现有 authority 或执行受控 CA 恢复。

## 新节点加入

创建 Join Grant 可以从任意当前 Root 成员发起；该成员只负责管理入口，返回的票据会自动指向当前 enrollment authority。这样即使入口节点已经是 Root learner，也不会把新节点加入流程绑定到它自身的签发能力。

新节点首次 enrollment 仍由当前 authority 成员签发叶证书。Root 完成 learner 接纳并确认 Node 已进入稳定 Root 成员后，后台把当前 authority envelope 发送给它；只有 envelope 安装、Store Sync 和 Root authority 记录都完成后，它才可能成为下一轮 authority 成员。新节点的空 Pack、Root learner 身份或叶证书本身都不能证明它拥有 signer 能力。

## 恢复与快照

Node Snapshot 可以包含本机密封 signer envelope，但只能在原 Node 身份恢复时使用；从快照创建新 Node 必须重新经过 authority 传输，不能复制旧 NodeID/RaftID 的签发资格。Cluster Backup 保存 authority epoch、成员和 envelope 摘要，不保存可在任意目标直接解封的 CA 私钥。空集群恢复必须先恢复 CA 根和 authority 多数，再开放新 Node enrollment。

## 当前实现边界

Root/Group/Fence 已具备多数成员和旧身份隔离基础；Root 现在持久化 `EnrollmentAuthority` 记录，并以严格多数的稳定 Root voter 作为 authority 成员。后台 coordinator 会在稳定 Root cut 上自动发布初始/下一 authority epoch，Root 联合变更期间不会提前推进 epoch。`nodepki.Signer.SealFor` / `OpenTransferredSigner` 已实现目标身份、CA 指纹和 authority epoch 绑定的密封原语，并通过本地加解密回归。`SignerWrappingPublicKey` / `InstallTransferredSigner` 已接入本地 wrapping key 生成、epoch 单调检查、原子 envelope 替换和 Store Sync；Server 会自动恢复已有本地 authority envelope。NodeAdmin 已增加 wrapping public key 获取与密封 envelope transfer RPC，目标端校验当前 Root authority、源 Node mTLS 和本地注册状态，coordinator 已按稳定 authority cut 自动向目标成员重试传输。真实三节点联测已覆盖 authority 扩展、bootstrap 手动移除与 Fence、authority 集合收缩、幸存节点继续签发，以及第四节点重新 enrollment；后续仍需把该场景纳入更广泛的独立进程和故障门禁。
