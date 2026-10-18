# 集群成员与数据分布

节点加入、移除、隔离和数据再平衡是 Runtime 的正常基础能力，不定义单独的“节点替换”或“节点故障处理”功能。扩容、缩容、维护下线、介质迁移以及节点永久损坏，最终都复用同一组成员和数据分布操作。

## 基础操作

- **Node Join**：新节点通过普通加入流程取得全新的 `NodeID`、`RaftID`、`StoreID` 和 `VolumeID`，随后进入 Root、Region 和 Group 的正常成员协调。
- **Node Remove**：管理员确认需要退出的节点后，提交持久成员变更；底座先处理数据 Group，再处理 Root 成员，最后隔离旧身份。
- **Node Fence**：已移除的 `NodeID/RaftID` 永不复用；旧机器或旧介质重新出现时不能直接重新入群。
- **Placement / Rebalance / Repair**：成员集合或容量变化后，Runtime 按当前 Region、Group、Range 和已确认持有状态补齐数据、校验 Pack 并恢复目标分布。

故障只是触发这些操作的一种情况。健康观察与持久生命周期意图必须分开：心跳超时、一次 I/O 错误或临时卸载只能形成 `Unavailable` 等观察，不能自动移除节点，也不能自动缩小多数派分母。

## 常见操作流程

节点不可用时，先尝试让原节点沿原身份恢复并追赶数据。确认原节点不再使用后，管理员执行普通 `node remove`；新机器再通过普通 `join` 加入，取得全新身份。最后由正常的 Placement、Repair 和 Rebalance 补齐数据并恢复容量平衡。

```text
Node Recovery（可选）
        ↓
Node Remove + Fence（确认需要移除时）
        ↓
Node Join（新身份）
        ↓
Placement / Repair / Rebalance
```

这不是一个额外的替换任务，也不需要新的命令、配置项或状态机。节点扩容和维护缩容也使用相同的 `Join`、`Remove` 与数据分布流程。

## 成员安全边界

Root 身份隔离能力已经存在，但它只服务于成员安全：仍承担 Root 或 Group 当前/待执行责任的身份不能被 `FenceNode` 直接处理。移除必须先走受控的 Root/Group 成员变更；成员真正移除后，旧身份永不复用，旧机器恢复也只能被隔离。历史 Region cohort 不会永久阻塞已经完成移除身份的 Fence。

永久移除不能只删除 Root 目录中的节点记录。对确认的单节点 Region，底座会在同一次明确的 `node remove` 中先记录 Region Group 退休，再完成 Root 注销和身份 Fence；移除完成后，新节点仍走普通 `join`，复用正常的 Region 增长、Repair 和 Pack 校验流程。直接从 Group 或 cohort 删除成员而不经过成员变更，可能错误缩小数据多数分母，必须拒绝。

如果整个 Region 都不可用，管理员仍然逐个执行普通 `node remove`，再让新节点加入同一个 Region 名称。目标 Region Group 已经没有 quorum 时，Root 在明确的删除请求中记录该 Group 已退出，不等待故障 Group 响应。最后一个旧成员移除后，底层可以建立新的内部代际，并让新节点从其它可用 Region 补齐数据；这个代际仍是 Runtime 内部状态，不是用户需要执行的重建流程。

## 操作顺序与门禁

1. 健康巡检只把失联节点标记为 `Unavailable`。节点恢复时保留原身份并追赶数据；如果仍无法达到多数，区域或集群保持 `Degraded`。
2. 经授权的操作者确认需要移除后，提交普通节点移除请求。Root/Group 必须以受控成员变更完成，不能只删除目录记录，也不能用移除绕过数据多数检查。
3. 新机器通过普通加入获得全新身份。空 Pack 不计作已有数据持有；加入后由正常 Placement、Repair 和 Rebalance 自动补齐、校验并调整分布。接纳入口必须由仍可用的集群级 enrollment authority 提供。
4. 数据多数恢复且没有未解决欠账后，区域或集群健康状态才回到 `Normal`。任何阶段不确定时按旧代际失败关闭。

## 介质与数据修复

对仍在役且索引完整的同一 Node，在线或停机登记健康新 Volume 后，后台记录修复可以从当前已确认集合的认证来源重建失卷记录并切换本地位置；旧卷不必先恢复，在线登记也不要求重启。这只修复已经扫描的当前引用，不自动判断整卷已经迁完、不安全移除旧卷，也不改变 Node 身份。

当前成员的后台 Repair 已把分页游标、当前 Region generation、Group epoch、Fence index、节点集合和未解决欠账保存到节点本地 `<store>/dubi-repair-state`；重启只在 Root 集合完全匹配时续扫，代际变化会从头重扫。

当前实现和实验已覆盖受控移除、身份 Fence、新节点普通加入以及部分跨节点 Repair；Volume Drain 的持久迁移账本、失败卷欠账、独立机器/区域故障域和长期容量均衡仍需继续验收。Raft Snapshot 负责新节点快速追赶 Runtime 状态；它不复制旧节点身份，也不代替 Cluster Backup/Restore。
