# DUBI 告警

告警只负责通知，不自动删除 Node、Region 或 Volume。所有破坏性操作必须由管理员确认并执行。

## 严重级别

### P0

- Root majority 未确认并持续超过 1 分钟。
- Node 进入 `unavailable` 或没有健康可写 Volume。
- `dubi_disk_pressured=1` 且持续增长。
- Volume 出现 `missing`、`invalid`，并且没有可用迁移目标。

### P1

- Node 不可达超过 5 分钟。
- Prometheus target 连续 2 分钟为 down。
- `dubi_node_repair_debt` 持续增长超过 5 分钟。
- Volume migration 长时间没有进度。
- Root leader 频繁变化。
- 可用空间低于容量的 15%。

### P2

- 可用空间低于容量的 25%。
- Compaction 长时间未执行。
- 节点频繁重启。
- 短暂 repair debt 或短暂跨区域 RPC 错误。

告警必须包含 `cluster`、`region`、`node_name`、`node_id`、`raft_id`，并设置恢复通知和持续时间，避免瞬时网络抖动产生噪音。

## 处理原则

1. 先执行 `dubi status`，确认是本地问题、单节点问题还是区域问题。
2. 先恢复原节点或原 Volume。
3. 只有确认无法恢复时，才人工执行节点或 Volume 的删除。
4. 删除后添加新资源，等待自动追赶和再平衡。
5. 以 `available`、Root quorum、可达节点数和 repair debt 为恢复依据。
