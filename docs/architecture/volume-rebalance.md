# Volume 故障与数据重平衡

Volume 是 Node 内的容量位置，不是副本或故障域。删除一个故障 Volume 不改变 NodeID、RaftID、Region、Group 成员或 Range 所有权；它只让该 Node 的部分物理持有变成 Repair debt，并使 Node/相关 Region 进入降级观察。

## 管理员操作

管理员可以先处理故障 Volume，再决定是否加盘：

1. 先尝试恢复原 Volume。原路径和 `DUBI_VOLUME` 身份恢复后，继续使用原 VolumeID，Repair 追赶缺口。
2. 确认无法恢复后，管理员明确执行 `volume drain`，再执行 `volume remove`。旧路径已经不可读时，Remove 仍可删除其注册元数据；这不代表数据已经恢复。
3. 可在 Remove 前或 Remove 后添加新 Volume。新目录获得全新的 VolumeID，绝不能伪装成旧 Volume。
4. 新 Volume 不添加也不算操作失败：Node 持续保持降级，Repair 改用其它合法目标；新盘稍后加入后，未解决任务可重新选择本机目标并继续。

Volume Remove 是人工确认的不可逆元数据操作，不由健康超时自动触发。系统不能因为旧路径消失就假设数据已被复制。

## 重平衡目标选择

Repair 按以下顺序选择目标，但每一步都必须重新读取当前 Root/Region 代际、当前引用和容量状态：

1. **同 Node 的健康 Volume**：优先把本 Node 原本应持有的记录写入健康 Volume。这样恢复本 Node 的物理持有，不改变 Node 级多数计算。
2. **同 Region 的其它 Node**：本 Node 没有容量、没有健康 Volume，或管理员已永久移除该 Volume 时，可在同 Region 选择没有该记录的其它 Node。多个 Volume 仍只算一个 Node，不能用同一 Node 的多盘凑多数。
3. **其它 Region**：同 Region 没有合法容量，或当前策略要求跨 Region 多数时，按稳定 Region cohort 写入目标 Region 的严格多数。单 Node Region 必须遵守跨 Region 多数规则。

底座的 `placement.SelectRepairTarget` 已把这套选择规则固化为纯计划：可优先指定原 Node，随后选择首选 Region 的缺失 Node，最后才选择能够补足严格多数的其它 Region。计划输入只接受当前确认 cohort、已认证的持有观察和短期容量/健康观察；Volume 不出现在多数计算中，同一 Node 的多盘不能产生第二票。`recordrepair.RepairSelectedTargets` 执行已选跨 Region 目标，但只返回新的物理持有观察，不发布 `GeoReady`；调用方必须重新收集所有 Region 证据并运行 `GeoPlan.Assess`。计划本身不执行 IO，实际搬迁必须在新的 Root/Region 读屏障下重新校验后提交。

每次迁移都按不可变记录执行，而不是直接复制整个 Pack：

```text
读取当前引用
  → 预约目标空间
  → 写入目标 Pack 并 fsync
  → 读取并校验完整记录摘要
  → 原子发布新的本地持有位置
  → 重新计算 Node/Region 多数
```

目标写入、索引发布或校验任何一步失败，都保留 Repair debt，不删除仍可读的源数据，也不把一次成功写入误报为多数确认。

## 状态与收敛

- 删除故障 Volume 后，Node 保持 Root/Group 成员身份，但本地状态为 `degraded`，直到本 Node 的目标持有恢复，或当前持久 Placement 明确改变其责任。
- 同 Region 临时扩散只增加真实 Node 持有，不降低原稳定多数分母；同一 Node 的多个 Volume 永远不增加多数票。
- 跨 Region 写入必须绑定当前 Region generation、Group epoch、Fence 和引用版本；旧 Region 或旧引用的持有不能复用为 `GeoReady`。
- 新 Volume 后加时，Repair 使用持久任务继续执行并带迟滞，避免在“本机有空间/无空间”之间反复搬迁。
- 在缺少目标容量或缺少权威源时，集群保持降级并暴露未解决 debt；不能为了恢复绿色状态删除最后一份有效数据。

当前已实现 Volume Drain/Remove 的安全边界和本地 Pack 写入停止，并提供了受 Root 当前成员认证保护的跨 Node Repair 传输原语。健康源卷的迁移现在由不可变记录索引驱动：目标 Pack 写入并读回校验后才原子替换索引，迁移游标持久化在记录资源目录，重启会自动续跑；同一 Node 没有空间时仍由上层 Repair 选择同 Region/其它 Region 的真实 Node。迁移完成后，保守 GC 会先更新 Pack catalog 再回收无引用 Pack，空 catalog 才允许安全注销 Volume。原路径缺失或标记损坏时，系统不伪造恢复结果，只清理本地迁移任务并保留跨 Node/Region Repair debt。`dubi volume compact --rewrite` 提供显式的混合存活 Pack 重写；维护入口受单并发和 128 MiB 节点维护预算限制。长期跨节点容量均衡和完整 Geo 端到端验收仍是后续生产资格工作。
