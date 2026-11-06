# Runtime 概念收敛与读路径审查

本次收敛保持正在进行的 Range 迁移长测版本不变。不改数据格式、确认门槛、成员集合或运行参数；CLI 改动仅调整帮助描述。

## 已落实的约定

1. 物理结构是 `Region → Node → 多个 Volume → Pack`。`--store` 是节点元数据目录，默认 `/var/lib/dubi`，不是可独立增删的存储资源。未指定初始卷时使用 `<store>/volume`；显式指定时仅使用指定卷。现有 `StoreID` 继续保护实例身份、资源归属和恢复映射，不新增 Store 生命周期。
2. Runtime 负责物理复制、持有验证、Repair、Rebalance 和恢复；Engine 定义存储语义，Module 定义业务规则及派生结果重建。搜索索引业务重建不能替代 Runtime 对持久字节的修复。
3. State/Object/Log/Index 随实际业务需求落地；Schema、签名/加密和业务调度先采用普通库或 Module 内部逻辑。Message/TimeSeries 为候选，不预设独立资源或身份。节点证书与底座后台任务仍由 Runtime 管理。
4. 当前每个 Tenant/Module/Engine 绑定范围有一个自动创建的初始完整 Range；多区间目录、自动拆分/合并尚未实现。后续补齐 Runtime 路由能力，不新增用户管理的容器。

`Range`、Raft Group、Region 与 Epoch/Fence 分别负责逻辑区间、共识、故障域和旧所有权隔离，继续保留；不是重复层级。

## 读路径代码审查

当前 `recordwrite.Reader.ReadLocal` 依次执行：

1. `readOwner`：在 Owner 一致性读切点读取权威引用与 LocalConfirmation。
2. `Store.Read`：读取并校验本地内容；缺失时从认证成员拉取，落入本地 Pack 后重读。
3. `observeCommittedLocal` / `ObservePublishedLocal`：取得放置计划、复核引用，校验本地持有并观察远端物理多数，验证精确确认记录与当前代际一致。
4. `CheckReadCurrent`：再次检查权威引用、确认记录和 Root 区域计划，拒绝读取过程中发生的覆盖或拓扑切换；这一阶段已不重复远端持有扫描。

有两类成本值得分别测量：同次读取中对同一内容的重复本地校验，以及 Root/Owner 读屏障与远端持有检查的串行往返。但复用必须绑定同一 RecordID、引用位置、请求、Range fence 和区域计划；一次历史成功或节点心跳不能替代有效证明。

当前普通读明确要求新鲜物理多数。把耐久性检查完全移到后台，会改变已有语义，而不仅是代码清理。`read_refresh_test.go` 已覆盖 lost majority、overwrite、changed cohort、Root unavailable 等拒绝场景。因此本轮保留全部门禁，不将“读取正确版本”和“冗余充足”混为概念，也不在未完成安全论证时删除后者的实现。

## 长测结束后的性能验证顺序

1. 复用现有 Trace：`rpc.get.begin`、`read.owner_reference`、`read.pack_read_or_fetch`、`read.initial_majority_check`、`read.final_currentness_check`，以及 `observe.*` 子阶段。父子计时包含关系不能重复相加。
2. 固定相同内容、请求数、入口和 Owner 分布，分开测本区域/跨区域、已持有/首次拉取、已有 Range/首次初始化。CLI 端到端耗时与服务端阶段耗时分别报告，不能将当前低速稳定性长测当成吞吐基准。
3. 优先评估同一请求内的验证结果复用和独立观察并发；有界取消、身份失效、拓扑切换和数据损坏必须继续拒绝，不缓存跨请求的多数证明来换取表面加速。
4. 若要放宽普通读对实时物理多数的要求，先明确在提交后失卷、Owner 切换、覆盖/删除与 Region 代际变化下的正确性和可用性，再修改契约及测试。不得将其伪装成不改变行为的优化。
5. 实现后的回归在 VPS 隔离环境运行，并按实际协议改动决定是否重跑稳定性长测。当前六小时任务继续验证冻结候选，不覆盖二进制、不重启服务、不提前启用诊断开关。

本次交付是概念、职责、开发约束和帮助文案的收敛，尚未改变读路径执行行为，也不宣称已获得延迟或吞吐提升。

## 长测后跟进

六小时冻结验收已结束。后续完成内存预算边界修正、Go 内存观测和首次 placement 主动发布，并仅在 TT02 验证新候选；普通读的安全门禁保持不变。样本、并发限流和新版本资格边界见[内存与首次 Range 延迟验证](memory-range-latency-verification.md)。本页前面的冻结描述对应原概念收敛阶段。

2026-11-05 后续读取优化：普通 Reader 将物理观察阶段末尾重复的 Root 复核合并到返回前的 `CheckReadCurrent`，仍在物理观察后检查 Owner 引用，并在最终门禁验证精确引用/确认与新鲜 Root 计划。写入和耐久重试的完整观察不变；并发节点发现和入群改用独立有界槽。资格与结果见[读取与发现入口验证](read-discovery-verification.md)。
