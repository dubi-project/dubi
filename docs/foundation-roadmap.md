# 底座可用推进清单

本清单的“可用”指：DUBI 可以作为后续 Capability 的共同集群底座运行、处理故障并恢复数据；不等于独立硬件、真实 WAN、滚动升级和长时负载均已取得生产资格。`record` 当前只是底座验证探针，不作为正式业务模块交付。实现与验收均保持无 YAML、无手工 Node/Raft ID、无副本数配置、无 Capability 专属 Pack。

## 当前优先级：先让基座自身跑稳

对外 Tenant API 暂停扩展；现有 Token 与可选身份探针保留为已完成的边界验证，但不作为底座可用的前置条件。下一段工作按以下顺序推进，不再用 API 进度替代集群进度：

1. **同区域运行闭环**：以真实多节点进程、共享 Pack 写入与物理多数为验收对象，核对启动/加入、任意 Node 路由、单节点故障、原 RequestID 重试、冷重启和当前成员自动 Repair。`record put/get` 的 Raft 内联探针只能验证控制与路由，不能充当 Pack 多数的证据。
2. **故障后的持续可用**：补齐缺失/损坏持有、修复中断续跑和“手动移除旧节点后新节点自动再平衡”；不缩小原稳定多数分母，不以新空节点冒充原持有。让可用状态与未解决欠账能被观察，而不只在测试断言里出现。
3. **多区域数据多数**：落实本地多数与多数 Region 的确认/后台补齐规则，给出 `GeoReady` 与降级状态；跨区故障不能靠 Root 可读或 Region 目录已确认来伪装数据安全。
4. Volume Drain/Pack 回收、节点内迁移、维护门禁和诊断已进入底座闭环；下一阶段再做 Node Snapshot 与集群备份恢复。上述底座闭环稳定后再继续模块公开 API；生产资格另按故障域、资源压力和升级门禁验收。

目前已有真实三节点 TLS/进程内 Capability 测试覆盖通用 Pack 写读、本地多数、路由和部分修复，及四节点跨 Region 非 Owner 转发；这些是开发证据，不等于上述故障闭环全部完成。

新增故障回归确认：一个 Owner 节点的本地记录 Store 不可用时，普通进程内 `Read` 可转向其他 Owner 并重新证明两节点物理多数；严格 `ReadLocal` 仍报本地故障。再失去另一份真实持有时读失败，不以路由成功冒充数据多数。内部流式取数也将关闭 Store/失卷归为可跳过的故障来源，Repair 可继续尝试同一已确认集合中的其他节点；授权和成员变更仍硬拒绝。此项解决故障节点上的读入口及故障来源切换，不是 Store 恢复或手动移除后的再平衡闭环。

每阶段完成时须记录真实代码入口、自动化验证、已知限制和下一阶段依赖；不能用规划接口或模拟持有代替已落盘且复验的物理数据。功能证据以 `spec.md` 的术语和多数派规则为准，生产资格另按 `docs/production-readiness.md` 追踪。

| 顺序 | 底座能力 | 可用验收 | 状态 |
| --- | --- | --- | --- |
| 1 | 通用数据写入与稳定本地多数 | 单 Region 三节点中，实际不可变字节落在稳定集合的严格多数独立 Node 后才能确认 `LocalReady`；权威引用与请求去重可恢复。仅一个位置成功或拓扑切换时失败关闭，不能把 Raft 提交、Pack 本地同步或响应 ACK 当成数据多数。 | 进行中：内部 `Writer.WriteLocal`/`Reader.ReadLocal` 已接入进程内、能力中立的 `capabilitydata.Service`，负责 Root 路由、Owner 执行和精确发布校验；两个测试 Capability 在同一 Group/Pack 写读并重启恢复。真实三节点 TLS 验证请求重试、丢失确认响应后的恢复与扩容后旧确认失效；当前成员已有自动物理补齐。非 Owner Region 的第四节点已通过节点认证的流式 RPC 透明写入/读取超过单帧的数据；首选 Owner 停机后同次调用切到其他 Owner，原 RequestID 重试恢复相同引用，丢失多数时失败关闭。仍须完成可重复的独立进程/故障门禁与基础健康状态；Tenant API 是后续独立阶段，不能把 demo 命令称为通用数据 `LocalReady` 入口。 |
| 2 | 自动 Repair 与手动移除后的自动再平衡 | 持有状态与目标分离；来源缺失、中断、重启后任务自动续跑。节点失联只标记状态；管理员确认永久失效后手动移除，新节点以全新身份加入，底座自动补齐并恢复多数，旧盘重现不得直接入群。 | 进行中：区域增长已用旧集合严格多数的精确封口观察授权成员任务，并允许新节点在旧成员离线时预热 Pack；候选确认前新增一次封口清单逐条物理多数复核。当前成员的进程内巡检按 Owner 引用分页补齐本地 Pack，修复游标、Region generation、Group epoch、Fence index、节点集合和未解决欠账已持久保存，进程重启后会校验当前 Root 集合再续扫；代际变化会丢弃旧游标并从新集合重扫。Volume Drain 后新增的本地迁移账本按记录索引分页搬迁健康源卷，目标 Pack 读回校验和索引 Sync 成功后才推进游标，后台进程重启自动续跑；原路径缺失时允许管理员显式丢盘移除，但不解除跨 Node/Region Repair debt。Group 层已支持显式维护移除和严格多数预检；`noderemoval` 现在按单一 Root 快照生成确定性跨 Group 计划，复用持久 GroupChange 断点续跑，并由 Server 装配真实 Group/Root Raft 执行器，支持成员移除、丢响应复查、旧身份 Fence 和本机 Node-authenticated `dubi node remove <raft-id>` 入口；区域代际现在允许在维护移除后保留旧 voter 多数、封口并由新节点普通加入触发恢复增长，协议层已通过现有区域增长回归；加入票据可从任意当前 Root 成员创建，并自动指向现行 enrollment authority，已覆盖整区失效后的同名 Region 新节点加入。跨 Node/Region 数据多数的完整生产验收仍未完成。详见[Volume 故障与数据重平衡](architecture/volume-rebalance.md)。 |
> enrollment authority 的 bootstrap 移除闭环已在真实三节点联测中通过：authority 自动扩展，bootstrap 手动移除并 Fence 后，幸存节点完成 authority 收缩并继续签发，第四节点可重新加入。表中阶段 2 的其余内容（独立进程故障门禁、真实再平衡和长期验收）仍保持未完成状态。

补充：阶段 2 已新增跨 Node Repair 的内部传输原语。目标 Node 自己从认证来源拉取并写入本地 Pack；本机 Store/Volume 不可用时，当前确认 cohort 的其它 Node 可作为临时目标，但本地故障仍保留为 Repair debt，不会被误报为本机已恢复。跨 Region 目标、持久搬迁任务和最终多数复验仍未完成。

阶段 3 补充：已新增 `recordrepair.RepairSelectedTargets`，可执行 `GeoPlan` 已选的跨 Region 目标，并返回新的物理持有观察；它不会修改 Region 分母或发布 `GeoReady`，必须由上层重新收集完整区域证据后复验。跨 Region Repair 现在有本地持久任务账本，节点重启后按当前 Range/Region 计划重新观察、修复并在完整 Geo 多数复验成功后删除任务。

| 3 | 多 Region 数据多数 | 稳定 Active Region 集合推导 `GeoReady`；至少三本地节点采用本地多数前台确认、后台跨区补齐，不足三节点必须同步得到多数 Region。故障不能缩小分母。 | 进行中：Root 已能在同一 applied cut 返回明确的多 Region cohort/Node 规划视图，并持久化 Range→Region Group 映射；`rangeplacement` 后台协调器只从稳定 confirmed cohort 自动发布确定性映射，不接受在线节点缩容。`placement.PlanGeo`、`StageGeo` 固定并写入每个 Region 的 generation、Group epoch、fence 和本地严格多数，现已覆盖 1/2 节点小 Region；`GeoDataCoverage`/`AssessPublication` 可验证同一 RecordID 是否达到区域严格多数并绑定当前引用切点；Owner Group 已持久化 `GeoCoverageRecord`，`CheckCommittedGeo` 纳入实时复核，`GeoMetadataMirror` 原语已能在目标 Region 账目中保存引用切点；新增 capability RPC 跨节点提交并等待目标 StateMachine Result，`Writer.WriteGeo`/`WriteGeoFromRange` 在配置传输器时先完成全部 Region 镜像再确认；`placement.SelectWritePolicy` 已明确大 Region 本地优先、小 Region 必须跨 Region 同步。日志尾部追赶和来源 Region 丢失后的重建仍待完成，完整小 Region 端到端故障联测也待补齐。 |
| 4 | Volume 与 Pack 生命周期 | 加卷后参与放置，限速均衡；卷 Drain/安全移除可续跑；引用与快照保护下的 Pack Compaction/GC 不误删活数据。 | 已完成底座闭环：Volume 有持久 `BeginDrain → RemoveDrained` 边界；Drain 停止新 Pack 写入但保留读取；健康源卷由持久游标驱动迁移，目标写入/读回校验后才替换索引，重启自动续跑；迁移后 catalog 先持久更新再回收死 Pack，混合存活 Pack 可通过 `dubi volume compact --rewrite` 显式重写。迁移/压缩受节点维护预算和单并发门禁限制。跨节点长期容量均衡与完整生产验收仍未完成。 |
| 5 | 底座级恢复 | 所有持久资源登记与一致性 Checkpoint；Node Snapshot 以新身份替换丢失节点；Cluster Backup/Restore 在独立目标封存、校验并从空集群恢复。底座不解析能力载荷。 | 未完成：Raft 快照已具备，不是 Node Snapshot 或 Cluster Backup。 |
| 6 | Tenant 与模块接入边界 | 应用凭据限定 Tenant；两个不同测试 Capability 在同一 Group/Pack 运行且互不越权。通用入口不把 Node 证书当租户身份，不要求模块模拟 File。 | 进行中：内部 Keyspace/Mux 隔离及[高熵 Token 的 Root 哈希登记、撤销、快照恢复和 Tenant 验证](architecture/tenant-entry.md)已实现；Node 管理命令可创建/撤销 Token。独立可选 HTTPS listener 的 `GET /v1/me` 已验证无 Node 证书的 Token 认证、撤销和内部 RPC 隔离。受约束模块数据 API、审计/限流未实现；当前 `record` 仍是 Node 认证的开发入口。 |
| 7 | 低负担运维与故障门禁 | 默认命令无需反复传 Store/地址；可查询真实读写、数据覆盖、修复、卷和备份状态，`doctor` 指出不可用原因；格式不兼容明确拒绝。多进程强杀、分区、失卷、恢复用例可重复运行。 | 部分完成：`status` 显示已确认 Root 视图、Root 多数门槛和 Region 分布、独立的本机卷健康、本机修复欠账，以及带 scope 标记的本地/集群健康结论；修复游标和欠账保存于节点本地状态，恢复时校验 Root 代际；修复告警只在完整无欠账扫描后解除。本地选择、磁盘背压与基础门禁已有。全群数据覆盖、逐节点在线探测及备份状态尚未接入。 |

实施顺序按依赖推进，但安全测试、资源界限与诊断随各阶段同步补齐，不把它们拖到最后。阶段 1 优先采用不可变记录作为模块无关的数据载体：先持久复制并复验，再发布权威引用；真正的确认状态必须绑定稳定参与集合和引用代际。来源 Node 的当前健康只用于重试排序，不能改变分母。对于只存在 Raft 内联状态的验证值，单独标明其确认语义，不能把它算作独立 Pack 数据已达到 `LocalReady`。

阶段 2 的身份隔离切片已落地：Root 只允许对已登记且没有 Root、Group 当前或待执行责任的 Node 持久 `FenceNode`；节点发现、Root join 与新 Group 成员发布拒绝其原身份，Root 快照校验 Fence。**在役失盘节点不能靠健康超时自动移除**，避免绕过原数据多数。Group 层已支持显式维护移除且保留当前 voter 严格多数；后续要补跨 Root/多 Group 的管理员入口、最终 Fence 编排、新节点普通加入后的自动 Repair 和最终物理复验，不建立替换任务。enrollment authority 已有 Root 持久 authority epoch/member 记录、稳定 Root 多数调度、wrapping public key 获取和密封 envelope 自动传输；真实三节点 bootstrap 移除后新节点加入闭环已通过，后续纳入更广泛的故障门禁。详见[节点故障、手动移除与自动再平衡](architecture/node-replacement.md)与[集群级 enrollment authority](architecture/enrollment-authority.md)。

小 Region 的结构与运行时边界：Root cohort、Region fence、Geo placement、物理 staging 与 Geo 回执校验已允许 1/2 节点，`PlanLocal` 与 `LocalConfirmation` 仍拒绝小 Region 本地前台确认。自动形成数据 Group 暂保持至少三节点；现有多数与手动移除检查不放宽，也不建立节点替换流程。整区域明确移除后由底层自动隔离旧代际并允许同名 Region 的新节点重新形成集合，用户不执行额外重建命令。

增长协调器现已把旧封口观察从“全员在线”改为固定旧集合的严格多数：每份必须有指定节点自己的 Root/Owner 读屏障、同一旧 fence 与同一已应用 seal；不可达或滞后的观察不计数，少于多数不授权成员任务。新候选发布仍要求完整新集合可观察，并重新扫描权威封口引用清单，逐条检查固定新集合的真实 Pack 持有是否达到严格多数；失败不发布 Root 新代际。维护移除后的恢复增长沿用相同封口和候选门禁，旧 Fence 只作为恢复来源，不能恢复业务写入；仍需真实独立进程联测验证完整 Pack 再平衡和跨 Region 修复。

阶段 1 已有 `recordwrite.StageLocal` 与 `recordwrite.ObservePublishedLocal` 两段可组合的内部原语。两段都按当前路由的 Owner Group，而不是不可变描述中的来源 Group，固定 Root 已确认 Region 集合；后者以 Owner Group 读屏障确认精确 RequestID/发布位置，重新逐节点观察物理记录，并复查 Root 代际与引用。它允许在发起节点重启后仅凭持久引用重做观察。`ConfirmationClaim` 将观察绑定引用发布位置和 Region/Group fence，`routedstate.EncodeLocalConfirmation` 经过 Owner Raft 进入 capstate v7；状态机核对当前 Range/Region fence、当前引用、稳定多数和独立节点，原子保存确认记录与去重 Result。覆写或删除引用时同事务撤销旧确认；快照保存并校验关联。确认记录是历史物理复核的可恢复账目，既非可转交签名，也不单独证明当前字节健康。

`recordwrite.CheckCommittedLocal` 作为内部最终门禁，重新复验物理多数、当前引用、持久确认 Result 和 Root 集合；旧 generation 的确认在扩容后不能复用。`Writer.WriteLocal` 将这些步骤组装为内部协议，原 RequestID/载荷重试会读已持久发布结果；确认响应丢失时先校验重试 Body 并复验当前回执、物理数据和 Root 集合，仍有效即可返回而无需重新传 Pack。若回执过期或实时多数不足，则继续补写并用自动生成的内部确认 RequestID 重新提交。`Reader.ReadLocal` 只接受当前确认和实时多数，执行读的 Owner 成员缺字节时从已验证来源拉取至本地共享 Pack，返回前再次检查；本地已标记损坏不会被悄悄覆写。进程内 `capabilitydata.Service` 已将这些原语接到通用模块契约，并支持非 Owner 节点透明转送给 Owner 执行，见[能力数据入口](architecture/capability-data-entry.md)；下一段先完善真实运行、缺失/损坏的 Repair 与降级状态，再推进多 Region 数据多数。Tenant 授权前台入口留待基座故障闭环之后。还须定义“已复制但尚未发布”“已发布但尚未满足 `LocalReady`”“已确认”的用户可见性；Root 代际变化或持有节点损坏不得把前两种状态伪装成已确认。后台 Repair 要能从权威引用与稳定集合重建欠账。
