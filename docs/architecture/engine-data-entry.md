# 进程内 Engine 数据入口

`internal/platform/cluster/enginedata.Service` 是模块与不可变数据底座之间的进程内契约，可以在任意健康 Node 上调用；它不是公开 RPC，也不负责 Tenant 应用身份认证。Server 在启动时用本 Node 的 Root、Owner、共享 Records/Pack 和内部 Group Client 装配它；调用者必须已经获准使用目标 Keyspace。当前 `record put/get` 探针已经通过这条底座入口，实际发布不可变引用并经过物理持有门禁；它仍不是最终 Tenant API。

写入者提供完整内部 Keyspace、路由 Key、不透明 Body、稳定 RequestID 和一个模块定义的 `Publication` 编码函数。底座先通过 Root ReadIndex 找当前 Range/Owner，构造带 Keyspace/来源 Group/Key/摘要的不可变 Descriptor，并将 Descriptor 交给模块编码能力命令。模块 Handler 必须在 Owner 事务里调用 `Transaction.SetRecordReference` 发布**完全相同**的描述；业务元数据由模块自己管理。底座将 Body 持久写到当前稳定 Region 的独立 Node 多数，发布引用后重新观察物理持有，再经 Owner Raft 保存代际绑定的本地确认并做最后复核。只有这些全部成功，`WriteResult.State` 才是 `local_ready`。当集群有多个已确认 Region 时，Owner 还要在本地写入前确认至少一个其它 Region 能建立自己的 Raft 多数；此判断来自 Root 当前已确认的集群 Region，而不是单个 Range 的固定 placement。所有其它 Region 都失去多数时拒绝 local-ready 写入，即便该 Range 的旧 placement 只含本地区域。此探测不等待远端数据复制，也不能排除探测后立刻发生区域故障的竞态；集群持久拓扑本来只有一个已确认 Region 时仍按本地多数确认。

失败返回 `unknown`，或在新的 Owner 读切点能确认该请求的精确当前引用时返回 `published_unconfirmed`。后者**不是**已证明不持久：确认可能已经提交但响应丢失；它也绝不是可对用户宣告的 `LocalReady`。调用方用同一 RequestID 和相同内容重试。无法完成读屏障、路由已变或请求被取消时，保守返回 `unknown`；不得把它解释成“肯定没有发布”。没有发布精确引用的 Handler 即使 Raft mutation 已 Applied，也不会得到 `local_ready`。

托管节点还在多 Region 的本地写入返回成功前，同步保存不透明引用的 Geo 补齐提示，覆盖进程内 Engine 入口和跨节点转发接收端。若本地确认已完成而提示同步失败，返回错误并保留已确认引用；即使结果中仍有 `local_ready`，调用方也必须检查错误，并以原 RequestID 重试，不能向用户报告整个操作成功。单 Region 不产生此 Geo 提示，小 Region 仍使用同步 GeoReady 路径。补齐提示使新写入无需等待历史记录全量扫描，不等于远端已持有数据。

读取者提供 Keyspace 和 Key。入口重查 Root 路由，按当前 Owner 引用、持久确认与实时物理多数门禁读取；执行读的 Owner 成员缺字节时，先从当前已确认 Cohort 中已认证的成员拉取到同一个共享 Pack，然后重新证明真实多数并复核引用与区域时效。拉取本身不授权返回数据；只有一份缓存、来源失联、未完整落盘或期间发生覆盖/区域变化，均不能作为成功读取。先补本地缺失副本也避免静默失联节点耗尽持有探测期限，导致在线节点一直无法补齐。两个 Engine 的同名 Key 依完整 Keyspace 隔离，Pack 不按 Engine 划分。

成员计划切换后，若当前引用已有持久本地确认，但仅 Cohort/Group epoch/fence 已过期，读取先证明当前实际物理多数，再允许当前 Owner leader 按需刷新这一条确认，复用 `EnsureCurrentLocalConfirmation` 的持久提案、精确 Result 和最终计划复核。随后丢弃旧读结果并完整重读一次；回调成功或仅 Submit ACK 都不能直接授权返回字节。无确认、发布身份不符、缺少多数、成员未确认、权限错误等不触发此路径。每个 Node 最多一个同步尝试，不排队，不新建后台任务；只处理不超过 4 MiB 的记录，尝试上下文最多 5 秒且服从调用方更短期限。忙碌、非 leader 或较大记录继续原有 Geo/成员转送和后台修复路径；正常已确认读没有额外确认提交。

若入口 Node 不托管 Owner Group，它按 Root 已发布的 Group 成员目录寻找 Owner 成员，通过节点 mTLS 和锁定目标 Node 身份的内部流式 RPC 转送同一个 RequestID、路由、描述与不透明能力命令；Body 按 1 MiB 分帧传送，不进入 Raft 命令。Owner 节点执行完整的物理多数、引用发布和最终复核，入口节点校验回传的引用、RequestID、摘要及确认信息后才返回 `local_ready` 或 `geo_ready`。两节点以下的 Region 自动选择跨 Region 多数写入；读取会复核当前 GeoCoverage、逐 Region 重新观察并从已验证 Node 补取字节。读取同样由 Owner 执行当前引用和多数门禁，再分帧回传并在入口校验完整摘要。`WriteLocal`/`ReadLocal` 显式要求本机 Owner，非 Owner 返回 `ErrOwnerNotLocal`；普通 `Write`/`Read` 对模块透明。

这条路径已用四节点、跨 Region 的非 Owner 入口验证超过单帧的写入和读回；停掉入口首选的 Owner 成员后，同次调用可尝试其他成员，并用同一 RequestID 恢复已提交但调用方丢失响应的写入。Root 领导切换时，入口本地 Root learner 的读屏障可能仍指向旧 Leader；单次本地读有时限，随后只向本地成员视图中的 Root 投票成员索取 quorum-confirmed ReadIndex，且必须等入口本地 Root 目录应用到该索引才解析路由。短暂失败可以重新取切点，无路由等稳定错误不重试；丢失 Owner 多数时不得返回 `LocalReady` 或 `GeoReady`。后续仍须补 Tenant 授权的外部入口和更完整的前台状态/故障诊断。这些开发验证不代表生产资格。

转发只面向 Root 已知的 Owner 成员。执行端先用本地 Root 目录核对 Range/Owner 目标，不额外增加正常路径的 WAN 屏障；执行前明确过期时以专用错误请求重查。入口在同一次调用中丢弃旧 admission Root 切点，最多四次重新解析，保留 RequestID、Key、Body、取消及期限；相同 Descriptor 复用原 Publication 编码。读遇到旧 Owner/Fence 也可刷新。写仅在所有已尝试目标都明确未提交、结果为 unknown 且无提交引用/确认时刷新；任一次超时、提交不确定或已发布结果都阻止跨 Owner 自动重试。授权和损坏错误不被路由刷新掩盖。此功能不包含 Range 迁移、拆分或跨 Owner 去重状态转移协议；结果不明时调用方仍按原 RequestID 和相同内容重试。

本机即使承载 Owner Group，也可能因本地不可变 Store 故障而无法执行读取。普通 `Read` 在本地执行失败后可转向其他当前 Owner 成员；远端必须重新确认权威引用、持久回执和**实际物理多数**才返回字节。`ReadLocal` 不转送并保留本机故障。关闭的 Store 或缺失的 Pack/Volume 被明确标记为不可用的物理持有，不可与成员资格/证书/位置错误混为一谈；后者仍硬拒绝。真实三节点测试关闭一个 Node 的记录 Store 后，从该 Node 的普通入口读到另两个 Node 验证的 Pack 数据；再停止另一真实持有节点时读失败，不能靠转送制造不存在的多数。当前不自动修复已损坏的本地 Store，也不把这项容错称为成员移除后的再平衡完成。
