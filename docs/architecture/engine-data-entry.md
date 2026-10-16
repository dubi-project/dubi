# 进程内 Engine 数据入口

`internal/platform/cluster/enginedata.Service` 是模块与不可变数据底座之间的进程内契约，可以在任意健康 Node 上调用；它不是公开 RPC，也不负责 Tenant 应用身份认证。Server 在启动时用本 Node 的 Root、Owner、共享 Records/Pack 和内部 Group Client 装配它；调用者必须已经获准使用目标 Keyspace。当前 `record put/get` 探针已经通过这条底座入口，实际发布不可变引用并经过物理持有门禁；它仍不是最终 Tenant API。

写入者提供完整内部 Keyspace、路由 Key、不透明 Body、稳定 RequestID 和一个模块定义的 `Publication` 编码函数。底座先通过 Root ReadIndex 找当前 Range/Owner，构造带 Keyspace/来源 Group/Key/摘要的不可变 Descriptor，并将 Descriptor 交给模块编码能力命令。模块 Handler 必须在 Owner 事务里调用 `Transaction.SetRecordReference` 发布**完全相同**的描述；业务元数据由模块自己管理。底座将 Body 持久写到当前稳定 Region 的独立 Node 多数，发布引用后重新观察物理持有，再经 Owner Raft 保存代际绑定的本地确认并做最后复核。只有这些全部成功，`WriteResult.State` 才是 `local_ready`。

失败返回 `unknown`，或在新的 Owner 读切点能确认该请求的精确当前引用时返回 `published_unconfirmed`。后者**不是**已证明不持久：确认可能已经提交但响应丢失；它也绝不是可对用户宣告的 `LocalReady`。调用方用同一 RequestID 和相同内容重试。无法完成读屏障、路由已变或请求被取消时，保守返回 `unknown`；不得把它解释成“肯定没有发布”。没有发布精确引用的 Handler 即使 Raft mutation 已 Applied，也不会得到 `local_ready`。

读取者提供 Keyspace 和 Key。入口重查 Root 路由，按当前 Owner 引用、持久确认与实时物理多数门禁读取；执行读的 Owner 成员缺字节时可从已验证节点拉取到同一个共享 Pack，返回前再次复核。两个 Engine 的同名 Key 依完整 Keyspace 隔离，Pack 不按 Engine 划分。

若入口 Node 不托管 Owner Group，它按 Root 已发布的 Group 成员目录寻找 Owner 成员，通过节点 mTLS 和锁定目标 Node 身份的内部流式 RPC 转送同一个 RequestID、路由、描述与不透明能力命令；Body 按 1 MiB 分帧传送，不进入 Raft 命令。Owner 节点执行完整的物理多数、引用发布和最终复核，入口节点校验回传的引用、RequestID、摘要及确认信息后才返回 `local_ready` 或 `geo_ready`。两节点以下的 Region 自动选择跨 Region 多数写入；读取会复核当前 GeoCoverage、逐 Region 重新观察并从已验证 Node 补取字节。读取同样由 Owner 执行当前引用和多数门禁，再分帧回传并在入口校验完整摘要。`WriteLocal`/`ReadLocal` 显式要求本机 Owner，非 Owner 返回 `ErrOwnerNotLocal`；普通 `Write`/`Read` 对模块透明。

这条路径已用四节点、跨 Region 的非 Owner 入口验证超过单帧的写入和读回；停掉入口首选的 Owner 成员后，同次调用可尝试其他成员，并用同一 RequestID 恢复已提交但调用方丢失响应的写入。Root 领导切换时，入口本地 Root learner 的读屏障可能仍指向旧 Leader；单次本地读有时限，随后只向本地成员视图中的 Root 投票成员索取 quorum-confirmed ReadIndex，且必须等入口本地 Root 目录应用到该索引才解析路由。短暂失败可以重新取切点，无路由等稳定错误不重试；丢失 Owner 多数时不得返回 `LocalReady` 或 `GeoReady`。当前转发只面向 Root 已知的当前 Owner 成员，Range 代际变化时调用方需用相同 RequestID 重试，由新调用重新解析 Root；尚未实现单次调用内的跨代际重路由。后续仍须补 Tenant 授权的外部入口和更完整的前台状态/故障诊断。这里不声称跨 Region 数据修复、GC、成员移除后的自动再平衡或生产资格。

本机即使承载 Owner Group，也可能因本地不可变 Store 故障而无法执行读取。普通 `Read` 在本地执行失败后可转向其他当前 Owner 成员；远端必须重新确认权威引用、持久回执和**实际物理多数**才返回字节。`ReadLocal` 不转送并保留本机故障。关闭的 Store 或缺失的 Pack/Volume 被明确标记为不可用的物理持有，不可与成员资格/证书/位置错误混为一谈；后者仍硬拒绝。真实三节点测试关闭一个 Node 的记录 Store 后，从该 Node 的普通入口读到另两个 Node 验证的 Pack 数据；再停止另一真实持有节点时读失败，不能靠转送制造不存在的多数。当前不自动修复已损坏的本地 Store，也不把这项容错称为成员移除后的再平衡完成。
