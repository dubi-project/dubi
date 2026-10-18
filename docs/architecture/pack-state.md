# 通用状态机与 Pack 的提交边界

Volume 生命周期补充：节点内可以先将 Volume 置为 `draining`，停止新的 Pack 写入并继续提供已提交 Pack 读取；Pack Pool 会先封存本地打开的 Collection，并允许受控维护回收完全无索引引用的 sealed Pack。最终移除只接受不存在 Pack 目录的 Volume，发现任何 Pack 数据都会拒绝，不会由底座猜测数据是否仍被能力引用或擅自删除。真正的跨 Volume 迁移和混合存活 Pack Compaction 仍需引用保护与数据多数协议。

Node Runtime 将同一个多卷 `pack.Pool` 注入 Engine Runtime，再交给各 Group 的 `enginestate`。这是通用事务值的物理存储机制，不是 File/Record/Search 的业务实现；不解析能力载荷，也不规定业务索引、TTL、压缩或授权规则。没有新增 CLI、YAML 或用户需要填写的身份。

注意：请求 Result/applied 不证明旧值仍然持有。后继覆盖/删除会替换当前逻辑引用；未来数据多数确认须绑定不可变内容。独立的[不可变记录原语](immutable-records.md)已经实现本地描述/索引/Pack 验证和正式 Runtime 生命周期，但尚未接入本页的事务，不改变下面的既有路径。

## 写入与读取

Handler 仍通过 `Transaction.Get/Set/Delete` 操作能力自己的逻辑键，返回不透明结果。默认小于 64 KiB 的值内联存于该 Group 的 Pebble；大于等于 64 KiB 的值通过节点共享 Pool 存入 Pack。单值上限 64 MiB，与是否装配 Pool 无关；键上限仍为 4 KiB。这里的大小不是 RPC 可接受请求大小承诺，路由命令与网络传输还有自己的上限。

这只适用于通用事务 API，不强迫所有模块或存储引擎都用 Pack。原来的无 Pool 内部构造器保留给隔离测试和内联状态；正式 Node Runtime 默认装配 Pool。既有内联数据不在启动时自动迁移，后续改写时才使用当前落盘规则。默认阈值是内部实现参数，不作为运维配置暴露。

一次已通过 Range Fence 检查的 Raft Apply 按以下顺序执行：

1. Handler 暂存逻辑变更，可以读取自己刚写入的值；同键只保留最终变更。拒绝或返回错误时，不发生 Pack 追加。
2. 对成功事务的最终大值取得 Pool 空间预约，完成 Pack 数据、检查点及目录同步。不同 Group/Engine 的写入共用 Volume 当前 Pack。
3. 在同一 Pebble Batch 中发布本地位置引用或内联值、删除被替换的旧索引、保存请求 Result，并推进 applied index；同步提交后才认为本地 Apply 完成。

Pack 引用使用 `local/pack/<完整逻辑键>`，内联值使用 `data/keyspace/<TenantID>/<EngineID>/<DatasetID>/<能力键>`；同一逻辑键不能同时出现两种表示。引用包含格式版本、TenantID、本地 Location 和载荷 SHA-256；`meta/pack-store` 绑定本节点 StoreID。Pack 外层保存 Engine/Tenant，记录 Key 额外绑定 GroupID 和完整逻辑键。读取既校验 Pack 自身 CRC，又核对这些归属、逻辑键及载荷摘要；错位置、缺失文件或冲突索引返回错误，不能伪装成空值或不存在。

本地位置不进入 Raft 命令或请求 Result，各节点独立选卷、追加并生成自己的引用。Handler 看到的值和结果不依赖物理布局。Transaction 和 ReadView 的逻辑键由底座绑定到 Route Target 的完整 Keyspace，不要求能力自行添加 Tenant/Dataset 前缀。同一 Group、同一 Engine 下，不同 Tenant 或 Dataset 的同名键互不覆盖；Value 内部诊断接口也必须显式传入 Keyspace。该存储隔离不等于身份认证或入口授权，公开多租户授权仍待实现。

## 崩溃、重放和删除

Pack 与 Pebble 不是跨引擎原子事务。顺序保证是“引用发布前，所指数据已同步”，而不是“两个文件同时提交”。

| 中断位置 | 恢复行为 |
| --- | --- |
| Pack 未确认，尚未提交索引 | applied/result 不推进；正常恢复仍按原日志重放。Pack 层处理自身不确定检查点。 |
| Pack 已同步，索引未提交 | 已追加记录可能成为无引用记录，但不对逻辑读取可见；重放可生成新记录，不能按扫描顺序收编旧记录。 |
| 索引/result/applied 已提交，响应未返回 | 重启恢复原结果；相同 RequestID 不再次执行 Handler 或追加 Pack，变更载荷的同 ID 请求仍按原契约报冲突。 |
| Pebble 提交返回不确定错误 | 当前 State 拒绝继续读、写、查询 Result 或生成快照；关闭重开后依据持久 applied/result 判断，不以内存进度猜测成功。 |

多个大值只在全部追加成功后发布整批索引；中间失败不会使半个事务可见，但已写入的无引用记录暂时保留。业务拒绝、事务内被覆盖的大值、Set 后 Delete 的大值都不追加。大值改小值、覆盖和 Delete 只替换/移除索引，不直接回收 Pack。当前保守 GC 会核对完整本地索引，只删除不含任何当前索引位置的 sealed Pack；混合 Pack、跨卷搬迁和快照/备份历史保护仍未完成。

## Raft 快照和恢复

快照包含逻辑键、不透明值、Fence、Result、applied 和成员信息，不传递 StoreID、VolumeID、PackID、Offset 或本地引用表。生成时读取并校验被引用记录，从一致性视图还原真实值；正式文件路径固定 Pebble Snapshot 后在锁外构建，旧内联路径仍持锁。坏数据或丢卷导致快照失败，不能生成缺数据的“成功快照”。当前无物理回收，因此快照期间不会有 GC 删除记录；接入 GC 前仍须增加资源保护登记。

Engine 状态及快照格式为 v7，内联/外置值都由完整 Keyspace 键确定 Tenant 归属，物理表示不改变镜像。恢复先校验整个镜像的版本、Group、index、成员、键域、重复键、归属、Range/Keyspace Fence 双向一致性及持久 Result，再写目标节点自己的 Pack。它包含[区域代际栅栏及成员变更位置](region-fences.md)，以及 v5 引入的持久交接封口/激活，防止成员变化后复活旧栅栏或未激活代际写入。v6 另保存[不可变记录权威引用](immutable-records.md)，v7 保存本地多数确认记录；快照仅复制引用与确认元数据、不携带其物理载荷，不能据此报告当前数据就绪。旧内联恢复用单个同步 Batch；正式文件恢复用私有 staging DB 完整校验、持久恢复屏障和有界批次，末尾 Sync 发布 applied/conf 并清除屏障。失败不服务半份状态；屏障发布后的失败须重开并重放同一持久引用。Pack 阶段可能留下无引用记录，与普通 Apply 相同。

当前 `meta/format=7` 标记仅在空状态库初始化。拒绝没有标记的非空旧状态、其他格式版本以及 v1/v2/v3/v4/v5/v6 快照：早期 Engine-local 键不能可靠判断 Tenant/Dataset，v3 没有区域成员位置栅栏，v4 不认识持久交接封口和写入门禁，v5 不认识权威记录引用，v6 不认识本地多数确认记录；不自动改名或猜测归属。没有执行任何现有数据迁移，迁移工具及生产升级路径尚未完成。无 Pool 的内部状态可将 v7 普通 KV 值恢复为内联值，但已有 Pack 引用的状态仍必须绑定原 Store。本地引用格式保持 v1，Pack 容器保持 `DUBIPK02`；物理混写不变。不能混用旧二进制或降级打开新目录；仍需 P11 的节点级协议/格式协商。

正式 Node 的 Root 与 Engine Group 使用有界有序文件流、SHA-256 引用和流式网络导入，已验证 Root 目录及 80 MiB 数据状态的三节点追平和空索引重启恢复。独立无 artifact runtime 的旧 JSON 仍限制总编码 64 MiB；超过上限返回错误，不因生成失败截断日志。恢复中的 Pack 追加走逐条空间预约，但整个快照的内存、Pebble/WAL 和临时文件尚未统一纳入预算。该输出由 Raft Snapshot 使用，不是独立节点备份或 Cluster Backup，也不代替通用资源登记。详见 [流式快照](streaming-snapshot.md)。

当前 Raft 日志仍携带完整命令，Result 也仍内联保存 Handler 返回值。把状态大值移到 Pack 不等于已经消除 WAL/Result 中的重复载荷，也不提供文件大对象直传、跨区域耐久目标或完整的容量治理。具体能力应返回有界结果；后续还需结果保留策略和统一资源预算。

## 验证

测试覆盖大/小值切换、覆盖、删除、暂存读、业务拒绝、请求去重和冲突、多大值失败时的原子可见性、引用归属/摘要/数据丢失拒绝、错误 Store 重开拒绝、索引提交前后不确定错误、跨 Store 快照重建位置、旧状态/旧快照拒绝且不迁移、多 Tenant/Dataset 同名键在读写/删除/重启/跨 Store 恢复后隔离，以及恢复失败保留原状态。

子进程在“Pack 同步之后、索引提交之前”和“索引同步之后、响应之前”不执行 Close 直接退出，检查重放、已确认值保留、无引用记录保留和不重复追加。Node Runtime 测试通过真实单节点 Raft Group 执行 Routed Apply、带 ReadIndex 的读取、生成快照、关停、成员目录驱动恢复及请求重放，并检查实际 Pack 只保留一次已发布记录。这不是硬件掉电测试，也不代表已验收跨 Region 多数派数据耐久路径。
