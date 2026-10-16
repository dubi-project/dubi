# 流式 Raft 快照实施记录

目标：参考 Byfile 的“持久分段文件 + 小型 Raft 引用 + 流式接收 + 校验后恢复”机制，支持大状态，并保持模块数据格式不透明。快照不是 Node Snapshot/Cluster Backup，不能替代后者的身份恢复和资源完整性协议。

## 当前已实现：有界流格式

`internal/platform/storage/snapshot` 定义 `DUBISN01`，已接入正式 Node/Server 的 Root 与 Engine Group 快照链路。未附加 artifact Store 的独立测试/嵌入式 adapter 仍使用有界内联镜像。该路径的完成不代表整个基座已取得生产资格。

流头包含格式版本、状态格式标识、GroupID、Raft index 和不透明 Conf；不含本地路径、Pack 位置或 Node 身份。键按字节严格升序；每条记录用长度前缀及 CRC32C 包含键、值；结束标记包含记录数。头部必须是规范编码并校验 CRC。消费者必须读到有效结束标记，外层还必须核验整个持久文件的 SHA-256、确切长度和预期 Group/index/conf。

默认内部硬边界：头 64 KiB、键 64 KiB、单值 64 MiB、整个编码流 1 TiB。流大小不是内存分配请求；解码先核对每个声明长度、剩余文件长度和整数范围，再分配单条记录。编码器仅保留前一键，解码器每次最多交付一条记录，不积累整个镜像。调用方若保留所有记录仍会耗尽内存，必须通过恢复管线进一步约束。

重复键、乱序、坏 CRC、截断、记录数不符、尾部多余字节和超限均拒绝；编码/解码失败后不能在同一实例继续写或继续恢复。临时文件是否发布由持久层负责，格式组件本身不做文件操作。

测试覆盖所有截断位置、逐字节损坏、合法 CRC 的乱序、恶意 uint64 长度、短写后禁止完成、格式 fuzz，以及通过管道传输 80 MiB 以上图像而不构造整份字节缓冲。该管道测试不是 Raft 追赶或磁盘耐久验收。

## 当前已实现：持久文件与不可变引用

同包提供 `Store.Create/Import/Open`，只打开由调用者准备并拥有的目录，不重建消失的目录；固定目录句柄、文件锁和路径身份检查防止替换后继续写到错误位置。`Reference` 只含版本、Group、index、大小及规范小写 SHA-256，不携带本地或远端路径，完整编码上限 1 KiB。

创建使用随机、排他新建的临时文件，流式写入并计算摘要；文件同步后通过硬链接发布摘要文件名，再同步目录，全部成功才返回引用。发布不覆盖旧文件，同名文件必须验证长度、Group/index 和完整摘要；损坏的同名文件拒绝，留待明确修复流程。导入先验证有界帧、有效结束标记、确切 EOF 和整个摘要，再走相同持久发布路径。

读取检查普通文件类型、文件身份、大小和 Group/index，流式计算完整摘要；调用者必须检查读取及 Close 的错误，提前 Close 不能取得“已验证”结论。应用格式、Conf 与逻辑状态的语义校验仍由状态机负责，Import 成功不是 Raft 安装确认。

写入期间分块检查取消与 Store 磁盘安全余量，单 Store 串行创建/导入，等待写入槽支持取消；已发布不可变文件的读取不被新文件构建阻挡。关闭等待活动写入释放目录句柄。这不是与 Pebble/Raft/外部进程共享的空间预约，也未完成节点级接收调度。取消检查不能强行中断任意阻塞的第三方 Reader，网络接线必须使用可随 RPC Context 中断的流。

正常失败仅清理本次创建且未公开引用的临时路径，不删除已有摘要文件；进程退出遗留的临时文件和已发布但未被引用的文件保留，不在启动时猜测 GC。测试覆盖三个发布时点的子进程直接退出、重开和幂等重建；这是软件退出/注入错误测试，不是掉电、文件系统断连或真实 ENOSPC 验收。

## 当前已实现：Raft 两阶段快照生成边界

Raft Group 提供可选 `SnapshotPreparer`：在 Group 锁内快速固定 index/conf 与不可变视图，随后在锁外构建镜像，同 Group 同时只允许一个构建。正常提交、ReadIndex 和 Tick 可在构建期间运行；关闭取消构建并等待其资源释放，再关闭状态机与日志。构建/释放失败不会截断日志，也不单独毒化正常共识。正式 engine runtime 和 Node Root adapter 均已接入。

`logstore.SaveLocalSnapshot` 区分本地历史 cut 与接收到的快照：本地快照只能取已持久提交且 term 相符的位置，保留构建期间更新的当前 ConfState 和 cut 之后的日志；快照自身仍携带该历史 cut 的成员信息。测试覆盖慢构建期间的写/读/Tick、并发构建拒绝、关闭取消、失败后重试，以及构建期间加入 Learner 后重启不回退成员信息。

## 当前已实现：enginestate 的一致性文件输出

`enginestate.PrepareSnapshotTo` 固定 Pebble Snapshot 与准确 index/conf，文件构建不持有 enginestate 锁。按逻辑键归并内联记录和 Pack 引用，再输出不透明真实值；一条条校验归属、摘要、Fence 和 Result，使用同一 Pebble 视图的点查验证关联，不为所有值或所有 Range 建立内存索引。Pack 仍按 Node 共享，不按此输出格式拆分物理 Pack。

捕获的视图有明确释放责任，State.Close 等待它释放后才关数据库；调用者还须保持节点的共享 Pack Pool 和 artifact Store 存活。底座未来接入 GC 前仍需将这些引用加入持久资源保护。

本地测试验证了真实 enginestate 中 80 MiB 值的文件输出及恢复到真实磁盘 Pack、捕获后新写入不混入旧 cut、相同逻辑状态在内联和外置布局下生成完全相同的引用/摘要、坏 Fence/冲突引用/取消不发布文件，以及关闭时的视图生命周期。大值源端使用测试内存 Pack，目的端使用真实 Pool；不是多节点大状态追赶验收。

正式 Node 通过 snapshot-enabled engine runtime 使用该接口；不附加 artifact Store 的独立 runtime 保留旧内联协议。节点停止时取消生命周期恢复 Context，而不是用单次客户端 RPC 的超时撤销已经被 Raft 接收的恢复。

## 当前已实现：enginestate 有界恢复与崩溃屏障

`enginestate.RestoreFrom` 已接到正式 snapshot-enabled adapter。先把流逐条写入私有临时 Pebble，并核验确切长度、完整 SHA（含 reader.Close 错误）、格式、Group、index、Conf 及所有逻辑 Fence/Result；这些预检不修改原状态、不追加 Pack。通过后才用一个同步 Batch 同时写入持久恢复标记并清除旧逻辑命名空间。

安装采用约 1 MiB 的批次（单条记录允许到格式硬上限 64 MiB），不把整个镜像装入内存或单 Batch。大值按目的节点共享 Pool 重新落 Pack；其余值、Fence 和 Result 落 Pebble，末尾 Sync 同时发布 applied/conf 并清除恢复标记。中途失败拒绝正常读写；重开发现标记时报告 applied=0，要求上层先从已持久化 Raft 引用重放相同 Group/index/SHA/Conf，不能换 Store、换图像、回退到内联镜像或把缺失文件视作空状态。最终提交结果不确定时也要求重开判断实际持久状态。

此路线参考 Byfile 的持久恢复屏障，当前使用分批写入而非 SST 直接导入：增加一次 staging 数据落盘和顺序读取，以保持 bounded memory 和清楚的恢复边界。临时 DB 的 Cache/Memtable 有内部限制，批次/单值上限不是进程 RSS 保证。逐批检查取消和磁盘余量；外部磁盘写入不受其空间预约控制。恢复期间持有状态锁，不对外提供半完成视图。

正常退出只删除本次生成且目录身份仍匹配的私有 staging 目录；路径被替换时拒绝清理，直接崩溃遗留目录暂不自动清理，artifact 仍是恢复权威。真实 Pool 的五个阶段子进程退出测试、Pack 错误、取消、提交“已落盘但报错/未落盘报错”、异 Store/内联重建、错摘要/错 Conf/坏 Fence/未知命名空间和缺失 artifact 均有测试。它们不是掉电/真实 ENOSPC 证据；正式接线先导入并保留完整 artifact，再由 Raft 持久引用并恢复状态。

## 当前已实现：Root 文件快照与目录校验

`rootstate.PrepareSnapshotTo/RestoreFrom` 与 engine 共用有序流、持久 artifact 及私有 staging 组件。Root 使用 `rootstate-v1` 格式；固定 index/conf 后锁外输出，恢复前完整核验文件，并用同一 staging 视图的点查验证 Node/NodeID、Group 成员、成员变更/活动任务、Tenant/Dataset/Range/RangeID 的双向关联及请求结果。未知命名空间、无效 JSON、错归属和缺关联均在修改原状态前拒绝。OwnerFenceIndex 属于数据 Group，不错误地用 Root index 限制它。

安装同样采用持久恢复屏障、约 1 MiB 批次及最终 Sync；恢复中断后正常接口拒绝服务，重开报告 applied=0，仅允许同一引用及 Conf 重放。Root 不包含物理 Pack 位置，也不增加运维配置。旧独立 State 的内联格式继续保留；新文件格式不能交给不支持它的旧二进制。

本地测试覆盖固定视图与关闭等待、完整目录恢复、15 类损坏目录预检、取消、已落盘但报错的提交、缺 artifact/替换引用或成员信息拒绝、5 个阶段子进程直接退出及恢复。另用 72 个 1 MiB 的合法合成请求结果验证超过旧 64 MiB 上限的文件恢复；这是尺寸/边界测试，不代表真实业务目录负载或物理掉电资格。

## 当前已实现：节点资源登记与正式装配

Node 自动登记 `<store>/dubi-snapshots`、父登记文件 `DUBI_SNAPSHOT_STORAGE` 和目录内 `DUBI_SNAPSHOT_OWNER`，身份绑定当前 Node/Raft/Store 与自动生成的 artifact 集合 ID，不增加用户参数。先同步待完成登记，再写不可覆盖的 owner 标记，最后原子发布 active；重启只恢复同一 pending 身份。注册完成后，缺目录/owner/登记、异 Store/Node 或目录替换均拒绝，不创建空替代品；运行期间也检查登记与 owner。

Node 在 Pack Pool 之后打开 artifact Store，在关闭 Group 后关闭 artifact Store/Pack/Volume；关闭先取消恢复生命周期，并发 Close 等待同一结果。流式 Import 不占 Host/Group 锁，完整持久后才交给 Group.InstallSnapshot；只允许已打开且支持文件恢复的 Group，包括正式 Root adapter。Server 将同一 Node artifact opener 提供给发送端。所有历史/孤儿快照保留，无资源 GC，尚非通用资源清单/Checkpoint 协议。

登记四阶段子进程退出、身份不再生、active 资源丢失不重建、pending 目录替换、运行中登记丢失、节点启动失败释放锁均有本地测试。

## 后续接线与验收

### 已实现的网络组件边界

`raftrpc` 支持 `DUBISW01` 前缀、最长 64 KiB 的小型 MsgSnap 和后续 artifact 文件流。Header 摘要绑定完整 envelope，引用再绑定 Group/index/完整文件长度和 SHA；旧内联消息仍保留原 64 MiB 上限，禁止裸引用走旧通道。数据帧最长 1 MiB，支持任意帧边界拆分，必须完整消费声明长度并确认真正 EOF；截断、第二个 Header、空帧和尾部多余帧均拒绝。

发送端与接收端已由正式 Node/Server 提供 artifact opener 和 `SnapshotStreamInstaller`；先持久导入再交给 Raft，并在状态持久安装后返回 applied index。发送端为文件流额外预约 2 MiB，并以 2 个跨 Peer/Group 的槽限制排队和活动流；接收端保留 2 槽/128 MiB 编码预算，文件流只预约有界帧空间，旧内联消息仍预约完整编码长度。Codec 解码单条值、恢复 staging 和 gRPC 内部资源不是这个编码预算的组成部分。

本地 mTLS 组件测试已传输并持久导入 80 MiB 文件，验证安装等待期间无提前 ACK、控制心跳可继续、字节和槽占用持续到确认。另有逐字节拆帧、错 envelope/引用/内容、取消、缺 EOF、错误 ACK、接收不完整、发送 Reader.Close 校验失败及队列/关闭回收测试。

正式 Server 三节点测试使用独立 Node/Raft/Store 身份、每节点证书和真实磁盘 Pack：停掉一个数据 Follower，两节点写入 80 MiB，并在 Root 新增 Tenant；分别为 Root 和数据 Group 生成快照/压缩日志。落后节点重启后经正式文件流追平，两类 Raft 日志中的快照均只保留小引用。随后保留旧本地索引、以空 Root/数据索引再次重启，从已持久引用恢复目录及全部值。该测试仍是本机多 runtime/目录，不代表独立主机、物理盘或 Region。

### 自动发送超时策略

发送端不再使用 2 分钟覆盖接收和安装的单一总超时。默认且无需配置：

- 连接/发送阶段连续 2 分钟没有成功发送一帧则取消；每个有界帧 Send 成功更新此期限。这里的“进展”只代表 gRPC 接受发送，不代表对端持久化。
- 源文件完整读取、摘要/Close 校验通过后，CloseAndRecv 切换到独立安装等待：15 分钟 + 按 1 MiB/秒计算的文件大小余量，上限 6 小时；不因心跳或其他请求续期。
- 整个 RPC 再设有限总期限：15 分钟 + 按 256 KiB/秒计算的发送余量 + 安装等待，上限 24 小时；按协议大小硬界限进行安全算术。

大小余量是内部保守预算，不是带宽或吞吐承诺；超大文件在极慢链路上仍可能超过总上限。当前协议没有对端安装进度帧或断点续传，安装阶段按该独立期限等待。超时只让发送端报告快照失败、释放其槽和缓冲；一旦对端 Raft 已持久引用，其恢复仍由 Node 生命周期负责，不能因客户端断开撤销，也不能提前成功 ACK。

本地 mTLS 测试验证停止接收时超时失败、安装耗时超过传输闲置期限但在安装期限内可成功、永不确认时失败，以及取消后的资源回收；另测计时器续期竞态、旧回调不能误杀新期限、已过期阶段不能被晚到的 Send 复活、总上限和父 Context 取消。参考 Byfile 的 15 分钟安装经验，但不是其参数/协议的直接复制；真实慢 WAN 资格仍待验收。

### 剩余接线

1. 持久 artifact：已有自动节点登记，继续通用资源清单、Checkpoint/GC 引用保护和跨引擎容量预约；在保护完成前不回收旧镜像。
2. 状态机：Root 与 engine 已接通。后续可在同一持久恢复屏障下优化 SST 导入，但不要求不可逆数据库格式升级、不累积无界 Batch。
3. Raft/传输：发送端已有分阶段有限超时；接收端已有首帧、空闲、总期限及有界 Recv worker，详见[资源界限](../operations/resource-limits.md)。继续实现连接/全入口准入、安装进度/重复传输、协议协商与滚动升级边界，验证引用持久化/日志压缩/安装 ACK 的故障组合。
4. 资格：超过旧上限的真实多节点快照追赶，慢流/取消/坏文件/失盘/进程退出、重复导入、快照引用与资源保留一致性，以及真实目标介质的恢复和空间验收。

不通过增加用户配置或把 64 MiB 内存上限简单调大来完成这一目标。
