# Tenant / Module 寻址迁移验证（2026-11-04）

## 范围

本次移除 Dataset 资源、目录和开发入口，落实服务端绑定的 Tenant/Module/Engine 寻址、自动初始 Range、范围内去重及新版持久格式。设计与接口见 [数据寻址与隔离](../architecture/data-addressing.md)。

首轮运行测试使用 TT01（209.200.246.159）与 TT02（209.200.246.165）的隔离目录、独立临时身份和端口。测试中的三/四节点为同一 VPS 上的独立 Runtime 与存储目录，通过真实 TCP/mTLS/Raft 通信，不是三个独立主机或 WAN 验收。这一轮隔离回归期间，原九节点服务没有部署新版或重启。后续经用户授权进行了九节点重新部署，见下节；没有修改 SSH 或 root 密码登录设置。

开发机仅执行静态检查、全仓构建、测试编译及 Linux 交叉编译；`go test -exec /usr/bin/true ./...` 只是编译检查，其输出不计作测试通过。远端有限 systemd 作业使用 GOMAXPROCS=2、GOMEMLIMIT=450MiB、MemoryMax=1200M、CPUQuota=100%，与正式服务分开；没有创建周期任务。

## 已验证

- 同 Tenant 不同 Module、同 Module 不同 Tenant 的同名 Key 隔离；同 Module 不同 Engine 的状态键隔离；重新取得相同绑定仍可访问原数据。
- 调用方相同 RequestID 在不同范围内不会冲突，同一范围重试保留内部 ID；绑定 Contract 对调用方返回原始 RequestID。
- 绑定与缺失读取不建 Range；首次写入自动准备；两个并发首次写入汇合到一个 Range；不存在的 Tenant 拒绝后，显式登记 Tenant 再写入可以成功。
- 原协调者只留下 Pending Range，非 Owner/Root learner 入口能经远端 Owner 续完初始化并读写；Owner/Root leader 故障后继续通过存活多数读取，失去多数时拒绝成功。
- 客户端不能通过旧 dataset 字段或伪造 Module/Engine 字段选择存储范围。
- 混合 Pack 中的多范围数据在重开、快照与恢复后保持归属；磁盘路径丢失、Store 故障、节点重启及补齐场景完成回归。
- 使用正式 backup worker 构建 Root/Engine/Pack/records-index 工件，连续两个备份点复用未变对象；恢复到全新 NodeID/StoreID 后核验各范围的 Range、权威引用和实际 Pack 内容。
- 旧无标记 Dataset Root 状态拒绝打开，旧数据没有被改名、补新版本标记或推测 Module 归属。

TT01 最终命令验收包含以下五个场景，每个重复两次，10 次执行均通过：

1. `TestEngineDataAnyNodeGateway`
2. `TestManagedRecordRoundTripAndRegionalReferenceFill`
3. `TestSingleRegionThreeNodeMajorityWriteRemainsLocalReady`
4. `TestRepositoryCLILifecycle`
5. `TestTenantTokenAdministrativeLifecycle`

组件回归覆盖 protocol、auth、router、rangebootstrap、rangeplacement、runtimecontract、rootstate、rootjoin、routedstate、enginestate、records、enginedata、recordwrite、recordrepair、node、raftrpc、recovery、repository，以及 Server。验收工具的 39 项 Python 测试通过。TT02 最终 Server 回归为 81 项顶层测试通过、0 失败，2 项跳过：`TestRestoreClusterRealRuntimeAcceptance`（未提供外部多节点备份导出）和 `TestAutomaticVolumeBalanceOnIndependentFilesystems`（未提供两块独立测试文件系统）。新增加的 `TestModuleIsolationThroughClusterBackupRestore` 已实际运行并通过；它不替代被跳过的九节点外部恢复验收。recovery 包的外部 Repository 验收也因没有输入而跳过。

## 失败记录与结论边界

早期轮次发现测试夹具硬编码旧 Root 版本、旧目录路径、旧 Engine/索引快照版本，以及 RecordHoldingService 方法数量断言落后于已有批量接口。修正后对应包通过。早期 `TestManagedRecordRoundTripAndRegionalReferenceFill` 曾在磁盘故障和重启后的一次写入超时（204.70 秒整场景）；专项复跑和最终两次重复均通过。没有足够证据把该超时归因到某个已修复缺陷，原始失败日志保留，后续长测继续关注。

此次完成的是源码与开发格式迁移，**没有转换原九节点集群的数据**。旧 Dataset 缺少可靠 Module 映射，旧数据/旧备份不能直接交给新版打开，未提供原地升级、滚动混跑或旧数据转换工具。旧版六小时长测成绩不沿用为新版资格。

自动初始 Range 不等于通用 Range 拆分/合并已经实现；公开 Tenant 业务 API 和各业务 Engine/Module 仍按各自任务交付。此次也不是新版本的九主机三区域长测或生产资格结论。

## 证据

- 基准提交：`94c8842b1b54521d90a234efb143481e57689f3e`，本次改动尚未提交。
- TT01：`/var/tmp/dubi-module-addressing-20261104/`，初轮及 `round2`、`round3`、`round4` 分开保存，失败记录未覆盖。
- TT01 最终结果：`round4/results/summary.tsv`、`command-final.tsv`、`command-final.log`、`binaries.sha256`。
- 隔离回归完成时，TT01/TT02 正式服务 PID 与启动时间均未改变，二进制仍为 `91cd77e9ab8751e6cb3a6c593e23b340be40642a968e61a95db68bccdfd7ebb8`。
- TT02 最终 Server/绑定入口回归：`/var/tmp/dubi-module-addressing-20261104/final/results/`。
- 本地证据副本：`/tmp/dubi-module-migration-evidence/`。其中 `source.sha256` 记录 630 个源码/协议/脚本/Go 依赖文件摘要，清单 SHA256 为 `c10c27a06fa356e3bdd7f21d6937096115a30119de78eb0b14397080d3e1c97c`。各轮测试二进制另有独立摘要；不要把早期轮次当作同一构建。


## 九节点重新部署与补充验收

用户随后明确授权：若持久格式改动较大，可清空九节点并重新部署。本轮停止九台旧服务，隔离原活动 Store，以 SG01 `init` 建立全新 Cluster，其余八台通过普通 Join 加入。全部使用 `/var/lib/dubi` 和默认 `/var/lib/dubi/volume`，SG/FF/TT 各三台，RPC 仍为 12443。没有导入旧 Dataset 数据，也没有改动系统 SSH 配置。

重新部署期间发现两个问题，原始失败记录均保留：

- TT02 首次 Join 的凭证指向 SG01，但第三个 Region 加入后 Root 自动调整投票集合，SG01 随后失去签发权限。旧入口拒绝继续签发。撤销未消费凭证、待成员调整稳定后重新签发，TT02 即可沿本次新建身份完成 Join。**此处是操作恢复，不是已修复凭证自动切换**；拓扑切换期间单入口凭证失效的边界仍需单独处理。
- 三区域首次写入测试中，TT 的 Range 已 Active，但后台尚未发布 Region placement，首笔写入返回 `Range has no Region placement`。修正 `enginedata.ensureRange`：新建/Pending 路径及并发看到 Active 的路径均须等待 placement；已准备好的路径只读本地目录。等待可取消且最长 30 秒，不自行修改分布或减少多数派要求。

新增 `TestActiveInitialRangeWaitsForPlacementAndHonorsCancellation` 使用实际 Root 状态构造 Active/无 placement 的间隙，验证取消、发布前不放行、发布后继续及就绪路径不触发共识。该回归及整个 enginedata 包在 TT01 通过。部署前的 WAN 首次写入失败保留于 `/tmp/dubi-fresh-20261104/pre-fix/`。

修复后在 TT02 重复执行 `TestEngineDataAnyNodeGateway`（50.16 / 45.13 秒）及 `TestSingleRegionThreeNodeMajorityWriteRemainsLocalReady`（40.57 / 35.46 秒），4 次均通过。九节点逐台更新到修正版，更新后全局健康检查通过。

- 重建初版二进制：`c9f63dc81b7db803922d0bec0e72a5854f413cb310e9e9d522745381abc8170a`。
- 修正版二进制：`65942584106c6b78693f643e7047a22a823990744adc040324cf79ca7e119f7f`。
- 修正版 630 文件源码清单：`ad0f47659535a51a3d39d419e646f0d9ac5fd753bec2551e308505d098652a52`。
- 新集群：`cluster_440c078307dea10c995f2369afa57874`。
- 本轮证据：本地 `/tmp/dubi-fresh-20261104/`；各 VPS `/var/tmp/dubi-fresh-20261104/`。部署计划、逐台更新记录、失败重试、测量结果分开保存。

验收工具原先将物理持有门槛误设为三个 Region 均有多数数据，首个观察实际为 SG 2、FF 2、TT 0。按规格和已有 `vps_foundation_faults.physical_coverage` 校正为“多数 Region × 各 Region 多数 Node”（本拓扑为至少 2 个 Region、各至少 2 个 Node）；没有改变服务端复制策略。读取批次另遇一次 SG01 SSH 握手阶段的连接重置，服务检查正常；保留原日志后，改为单主机串行、跨主机并行发起检查，完整 36 项读取复查通过。

### 新集群最终结果

- 九台 NodeID、RaftID、StoreID、VolumeID 均与原集群不同，新的 ClusterID 一致；三个 Region 各三台，Root 为每区一名 voter。
- 默认 CLI 上下文可用，数据命令无需额外指定 `--store` 或 `--server`；服务统一使用原 systemd unit 与更新后的本机配置，临时 Join override 和 SG03 旧验收 override 已撤下保存。
- 三个全新 Tenant 使用相同 Key、相同调用方 RequestID 写入不同内容，各自自动获得不同 Range 和区域 Owner；三个跨节点幂等重试返回原 applied index。
- 九个节点分别作为写入口均成功，包含 Root learner 和非 Owner 区域；完整跨节点读取复查 36 项通过。
- 12 条记录逐节点核验 RecordID、实际持有、StoreID、VolumeID，均达到多数 Region 内的节点多数；不要求所有九台持有相同记录。
- FF01、SG02、TT01 三名 Root voter 逐一正常重启，保留本次新建身份；重启节点上的 9 项读取及随后全节点 36 项读取均通过。合计这三个完整批次为 81 项读取校验。
- 最终九节点健康检查均为 `available`、Root quorum 已确认，无待处理 Repair/Geo debt。随后仅删除隔离保存的旧活动 Store 与原默认 Store；九台清理均完成，旧日志与配置证据保留。九台有效 SSH 设置仍为 `permitrootlogin yes`、`passwordauthentication yes`。

本轮小载荷功能样本：普通九入口写入 0.589–1.360 秒，中位 0.871 秒；包含首次 Range 初始化的三笔写入为 2.709 / 4.275 / 7.782 秒。完整首轮读取 0.693–1.696 秒，重启后完整读取 0.604–2.900 秒。计时在 VPS 上包围 CLI 执行，不含开发机到 VPS 的 SSH 握手；这些只是 12 条小记录的功能样本，不能作为吞吐、尾延迟或六小时长测成绩。

最终汇总位于 `report.json`，完整通过阶段见 `acceptance-finish.log`，早期 SSH 中断及脚本断言记录分别保留在 `acceptance.log`、`acceptance-resume.log`。新版基础功能验收完成；旧凭证在签发权限切换后需要重签的边界仍然存在，不能把本轮结论扩展成无人工介入的任意拓扑切换或生产可用资格。
