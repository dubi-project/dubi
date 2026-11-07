# 基础只读管理 API v1

租户读写管理接口另见 [租户管理 API](tenants.md)，实时趋势与检查记录见 [监控与诊断 API](monitoring.md)。本文列出的基础观察接口仍为只读。

状态：已实现。所有路径前缀为 `/api/v1/admin`，仅支持 GET。任意健康 HTTP Gateway 都可接收查询；目标由路径中的 NodeID/RegionID 决定。首次授权需要可确认的 Root 状态，不能在共识不可用时绕过授权读取缓存。

## 接口

| 路径 | data 类型 | 查询参数 |
| --- | --- | --- |
| `/cluster` | 集群概览，含区域摘要 | 无 |
| `/regions` | 区域列表 | status、limit、cursor |
| `/regions/{region_id}/nodes` | 区域内节点列表 | name、status、limit、cursor |
| `/nodes` | 节点列表 | region、name、status、limit、cursor |
| `/nodes/{node_id}` | 单节点 | 无 |
| `/volumes` | 集群 Volume 列表，附节点和区域归属 | region、name、node_id、status、limit、cursor |
| `/nodes/{node_id}/volumes` | Volume 列表 | limit、cursor |
| `/nodes/{node_id}/volumes/{volume_id}` | 单 Volume | 无 |
| `/maintenance` | 按节点组织的维护观察列表 | node_id、region、kind、limit、cursor |
| `/nodes/{node_id}/maintenance` | 指定节点的维护观察 | kind |
| `/gateway` | 当前接收请求的入口与证书状态 | 无 |

列表使用 `data.items` 和 `data.next_cursor`。limit 默认 50、最大 200；空 next_cursor 表示本页结束。游标绑定路径与筛选条件，可以调整页大小，不能跨接口或换筛选条件复用。资源默认按添加顺序倒序分页：节点使用注册提交顺序，区域使用首次形成的提交顺序，Volume 使用持久保存的添加时间；相同顺序以标识稳定排序。旧记录没有添加信息时排在有记录的数据之后，不猜测创建时间。升级后旧排序游标失效，刷新第一页即可。成员变更时重新从第一页读取，不承诺跨页快照。

name 是精确匹配。status 可为 healthy、degraded、unavailable、unknown。kind 可为 repair、rebalance、geo_sync、volume_migration。重复、空值、未知参数或非法游标返回 400。区域内节点列表与 `/nodes?region=...` 使用同一数据源与结构。

Volume 列表的 status 使用卷健康状态：healthy（正常）、missing（不可访问）、invalid（无效）、pending（待就绪）、removed（已移除）、unknown（未知）、error（异常）。未传 status 时返回全部状态。旧观察或无法识别的健康状态按 unknown 筛选，与 UI 状态标签一致；筛选在分页前执行，并与 region/node_id 取交集。没有观察的节点仍通过部分结果提示，不虚构 unknown Volume 行。

`/volumes` 的 name 匹配所属节点名称；每项包含 `node_id`、`node_name`、`region_id` 及原有 Volume 字段。按 Volume 添加时间倒序、`(node_id, volume_id)` 稳定排序并分页，页大小统计 Volume 数量。不同节点上的同名 Volume 独立展示。复用有界节点采集，聚合只保留当前游标之后最小的 limit+1 条记录；不要求浏览器逐节点拉取全量数据。无可用观察的节点不伪造 Volume 行，返回 `meta.complete=false` 和具体节点错误；可用旧观察标记 stale。空列表且 complete=false 不代表集群没有 Volume。跨页不承诺快照，节点恢复后刷新第一页可重新获取其 Volume。

## 响应与身份

所有成功管理响应使用 `data` 和 `meta`。`meta` 包含：

- `request_id`：服务端生成，与 `X-Request-ID` 响应头一致；错误响应也带该头和关联 ID。
- `generated_unix`：响应生成时刻，单位秒。
- `complete`：本次查询所需观察是否完整，不能代替健康状态。
- `errors`：有界缺失/失败原因；最多 64 条，`errors_truncated` 标明是否截断。

正式 Go 字段定义见 [api/v1/management.go](../../api/v1/management.go)。现有 `/gateway` 也已改用这一包装；登录、身份和 Config 业务响应保持原契约。

管理 Token 使用 `Authorization: Bearer ...`，Web UI 可使用既有管理员会话 Cookie。monitor 与 Tenant 身份不能访问管理接口，管理身份也不会因此获得 Tenant 内容读取权限。

| HTTP 状态 | 语义 |
| --- | --- |
| 200 | 查询成功，可以包含明确标记的部分观察 |
| 400 | 参数或游标错误 |
| 401 | 缺少有效凭据，或凭据已过期/撤销 |
| 403 | 身份有效但没有管理权限 |
| 404 | 已确认目标不在目录中，或接口不存在 |
| 405 | 方法不支持，Allow 为 GET |
| 503 | 无法确认授权/成员目录、入口繁忙，或目标详情没有可用观察 |

Root 重新选主时可能暂时返回 503。页面可以退避重试，不能将其解释为节点或区域被删除。

## 节点与区域

节点包含稳定 `node_id`、可为空且可修改的 `name`、`region_id`、`membership`、`status`、`reasons`、容量和维护摘要。首版 membership 为 `member`，表示当前成员目录中的节点，不代表数据已经补齐；此查询不混入历史已移除节点。

新生成的 ClusterID、NodeID、StoreID 和 VolumeID 使用类型前缀加 12 位小写十六进制随机数。已有 32 位身份保持原值；API 客户端应把 ID 当成完整的不透明字符串，不硬编码长度，不把页面上的缩略文本作为请求参数。Web UI 的复制按钮复制完整 ID。集群节点应先统一升级至支持短 ID 的版本，再添加新节点或新 Volume；旧版本不认识新身份格式。

区域来自已确认的当前成员目录。即使区域全部节点失联，区域和其节点数量仍保留；节点被管理员全部移除后，该目录中不再列出这个空区域。不会自动缩小多数派分母或执行成员变更。

每份观察有 `observed_unix`、`stale`、`complete`。节点失联时登记信息仍返回，健康变为 unavailable；未能在请求预算内取得观察时可以是 unknown。缺失观察的时间、容量、Volume 数量和维护信息为 null，不伪装成零。

容量是文件系统观察：`filesystem_total_bytes`、`filesystem_available_bytes`。节点内多个 Volume 位于同一文件系统时汇总去重；跨节点共享文件系统不保证去重，集群/区域返回 `capacity_scope=sum_of_observed_node_filesystems`。容量不是 DUBI 逻辑数据量或 Pack 实际占用。

节点的 `capacity_observed_volumes` 与 `volume_count` 表示容量观测覆盖情况；没有可读文件系统时容量为 null。集群/区域的 `capacity_observed_nodes` 与 `node_count` 标明节点覆盖范围。部分卷故障时只汇总可观测文件系统，并标记不完整。

节点 `writable_bytes` 是现有策略下单次放置可以参考的可写额度，来自最合适的健康卷；不是所有 Volume 可写空间之和，不向区域或集群简单累加，也不构成空间预留或写入保证。

## Volume 与维护观察

Volume 返回稳定 ID、实际目标节点路径、登记状态、健康、错误、写隔离、文件系统容量，以及相关迁移观察。不会用接收请求节点的本地路径代替目标节点路径。旧观察明确标记 stale；不能依靠不完整或过期观察确认一个 Volume 不存在。

维护信息来自已有 Runtime 工作，不建立新任务资源。四类观察：

| kind | 当前提供的信息 |
| --- | --- |
| repair | 实际扫描阶段（多阶段并存为 multiple）、已扫描记录数、未解决数量、是否需要完整扫描、最近进展与有界错误 |
| rebalance | 已持久化节点交接阶段 prepared/dropped/canceling；无活动交接为 no_active_handoff，观察到容量平衡工作正在执行为 checking |
| geo_sync | 待补齐的跨区域记录和元数据条目合计 |
| volume_migration | 是否运行、源/目标卷、已迁移记录数、错误 |

`no_active_handoff` 不证明集群已经均衡。没有可靠来源的 processed、total、pending、last_progress_unix 为 null；无来源时 `supported=false`、phase=unknown，不编造百分比、预计结束时间或累计吞吐。Repair 诊断存在截断时 complete=false；队列为空不证明所有数据已满足多数持有。

## 资源与验证边界

查询不触发修复、迁移或数据扫描，只读取已有状态。每入口最多 8 个并行节点探测、单次探测最多 3 秒、聚合探测最多 8 秒，仍受 Gateway 总请求时限限制。节点观察缓存 5 秒，同一节点并发刷新合并；刷新失败可返回最多 60 秒内的旧观察并标记 stale。缓存最多 256 个完成条目，保留数据估算预算 16 MiB。旧版本节点如果不支持新增观测字段，会明确返回缺失观察，不能伪造完整详情；待节点更新后自动重新采集。成员目录读取上限 4096 个节点，超限明确失败，不静默截断。

建议 UI 每 10 秒刷新，后台页面降低频率。暂无历史趋势数据库、CPU 曲线、维护完成时间预测或管理写操作。

VPS 验证包括：服务单元测试、四节点两逻辑区域的真实 Runtime 与区域失联检查、TT01 三个独立演示节点的 125 项 HTTPS 检查。演示 HTTPS 使用正常证书校验，包含全部查询路径、远端卷、分页、认证、会话与撤销。四节点测试中的 Region 为逻辑区域，不是跨洲网络测量；未升级九节点主测试集群，也未新增六小时长测。

凭证、仓库与备份的读写契约见 [访问与备份 API](access-backup.md)，继承本文件的认证、CSRF、响应结构与查询游标规则。
