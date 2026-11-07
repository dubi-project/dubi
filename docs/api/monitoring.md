# 监控与诊断 API

监控由 Runtime 提供真实节点状态，HTTP Gateway 负责身份校验和查询聚合。页面不执行成员变更、自动修复或数据迁移。业务数据仍由 Tenant 凭证访问，管理员不会获得业务内容读取权限。

## 接口

所有路径前缀为 `/api/v1/admin`，使用管理员 Bearer Token 或管理员会话。Cookie 发起 POST 时必须带原有 CSRF 头；monitor 和 Tenant 凭证不能调用这些管理接口。响应沿用 `data/meta` 包装及关联请求 ID。

| 方法、路径 | 参数与结果 |
| --- | --- |
| GET `/monitoring` | 可选 `region`、`node_id`、`window`；返回节点、趋势点、健康摘要及采样信息 |
| GET `/diagnostics` | 可选 `region`、`node_id`、`q`、`status`、`limit`、`cursor`；返回诊断记录摘要列表 |
| GET `/diagnostics/{id}` | 返回检查时各节点的四项检查结果 |
| POST `/diagnostics` | JSON：`request_id`、可选 `region_id`、`node_id`；检查并保存结果 |

`window` 仅支持 900、3600、21600 秒，API 默认 3600，页面默认 900。筛选参数取交集；不存在的节点或区域返回 404。未知、重复、空查询参数返回 400。监控一次最多 256 个节点，超过时需缩小范围；前端对返回范围做节点、维护状态筛选和分页。

诊断列表按不可变 `created_index` 倒序，最新检查在前。`status` 为 passed、warning、unknown；warning 优先于 unknown。`q` 匹配检查标识或显式范围标识。列表筛选匹配检查所包含的节点，结果状态仍代表整次检查。limit 默认 50、最大 200，游标绑定筛选条件。检查记录在 Root 中保存最近 50 次，每次最多 64 个节点、64 KiB；超限检查返回 400。已淘汰记录返回 404。

POST 的 `request_id` 必须在重试时保持不变。已提交的同范围请求返回原记录，不重新检查；同 ID 改变范围返回 409。ID 已用于其他操作或对应记录已被淘汰时返回 409。503 表示结果可能尚未确认，应查询记录或重试同一 ID。手动检查保存记录需要 Root 可用，不绕过现有授权和共识。

## 数据与采样口径

每个节点每 10 秒采样，在进程内保留最近 6 小时，固定容量 2162 条。节点重启后趋势重新积累，诊断记录仍保留。查询按时间窗口压缩至不超过 121 个趋势点；缺失区间不补零，不插值。每点含 `observed_nodes`，只聚合取得的样本；`meta.complete=false` 和 `meta.errors` 提示缺失观察。无法取得的 CPU、内存、吞吐、延迟或就绪状态为 null。

- `cpu_percent`：DUBI 进程 CPU，占用一核为 100%，多核可超过 100%。首个样本没有 CPU 差值时为 null。
- `cpu_cores`：进程可用的逻辑 CPU 核数（Go Runtime 观测）。页面 CPU 占比为 `cpu_percent / cpu_cores`，保留原始单核口径字段供兼容；不代表容器 CPU 配额使用率。
- `memory_total_bytes`：Linux 节点/虚拟机总物理内存，不是 Go 堆上限或容器限额；页面内存占比为 `rss_bytes / memory_total_bytes * 100`。分母缺失时不显示进度条，仍保留可用的 RSS 数值。
- `rss_bytes`：当前进程 RSS，Linux 从进程统计读取，不使用历史峰值替代；不支持的平台为 null。
- `read_rate/write_rate`：业务 HTTP 入口完成请求数除以实际采样时间，包括错误请求。当前覆盖 Config 的 GET、PUT、DELETE。管理查询、静态资源、健康检查、内部复制和 RPC 业务调用不计入；后续模块必须显式接入业务统计，不能默认声称覆盖全部流量。
- `read_p95_ms/write_p95_ms`：固定延迟直方图的 P95 桶上界估计。集群曲线先合并桶计数再计算 P95，不平均各节点 P95；无请求时为 null。页面摘要显示各节点 P95 的最大值，并明确标注口径。
- `ready`：本地 Runtime 启动恢复完成状态，不代表跨区域持久性、全部数据已修复，或客户端网络可以访问 HTTP 端口。

监控查询最多 8 路节点采集并发、每节点 3 秒、整次采集预算 9 秒；每入口最多同时执行 2 次采集，繁忙返回 429。节点间 RPC 使用现有 mTLS，要求已验证证书身份和当前成员身份一致。返回节点身份、样本数量、时间和数值均有校验。内存采样不写 Root；仅手动诊断记录进入 Root。

## 诊断和后台任务

每个节点检查四项：内部管理通道可达性、Runtime 就绪、Volume 健康与写入隔离、后台维护待处理和错误。未知状态不能判定通过。诊断保存检查当时的摘要，不保存用户数据、原始日志、密钥或堆栈。

后台任务页复用 Repair、Geo Sync、Rebalance、Volume Migration 的实际维护观察。尚未提供的总量和进度为未知，不伪造百分比；空闲不是一条已完成任务。该页不提供持久任务历史。

## 外部监控接入

`/healthz` 和 `/readyz` 保持原有语义。`/metrics` 需要 monitor 或 admin 凭证，通过 Authorization 头传入，不能放进 URL。每个节点分别采集，任意一个节点的 `/metrics` 不代表整个集群。

新增指标：

- `dubi_http_business_requests_total{operation="read|write"}`
- `dubi_http_business_duration_milliseconds`：带 operation 标签的延迟直方图
- `dubi_http_business_errors_total`：HTTP 状态码 >= 400 的业务请求
- `dubi_process_resident_memory_bytes`
- `dubi_process_cpu_percent`

计数器在进程重启后归零。指标标签不含 Tenant、业务 Key、Token 或请求 ID。指标导出先复制内存统计，再写响应，慢采集端不会持有业务统计锁。长期保存和告警使用现有 Prometheus 接入；内置 6 小时趋势不是持久时序存储。

## 系统消息

系统消息由 Runtime 后台检测生成，不依赖打开 Web UI。只有当前 Root Leader 执行检测：启动就绪后等待 15 秒开始，之后每轮完成后等待 30 秒；每轮预算 20 秒，最多观测 256 个节点。稳定状态不写入 Root，仅资源状态变化更新持久检查点并生成消息。失去 Leader 身份时停止提交检测结果。消息保存最近 500 条，每类资源检查点最多 4096 个，超出范围报告后台错误，不改变集群运行状态。消息及去重检查点参与 Raft Snapshot 和底座备份；新命令需要所有节点使用支持系统消息的版本。

第一版事件：

- 新节点加入并取得响应、节点连续两轮不可达，以及重新取得节点响应。无法完成的观测不当作恢复；恢复连接消息不宣称数据修复已完成。
- Volume 不可用、写入隔离，以及文件系统容量达到 85%；恢复可写且用量低于 80% 后生成恢复消息。不同 Volume 共用文件系统时，容量口径仍是所在文件系统。
- 租户逻辑存储用量达到配额 80%，低于 75% 后恢复。使用已有异步用量，不影响业务写入路径。
- 新访问凭证创建；未撤销的管理员、监控和租户凭证将在 7 天内到期，或已经到期。永不过期凭证（`expires_unix=0`）及浏览器会话不生成到期提醒，消息不含 Token、密钥或原始错误日志。
- 集群备份完成或失败。首次启用时已有的历史备份只建立检查点，不重发历史任务消息；之后新增或变化的终态任务生成消息。

消息不是外部告警替代品；Root 无法提交时不能生成新的持久消息。上层模块将来的事件需显式接入，当前不自动声称覆盖全部模块。

管理 API（现有管理员认证、CSRF 和 Root 提交规则）：

- `GET /api/v1/admin/notifications`：时间倒序列表，支持 `limit`（1–200）、`cursor`、`q`、`level`（warning/error/success/info）、`read`（true/false）。返回匹配总数 `total`、全部保留数 `retained`、集群未读数 `unread`、阅读边界 `through_index`、保留上限 `retention`。
- `GET /api/v1/admin/notifications/{id}`：单条消息；超出保留数量返回 404。
- `POST /api/v1/admin/notifications/read`：单条传 `request_id`、`id`、`read`；全部已读传 `request_id`、`all=true`、`read=true`、列表返回的 `through_index`。边界之后的新消息保持未读。重试沿用相同请求 ID 与参数。

已读状态以集群为单位保存在 Root，所有管理账号、Token 会话和节点共享同一份未读数量与阅读记录。任意管理员标记已读后，其他入口也会读到该状态（Web UI 最多等待下一次 15 秒刷新）。此前的凭证级记录在首次共享写入时合并已读集合并清理，不再维护账号或凭证维度。打开消息详情会申请标记已读；标记已读不会处理异常或触发维护操作。顶栏每 15 秒读取最近 5 条消息和未读数量，完整列表仅在打开消息页后读取，不在正常刷新时插入提示条。
