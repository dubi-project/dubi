# 启动 DUBI 节点

当前入口开放 Node 认证的内部集群服务和开发验证用 Record RPC；可选的独立 Tenant HTTPS 端口目前只有身份探针，仍没有面向租户的公开数据 API。正式进程注册了最小 `record` Engine，可在三节点同区域确认数据 Group 后，经 CLI 完成 Dataset 创建、Record 写读；其他 Engine 未注册。Node 证书持有者相当于集群内部操作者，不构成租户身份和权限隔离。

## 1. 初始化首节点

```sh
dubi init
```

命令输出持久 `ClusterID`、`NodeID`、`RaftID` 和 PKI 文件路径，并在 `<store>/dubi-node-pki` 原子创建 Node CA 与当前节点证书。内置 PKI 自动使用集群虚拟 TLS 名称 `dubi.internal`；它不需要 DNS 解析，也不与节点的 IP、主机名或 advertise 地址绑定。同时自动持久化首节点的 Root 成员信息，不需要填写 RaftID 或 peer 列表。对同一目录重复执行会验证并返回原身份及证书，保留已有 Root 成员与日志，不会生成第二套。

`init` 只用于创建新集群和首节点。不能在其他节点执行 `init` 后把它冒充为同一集群成员。

Group/日志现在有独立持久登记，已登记目录或日志丢失时拒绝重建。旧开发格式没有自动迁移通道，不要直接覆盖重要 Store 升级，也不要手工删除登记让初始化通过；详见 [Group 存储边界](../architecture/group-storage.md)。

当前 Store/Volume 元数据为 v2，首次 init 自动在既有 Store 清单中登记并建立不可变记录索引；启动和加入使用同一持久意图恢复，不增加参数。v1 开发目录明确拒绝打开，未提供自动迁移。已 active 的记录登记/索引丢失，重复 init 也不能重新创建空资源；应保留故障现场，不要删改身份文件来绕过检查。Pack 容器仍为 `DUBIPK02`，Engine 状态当前为 v7；见[不可变记录与资源生命周期](../architecture/immutable-records.md)。

## 2. 准备加入已有集群

一次性凭证、Root 授权记录、受限 mTLS 签发、持久回执、网络 Root learner 接纳、本地阶段/地址记录及实际快照追赶/重启已有实现与集成测试，详见[节点加入实施记录](../architecture/node-enrollment.md)。CLI 已接通创建/撤销凭证和一步加入运行；目前加入的是 Root learner，尚不代表自动晋升、跨区域持久性策略或生产资格完成。

在运行首节点的主机上创建一次性凭证：

```sh
umask 077
dubi join token > join-ticket.txt
```

使用该主机当前 Store 的 Node mTLS 身份访问运行中的服务；`init` 选择 Store，服务 Ready 后自动记住本机实际连接地址。正常运行 `dubi join token` 无需参数；`--store`、`--server` 留给同机多节点或远程排障。凭证默认有效 15 分钟，`--ttl` 最多 24 小时。stdout **只输出 bearer secret**；stderr 提示到期时间和公有 GrantID，不输出 secret/私钥。不要把输出写到日志、仓库或工单，私密文件须安全传递。发送给签发器的只有公钥与 secret 摘要；创建失响应会在同一调用内有限重试同一 draft，不重新生成凭证身份。进程退出前仍未取回凭证时，残留的未使用 grant 等待过期或撤销，不会恢复其 secret。

撤销尚未消费的凭证（GrantID 来自创建提示）：

```sh
dubi join revoke --grant <GrantID>
```

撤销不会移除已加入 Node，也不撤销其证书。已消费/已撤销/冲突的 grant 作为明确终态拒绝，不能伪装成网络错误自动重试。

在新节点直接运行（文件只允许当前用户访问，`0600` 或更严格）：

```sh
dubi join --ticket-file join-ticket.txt
```

自动生成身份与 CSR，按到签发节点的路由选择联络地址，登记卷，完成 mTLS 签发、持久回执、Root learner 接纳及本地状态记录，再运行服务。无需填写 NodeID、RaftID、证书名称或 peer 列表。默认仍为 `/var/lib/dubi` 和端口 `7443`；可用 `--store`、`--listen`、`--region`、`--advertise` 表达真实部署条件。可选 `--name sh-01` 设置管理员显示的节点名称；不传则保持已有名称或为空。`--volume` 可重复指定初始卷；未指定则使用 `<store>/volume`。凭证也可通过 `--ticket-file -` 从标准输入读取，不支持把 bearer 值直接放进命令行参数。

中断后重复原命令会复用原 CSR；回执已持久化后，可不再提供凭证：

```sh
dubi join
```

已保存 Root 接纳记录的重启不会再调用签发器或 Join RPC；既可继续使用 `join`，也可用 `server`。第一次 Ready 必须经过本地追赶、Root ReadIndex 确认和持久激活；收到叶证书不等于完成加入。首次使用非默认 Store/端口时重试保持原设置；记录损坏、缺失身份/回执，或已有 Raft 资源却缺失加入记录时拒绝重新初始化。加入流程互斥，监听端口从入网前一直保留到服务接管。

以下 `prepare/install` 是内部流程的分步诊断入口，不是正常部署所需操作，也不单独完成 Root 接纳。在新节点生成稳定但尚未绑定 Cluster 的 NodeID/RaftID、Ed25519 私钥和 CSR：

```sh
dubi join prepare \
  --store /var/lib/dubi \
  --region region-b \
  --advertise node-b.example.internal:7443 \
  > join-request.json
```

重复执行相同参数返回相同 CSR；改变 Region 或 advertise 地址会拒绝。CSR 自动使用 `dubi.internal`，身份 URI 为 `spiffe://dubi/join/node/<NodeID>/<RaftID>`；它只证明请求方持有该未绑定 Node 身份的私钥，并不表示它已经属于任何 Cluster。

集群侧的 enrollment 核心会先校验外部 admission proof，再通过 Root ReadIndex 和 Raft 命令持久登记 Node，确认登记成功后才允许 Node CA 签发叶证书。签发 bundle 只包含 ClusterID、Node CA 和节点叶证书，不包含 CA 签发器。取得 bundle 后在新节点安装：

```sh
dubi join install --store /var/lib/dubi --bundle join-bundle.json
```

安装端会重新验证 CA、自签名、Cluster 级 CA URI、叶证书链、NodeID/RaftID、统一 TLS 名称、CSR 私钥匹配关系和有效期，然后一次性绑定 ClusterID 并原子发布 leaf-only PKI。

不要用文件复制或离线签发绕过 Root 登记。分步 `prepare/install` 只负责产生请求和安装受控签发的 bundle，不提供完整入网恢复链。正常部署使用上述 `join --ticket-file`。Region 默认 `default`；只有叶证书而没有 Root 状态或合法接纳记录的节点仍不能启动，也不能使用 `init` 绕过这个边界。

## 3. 节点证书与本地 CA

节点证书必须：

- 由集群托管 Node CA 签发，同时允许 ServerAuth 和 ClientAuth；
- 内置 PKI 覆盖统一虚拟名称 `dubi.internal`，不绑定 `advertise`；
- Subject Common Name 等于持久 `NodeID`；
- 只含一个 DUBI 身份 URI：`spiffe://dubi/<ClusterID>/node/<NodeID>/<RaftID>`；
- 私钥文件不能被 group 或 other 读取。

`dubi init` 已签发满足这些条件的首节点证书。进程固定读取 `<store>/dubi-node-pki/node.crt`、`node.key` 和 `node-ca.crt`，校验身份绑定、CA 指纹和 `dubi.internal`。证书名、证书路径、NodeID、RaftID 均由程序管理，不提供自定义或专家覆盖入口。

CA 私钥不会以裸 `node-ca.key` 落盘，而是保存在绑定 Cluster/Node/Raft 身份的认证加密信封中；解封密钥和信封都仍位于同一受保护目录，所以它们不能抵御已经取得整个 Node Store 读取权的攻击者。`0700` 目录和 `0600` 私密文件权限才是当前静态保护边界。加入协议完成前，不要复制签发器文件到其他节点；Node CA 也必须另行做受控安全备份，否则首节点存储完全丢失后无法继续签发节点证书。

## 4. 直接运行

初始化后的首节点直接启动，不需要配置文件：

```sh
dubi server
```

如需验证独立的应用认证边界，可显式启用身份探针：

```sh
dubi server --api-listen 127.0.0.1:7444
```

该端口使用集群 CA 签发、由节点自动续期的 TLS 服务端证书；客户端须信任该 CA，并以 `dubi.internal` 为 TLS 验证名称。请求 `GET /v1/me` 时使用 `Authorization: Bearer <Token>`，无需 Node 客户端证书。Token 由 `dubi tenant token create <tenant>` 创建，撤销使用 `dubi tenant token revoke <credential-id>`。`dubi join` 也接受同一 `--api-listen` 参数，已加入节点可直接使用 `dubi server --api-listen ...`。该端口不注册任何内部 RPC 或数据操作，当前默认不开放；不要把身份探针当作可用的应用读写 API。若要从其他机器访问，需要明确指定可达的 `--api-listen` 地址并安全分发 CA；不提供 `InsecureSkipVerify` 之类的绕过选项。

最常见的完整流程就是：

```sh
dubi init
dubi server
dubi status
dubi volume list
```

另开一个终端查询实时 Root 目录：

```sh
dubi status
```

`status` 从 `~/.dubi/current.json` 找当前 Store 与本机服务端口，使用 Store 中现有 Node 证书调用受保护内部 RPC。输出包含已确认的 Root 应用位置、投票成员、learner、节点登记和 leader 提示；`cluster` 额外给出这次请求确认的 Root 投票数、理论多数门槛和按 Region 汇总的节点/投票分布。`cluster.root_quorum_confirmed=true` 代表本次 Root 读确认成功，不能据此推断每个远端节点在线。`local_storage` 单独报告**本机**已登记卷的健康与容量，失卷显示 `missing`，错误挂载或标记错误显示 `invalid`。若本地卷登记本身无法读取，仍输出已确认 Root 视图，并在 `local_storage_error` 说明问题。连接的是本机节点时，`local_repair` 另显示后台修复最近检查时间、是否启用及尚未解除的错误；`repair_debt` 与 `total_issues` 给出未解除错误数量，最多列出 64 个 Group 错误。`volume_migration` 显示健康源卷迁移的持久游标、已迁记录和最近错误；`volume_write_errors` 显示本机 Pack 写入被隔离的卷；`geo_metadata_debt`/`geo_record_debt` 显示尚未完成的跨区域后台补齐，`maintenance_budget` 显示维护流控占用和拒绝次数。`last_check_unix=0` 表示尚未运行过。修复错误要到该 Group **完整一轮无欠账**才清除；修复游标、当前 Region generation、Group epoch、Fence index、节点集合和欠账保存在节点本地状态，重启后只有在当前 Root 集合完全匹配时才续扫，否则从新集合重新开始。空错误列表不证明所有数据已经扫描或达到物理多数。修复状态查询失败时 Root 视图仍可见，并在 `local_repair_error` 说明。服务停止、Root 失去读多数或证书不匹配时命令失败。目录中的节点即使当前离线也仍列出；这里不报告逐节点在线状态、全群数据持有多数或业务读写就绪。可复制的本地三节点操作见[本地运行指南](local-cluster.md)。

需要快速判断是否应该介入时运行：

```sh
dubi doctor
```

`doctor` 使用与 `status` 完全相同的认证 Root/Node 探测，但默认输出简短结论：`healthy` 表示当前观察到 Root 多数、本机卷和修复状态均正常；`degraded` 表示存在失联节点、失卷、写隔离或 Repair/Geo 欠账；`unknown` 表示尚未取得完整观察。发现 `degraded` 或 `unknown` 时命令以非零状态退出，并逐项列出原因，方便值班脚本和人工处理。需要完整机器输出时使用 `dubi doctor --json`；它只读，不会自动移除节点、删除 Volume 或触发恢复。

数据目录优先级为 `--store`、`DUBI_STORE`、当前用户 `~/.dubi/current.json`、`/var/lib/dubi`。首次监听默认 `0.0.0.0:7443`；服务 Ready 后记住实际绑定地址、端口与已登记的 advertise 地址，后续 `dubi server` 重启自动复用。本地上下文只存 Store 绝对路径、节点身份校验信息和网络地址，不存证书或密钥；真实身份、Volume 和集群目录仍在 Store。换 Unix 用户时用户目录不同，需先用 `--store` 明确选择。上下文损坏或身份不匹配会报错，不回退到空目录重新初始化。相对 Store 路径以初始化时的工作目录解析成绝对路径保存；默认目录须由运行 DUBI 的账号持有写权限。

首次启动自动登记本节点的 Region 和集群联络地址，经过 Root 确认后才 Ready。具体监听地址直接使用实际绑定地址/端口；wildcard 监听通过系统默认路由推导本机 IP，UDP connect 只查询路由、不发送探测包。默认 Region 为 `default`，可用 `--region` 表达实际故障域；不同物理区域应如实区分。可选 `--name` 只设置节点显示名称，名称可以为空或之后修改，不改变 NodeID、RaftID、地址或 Region。推导不证明外部可达性，NAT、隧道、多网卡或特殊路由可用 `--advertise host:port` 指定发布地址。这里是网络/部署参数，不是内部身份配置。

重启默认复用已登记地址和 Region，不重新猜测，也不允许用旗标静默覆盖。首次 `--listen ...:0` 分配的实际端口会记入本地上下文，后续无参数重启可复用；监听端口与发布端口不同时，需要用相同 `--advertise` 明确确认有意的端口映射。受控地址/Region 变更协议尚未完成，不能直接改 Root 或本地记录。

加入节点追赶后，正式进程自动调整 Root 投票拓扑，无需手动晋升或配置票数。单区域至少三节点时形成奇数投票集合；至少三个区域时每区一个 Root voter，其余为 learner，仍是正常集群节点。两 voter 必须同时在线，不是单节点容错配置。角色变更先检查追赶/活跃并采用联合共识，不能因节点失联就降低多数门槛；这不代表业务数据已完成跨区域复制。布局、边界和验收见[自动 Root 拓扑](../architecture/root-topology.md)。

首节点的正式 `server` 入口会自动加载 init 已有的托管签发器并注册受限 enrollment/Node 续期 RPC，不生成另一套 CA，也没有证书路径或 server-name 参数。加入节点不加载或接收签发器。此接线已通过 CLI 启动、创建/撤销凭证、自动加入及回执恢复的真实 mTLS 测试；签发器高可用和 CA 轮换仍未完成。

运行进程已支持同一 CA/节点公钥下的叶证书自动续期和热加载。Ready 后检查一次，以后约每 6 小时检查，距到期 30 天申请；失败按 1 分钟至 1 小时退避重试并报告。普通节点使用原加入回执找到签发节点，不需要新 ticket。服务端和出站客户端的新握手按内部 30 秒检查周期获取新叶。错误候选不会覆盖仍有效的缓存，但旧证书到期必须拒绝；不会自动接受换 CA、换私钥或替换 Store/PKI 目录。不要以手工改 PKI 文件作为日常运维方案。连接约每 6 小时平滑更新，新请求重新握手；既有流最多保留 25 小时，但会话证书/CA 到期会提前关闭底层连接，不能借宽限期延长授权。全部采用内部默认策略，无需新增参数。原签发器长期不可用和过期后恢复仍待完善。详情及测试边界见[节点加入实施记录](../architecture/node-enrollment.md)。

重启仍执行同一条 server 命令。身份、证书、Root 成员与日志均从 Store 恢复，其他成员地址来自持久 Root 目录。`server` 不执行首次引导；缺失 Root 或未完成加入时拒绝启动。全新目录先运行 `init`；旧版已生成 bootstrap PKI 但没有 Root 状态的首节点也可运行一次 `init` 完成自动引导。不要在加入节点上运行 `init`。

旧的 `--config` / YAML 启动入口已移除，不再维护 `root_bootstrap`、`raft_id`、`tls.server_name` 等配置。内部共识默认值由程序管理。

Volume 采用以下规则，节点身份、证书和共识状态始终保留在 Store 根目录下：

- 首次未指定卷：只创建并登记 `<store>/volume`，默认即 `/var/lib/dubi/volume`。
- 首次指定一个或多个外部卷：仅使用指定卷，不创建 `<store>/volume`。
- 后续加卷：追加到已登记的卷集合；已有默认卷保持 Active。内部 Pack Pool 会在后续写入时重新读取卷集合并选择可写卷；已 Drain 的健康 Volume 会由持久迁移任务自动把记录搬到其它健康 Volume，重启后继续。
- 后续安全移除任意卷（包括默认卷）：停止向其分配新写入，健康介质上的活数据自动迁到当前节点其他卷并校验、更新本地索引，确认源卷无剩余 Pack 后才完成注销；没有目标卷或容量不足时保持降级并留下迁移欠账。旧介质明确丢失时，管理员仍可显式移除其注册元数据，系统不会把这当作数据已恢复。
- 重启恢复持久卷登记；已移除的默认卷不会因未传卷路径而被重新创建，掉盘也不能触发空卷创建或默认卷回退。

首次使用外部卷时，先准备已挂载磁盘上的空目录，再把路径交给 DUBI；不需要填写 StoreID 或 VolumeID：

```sh
dubi init --volume /mnt/disk1/dubi --volume /mnt/disk2/dubi
dubi server
```

`join` 和 `join prepare` 同样接受可重复的 `--volume`。卷路径持久保存，重启不需要重复传入。初始化已有节点时，不传卷路径会保留原有卷集合；传入不同集合会拒绝，后续扩容使用 add。

```sh
dubi volume list
dubi volume add /mnt/disk3/dubi
```

Volume 维护也可以显式触发 Pack 回收。普通模式只回收完全无引用的 sealed Pack；磁盘长期积累大量混合存活 Pack 时，再使用重写模式：

```sh
dubi volume compact
dubi volume compact --rewrite
```

该操作不需要手工指定 Store 或节点地址；运行中的节点通过本机认证 RPC 执行，停止的节点直接操作本地 Store。它受维护并发和字节预算限制，不会把同一 Node 的多个 Volume 当成多个副本。

这些命令也支持 `--store` / `DUBI_STORE`。`volume add` 在节点运行或正常停止时都是同一条命令：运行中通过已记住的本机地址向 Store 所属节点提交，停机时直接登记；不需要为扩容重启节点。只有匹配本机 Store 身份的 Node mTLS 证书才能发起在线登记，客户端也在发送请求前核对服务端是这台节点；地址不通或指向别的节点不会改动卷登记。在线加卷不依赖 Root 多数，但仍要求该节点进程、Store 和证书可用。排障时可显式给 `volume add` 指定 `--server`。list 可在节点运行期间使用，显示卷 ID、路径、生命周期、健康及容量；同一文件系统的多个目录不会重复计入总容量。缺失或损坏的卷保留原登记并报告故障，不自动创建空卷。重复添加相同路径会保留原 ID；登记中断或响应丢失后可重试，持久化结果不确定时需要重启节点恢复登记后再重试。

如果原卷丢失而节点身份和记录索引仍完整，准备新盘上的空目录并执行 `dubi volume add <新目录>`；节点还在运行时不必先停止。当前 cohort 的其他节点仍有可验证的记录时，进程内后台修复会自动将当前引用的缺失字节写到健康卷；无需逐条执行修复命令。若节点已经停止，可先加卷再启动。`dubi volume list` 仍会把原卷显示为 missing，新卷显示为 healthy；请保留原卷和 Store 故障现场，不要把空目录伪装成原卷。这个过程尚不构成整卷迁完或安全移除的证明，也不能处理损坏的记录索引、永久 Node 移除后的自动再平衡或已失去可验证来源的数据。

### 移除节点

节点失联不会自动移除。确认需要永久退出该节点后，在当前 Root Leader 节点执行：

```text
dubi node remove <name-or-raft-id>
```

有名称的节点也可以直接按名称移除：

```sh
dubi node remove sh-01
```

纯数字参数默认视为 Raft ID；纯数字节点名称使用 `dubi node remove --name 123`。名称必须精确匹配，空名称、未知名称或歧义名称均不会删除任何节点。系统在 Root 读确认后解析名称，并固定对应的 Raft ID 执行原有移除流程，结果同时返回名称与实际节点 ID。临时离线仍占用名称；正式移除并隔离的旧身份不会再被名称查询选中。完成删除后再执行同一名称会报找不到节点；中断恢复也可用首次结果或状态记录中的 Raft ID 指定原节点。

命令会复用持久 Group 成员任务，先移除数据 Group 成员，再移除 Root 成员，最后隔离旧 NodeID/RaftID。中途停止后重复执行即可继续；如果节点只是暂时离线，不要执行该命令，待原节点恢复后让它沿原身份追赶数据。新节点仍通过普通 `dubi join` 加入；同一 Region 内的旧 Pack 数据会由后台 Repair 自动补齐，已由 `scripts/node_removal_rebalance_lab.sh` 的真实三进程隔离卷实验验证。跨机器/跨区域故障域、长期容量均衡和完整生产级物理再平衡仍未完成。该流程同时适用于缩容、维护下线和永久损坏后的成员清理，不定义单独的节点替换命令。

### 修改节点名称

节点名称是集群内唯一的可选管理员显示标签，允许为空，不参与 NodeID、Raft 或数据路由。启动或改名时如果名称已被其他当前节点使用，Root 会拒绝操作；正式移除的旧节点名称可以被新节点复用。启动时可以直接设置：

```sh
dubi server --name sh-01
dubi join --ticket-file join-ticket.txt --name sh-01
```

运行中的节点可以通过本机记住的管理地址修改或清空：

```sh
dubi node rename sh-02
dubi node rename --clear
```

已实现持久卷登记、启动恢复、在线加卷、混合 Pack 集合及 Node Runtime 持有的多卷 Pack Pool。Pool 在实际写入时自动选卷并预约字节，默认保留 max(1 GiB, 文件系统容量的 5%)；不需要新增 CLI/YAML 配置。volume list 仍只显示物理容量/健康，不显示进程内预约。通用事务已自动将大值写入 Pool，并在 Raft Apply 中原子发布本地索引和应用进度；位置不进入跨节点快照。具体能力数据 API、数据均衡、在线安全移除仍待实现；不能将控制面 Ready 视为所有数据卷均可写。

收到 SIGINT 或 SIGTERM 后，进程先取消后台调度与 Node 级恢复，再停止 RPC 连接并等待已进入的处理函数完成清理；之后关闭出站连接与 Transport，最后关闭 Group、状态机和 Store 锁。停止连接不等于处理函数已经返回，不能提前释放 Store。已接受的持久恢复不因 RPC 断开而回滚；阻塞的本地文件系统操作可能延长关闭时间。
