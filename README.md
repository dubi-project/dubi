# dubi

DUBI 是一个独立的分布式数据系统：一个 `dubi` 二进制、一个集群控制面和按需启用的数据能力。

底座已完成一轮三区域九节点六小时稳定性验收，当前进入 Engine 与 Module 的业务功能阶段；具体结果与待查事项见 [验收记录](docs/development/read-discovery-soak.md)。内置 `record` 仍是底座验证入口，不是正式业务模块。后续按业务闭环推进，不以完成所有底座优化为前提；实现细节见 [底座架构文档](docs/architecture/immutable-records.md)，更远期的生产验收差距另见 [清单](docs/production-readiness.md)。

开发新的 Engine、Module 或 Gateway 前，先阅读 [开发规范](docs/development/engine-module-gateway.md)，并按规范核对现有代码、接口和测试环境。该文档包含新线程启动说明模板。

开发规范第 12 节记录 Engine/Module 分阶段规划：State + Config 起步，再交付 Session/Cache、Blob + File、统一 Bus（req/reply、pub/sub、queue，持久部分使用 Record + State）、Index + Search/Log，以及明确列入后续计划的通用时序数据与 Trace 模块。`dubi-ex` 的外接 event、queue 服务是可选应用扩展，不是内置 Bus 的依赖。每阶段有明确的首版范围和完成条件；这是实施建议，不表示这些业务功能已经落地。

内置 HTTP Gateway 统一承载管理 API、业务 API、监控入口与内嵌 Web UI，职责与实施顺序见 [HTTP Gateway 架构](docs/architecture/http-gateway.md)。该文档区分现有入口和待实现功能。当前已可用的入口骨架包括配置持久化、托管 HTTPS 续期、管理 Token 登录、跨节点会话撤销和监控授权；内嵌页面仅用于接线验证，正式 UI 待 Stitch 设计。

State Engine 与 Config Module 首版已接通真实 Runtime：支持读写删除、版本/CAS、仅不存在时创建、请求幂等及 Tenant/Module 隔离。启用公共 listener 后可使用 `/api/v1/config/entry?key=...` 的 GET/PUT/DELETE；值最大 64 KiB。使用方式、持久格式及 VPS 验证范围见[State / Config 实现](docs/development/engine-module-gateway.md#13-state--config-首版实现)。Go SDK、列表、Watch、TTL 和完整控制台仍未提供。

新的数据寻址设计采用 Tenant + Module 隔离，Engine 复用实现；取消独立 Dataset，Runtime 自动管理内部键与 Range，Pack 聚合跨 Range 记录。当前实现与格式兼容边界见 [数据寻址与隔离](docs/architecture/data-addressing.md)。概念收敛及读路径审查见 [Runtime 简化记录](docs/development/runtime-simplification.md)。

集群底座的概念以 [`spec.md`](spec.md) 为准；详细规划见 [`../dubi.md`](../dubi.md)，目录职责见 [`docs/architecture/layout.md`](docs/architecture/layout.md)。

Byfile 提供经验证的底座代码与实现经验；DUBI 不依赖 Byfile 的模块、服务、协议或数据格式。

当前第一阶段的可运行闭环是：无手工 ID/YAML 初始化和加入、三节点形成 Root 与区域数据 Group、创建 Range、跨节点 Pack 读写、少一节点后继续读写、同一 RequestID 重试得到同一结果、原 Store 重启恢复，以及第四节点自动加入并完成区域交接。`scripts/foundation_three_node_lab.sh` 会在隔离稀疏卷中自动重复前三节点门禁；它已在开发机通过。`scripts/node_removal_rebalance_lab.sh` 进一步验证：停掉节点后，管理员按名称显式移除并 Fence 旧身份，新节点按普通流程加入同一 Region，旧 Pack 数据由自动 Repair 补齐并可从新节点读回。区域扩容已接入新增节点的封口引用补齐；测试专用处理器已验证三条非空清单引用（物理字节分散在三个旧节点，含空载荷）补齐到第四节点，并在其重启后保留。具体命令见 [本地运行三节点](docs/operations/local-cluster.md)。

磁盘压力的开发验证使用隔离稀疏盘，不填满宿主机；见 [磁盘压力保护](docs/operations/disk-pressure.md) 和 `scripts/disk_pressure_lab.sh`。

底座实现的逐项来源与验证见 [`docs/architecture/adoption.md`](docs/architecture/adoption.md)。

首次使用运行 `dubi init`，随后 `dubi server`；同一账号下直接运行 `dubi status`、`dubi doctor`、`dubi volume list`，不必反复传节点元数据目录或服务地址。`init` 在 `~/.dubi/current.json` 记住当前节点元数据目录；服务 Ready 后记住实际监听地址。节点元数据目录默认 `/var/lib/dubi`，也可在首次 `init` 时用 `--store` 指定；无需 YAML、节点 ID、Raft ID、成员列表或证书名称。多节点同机演示才需要显式选择 Node。

`--store` 保存身份、证书、共识状态、索引和卷登记，不是独立的集群存储资源。一个 Node 管理多个 Volume，内部 StoreID 只用于实例身份校验。未指定卷时，初始化会登记 `<store>/volume`；使用 `dubi init --volume <空目录> --volume <空目录>` 可直接选择多个外部卷，不创建默认卷目录。后续加卷保留原默认卷；重启按登记恢复，不自动重建丢失卷。`dubi volume list` 查询持久登记与健康；`dubi volume add <空目录>` 追加卷，可通过本机认证管理入口在线执行。StoreID/VolumeID 自动生成，重启保留；通用 Pack 已实现追加、校验读取、封存、崩溃恢复，以及按 Volume 统一的持久集合清单与自动滚动（不同 Engine 可混写同一 Pack）（见 [Pack 实现边界](docs/architecture/pack.md)）；Node Runtime 已装配带字节预约的多卷 Pack Pool，默认自动选卷、不新增配置。通用 enginestate 已接通大值 Pack 落盘、本地引用/result/applied 原子发布，以及跨节点重建位置的 Raft 逻辑快照（见 [提交边界](docs/architecture/pack-state.md)）。内置 `record` 已提供最小写入器；显式 Node 移除、全新身份普通加入及旧 Pack 数据自动补齐已有同机实验闭环；Volume 长期容量均衡、跨机器/跨区域故障验收仍未完成。

当前 `dubi init` 自动创建新集群稳定身份、托管 PKI 和首节点 Root；运行中的首节点可用 `dubi join token` 生成一次性凭证，新节点用 `dubi join --ticket-file <私密文件>` 自动登记身份/卷、完成 mTLS 签发、Root 接纳并运行。加入回执和本地阶段持久化，支持中断恢复及无凭证重启；托管后台按已接纳节点自动调整 Root 投票拓扑。`dubi status` 使用本节点证书读取实时 Root 视图，可查看节点、Region、地址、投票成员和 leader，并单独显示本机卷健康与后台修复最近观察到的错误；这些观察不证明全群数据多数。三节点同区域形成数据 Group 后，可用 `dubi tenant create` 和 `dubi record put|get` 验证不可变 Pack 写入、物理多数、跨节点读取、单节点离线后的继续读写、RequestID 幂等和重启恢复。这是使用 Node 证书的开发验证入口，不是面向租户的公开数据 API；跨 Region 数据多数及生产故障验收也尚未完成。凭证保护与启动边界见 [`docs/operations/server.md`](docs/operations/server.md)。

### 构建内嵌 React 管理界面

前端源码在独立仓库 `dubi-project/webui`，与本仓库并列检出。在本仓库执行 `./scripts/build.sh`，会构建 React + TypeScript 前端并嵌入最终的 `dubi` 二进制；可以传入前端仓库路径。开发构建需要 Node/npm，部署运行不需要。普通 `go build ./cmd/dubi` 使用本仓库已有的前端静态快照。详细约定见 [HTTP Gateway](docs/architecture/http-gateway.md#12-react-构建与嵌入)。
