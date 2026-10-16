# dubi

DUBI 是一个独立的分布式数据系统：一个 `dubi` 二进制、一个集群控制面和按需启用的数据能力。

当前首要目标是把独立的集群底座跑通；文件、缓存、消息、搜索等模块在此之后接入。现阶段先用内置 `record` 验证底座，不把它当成正式业务模块。按 [底座可用推进清单](docs/foundation-roadmap.md) 逐阶段验收；实现细节见 [底座架构文档](docs/architecture/immutable-records.md)，更远期的生产验收差距另见 [清单](docs/production-readiness.md)。

集群底座的概念以 [`spec.md`](spec.md) 为准；详细规划见 [`../dubi.md`](../dubi.md)，目录职责见 [`docs/architecture/layout.md`](docs/architecture/layout.md)。

Byfile 提供经验证的底座代码与实现经验；DUBI 不依赖 Byfile 的模块、服务、协议或数据格式。

当前第一阶段的可运行闭环是：无手工 ID/YAML 初始化和加入、三节点形成 Root 与区域数据 Group、创建 Range、跨节点读写、少一节点后继续读写、原 Store 重启恢复，以及第四节点自动加入并完成区域交接。区域扩容已接入新增节点的封口引用补齐；测试专用处理器已验证三条非空清单引用（物理字节分散在三个旧节点，含空载荷）补齐到第四节点，并在其重启后保留。具体命令见 [本地运行三节点](docs/operations/local-cluster.md)。

磁盘压力的开发验证使用隔离稀疏盘，不填满宿主机；见 [磁盘压力保护](docs/operations/disk-pressure.md) 和 `scripts/disk_pressure_lab.sh`。

底座实现的逐项来源与验证见 [`docs/architecture/adoption.md`](docs/architecture/adoption.md)。

首次使用运行 `dubi init`，随后 `dubi server`；同一账号下直接运行 `dubi status`、`dubi doctor`、`dubi volume list`，不必反复传 Store 或服务地址。`init` 在 `~/.dubi/current.json` 记住当前 Store；服务 Ready 后记住实际监听地址。初始 Store 默认 `/var/lib/dubi`，也可在首次 `init` 时用 `--store` 指定；无需 YAML、节点 ID、Raft ID、成员列表或证书名称。多节点同机演示才需要显式选择 Node。

未指定卷时，初始化会登记 `<store>/volume`；使用 `dubi init --volume <空目录> --volume <空目录>` 可直接选择多个外部卷，不创建默认卷目录。`dubi volume list` 查询持久登记与健康；`dubi volume add <空目录>` 追加卷，目前要求节点停止。StoreID/VolumeID 自动生成，重启保留；通用 Pack 已实现追加、校验读取、封存、崩溃恢复，以及按 Volume 统一的持久集合清单与自动滚动（不同 Capability 可混写同一 Pack）（见 [Pack 实现边界](docs/architecture/pack.md)）；Node Runtime 已装配带字节预约的多卷 Pack Pool，默认自动选卷、不新增配置。通用 capstate 已接通大值 Pack 落盘、本地引用/result/applied 原子发布，以及跨节点重建位置的 Raft 逻辑快照（见 [提交边界](docs/architecture/pack-state.md)）。内置 `record` 已提供最小写入器；节点内均衡和安全移除尚未实现。

当前 `dubi init` 自动创建新集群稳定身份、托管 PKI 和首节点 Root；运行中的首节点可用 `dubi join token` 生成一次性凭证，新节点用 `dubi join --ticket-file <私密文件>` 自动登记身份/卷、完成 mTLS 签发、Root 接纳并运行。加入回执和本地阶段持久化，支持中断恢复及无凭证重启；托管后台按已接纳节点自动调整 Root 投票拓扑。`dubi status` 使用本节点证书读取实时 Root 视图，可查看节点、Region、地址、投票成员和 leader，并单独显示本机卷健康与后台修复最近观察到的错误；这些观察不证明全群数据多数。三节点同区域形成数据 Group 后，可用 `dubi dataset create` 和 `dubi record put|get` 验证 Range 栅栏、Raft 多数写入、跨节点读取和重启恢复。当前 `record put|get` 走的是 Raft 内联值验证路径，**不**代表不可变 Pack 数据已达到 `LocalReady`；后者已有内部写/读门禁及自动 Repair 联测，但尚未接入正式租户/Capability 入口。这是使用 Node 证书的开发验证入口，不是面向租户的公开数据 API；跨 Region 数据多数及生产故障验收也尚未完成。凭证保护与启动边界见 [`docs/operations/server.md`](docs/operations/server.md)。
