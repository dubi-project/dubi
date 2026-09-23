# dubi

DUBI 是一个独立的分布式数据系统：一个 `dubi` 二进制、一个集群控制面和按需启用的数据能力。

集群底座的概念以 [`spec.md`](spec.md) 为准；详细规划见 [`../dubi.md`](../dubi.md)，目录职责见 [`docs/architecture/layout.md`](docs/architecture/layout.md)。

Byfile 提供经验证的底座代码与实现经验；DUBI 不依赖 Byfile 的模块、服务、协议或数据格式。

当前仓库先固定边界和目录骨架，再逐项纳入底座实现。第一个可交付目标是 **Foundation + Record**。

底座实现的逐项来源与验证见 [`docs/architecture/adoption.md`](docs/architecture/adoption.md)。

当前 `dubi init` 已能创建新集群稳定身份，并原子发布 Node CA 和首节点双用途证书；`dubi join prepare/install` 及“授权 → Root 登记 → 签发 → 叶证书安装”的 enrollment 核心也已落地。远端 enrollment 传输和持久一次性 admission 凭据尚未接入，不能据此开放无人值守加节点。内部 `dubi server` 生命周期已经可运行，但公开数据 API 尚未提供，空 Capability Mux 会拒绝所有数据能力请求。启动边界见 [`docs/operations/server.md`](docs/operations/server.md)。
