# Tenant 对外入口

目标是让应用连接任意健康 Node，按自己的 Tenant 权限使用模块 API；Node 仍按 Root 路由到当前 Owner。对外认证不替代内部节点 mTLS、Owner Fence 或物理多数确认。当前已完成 Token 凭据基础、Node 管理命令及独立 HTTPS 身份探针，并接通 **Config 首版业务 API**；其他 Module 接口仍按后续规划实施。

本文聚焦 Tenant 业务身份；内置 Gateway 的管理身份、监控与 Web UI 统一规划见 [HTTP Gateway 与 Web UI](http-gateway.md)。管理 API 使用独立授权，不能由 Tenant Token 获得集群管理权限。

## 已确定的边界

- Node 证书仅证明集群节点身份，不是 Tenant 应用凭据。现有 `record put/get` 和 `tenant create` 命令仍是 Node 认证的开发/管理入口，不能改名后作为租户 API 暴露。
- 公共请求的 TenantID 由验证过的凭据得出，不信任请求体、路径或 Header 中自报的 TenantID。授权检查发生在任何 Root 路由、Owner 转发和 Pack 读写之前；跨 Tenant 请求不进入内部 Engine RPC。
- 目标地址为 `Tenant + Module + Engine + Key`，不同 Tenant 或不同 Module 的同名键互不覆盖。Module 由服务端 API 路由固定，Engine 由服务端依赖绑定，客户端不能覆盖身份。同一 Tenant 的多个应用可访问同一 Module 的数据；不创建 Project 或 Dataset 资源，不要求业务方手工拼隔离前缀。内部代码已切换为 Module 隔离，兼容边界见 [数据寻址与隔离](data-addressing.md)。
- 平台不能把客户端提供的任意能力 mutation 当作通用写入 API。模块必须注册受约束的公开操作和编码器；由模块从已授权请求构造不透明内部命令，底座只负责 Keyspace、路由、确认、资源门禁与结果。现有 `enginedata.Publication` 是受信进程内模块接口，不直接序列化到网络。
- 对外 API 与节点内部 RPC 必须隔离。不能为了允许无 Node 证书的应用连接，就把当前内部 mTLS 服务改为可匿名握手而依赖每个现存 RPC 恰好自行检查身份。
- 写入的稳定 RequestID 由客户端或 SDK 在重试期间保留；只有 Owner 最终核验当前引用、持久确认和实时多数后才能呈现 `LocalReady`。未知结果必须明确标注可用同一 RequestID 重试，不能翻译成“未写入”。

## 建议的首版实现

1. 使用每 Tenant 可创建、轮换和撤销的高熵应用 Token；Root 只保存 Token 哈希、TenantID、过期时间及撤销应用索引，不存明文。凭据只在创建时展示一次。Node 在 Root 确认的状态切点验证，长请求在进入业务动作前后复核撤销/有效期。Token 不授予 Node 管理权限。
2. 在独立的对外 TLS listener 上提供公共 API，与现有 Node mTLS listener 分开。监听地址采用一个简单默认值，部署者需要跨机器访问时才显式指定；不引入 YAML、手填 Node/Raft ID 或逐节点凭据配置。该 listener 的业务部分只注册明确列出的 Tenant 服务；管理与监控入口按 HTTP Gateway 规划分别授权，不注册内部节点 RPC。
3. 首版只开放已定义公开语义的模块操作；通用底座提供统一认证、Tenant 绑定、限额、请求 ID、状态/错误映射与审计归因。`record` 开发探针不自动变成正式模块 API。
4. 验收包含：任意 Node 的同 Tenant 读写、跨 Tenant/伪造 Tenant 拒绝、Node 证书不能冒充应用 Token、撤销与轮换在不同 Node 上生效、失去 Root 多数时授权失败关闭、Owner 故障后的同 RequestID 重试，以及公开 listener 无法调用任何内部 Group/Root RPC。

## 当前已落地

- `dubi tenant token create <tenant>`：对已登记 Tenant 使用现有 Node 证书连接内部管理 RPC，缺省有效期 90 天；返回 Token 明文及 CredentialID，仅此一次。使用 `dubi tenant create <tenant>` 登记 Tenant，不创建存储容器；首次写入自动准备 Range。Root 命令只含 SHA-256 摘要。若响应丢失，重试同一创建 RequestID 会明确拒绝重发明文；可使用新的 RequestID 创建新 Token，旧 Token 无法恢复且到期自动失效。
- `dubi tenant token revoke <credential-id>`：Root 以应用索引持久标记撤销；同一 RequestID 可幂等重试。Token 验证要求 Root 确认读切点与本地应用进度，比较摘要时采用常量时间比较，随后按凭据绑定的 TenantID 授权完整 Keyspace；Root 多数不可用时失败关闭。
- Root 的凭据记录、撤销事实均进入 Raft/快照验证与恢复。管理入口仍只允许 Node 证书，不接受应用 Token；应用 Token 可调用 Config HTTP API。
- `dubi server --api-listen 127.0.0.1:7444` 可选启用独立 Tenant HTTPS listener。`GET /v1/me` 接受 `Authorization: Bearer <Token>`，返回凭据绑定的 TenantID 与 CredentialID；缺少/无效/撤销的 Token 返回 401，Root 授权不可确认返回 503。无需 Node 客户端证书，但必须验证服务端证书和集群 Node CA。内部 Node mTLS listener、Group/Root RPC 与 `record` 探针不在该端口注册。当前仍显式启用此 listener。
- Config 公开 GET/PUT/DELETE `/api/v1/config/entry?key=...`，固定 Config + State 绑定，只接受凭据中的 Tenant。写前及返回结果前复核 Token，写后复核失败按结果不明处理。复用底座 Pack 和物理多数确认；完整限制及示例见[开发规范第 13 节](../development/engine-module-gateway.md#13-state--config-首版实现)。

## 后续实现

Config 已有受约束操作编码器、任意 Node 转发、错误映射及请求大小/并发/时限限制；更细的权限范围、租户配额、审计、列表、Watch 和其他 Module 仍待实现。不得在当前内部 Node listener 上放宽 mTLS，也不得把 `record` 开发探针改名为 Tenant API。
