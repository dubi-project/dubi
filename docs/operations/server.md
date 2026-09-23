# 启动 DUBI 节点

当前入口只开放内部集群服务，还没有公开 Record API。节点可以建立和恢复 Root/数据 Group，但空 Capability Mux 会稳定拒绝全部能力读写；这不代表 Record 已交付。

## 1. 初始化身份

```sh
dubi init --store /var/lib/dubi
```

命令输出持久 `ClusterID`、`NodeID`、`RaftID` 和 PKI 文件路径，并在 `<store>/dubi-node-pki` 原子创建 Node CA 与当前节点证书。内置 PKI 自动使用集群虚拟 TLS 名称 `dubi.internal`；它不需要 DNS 解析，也不与节点的 IP、主机名或 advertise 地址绑定。对同一目录重复执行会验证并返回原身份及证书，不会生成第二套。

`init` 只用于创建新集群和首节点。不能在其他节点执行 `init` 后把它冒充为同一集群成员。

## 2. 准备加入已有集群

在新节点生成稳定但尚未绑定 Cluster 的 NodeID/RaftID、Ed25519 私钥和 CSR：

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

当前尚未接入远端 enrollment RPC 和持久一次性 admission 凭据，因此还没有完整的线上 request 提交/审批/取回 bundle 通道；不要用文件复制或离线签发绕过 Root 登记。现有 CLI 只负责安全地产生请求和安装已经由受控协调器签发的 bundle。

## 3. 节点证书与本地 CA

节点证书必须：

- 由配置中的 Node CA 签发，同时允许 ServerAuth 和 ClientAuth；
- 内置 PKI 覆盖统一虚拟名称 `dubi.internal`，不绑定 `advertise`；
- Subject Common Name 等于持久 `NodeID`；
- 只含一个 DUBI 身份 URI：`spiffe://dubi/<ClusterID>/node/<NodeID>/<RaftID>`；
- 私钥文件不能被 group 或 other 读取。

`dubi init` 已签发满足这些条件的首节点证书。未在 YAML 中填写 `tls` 时，进程默认读取 `<store>/dubi-node-pki/node.crt`、`node.key` 和 `node-ca.crt`，并以 `dubi.internal` 校验证书。专家模式可显式覆盖证书路径和 `tls.server_name`，用于企业外部 PKI；普通部署不需要理解或填写这些字段。

CA 私钥不会以裸 `node-ca.key` 落盘，而是保存在绑定 Cluster/Node/Raft 身份的认证加密信封中；解封密钥和信封都仍位于同一受保护目录，所以它们不能抵御已经取得整个 Node Store 读取权的攻击者。`0700` 目录和 `0600` 私密文件权限才是当前静态保护边界。加入协议完成前，不要复制签发器文件到其他节点；Node CA 也必须另行做受控安全备份，否则首节点存储完全丢失后无法继续签发节点证书。

## 4. 配置与运行

从 [`deploy/dubi.example.yaml`](../../deploy/dubi.example.yaml) 复制配置，填写实际地址；默认 PKI 路径无需重复配置：

```sh
dubi server --config /etc/dubi/dubi.yaml
```

配置使用严格 YAML：未知字段、多文档、非法地址、重复 RaftID、非正数间隔和超过 64 MiB 的消息上限都会拒绝。相对路径以配置文件目录解析。

首次建立多节点 Root 时，`root_bootstrap` 必须列出全部初始 RaftID 和地址；列表不是副本数配置。已有 Root 中确认的 Node 地址优先，bootstrap 地址只用于首次启动或目录尚未可用时兜底。

收到 SIGINT 或 SIGTERM 后，进程会停止 RPC 和后台调度，关闭出站连接与 Transport，再关闭全部 Group、状态机和 Store 锁。
