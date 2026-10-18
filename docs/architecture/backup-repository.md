# Backup / Restore Repository

本文确定 DUBI 底座级 Cluster Backup/Restore 的 Repository 边界和第一版实现方向。Repository 是 DUBI 内部的独立概念，不属于任何 Engine 或 Module。

## 1. 边界

Cluster Backup/Restore 只处理底座统一持久化层和集群一致性，不解析业务数据格式：

- Root/Group 控制状态、持久资源清单、Pack/Segment/对象字节和校验信息由底座统一处理；
- File、Cache、Message、Search、Log 等模块的业务语义不进入备份层；
- 明确可重建的 Cache 状态可以排除；
- 模块自己的文件导入/导出仍由模块负责，不能替代整集群恢复；
- 上层如果绕过底座写入未登记的持久文件，不能被标记为完整 Cluster Backup。

底座不理解 Engine 内容，但仍必须协调统一的提交切点、Pack 封存、WAL/Checkpoint 和 GC/Compaction 保护，不能简单复制正在写入的文件。当前 Worker 会把打开的 Engine Group 快照作为不透明 Artifact 一并封存；快照生成失败时整个 Recovery Point 失败。

## 2. Repository 资源

Repository 是保存 Recovery Point 的外部持久化目标。Repository 配置保存到 Root Group，由集群统一管理；不要求每个节点重复配置。

CLI 不使用 URI 表达全部参数，而使用结构化参数：

```text
dubi repository add <name> --type file --path <absolute-path>
dubi repository add <name> --type s3 --bucket <bucket> --prefix <prefix> [--endpoint <url>] [--region <region>]
dubi repository list
dubi repository show <name>
dubi repository update <name> ...
dubi repository remove <name>
dubi repository set-default <name>
dubi repository default <name>       # set-default 的兼容别名
```

`update` 可以只传需要改变的非密钥字段；CLI 会先读取当前配置再提交完整的新描述，并保留已有的加密凭据包。清除或替换凭据必须走 recovery 平台的加密凭据流程，不能把明文作为普通 CLI 参数传入。

当前支持两种后端：

### file

`file` 是默认后端。`--path` 是绝对路径，并按当前 Node 解释。

```bash
dubi repository add local --type file --path /backup/dubi
```

当每个 Node 都把自己的独立备份盘挂载到同一路径时，实际布局是：

```text
一个逻辑 Repository
  ├── node-01:/backup/dubi
  ├── node-02:/backup/dubi
  └── node-03:/backup/dubi
```

它不是多个独立备份任务。Root 创建一个集群级任务，各节点并行读取自己的数据并把对象写入本地目录；每个节点保存该 Recovery Point 的本地 Manifest，Root 在所有节点完成后封存集群任务。恢复时按 Root 任务记录读取各节点的本地 Manifest 和对象，再执行 Repair/Placement/Rebalance。

`file` 不要求 NFS/NAS 或其它共享目录。NFS、NAS、独立存储服务器挂载后也可以作为 `file` 路径，但不是单独的 `nfs://` 后端。

本地目录与节点属于同一故障域时，节点和本地备份盘同时丢失会丢失该节点上的备份对象。因此：

- `file` 适合 Volume/Pack 损坏、误删和节点可恢复场景；
- 完整区域/集群灾难恢复应使用 S3、独立存储阵列、磁带归档，或将对象额外写到其它故障域；
- 不能把本地 `file` Repository 的安全性等同于独立 S3 Repository。

### s3

`s3` 支持 AWS S3 和 S3-compatible 服务。位置参数独立保存，不把 Endpoint、Region 等强行塞进 URI：

```bash
dubi repository add primary \
  --type s3 \
  --bucket backup-bucket \
  --prefix dubi/cluster-a \
  --region us-east-1 \
  --endpoint https://s3.example.com
```

标准 AWS S3 可以省略 Endpoint；S3-compatible 服务通常需要显式指定。

Access Key、Secret Key 等凭据作为 Repository 记录中的加密字段保存，不能出现在命令行、Raft 日志、Manifest 或普通日志中。用户侧不暴露独立 Secret Store 概念；加密密钥生命周期属于 recovery 平台内部实现。

S3 是共享目标，但每个 Node 仍只上传自己封口后的 Pack 和 Root Artifact。Worker 会在配置的 Prefix 下自动使用 `nodes/<NodeID>/` 命名空间，因此同一 Recovery Point 的不同节点不会争用同一个 Manifest；对象字节也不会在节点之间转发。恢复时按对应 Node 的命名空间读取其 Manifest，再交给新的节点身份执行恢复和数据修复。

磁带不是第一版在线 Repository 后端。以后应先封存并校验 Recovery Point，再将不可变归档导出到磁带；恢复时先导回可读 Repository，再执行 DUBI Restore。

## 3. Recovery Point 与增量

每次备份在逻辑上都是独立的完整 Recovery Point，在物理上按内容哈希复用对象：

- 新增或变化的对象才上传；
- 未变化对象不重复写入；
- 每个 Recovery Point 都有独立 Manifest，不依赖前一份差异链；
- 所有对象完整校验、Manifest 封存后才标记为可恢复；
- 备份期间，GC/Compaction 不能删除被当前任务引用的对象；
- 失败、取消、校验不完整的任务不能作为恢复源。

Manifest 至少记录源 Cluster、Recovery Point 代际、持久资源清单、对象摘要、格式版本、节点和 Store 标识以及校验结果。节点本地 `file` Repository 的每个节点各自保存自己的 Manifest 和对象；Root 的 Backup Job 完成状态把这些本地结果绑定到同一个 Recovery Point。对象不需要在节点之间搬运。

Root 为每个 Backup Job 在创建时分配不可变 `Epoch`（取创建日志位置），它是该 Recovery Point 在 Repository 内的唯一封存命名空间；领取租约、节点接管和完成状态变化不会改写这个 Epoch。

## 4. 任务与恢复

备份命令可以从任意节点执行：

```bash
dubi backup run
dubi backup list
dubi backup status
```

命令入口节点只负责提交集群级任务；Root 持久化任务和一致性切点，各节点通过可接管的工作租约并行执行。入口节点不是全量数据中转站。

恢复默认面向空集群：

```text
读取并校验 Recovery Point
        ↓
恢复 Root/Group 控制状态和底座持久对象
        ↓
新节点通过普通 Join 和 Raft Snapshot 追赶
        ↓
Placement / Repair / Rebalance
        ↓
恢复集群级可服务状态
```

恢复不复制旧节点目录、旧 NodeID/RaftID、旧成员关系或旧 Pack 路径；恢复后的节点使用自己的身份。恢复完成前必须验证所有注册的底座持久资源可打开、对象摘要一致、Group 状态可服务。

## 5. 第一阶段实现边界

第一阶段先完成：

1. Root 中的 Repository 配置、默认 Repository 和生命周期操作；
2. `file` 本地对象仓库：内容寻址对象、原子封存、Manifest、完整性校验；
3. 结构化 `s3` 配置和加密凭据字段；
4. 集群级备份任务、节点本地 Manifest 和完成状态；
5. 从完整 Recovery Point 恢复到空集群的最小闭环。

第一阶段不实现：

- Node Snapshot；
- Node Replacement 独立资源或命令；
- File/Search/Log 等模块专属导入导出；
- 直接在线写磁带；
- 依赖外部 Secret Manager 或环境变量保存凭据。

当前代码状态：`internal/platform/recovery/repository` 已实现 Repository 校验、AES-256-GCM 凭据包原语、Store 本地密钥、file 内容寻址对象、S3-compatible 对象读写、节点命名空间、完整性校验和原子 Manifest；`internal/platform/recovery/{backup,restore}.go` 已实现面向 `ObjectStore` 的不透明 Artifact 备份/恢复规划器。Pack Pool 已提供 `SnapshotPacks`：在一个本地封口切点封存当前 Pack，并返回带 inode pin 的只读 Pack 文件清单，复制过程不需要解析 Pack 内容。Server Worker 已支持 file/S3 Repository、Pack/Root/Engine Group Artifact，节点通过内部 NodeAdmin mTLS 同步 Repository 密钥，`--credentials-file` 可加密更新 S3 凭据。节点之间不交换对象字节；新的 Engine Group 快照登记仍只覆盖当前已打开 Group，完整集群 Restore 执行和新节点的资源安装流程仍未完成。

