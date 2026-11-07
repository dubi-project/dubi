# 访问凭证、备份仓库与备份管理 API

以下接口属于 `/api/v1/admin`，必须具有管理员 Token 或有效管理员会话。Tenant、monitor 身份不得访问。Cookie 写请求要求同源 Origin 与 `X-Dubi-CSRF`；JSON 请求体限制 16 KiB，拒绝未知字段。响应统一为 `{data,meta}`，继承 request ID、no-store 和错误契约。任何节点均通过现有 Root 一致性屏障、提交路径读写同一目录。

## 访问与凭证

| 方法及路径 | 内容 |
| --- | --- |
| GET `/credentials` | 所有管理、租户、监控凭证的脱敏目录；不列登录会话 |
| POST `/credentials` | 签发凭证，返回一次性 Token |
| GET `/credentials/{credential_id}` | 凭证详情及当前有效状态 |
| POST `/credentials/{credential_id}/revoke` | 撤销凭证；管理员凭证的会话同时失效 |
| DELETE `/credentials/{credential_id}` | 删除已撤销凭证记录；有效或已过期但未撤销的凭证返回 409 |

列表参数：`q`、`kind=admin|tenant|monitor`、`status=active|expired|revoked`、`expiring=true`、`tenant_id`、`limit`（1–200）、`cursor`。搜索覆盖名称、标识、租户名称/标识和签发请求标识；游标与过滤条件绑定。按有效、已过期、已撤销分组排序，各组按签发时间倒序，同秒以不可变 `created_index` 倒序、标识作为稳定兜底。先全局排序再分页，返回 total 与未过滤的 summary（包括 active、expired、revoked 数量）。状态改变后刷新第一页；分页不承诺跨请求的原子快照。

创建 JSON：`request_id`、`name`（最多 80 字符）、`kind`、`ttl_seconds`（1 秒至 366 天），或显式提供 `never_expires:true` 与 `ttl_seconds:0` 表示永不过期；省略有效期或仅提供零值不自动签发永久凭证。tenant 类型还必须提供 `tenant_id`，其他类型不得附带租户。权限由类型与租户策略确定，不接受客户端模块、Engine 或任意权限声明。名称及签发请求 ID 进入 Root 凭证元数据；旧的未命名凭证显示其 ID，不创建迁移任务。Root 仅保存 Token 摘要，不返回摘要或原 Token。签发请求重试不能重新取回 Token，返回 `token_not_recoverable`；结果不确定时按 request ID 查询，撤销不用的凭证再重新签发。

新签发的凭证标识为 `credential_` 加 12 位小写十六进制哈希，Token 密钥仍为独立生成的 256 位随机值。已有 `cred_` 标识及 Token 保持有效，不改写已签发的身份引用。Root 原子检查标识冲突，不能覆盖已有凭证。永不过期的 `expires_unix` 为 0，不计入到期提醒；登录会话仍最多 12 小时。CLI 支持 `--ttl 0`，默认仍为 90 天。

撤销和删除 JSON：`{request_id}`。当前登录所用的管理员父凭证不可在此会话中撤销，返回 `current_credential`；需要使用另一个管理员身份操作。已撤销凭证的重复撤销无副作用。创建、撤销和删除均重新验证管理员权限。删除时 Root 再次确认已撤销，并同时清除管理员父凭证关联的会话记录；重复删除不存在的凭证成功。删除不会使旧 Token 恢复有效，也不删除租户数据。CLI 的节点本地管理能力保留。

## 仓库

| 方法及路径 | 内容 |
| --- | --- |
| GET `/repositories` | 仓库卡片目录、默认项、已完成备份数量 |
| POST `/repositories` | 新建仓库；type 默认为 file |
| GET `/repositories/{name}` | 脱敏仓库设置 |
| PUT `/repositories/{name}` | 更新，必须提供当前 revision |
| POST `/repositories/{name}/default` | `{request_id}` 设置默认仓库 |

创建/更新字段：`request_id,name,type,path,bucket,prefix,endpoint,region,path_style`；更新还需 `revision`。file 使用规范的绝对路径，不接受 S3 参数。S3 使用独立参数，不使用 URI；新增需要成对的 `access_key` / `secret_key`，支持可选 `session_token`。更新留空凭据保留已有加密信封；替换临时凭据时需重新提交 Access Key / Secret Key。

S3 密钥经现有集群仓库密钥 AES-256-GCM 加密后才进入 Root。列表和详情不返回明文、密文或 nonce。没有取得本地集群密钥的入口返回 `key_unavailable`，不会自行生成不兼容的新密钥。加密提交的结果不确定时先刷新核对，已处理 request ID 不重新生成信封。

revision 覆盖存储位置与加密信封，Root 提交时原子检查；CLI 更新同样会改变 revision。备份执行期间拒绝修改仓库；已有备份后禁止修改目的地位置，避免破坏历史恢复点，仍可在任务结束后轮换凭据。Web UI 不声称“已配置”等同于可访问，不发明连接检查结果。

file 路径由各节点按本地文件系统解释。多个进程共用一台测试主机时，须为各进程提供独立挂载视图，模拟各自的本地备份盘；不能让它们写入同一个 Manifest 目录。S3 由现有 worker 按节点命名空间保存。

## 备份与恢复准备

| 方法及路径 | 内容 |
| --- | --- |
| GET `/backups` | 备份记录，按创建索引从新到旧分页 |
| POST `/backups` | `{request_id,repository}` 创建持久集群备份任务 |
| GET `/backups/{job_id}` | 任务状态、实际节点进度及统计 |
| GET `/backups/{job_id}/restore-preparation` | 已封存恢复点的源集群、仓库、节点与恢复步骤 |

列表参数：`q,status,repository,limit,cursor`，状态为 pending/running/completed/failed。任务 ID 派生自请求 ID；相同请求重试返回同一任务，不新增备份。创建时记录时间，旧任务没有时间则显示未记录，不按索引猜时间。节点数取任务创建时的成员集合，不按当前节点数量假定完成。worker 完成才汇总其对象/字节数；它们是已有 worker 的统计值，不冒充实时上传百分比、传输速率或节省空间。

任务沿已有 Backup Worker / Recovery Point 流程后台执行，所有节点完成并发布恢复点后才标记 completed。页面手动刷新读取真实进度。错误详情对 HTTP 输出脱敏；定位需要对应节点日志，不通过 API 返回存储后端可能含敏感信息的原始错误。

恢复准备仅接受 completed，失败或未完成的任务返回 `backup_incomplete`。它提供新空集群的恢复参数，不会在源集群启动覆盖恢复，也不声称已经验证仓库对象当前仍存在；完整对象校验、目标映射与写入由已有离线恢复工具在目标环境完成。此页面没有新增 Node Snapshot、Node Replacement、跨节点备份中转或租户/模块备份配置。

## Web UI

正式页面为 `management/Credentials.tsx` 和 `management/Backups.tsx`。复用已确认样式，仓库卡片筛选下方备份列表，详情抽屉按需查询；新建、设置、默认仓库、签发、撤销、删除已撤销凭证、立即备份及恢复准备均使用上述接口。凭证列表在类型标签后提供“有效”和“已撤销”标签及数量，创建有效期支持“永不过期”，撤销和删除均要求确认。凭证创建后只在弹窗内暂存 Token，关闭即清除；仓库密钥仅表单暂存，保存/关闭后丢弃。无 localStorage、秘密 URL、浏览器持久凭证或演示数据回退。

列表展示读取中、错误、空结果；过滤条件变化清空旧页，取消过期请求；401 返回登录，403/404 清除旧数据，503 明示错误。更改操作提交时禁用重复点击与关闭弹窗；不确定结果保留请求标识。正式入口与本地 preview 页面分离。

## 临时 VPS 验收（2026-11-06）

- 环境：TT01 `209.200.246.159` 的 `dubi-gateway-demo-1/2/3`，HTTPS 端口 18444/18446/18448；目录 `/var/tmp/dubi-http-demo-20261106`。原九节点服务未更新。
- Root、HTTP Gateway、Repository 包测试在 VPS 运行通过，覆盖授权/CSRF、加密信封、撤销、游标、并发版本与既有备份目录保护。
- `scripts/vps_operations_acceptance.py` 最终 117 项 HTTPS 检查通过，包含三个入口的一致性、父凭证撤销使会话失效、Token 不可重复获取、S3 凭据保存/保留/脱敏、真实备份、分页及恢复准备。
- 连续两次 file 集群备份均完成 3/3 节点封存，独立目录中均可读取 Manifest；每个节点有 3 个未变化对象在两次备份间复用。三个同机测试进程通过 systemd `BindPaths` 使用各自的本地备份目录，避免将同机共享路径误当成三块独立备份盘。已有完成 Manifest 保留在对应节点目录。
- 使用真实 HTTPS 的浏览器操作通过：登录、凭证签发/详情/撤销，仓库新建/设置/默认切换，卡片筛选、备份提交/完成、节点详情、恢复准备。Token 与密钥未写入截图或报告。
- S3 本轮验证为加密配置 API 与 Repository 包的 S3 协议测试，没有使用真实云厂商账号执行端到端备份。file 验收为真实任务，不以其结果替代云 S3 验收或新集群完整恢复验收。
- 保留 `local`、`secondary` 两个可用 file 仓库；测试使用的 `qa-s3-*` 虚拟配置已移除。证据在 VPS 的 `operations-acceptance.json` 及 `/var/tmp/dubi-operations-{http-final,root-final,repository}.log`；报告只包含脱敏结果。

列表默认最新创建的在前：仓库卡片和备份按不可变创建顺序倒序；仓库修改或设置默认不改变创建顺序。历史仓库未记录创建顺序时排在已记录数据之后，标识用于稳定兜底，不能将更新时间冒充创建时间。租户和凭证的旧版排序游标不再接受，升级后刷新第一页。

## 凭证改进的验证范围

本次永久凭证、短标识、撤销后删除及状态优先排序只更新代码和本地预览。前端类型检查和构建通过，内嵌资源已更新；本地浏览器验证通过状态排序、永久有效期、状态标签数量、创建/撤销/删除、删除取消和手机布局。正式组件通过本地模拟接口验证永久凭证请求、撤销/删除方法、状态筛选，以及删除失败重试沿用同一请求标识；这不替代真实服务端验收。新增认证、Root 状态与 HTTP 生命周期回归用例已交叉编译，服务端 Linux 二进制构建通过；尚未在 VPS 执行这些新用例，也未发布到临时 VPS。上面的历史 VPS 验收不覆盖这些新改动。
