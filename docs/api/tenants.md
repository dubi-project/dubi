# 租户管理 API v1

租户管理位于 `/api/v1/admin`，复用管理响应 `data` / `meta`、管理员 Token 或管理会话、关联请求 ID、同源 CSRF 和错误模型。Tenant Token 与 monitor 身份无管理权限。管理授权不授予租户业务数据访问权限。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/tenant-modules` | 已启用且可授权的业务模块；当前为 Config |
| GET | `/tenants` | 查询租户，支持 q、status、limit、cursor |
| POST | `/tenants` | 创建租户 |
| GET | `/tenants/{tenant_id}` | 名称、模块、配额、用量和凭证元数据 |
| PUT | `/tenants/{tenant_id}` | 更新名称、模块范围与配额 |
| POST | `/tenants/{tenant_id}/credentials` | 签发应用 Token，仅返回一次明文 |
| DELETE | `/tenants/{tenant_id}/credentials/{credential_id}` | 撤销指定租户的凭证，不删除租户数据 |

本版本不提供删除租户或批量删除其数据的操作。

## 标识、名称与授权

`tenant_id` 为 1–128 字节，首字符为字母或数字，其余允许字母、数字、点、横线、下划线。集群内唯一且不可修改。`name` 必填、最多 80 个 Unicode 字符、无控制字符和首尾空白，可修改，不要求唯一。

`modules` 是显式允许列表，空数组表示禁止业务访问，不能省略或传 null。只接受当前已启用模块，新模块不自动加入租户权限。Gateway 依据凭证绑定租户，Module 身份由服务端固定；Config Module 入口校验模块授权，响应前再次复核。既有 Token 同样服从更新后的租户授权。

历史 Tenant 没有模块字段时，保留当时已开放的 Config 行为。内部 items 探针仅用于兼容开发测试，不成为公共模块选项。保存租户设置后写入显式模块列表。

## 写请求与并发

创建示例：

```json
{"request_id":"tenant-create-01","tenant_id":"commerce","name":"电商业务","modules":["config"],"storage_limit_bytes":10737418240}
```

更新使用同样字段，增加最近 GET 返回的 `revision`。路径与请求体的 tenant_id 必须相同。版本比较和写入在 Root 的同一个状态机操作中执行；旧版本返回 409 `version_conflict`，不会覆盖别人的修改。标识重复返回 409 `already_exists`。名称、模块范围和配额作为一个设置整体保存。

`request_id` 是调用者为本次操作生成的稳定标识。超时或 503 `outcome_unknown` 后，先核对结果；重试相同操作保留原请求标识和请求体。相同标识不同内容返回冲突。HTTP 响应头 `X-Request-ID` 是本次 HTTP 调用的诊断 ID，与持久操作标识不同。

浏览器写请求必须携带既有会话 Cookie、匹配当前入口的 Origin、`X-Dubi-CSRF` 和 `Content-Type: application/json`。请求体最多 16 KiB，拒绝未知字段和多余 JSON。读取请求体后重新确认管理员授权。新签发 Token 在返回前再次确认授权。API 使用 Gateway 的并发和 15 秒请求预算。

## 列表与详情

q 对标识和名称做大小写不敏感的子串匹配；status 为 active、inactive、unauthorized。状态依次根据是否授权模块、是否有有效凭证计算，不是集群健康状态。

列表按创建顺序倒序（不可变 `created_index`），最新添加的在最前面，修改名称或配额不会改变位置；租户详情中的凭证按签发时间倒序，同秒按创建顺序倒序。历史租户没有记录创建顺序时排在已记录数据之后，同序以标识稳定排序。默认 50 条、最大 200 条，游标绑定查询条件。`data.total` 是当前筛选总数；`data.summary` 提供全体租户数、有效凭证数和无有效凭证租户数。列表不返回完整凭证数组；详情返回凭证 ID、签发时间、到期时间及 active/expired/revoked 状态，不返回 Token 或验证摘要。

目录读取有明确上限：4096 个租户、16384 条凭证记录。超限返回不可用，不能静默截断。凭证时间使用 Unix 秒。

## 整体存储配额

`storage_limit_bytes` 为整数，0 表示不限额，最大 1 PiB。当前按存活的逻辑键字节与值字节合计；空值仍计算键，删除为 0。不同模块和 Engine 使用独立键地址，但共同计入同一 Tenant 账本。不计副本、Pack 编码开销、回执、墓碑和集群备份。

- `used_bytes`：后台已汇总的逻辑用量，会滞后于最新写入；业务请求尚未满足物理确认而返回结果不明，也可能已有持久发布和计费。
- `reserved_bytes`：兼容字段；异步软配额不再创建全局预留，正常为 0。
- `accounting`：是否有完整配额账本；false 时用量为 null，不能用零代替。
- 超额写返回 409 `quota_exceeded`；已有数据保留。读取、删除、等量覆盖和缩小数据可继续执行。将限额调低到已有用量以下，不删除数据或撤销已经开始的写入。用量和限额在节点间异步同步，允许短暂超额；同步后阻止继续增长。

新管理 API/CLI 创建的租户从创建时启用计量。本开发版本不实现历史测试数据自动迁移或补账。没有完整账本的测试租户，用量显示不可用，设置有限配额返回 409 accounting_unavailable；验收使用新创建的租户。

Runtime 机制和验证边界见 [租户配额](../architecture/tenant-quota.md)。

## 凭证

签发请求为 `{"request_id":"issue-01","ttl_seconds":7776000}`，有效期 1 秒至 366 天；永不过期须显式提供 `never_expires:true` 与 `ttl_seconds:0`，返回的 `expires_unix` 为 0。响应包含 credential_id、token、expires_unix。Token 只在成功签发时显示，服务器只保存高熵 Token 的摘要。重放已处理的签发请求返回 409 `token_not_recoverable`，不会另发一枚 Token 或从摘要恢复明文。结果不明时核对凭证列表，撤销不再使用的凭证，再显式创建新凭证。

撤销请求为 `{"request_id":"revoke-01"}`，重复撤销已撤销的凭证仍成功；凭证不属于路径租户时返回 404。撤销影响后续授权，不删除数据。UI 撤销前确认，Token 只在一次性结果弹窗中保留，不写入浏览器存储、URL 或日志。
