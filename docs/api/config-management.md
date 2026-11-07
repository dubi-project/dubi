# 配置中心管理 API

管理员可在内置 Web UI 选择租户，查看、搜索、创建、修改和删除配置。
管理入口固定调用 Config Module → State Engine → Runtime，不接受客户端指定
Module 或 Engine。租户业务凭证仍使用原 `/api/v1/config/entry` 契约；管理帐号
不需要为每个租户另建业务凭证。

## 接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/admin/config?tenant=ID&limit=20&q=keyword&cursor=...` | 分页列出配置元数据 |
| GET | `/api/v1/admin/config/entry?tenant=ID&key=KEY` | 读取配置内容和完整版本 |
| PUT | 同上 | 创建或修改配置 |
| DELETE | 同上 | 删除配置 |

使用标准管理响应 `{data, meta}`。必须有有效管理员凭证或登录会话；会话写请求
需要 `X-Dubi-CSRF`。每次操作确认租户存在、已授权 Config，响应前重新确认权限。
Tenant、Module、Engine 身份覆盖头被拒绝。读写继续执行 Config 模块授权，写入经过
原 Runtime 配额、持久化和确认路径。

列表返回 `items: [{key, version, value_bytes, created_order, confirmed}]` 和可选
`next_cursor`。`value_bytes` 未知时为 null；版本是完整、不透明的版本标识，不是数字计数。
`confirmed` 表示发布已确认，不是本次读取内容的证明。只有详情读取会取得并校验实际内容。
列表不批量读取配置正文。

`limit` 为 1–100，默认 20。`q` 为不区分大小写的 Key 包含搜索，最多 256 字节。
游标绑定租户与搜索条件，不可跨租户或更换搜索条件后复用。每次最多扫描 512 条索引/
元数据记录；稀疏搜索可能返回空页和继续游标，客户端应允许继续翻页。分页为实时视图，
不是跨请求冻结快照；并发创建、删除时可刷新第一页。

创建顺序在 State 提交事务中分配，新增在前、修改保持原位置、删除后重建视为新项。
历史无顺序字段的记录在有顺序记录之后按 Key 排列，不伪造创建时间。索引、计数器和
State Head 同一事务更新，随 Raft 状态及集群备份恢复。当前 Runtime 的一个 Keyspace
对应完整初始 Range；有界元数据扫描明确拒绝部分 Range，未来拆分需提供跨 Range
分页和索引迁移方案，不能直接复用此完整范围实现。

## 写入条件与重试

请求体沿用 ConfigMutation：`request_id`、Base64 `value`、`if_absent`、`version`。
管理接口要求创建使用 `if_absent: true`，修改/删除携带读取到的完整 `version`，
不提供无条件覆盖操作。Key 最多 2 KiB UTF-8，值最多 64 KiB 解码后字节；空值允许。

同一逻辑操作重试必须保留请求标识和完整请求内容。`version_conflict` 不自动改用
新版本覆盖；重新读取后由管理员确认修改。超时、网络断开或 `outcome_unknown` 不表示
没有写入。页面保留原请求，锁定内容，再次确认时重试原操作；不把配置正文存入浏览器
持久存储。已重放成功的请求可能描述历史结果，需再次读取获取最新内容。

## Web UI

- `#/modules/config?tenant=ID` 保存导航上下文，刷新保留租户；业务模块间沿用选择。
- 统一租户栏支持服务端搜索、分页加载和租户详情；没有“全部租户”混合数据模式。
- 列表显示 Key、大小、版本、确认状态和详情；详情中编辑/删除。
- UTF-8 文本与 JSON 编辑；非文本字节或可能被文本控件改变的换行内容只读显示 Base64，
  通过 SDK 修改，避免转换损坏数据。
- 全局操作提示从页面顶部弹出，不影响布局；表单校验和可重试错误保留在表单中。

## 租户控制台入口

租户账号使用 `/api/v1/tenant/config` 和 `/api/v1/tenant/config/entry?key=KEY`，
租户由服务端登录会话绑定，不提交租户参数。管理员和读写成员可写，只读成员只读。
具体账号与权限契约见 [统一账号与租户控制台](tenant-console.md)。
