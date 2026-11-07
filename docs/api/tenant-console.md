# 统一账号与租户控制台

系统与租户使用同一个 `/api/v1/auth/login` 账号密码入口。账号名全局唯一，
归属与角色来自服务端账号目录，浏览器不提交身份范围。现有系统账号保持系统管理员权限。

## 账号归属与角色

账号增加 `tenant_id`、`tenant_name`（只读展示）和 `role`。
`tenant_id` 为空表示系统账号；非空表示所属租户。归属在创建后不可更改。
系统账号只支持 `admin`；租户账号支持以下角色：

| 角色 | 本租户业务数据 | 本租户账号与业务凭证 | 系统管理 |
| --- | --- | --- | --- |
| admin（管理员） | 读写 | 管理 | 无 |
| readwrite（读写） | 读写 | 无 | 无 |
| readonly（只读） | 读取 | 无 | 无 |

所有账号均可修改自己的显示名称及密码。每个已建立账号的范围至少保留一个启用的管理员；
租户第一个账号由系统管理员创建，必须是租户管理员。用户名全局唯一，包括停用账号。
账号标识为 `account_` 加 12 位随机十六进制值，并执行目录冲突检查。

角色、密码或停用状态改变会提升认证版本并撤销旧会话；删除账号同时撤销其会话。
仅修改显示名称不使会话失效。密码沿用加盐密码哈希；响应不返回哈希、盐或密码。
内部沿用既有 AdminAccount 记录类型，避免建立第二套账号目录。

## 系统后台

`/api/v1/admin/accounts` 管理全部账号，列表及详情显示系统/租户归属、租户名称、角色。
列表可按 `scope=system|tenant`、`tenant_id`、`q` 筛选，并使用 `limit/cursor` 分页。
排序保持启用优先，同状态创建时间倒序。

创建接受 `tenant_id`、`role`、`username`、`name`、`password`、`request_id`。
更新与删除沿用 `revision` 条件，更新不能改变归属或用户名。
系统后台创建租户账号时选择所属租户；账号归属创建后只读展示。

## 租户 API

租户控制台只接受账号登录的安全会话 Cookie。写请求必须同源且带 `X-Dubi-CSRF`。
所有租户接口固定使用已验证账号的租户，不接受 `tenant/tenant_id` 覆盖范围，
也不接受 `X-Tenant-ID`、`X-Module-ID`、`X-Engine-ID` 身份覆盖头。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/v1/me` | 当前账号、角色、租户及会话信息 |
| GET | `/api/v1/tenant/overview` | 本租户名称、配额、逻辑用量、模块、账号与凭证计数 |
| GET/POST | `/api/v1/tenant/accounts` | 本租户账号分页/创建，仅管理员 |
| GET/PATCH/DELETE | `/api/v1/tenant/accounts/{id}` | 本租户账号详情/修改/删除，仅管理员 |
| GET/PATCH | `/api/v1/tenant/profile` | 当前账号资料 |
| POST | `/api/v1/tenant/password` | 当前账号修改密码，需当前密码 |
| GET/POST | `/api/v1/tenant/credentials` | 本租户业务凭证分页/创建，仅管理员 |
| GET/DELETE | `/api/v1/tenant/credentials/{id}` | 凭证详情/删除已撤销记录，仅管理员 |
| POST | `/api/v1/tenant/credentials/{id}/revoke` | 撤销凭证，仅管理员 |
| GET | `/api/v1/tenant/config` | 本租户配置分页 |
| GET/PUT/DELETE | `/api/v1/tenant/config/entry?key=KEY` | 本租户配置读取/创建/修改/删除 |

配置列表和写入条件遵循 [配置中心契约](config-management.md)，但请求不带 `tenant`。
账号、凭证的列表先按所属租户过滤，再计算数量与分页；游标绑定租户和筛选条件。
非本租户的资源 ID 返回 404；权限不足返回 403；失效会话返回 401。
租户会话不能访问 `/api/v1/admin/*` 或 `/metrics`。概览不返回集群拓扑、节点或其他租户数据。

租户凭证只允许业务用途，必须明确 `modules` 与 `permission=readwrite|readonly`，
模块不得超出租户当前授权。支持有效期或 `never_expires`。Token 仅创建成功时展示一次，
服务端保存验证材料；浏览器不保存到 URL、localStorage 或日志。
验证时同时检查凭证模块与租户当前模块授权；Config 的写操作再次检查只读权限。
存量未指定模块/权限的业务凭证沿用租户模块范围、读写权限。
后续 Module 必须在每个写操作执行 `Principal.AuthorizeWrite()`，再走 Engine/Runtime。

## Web UI

一套构建产物、共享视觉组件，登录后按账号归属进入独立路由和菜单：

- `#/tenant/overview`：本租户概览。
- `#/tenant/access/credentials`、`#/tenant/access/accounts`：仅租户管理员可见。
- `#/tenant/module/config`：配置中心；没有租户切换器，只读角色禁用写入操作。
- `#/tenant/module/search`：全文搜索尚未实现，明确显示未开放，不提供演示数据。

模块入口统一使用 `#/tenant/module/{module}`，代表当前租户的整个模块工作区，
后续可承载模块概览、数据管理和租户级模块配置，不限定为数据页面。

租户页面不挂载系统通知、集群拓扑或全局租户查询组件。通知仍保留现有集群共享语义，
本次不增加按账号已读状态。系统/租户账号都使用同一入口，刷新保持当前 Hash 路由。

## 部署约束

本次账号与业务凭证记录新增授权字段。临时环境先更新六个区域节点，再同时停止并
升级三个 Root 节点，全部启动成功后才创建租户账号及受限凭证，避免旧版本忽略新字段。
新权限记录写入后，不可将旧版本当作可直接回滚的版本；恢复应使用理解这些字段的构建。
