# 本地运行 DUBI 三节点

这是在一台开发机上验证底座命令和第一条数据读写链路的最小流程。三个 Store 和三个端口模拟三个 Node，但仍共用一台机器、磁盘与网络；不能据此判断真实故障域耐久性。可验证初始化、自动加入、Root 多数、数据 Group、Range、Record 写读与重启恢复。这里重复写 `--store` / `--server` **仅因为同一账号在一台机器上模拟三个节点**；正常一机一节点时运行 `dubi init`、`dubi server`、`dubi status`、`dubi volume list` 都不用这些参数。Record 命令使用 Node 证书，仅用于开发验证，不是租户客户端 API。

要自动跑完启动、三节点加入、Pack 多数读写、停一节点继续写、同 RequestID 重试和重启追平，可在仓库根目录执行：

```sh
bash scripts/foundation_three_node_lab.sh
```

脚本会在 macOS/Linux 上创建临时稀疏卷，避免开发机根卷余量触发正常的磁盘保护门禁；默认只占用实际写入的空间，结束后自动卸载和清理。可用 `DUBI_LAB_PORT_BASE=18443` 改端口，`DUBI_LAB_SIZE_GIB=32` 调整测试卷的逻辑容量。该脚本是开发证据，不替代跨机器、跨区域或长时间故障验收。

本地示例只指定 `--listen` 来区分三个端口；节点对外发布的地址会从实际监听地址自动确定。只有监听地址与其他节点应连接的地址不同（如 NAT 或端口映射）时，才需要额外指定 `--advertise`。

先在仓库 `dubi/` 目录构建二进制，并记下临时目录路径；在其他终端将第一终端打印的路径重新设为 `DUBI_DEMO`，例如 `export DUBI_DEMO=/tmp/dubi-demo.实际后缀`：

```sh
export DUBI_DEMO=$(mktemp -d /tmp/dubi-demo.XXXXXX)
go build -o "$DUBI_DEMO/dubi" ./cmd/dubi
printf '%s\n' "$DUBI_DEMO"
```

在终端 A 初始化并启动首节点。`init` 输出的 Node/Raft/Store 身份全部自动生成；不需要 YAML 或手填成员：

```sh
"$DUBI_DEMO/dubi" init --store "$DUBI_DEMO/n1"
"$DUBI_DEMO/dubi" server --store "$DUBI_DEMO/n1" --listen 127.0.0.1:17443
```

在终端 B 输入同一个 `DUBI_DEMO` 目录，再运行第二节点。一次性凭证直接经管道进入 `join`，不要复制到终端命令参数或日志：

```sh
"$DUBI_DEMO/dubi" join token --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443 | \
  "$DUBI_DEMO/dubi" join --ticket-file - --store "$DUBI_DEMO/n2" \
    --listen 127.0.0.1:17444
```

在终端 C 同样运行第三节点，使用新的凭证：

```sh
"$DUBI_DEMO/dubi" join token --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443 | \
  "$DUBI_DEMO/dubi" join --ticket-file - --store "$DUBI_DEMO/n3" \
    --listen 127.0.0.1:17445
```

在第四个终端查看当前 Root 控制面和默认卷。三节点自动晋升可能需要几秒；反复查询直到 `root.voters` 有三个 ID。`root.peers` 是持久目录，不是实时在线探测；`root.leader_id` 是该次观察的 leader 提示。

```sh
"$DUBI_DEMO/dubi" status --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443
"$DUBI_DEMO/dubi" volume list --store "$DUBI_DEMO/n1"
```

三节点形成同区域数据 Group 并确认 Range 承载能力后，创建一个 Tenant 下的 Record Dataset，再写入一个值。首次在 Group 尚未确认时会明确失败；稍等几秒后原样重试即可，不要改已有 Store 或手动指定 Group/Range ID。写命令会在错误输出打印 `request-id`，结果不确定时用 `--request-id` 携带同一个值重试：

```sh
"$DUBI_DEMO/dubi" dataset create --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443 demo items
"$DUBI_DEMO/dubi" record put --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443 demo items greeting hello
"$DUBI_DEMO/dubi" record get --store "$DUBI_DEMO/n2" --server 127.0.0.1:17444 demo items greeting
"$DUBI_DEMO/dubi" record get --store "$DUBI_DEMO/n3" --server 127.0.0.1:17445 demo items greeting
```

`get` 的 JSON 中 `found: true` 且 `value: "aGVsbG8="` 表示读回了 `hello`（字节值按 JSON/Base64 表示）。本验证入口当前接受不超过 1 MiB 的 Record 值；CLI 的 `put` 示例把末尾参数当作 UTF-8 文本。创建 Dataset 会自动登记 Tenant 并选择本 Region 已确认的数据 Group，不需要配置副本数。

按 Ctrl-C 停止终端 B。n1 与 n3 仍在线时，可以验证多数节点继续接受新写入：

```sh
"$DUBI_DEMO/dubi" record put --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443 demo items failover two-online
"$DUBI_DEMO/dubi" record get --store "$DUBI_DEMO/n3" --server 127.0.0.1:17445 demo items failover
```

然后用原 Store 和端口重启 n2：

```sh
"$DUBI_DEMO/dubi" server --store "$DUBI_DEMO/n2" --listen 127.0.0.1:17444
```

随后可以从 n2 执行 `status`，核对原 Cluster/Node/Root 成员。仅一节点存活而 Root 已有三个 voter 时，`status` 的读屏障会失败，不会把旧目录缓存显示成已确认的实时状态。

重启 n2 后，再执行一次 `record get --store "$DUBI_DEMO/n2" --server 127.0.0.1:17444 demo items greeting`，应读到相同的值；`failover` 这个在 n2 停止期间写入的键也应读到 `two-online`。`server` 进程就绪和数据 Group 重新选出 leader 可能有短暂间隔，期间的读错误应稍后重试，不能据此判断数据丢失。

若要验证区域扩容，在 n1、n2、n3 都运行时，用新的凭证加入第四个 Node（另开终端）：

```sh
"$DUBI_DEMO/dubi" join token --store "$DUBI_DEMO/n1" --server 127.0.0.1:17443 | \
  "$DUBI_DEMO/dubi" join --ticket-file - --store "$DUBI_DEMO/n4" \
    --listen 127.0.0.1:17446
"$DUBI_DEMO/dubi" record get --store "$DUBI_DEMO/n4" --server 127.0.0.1:17446 demo items greeting
```

第四节点加入后，区域增长会自动封口、追赶成员、执行新增节点的引用清单补齐，再确认下一代；交接期间数据入口会明确拒绝请求，稍等后原样重试读取即可。这里的公开 `record` 演示值走 Raft 状态，引用清单为空；仓库中的自动化联测另用测试专用处理器验证多个旧节点分别持有的引用数据及空载荷补齐，不增加用户命令。

全部演示结束后对运行中的终端按 Ctrl-C；临时目录保留，便于检查或下次用原 Store 重启。此流程不需要删除已有节点或重新 `init`。
