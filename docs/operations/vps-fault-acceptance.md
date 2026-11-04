# VPS 故障验收与部署工具

这些工具用于 Debian 12、Linux amd64 测试集群，通过开发机现有 SSH 密钥访问 VPS。节点、Store 和 Volume 身份从当前服务进程及认证 Root 状态发现，命令选择节点名称即可。SSH 密钥留在开发机；工具不修改 SSH 配置、Root 密码访问或节点 TLS 配置。

## 执行前提与报告

`plan` 只读取状态并生成可审阅的计划，不停止服务、创建 Dataset 或注入故障。`apply`/`run` 会重新检查拓扑、Node/Store 身份和已有长测进程；检测到 `vps_cross_region_soak.sh`、`vps_cross_region_load.py` 或本轮 `dubi` 验收控制进程时拒绝变更。当前六小时运行期间只做计划与隔离测试。

计划绑定当时的成员列表和身份，成员变化后应重新生成计划。故障工具还要求运行二进制与计划一致，初始集群全部健康；不把已故障集群当作注入实验的基线。保护针对已识别的测试进程和当前状态采样，不是分布式维护锁，执行时应暂停其它部署/拓扑操作及未纳入识别的压力工具。

每次使用一个不存在的 `--output` 目录，保留：

- `plan.json`：目标、身份、路径、版本和动作顺序。
- `events.jsonl`：逐项 SSH 观察与操作结果，采用追加写入，避免长时间等待反复重写全部日志。
- `result.json`：已完成的检查及 `passed`/`stopped` 结果。失败停止不会被计为通过；规划阶段失败可能只有事件日志。

工具目前已通过 VPS 隔离环境中的 24 项安全/顺序回归，并进行真实九节点只读清单验证。Node Remove/Join、失卷及容量耗尽的真实故障演练应在固定版本长测结束后独立执行，这份工具说明本身不是这些故障场景通过的证据。

## 物理持有探针

先在开发机构建只读探针，供故障脚本上传到测试节点：

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build \
  -o /tmp/dubi-vps-record-holding ./scripts/vps_record_holding
```

探针通过现有 mTLS RecordHolding RPC 请求精确描述的记录，核对证书身份、Cluster/Node/Raft ID、不可变描述、RecordID、StoreID/VolumeID 和已确认 Root 切点。它只验证 Pack 字节，不拉取或写入数据。允许对同一节点发起实际认证 RPC，避免通用 GroupClient 的 self-target 限制。

演练在故障前创建独立测试 Tenant/Dataset 和少量记录，保留发布时的 Origin Group 与内容。故障后先观察新节点或新卷上的物理持有，再进行目标节点逻辑读取，避免一次读取触发取数后被误算成自动 Repair。节点与区域数量依据完整 Root 成员目录计算，不按在线节点缩小分母，也不把同一 Node 的多个 Volume 当成多票。区域统计记录采样到的真实持有分布，不把它包装成新的 GeoReady 回执；跨区域写入场景还检查正式写入响应的 `geo_ready` 状态。

## 节点场景

```bash
python3 scripts/vps_foundation_faults.py plan \
  --host 206.206.78.113 --ssh-key ~/.ssh/dubi-vps-test_ed25519 \
  --node sg03 --case node-recover \
  --holding-probe /tmp/dubi-vps-record-holding \
  --output /tmp/dubi-node-recover-plan

python3 scripts/vps_foundation_faults.py run \
  --plan /tmp/dubi-node-recover-plan/plan.json \
  --ssh-key ~/.ssh/dubi-vps-test_ed25519 \
  --output /tmp/dubi-node-recover-result
```

| 场景 | 动作及必需证据 |
| --- | --- |
| `node-recover` | 停止目标服务，等待幸存 Root 多数与读取恢复，离线期间写入，再沿原 Store 启动。NodeID、RaftID、StoreID、VolumeID 保持不变，旧记录及离线期间记录须自动产生可验证的本地物理持有。 |
| `node-remove-join` | 停止目标，管理员按名称 Remove，幸存节点明确拒绝旧凭据，再用普通 Join 在同一主机、名称和端口加入新 Store。四类身份均不得复用；自动补齐后再冷重启验证新身份及持有。 |

`--host` 必须是幸存节点，不能选择目标自身。任何场景若停止目标会失去原 Root 多数，计划阶段就拒绝。节点名称必须在当前 Root 中唯一匹配。

Remove/Join 场景会保留原 Store，在 `/var/lib/dubi-acceptance-<run-id>` 创建新 Store，并写入 `/etc/systemd/system/dubi.service.d/90-dubi-acceptance.conf`。Join 成功后该 override 使用普通 `server --store <new-store>`，再重启验证，保证以后开机继续使用新身份。只覆盖本工具标记的旧 override，原配置留在计划及备份中；不会自动恢复已 Fence 的旧身份。

新身份使用已确认目录中的名称、Region 和地址，即使原 unit 只有 `server --store` 或保留改名前的名称也能保持拓扑。Join 后再次核对这些字段；不能因节点可达就接受它加入了错误 Region。非标准 unit 参数会在只读规划阶段拒绝，应先检查配置。

一次性 Join 凭据只通过加密 SSH stdin 传递，在目标保存为 Root 专用的 0600 文件，不进入 argv、计划或事件日志。成功切换为普通 server 后删除该凭据文件。失败时保留现有 Store、日志和状态供检查。

## Volume 与容量场景

```bash
python3 scripts/vps_foundation_faults.py plan \
  --host 206.206.78.113 --ssh-key ~/.ssh/dubi-vps-test_ed25519 \
  --node sg03 --case volume-add-after \
  --holding-probe /tmp/dubi-vps-record-holding \
  --output /tmp/dubi-volume-plan
```

使用同一个 `run --plan ... --output ...` 入口执行。计划要求目标初始只有一个健康 Active Volume，以便精确控制记录放置；有多个匹配项时可用 `--volume-id` 指定，但本组场景仍要求单 Active Volume 基线。工具先证明目标旧卷持有测试数据，再停机并将已登记目录改名保留，模拟不可读卷；它不删除 Pack。旧路径必须是可移动的目录，不能直接移动硬盘挂载点。此类路径故障不等于物理介质 EIO/坏扇区验收。

| 场景 | 故障后的安排 |
| --- | --- |
| `volume-add-before` | 故障前加入健康卷，故障后显式删除不可读旧卷，验证迁移/Repair 到已存在的健康卷。 |
| `volume-add-after` | 先故障并删除旧卷，验证幸存节点仍可读写，再加新卷，等待自动补齐并核对新 VolumeID。 |
| `volume-no-add` | 先不提供新本地容量，验证新记录由其它节点真实持有、原成员分母不变、目标保持降级。上述断言记录后再加恢复卷完成测试收尾；收尾不作为“不加卷阶段已健康”的证据。 |
| `volume-no-capacity` | 故障前准备独立有界测试卷；失去旧卷后把有界卷可写空间降至保护阈值以下，验证同区域幸存多数及其它区域持有。 |
| `volume-no-capacity-cross-region` | 需要专用单节点 Region 和至少三个 Region，验证本地无容量时正式写入同步返回 GeoReady，且其它 Region 有真实多数持有。当前 3×3 拓扑不满足此前提，工具会明确拒绝该场景计划。 |

容量场景只在工具创建的 3 GiB loop ext4 文件系统内分配保留文件，降低该 Volume 的可写预算；不填充 VPS 系统盘。要求 Root 挂载能力、`mkfs.ext4`/`mount`/`fallocate` 及足够的主机空闲空间。挂载失败会停止，不跳过后宣称通过。场景先记录无本地容量时的可用与降级证据，再释放保留文件；最后将有界卷 Drain/Remove 到持久恢复卷，核对新持有后才卸载镜像。Drain 失败绝不卸载仍有活数据的卷。

原离线目录及测试 Dataset 保留作审计。工具不会用旧 VolumeID 复活已删除卷，也不会自动删除成员。发生异常时停止进一步动作并留档；如果只是服务被本工具停止且原身份仍在已确认 Root 中，会尝试重新启动该身份，但不会自动撤销 Fence、重命名旧卷回去或卸载未安全移除的卷。`--timeout` 控制各阶段等待，默认 1800 秒；超时可能留下明确的降级状态，需要按正常运维步骤恢复。

## 逐节点部署

```bash
python3 scripts/vps_cluster_deploy.py plan \
  --host 206.206.78.113 --ssh-key ~/.ssh/dubi-vps-test_ed25519 \
  --binary /tmp/dubi-next --output /tmp/dubi-deploy-plan

python3 scripts/vps_cluster_deploy.py apply \
  --plan /tmp/dubi-deploy-plan/plan.json \
  --ssh-key ~/.ssh/dubi-vps-test_ed25519 \
  --output /tmp/dubi-deploy-result
```

计划记录完整 Linux amd64 ELF 文件 SHA-256，上传前再次核对源文件；全部目标的暂存文件通过哈希检查后才开始重启。初始 Root Leader 后置，逐台操作；每台检查 Node/Store 身份、实际运行进程哈希、Root 多数及已完成修复检查，失败立即停止。最后重新发现全体节点，要求版本一致及集群健康。

默认要求部署前全局健康。修复现有 Geo 补齐缺陷时，可在 `apply` 显式指定 `--allow-geo-debt`，仅允许部署前已有的 Geo 待办：每节点仍须确认 Root 多数、完成本地修复检查、拥有健康 Active Volume，且无失联、写入隔离、本地修复错误、卷维护或未知降级原因。运行中的长测仍会阻止部署。报告记录此选项和各节点初始降级原因；最后的全局健康门槛保持不变，全部节点安装成功也不能将未收敛状态记为通过。

安装使用同文件系统原子替换，旧二进制按原 SHA-256 留在可执行文件旁。工具不会自动降级旧程序：状态格式兼容性需要独立验证，不能以保存了旧二进制代替可安全回滚。失败报告列出已完成节点；恢复到健康后可重新使用同计划与新的输出目录，已经运行候选版本的节点会跳过安装。若成员、Store 路径或源文件发生变化，需重新生成计划。

## 跨节点容量均衡的隔离验收

`internal/command/node_balance_acceptance_test.go` 提供真实托管三节点、mTLS 和独立文件系统的机制测试。先交叉编译 command 测试程序，再上传 VPS；开发机不运行测试。仅在 VPS 上将 `DUBI_TEST_NODE_BALANCE_MOUNTS` 设为三个专用临时挂载目录的 JSON 数组，并运行 `TestManagedNodeCapacityBalanceOnDisposableFilesystems`。测试会校验三个目录设备号不同；未提供目录时显式跳过，不能算通过。

测试验证持久 Keeper 保留、源节点同身份重启后继续交接、源索引撤销与 Pack 空间回收、其它节点的多数持有及任意节点读取。随后让真实 Root 备份任务保持活动，确认源持有不会被均衡删除；最后只在专用文件系统里制造容量差，验证后台自动发现并完成下一次交接。保持活动的备份用于测试排他边界，不是备份对象传输或恢复验收。

2026-10-28 在 FF01 的 `/var/tmp/dubi-node-balance-2y97l5gc` 完成一轮 132 秒验收，三个挂载均已卸载，镜像已清理。空间预约采样与登记改为同一共享锁后，`/var/tmp/dubi-node-balance-krho68t5` 中此场景再次通过；同批任意节点测试暴露 Root Leader 选举临时错误未重试，整批报告保留 failed，镜像作为失败证据保留且全部卸载。该路由问题另行修复并重新验证，不能改写原报告。所有测试使用临时 Store、端口和目录，没有把候选部署到原九节点集群；同机环回文件系统不替代独立机器或真实坏盘故障域。

路由修正后，FF01 `/var/tmp/retry-command-gateway.log` 连续五轮通过：每轮先通过正常 Raft 领导转移，让首选 Owner 成为 Root Leader，再停掉它；同次调用必须恢复 Root 屏障、转向幸存 Owner，并用同一 RequestID 读回原提交，随后失去 Owner 多数时必须拒绝成功。Root voter 的临时 Unavailable 可以重查；混合授权、损坏或格式错误不得因为另一个临时错误而被掩盖。
