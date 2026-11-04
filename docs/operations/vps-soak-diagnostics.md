# VPS 长测诊断与现场采集

`scripts/vps_cross_region_soak.sh` 在测试节点执行低速写入、本地读取和跨区域读取。它与 `scripts/vps_soak_diagnostics.py` 配套使用。部署脚本时将两个文件复制到同一目录；脚本依赖 Debian 12 自带的 Bash、Python 3、coreutils、systemd/journalctl。

## 任意节点查看修复明细

`dubi status` 的 `cluster.repair_observations` 保留每次认证 RPC 返回的节点修复信息：

- `name`、`region_id`、`node_id` 和 `raft_id`：节点标识。
- `worker_enabled`、`last_check_unix`：修复任务启用状态、最后检查时间。
- `repair_debt`：有未解决错误的 Group/发现任务数量，不等于缺失记录数。
- `issues`：对应的 `group_id` 和原始 `error`，最多 64 条。
- `issues_truncated`：总数量大于返回明细数量时为 `true`，不能把截断明细当成完整列表。
- `geo_debt`：Geo metadata 与 record 待修任务数量。
- `write_quarantines`、`volume_write_errors`：写入隔离数量及对应 Volume/错误。

这些明细来自该次 RPC 的同一份返回结果；后续修复成功不会改写已输出的状态快照。它们是各节点采样时刻的诊断观察，不是跨节点原子快照。命令继续校验节点证书和 Root 成员身份；不需要新增管理命令或节点间 SSH。

`last_check_unix=0` 表示尚无完成的检查记录；即使 `repair_debt=0`，也不能据此证明修复扫描正常。可结合服务启动时间和本地日志排查。

## 长测输出

每次使用一个全新 `DUBI_SOAK_DIR`，已有目录会被拒绝，防止覆盖上轮证据。运行前后保留工作负载 CLI 的 SHA-256；运行中替换该二进制会使验收失败。启动时还保留 CLI version、脚本和诊断工具的 SHA-256。

`run.info` 的 `key_epoch` 是实际记录身份的一部分：第 N 次成功写入的 Key 为 `soak-<key_epoch>-N`，内容为 `dubi-cross-region-soak:<key_epoch>:N`。独立物理抽查应使用这个字段和真实成功的写入行，不能从目录名或采样时间猜测 Key。先用只读认证持有接口核对精确描述和当前固定成员多数，再做可能触发拉取的逻辑读；请求成功率、Geo 待办计数和物理持有证据分别保存。

长测目录包含：

| 文件 | 内容 |
| --- | --- |
| `run.info` | 起止时间、参数、PID、轮数、退出码和 Geo 降级许可 |
| `metrics.tsv` | 每次写读结果、耗时及每次集群检查的状态/Leader |
| `status-*.json`、`*.check.err` | 定期状态及对应健康门槛结论 |
| `failure-put-*`、`failure-get-*` | 失败请求的响应和错误，包括读回值校验错误 |
| `incident-*-status.json` | 原样保留触发事件的状态；请求失败时可能是最近一次状态，不能当成请求失败当刻的全新采样 |
| `incident-*-systemd.txt`、`incident-*-journal.txt` | 工作负载节点服务状态和最近日志，采集有超时与行数上限 |
| `binary-*.txt` | 启停二进制哈希、版本及脚本哈希 |
| `summary.json`、`summary.md` | 成功/失败数量、成功率、成功请求平均值/P50/P95/P99/最大耗时、各读取目标统计、集群状态分布与健康检查失败 |

正常结束、基线失败、健康门槛失败、连续请求失败、SIGINT 或 SIGTERM 均触发最终报告。SIGKILL、断电无法执行退出处理，随后可使用 `report` 从已留存的证据生成报告。孤立请求失败也会保留，后续请求恢复不会让最终验收变成成功。

报告区分 `running`、`finished_early`、`failed`、`completed`。提前达到调试轮数上限不会被当成通过完整 6 小时验收。`available/degraded` 是集群状态分布，只有对应健康门槛判定才代表检查失败。P95/P99 按成功请求的最近秩计算；小样本时接近最大值，不代表正式性能基准。

默认仍严格拒绝 `degraded`。每个节点还必须提供 worker 启用且已完成初次检查的证据，`last_check_unix=0` 或缺少这些字段会被拒绝，需使用包含远端修复明细的 CLI。仅显式设置 `DUBI_SOAK_ALLOW_GEO_DEGRADED=1` 时，才容许 Root 多数已确认、节点与卷健康、无本地修复和写入隔离债务的 Geo 待修状态；报告记录该设置。这类运行不能证明所有 Geo 任务都已收敛，也不能代替完整故障域验收。

## 独立物理校验与流量联动

`scripts/vps_soak_watch.py` 在 VPS 上执行有界的物理持有采样。它读取已经通过同版六项故障功能和九入口读写准备检查的私有配置、工具校验清单；使用 `--start-workload` 时独占本机长测锁，并创建一个全新的六小时流量目录。配置、工具或证据不匹配时不会启动流量。它是验收工具，不是节点服务或定时任务，不需要把测试 SSH 密钥放到节点。

每个状态和持有 RPC 的原始 stdout、stderr、退出码或超时信息，都在 JSON 解析、拓扑检查和健康门槛之前写入独立编号文件；`result.json` 保留最后命令、失败阶段、具体节点原始错误及已完成的采样。即使下一次状态恢复正常，也不覆盖触发失败的响应。节点不可达、本地修复/写入隔离、Volume 异常、身份/Store 错配仍然直接失败；没有放宽门槛或用重试抹掉失败。

该工具拥有它启动的流量子进程。物理检查失败、流量请求失败或观察器收到终止信号时，会停止该子进程组，让原流量脚本保留退出报告与现场；超时强制清理会明确写入 `workload-stop.json`。不会停止 DUBI 服务或向磁盘上记录的任意 PID 发信号。正常结束仍等待最后写入满三分钟，再检查首、中、末样本与全局健康。失败后必须使用新目录重新开始，不能接续旧轮次宣称通过。

不使用 `--start-workload` 时只读取既有流量并做物理抽查，报告 `owns_workload=false`；这种事后抽查不能补齐旧轮次的连续采样缺口，也不能证明旧验收已经通过。稳定性长测结果与故障后全量收敛门槛分别记录；长测成功不撤销其他失败记录。

需要在运行中或事后重新汇总时执行：

```bash
python3 scripts/vps_soak_diagnostics.py report /path/to/run
```

## 自动收集各节点日志

在持有测试 SSH 密钥的开发机上执行一次有界的等待采集。密钥留在开发机；不复制到测试节点，不更改 SSH 配置。以下命令只读 VPS 的长测输出、服务状态、进程二进制和 journal，不重启节点或读取 Pack、Repository 凭据、节点私钥：

```bash
python3 scripts/vps_soak_diagnostics.py collect \
  --host 206.206.78.113 \
  --ssh-key ~/.ssh/dubi-vps-test_ed25519 \
  --run-dir /var/tmp/dubi-vps-soak-EXAMPLE \
  --output /tmp/dubi-soak-evidence-EXAMPLE \
  --wait
```

省略 `--wait` 可立即采集已经结束或仍在运行的长测。`--output` 必须不存在；省略时自动选择本机临时目录。

`--wait` 每 30 秒只检查该轮 `run.info`，结束后采集，最长等待 7 小时；它是本次运行的普通进程，不创建定时任务。开发机需保持联网且不休眠。若等待进程中断，可以稍后执行不带 `--wait` 的命令补采；VPS 本身的负载进程独立运行。

采集器优先使用失败状态中的 Root 成员地址，通过开发机 SSH 并发只读访问全部节点。SSH 使用 BatchMode、连接超时、严格 known_hosts 检查；各节点应已由管理员验证并登记主机密钥。访问失败会单独留档并返回非零退出码，其余节点继续采集。

输出目录的 `run/` 保留原始长测文件及汇总；`peers/` 按节点保存日志、服务状态、当前本地修复明细、真实运行进程的二进制 SHA-256 和 CLI version。Store 路径从服务进程参数识别，不要求各节点使用相同目录。`collection.json` 记录采集时间、日志窗口、节点结果和复制错误。

已结束运行的日志窗口为结束前 10 分钟至结束后 30 秒，每节点最多 1500 行；命令输出截断会显式标记。当前进程哈希、版本和补充状态是**采集时刻**的观察，不能追溯证明故障时刻使用的二进制；触发状态和工作负载启停哈希单独留存。节点重装或 journal 已轮转时，应结合这些原始证据判断，不将缺失日志当成未发生故障。

## 专项验证

`scripts/vps_soak_diagnostics_test.py` 使用独立临时目录和构造命令验证失败留档、值校验、退出处理、旧证据保护、报告统计和 SSH 部分失败，不访问节点实际 Store。`internal/command/status_test.go` 覆盖认证状态查询和远端问题明细的保留、截断以及异常响应拒绝。
