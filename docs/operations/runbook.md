# DUBI 基础运维手册

## 通用检查

```bash
dubi status
dubi doctor
dubi volume list
journalctl -u dubi --since '15 minutes ago'
```

不要因为单次超时就删除 Node。先确认故障是否持续，以及 Root quorum 和区域多数是否仍然存在。

开发用 VPS 的可重复故障演练和逐节点部署见[验收工具说明](vps-fault-acceptance.md)，长测报告及现场采集见[长测诊断](vps-soak-diagnostics.md)。故障演练必须与固定版本长测分开执行。

## Repair 扫描进度

`dubi status` 的 `local_repair.scan_progress` 和各节点 `repair_observations[].scan_progress` 展示本机 Group 的扫描观察；`dubi doctor` 默认文本也展示本机进度，无需额外参数：

- `phase`：`pending` 等待下一页；`waiting_root` / `waiting_owner` 等待一致性屏障；`repairing` 检查或补齐本机数据；`confirming` 检查或重确认本地多数；`idle` 本轮遍历结束，等待下一轮。
- `scanned`：本轮已经处理的引用数。每页使用自己的 Owner 切点，持续写入会改变总量，因此不提供完成百分比，也不代表集群总记录数。
- `unresolved`：本轮累计未解决项；`needs_clean_sweep`：已知记录欠账尚未经过完整干净扫描解除。即使新一轮 `unresolved=0`，也不能提前清除后者；重启仍保留。
- `started_unix`、`last_progress_unix`、`last_clean_unix`：本轮开始、最近一页成功落盘、最近完整干净扫描的 UTC Unix 秒。零表示尚无记录；doctor 将零显示为 `never`。成员代际或 fence 改变后重扫，不沿用旧集合的干净时间。

响应至多列出 64 个 Group，`total_scan_groups` 保存完整数量；doctor 在截断时提示。旧服务没有这项能力时，`scan_progress_supported=false`，doctor 显示进度不可用，不把缺失字段解释为已经扫完。未满三个节点、尚未确认或处于成员切换中的 Group 不进入本地多数扫描；跨区域任务另看 `geo_metadata_debt` / `geo_record_debt`。

这些字段只解释后台本地扫描的推进情况；`idle`、最近干净扫描以及零待办都不能代替当前 Pack 字节和跨 Region 多数的实际读验。固定版本长测期间不更新正在测试的进程。

## 持久化启动

生产环境不要依赖 `systemd-run` 创建的 transient unit。使用仓库中的 [`deploy/systemd/dubi.service`](../../deploy/systemd/dubi.service) 和 [`dubi.env.example`](../../deploy/systemd/dubi.env.example)，将二进制安装到 `/usr/local/bin/dubi`，为本机填写 `/etc/dubi/dubi.env`，然后执行：

```bash
install -d -m 0750 /etc/dubi
install -m 0644 deploy/systemd/dubi.service /etc/systemd/system/dubi.service
systemctl daemon-reload
systemctl enable --now dubi.service
systemctl status dubi.service
```

`DUBI_API_LISTEN` 默认绑定回环地址。跨机器 Prometheus 采集时，改为私网地址并用防火墙限制来源；不要直接暴露公网。

## Node 故障

1. 检查 `dubi status` 中 Node 是否不可达。
2. 如果告警来自 `DubiNodeDown`，先确认是监控链路断开还是节点进程确实停止。
3. 尝试登录原机器，检查进程、磁盘和网络。
4. 原机器可恢复：启动 Node，等待自动追赶，直到 repair debt 清零。
5. 原机器不可恢复：人工执行 `dubi node remove --name <name>`。
6. 新机器使用原 Region 名称执行 `dubi join`。
7. 确认新 Node 已加入并完成再平衡。

## Volume 故障

1. 使用 `dubi volume list` 判断 `healthy`、`missing` 或 `invalid`。
2. 可读 Volume：先 `dubi volume drain <volume-id>`，等待迁移完成，再 `dubi volume remove <volume-id>`。
3. 不可读 Volume：确认数据已由其他 Node/Region 持有后，直接执行人工 `dubi volume remove <volume-id>`；系统会把 `missing/invalid` 卷先登记为 draining，再完成删除，不要求故障介质仍可打开。
4. 添加新 Volume，观察容量、迁移进度和 repair debt。

正常容量均衡无需新命令：节点比较不同健康 Active 磁盘的可用比例，差距超过 10 个百分点时，按完整 sealed Pack 选择搬迁，搬迁量不超过达到均衡所需空间的一半，并保留磁盘安全空间。每次检查最多 256 条索引，游标可续扫；任务仍使用持久 Volume migration 账本，重启后继续写入原目标卷。后台每步最多搬一条记录，已推进的页面以 100 ms 继续，空闲或失败时回到 5 秒间隔；只有目标写入、读验和索引 Sync 成功才切换位置，复制结束后保留持久回收阶段，旧 Pack 按完整索引成功回收后才清除任务；重启或回收失败会续跑这一阶段。源卷保持 Active；人工 Drain 可以接管自动均衡任务。

同一文件系统的多个目录不触发容量搬迁，也不增加容量或耐久票数。新的完整容量扫描至多每分钟启动一次，尚未完成的发现页继续有界推进；有备份 Pack 文件句柄尚未释放时不启动新的容量搬迁，避免把尚未释放的空间误当成新的容量差。任务目标损坏、被 Drain 或移除时停止这项可选搬迁，已经验证并提交的位置保留，缺失字节交给普通 Repair。空间不足不会触发自动删除 Node/Volume，也不会降低多数门槛。

`status.local_repair.volume_migration` 的 `target_volume_id` 和 `source_pack_id` 非空表示节点内卷容量均衡；未发生错误的正常均衡不把集群标为 degraded。`last_error` 非空仍会报告降级。`writable_bytes` 和 `capacity_supported` 是本节点单次写入可用空间的观察；跨节点修复会在当前放置集合中优先选择有空间的目标，最终仍由目标磁盘预约和真实多数复验决定成功。

至少三节点的稳定 Region 还会自动执行跨节点容量均衡。默认空闲检查间隔约 30 秒，按最多 64 条索引发现候选；不同节点可用比例差超过 10 个百分点时，才按保守容量差预算尝试一条记录。源先要求其它独立节点的当前 Region 严格多数持久保留并校验真实字节，再重新确认放置代际和备份状态，最后同步删除自己的位置索引。进度、单调交接序号和 Pack 回收水位持久保存；源重启后继续释放保留或回收。任务共用节点维护门禁和 IO 限速。

保留承诺不按心跳超时释放，故障源先尝试原身份恢复；管理员明确 Remove/Fence 后才可清理该源的遗留承诺。目标协议不支持、保留失败、代际变化或活动集群备份均禁止删除源持有。后台 Repair 每轮复验卸载提示对应的真实其它节点多数，失去多数或放置改变时恢复正常补齐；前台按需缓存可以重新落回本地。旧 Pack 每分钟至多触发一次回收，在用备份句柄未释放时延后。该机制不自动删节点/卷，不更改 Region/Range 放置，不把磁盘或目录当作独立节点票数。

备份在同一个 records 锁下捕获索引和 sealed Pack 文件句柄，随后上传不占 Store 锁。压缩可以移除旧路径，已捕获的句柄仍可读取原始字节；所有句柄关闭后才释放这些文件的实际磁盘空间。不要手工关闭、移动或删除 Pack 文件来干预这个过程。

## Region 故障

1. 不执行自动 Region 删除，也不启动专用 Region 重建流程。
2. 先尝试恢复原 Region 的机器。
3. 无法恢复时，逐个删除原 Node。
4. 新 Node 继续使用原 Region 名称加入。
5. 等待跨 Region 数据追赶和集群恢复。

## Root quorum 丢失

- 暂停会改变拓扑的操作。
- 不强制写入、不手工修改 Raft ID、不删除剩余 Root 成员。
- 优先恢复原来的 Root 节点或网络连接。
- quorum 恢复后，再继续节点、Volume 或区域管理操作。

## 磁盘压力

1. 查看 `dubi volume list` 和 `dubi status`。
2. 停止非必要写入和大规模维护任务。
3. 添加 Volume 或清理已确认的 Pack 垃圾。
4. 不直接删除未知文件，不手工修改 Pack 或 Pebble 文件。

## 恢复确认

恢复完成必须确认：

```text
cluster = available
Root quorum = true
所有预期 Node reachable
repair debt = 0
Volume 没有 missing/invalid
```
