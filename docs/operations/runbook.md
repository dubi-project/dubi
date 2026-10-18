# DUBI 基础运维手册

## 通用检查

```bash
dubi status
dubi doctor
dubi volume list
journalctl -u dubi --since '15 minutes ago'
```

不要因为单次超时就删除 Node。先确认故障是否持续，以及 Root quorum 和区域多数是否仍然存在。

## Node 故障

1. 检查 `dubi status` 中 Node 是否不可达。
2. 尝试登录原机器，检查进程、磁盘和网络。
3. 原机器可恢复：启动 Node，等待自动追赶，直到 repair debt 清零。
4. 原机器不可恢复：人工执行 `dubi node remove --name <name>`。
5. 新机器使用原 Region 名称执行 `dubi join`。
6. 确认新 Node 已加入并完成再平衡。

## Volume 故障

1. 使用 `dubi volume list` 判断 `healthy`、`missing` 或 `invalid`。
2. 可读 Volume：先 `dubi volume drain <volume-id>`，等待迁移完成，再 `dubi volume remove <volume-id>`。
3. 不可读 Volume：确认数据已由其他 Node/Region 持有后，再执行人工删除。
4. 添加新 Volume，观察容量、迁移进度和 repair debt。

## Region 故障

1. 不执行自动 Region 删除或 Region rebuild。
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
