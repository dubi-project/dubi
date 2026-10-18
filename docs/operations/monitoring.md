# DUBI 监控

DUBI 的监控接口是只读观测面，不参与 Raft、数据写入、节点删除或 Volume 删除。当前接口复用可选的 `--api-listen` HTTPS listener：

```text
GET /healthz   # 进程存活
GET /readyz    # Node 已完成启动和本地 Group 恢复
GET /metrics   # Prometheus 文本格式
```

## 启用

开发或单机检查：

```bash
dubi server --api-listen 127.0.0.1:7444
curl --cacert <cluster-ca.pem> https://127.0.0.1:7444/healthz
curl --cacert <cluster-ca.pem> https://127.0.0.1:7444/readyz
curl --cacert <cluster-ca.pem> https://127.0.0.1:7444/metrics
```

跨机器采集时，`--api-listen` 只能绑定内网地址，并通过防火墙限制 Prometheus 来源。该端口不要直接暴露到公网；`/metrics` 包含节点、Root 和 Volume 状态。

生产 Prometheus 配置示例见 [`deploy/prometheus/prometheus.yml.example`](../../deploy/prometheus/prometheus.yml.example)，告警规则见 [`dubi.rules.yml`](../../deploy/prometheus/dubi.rules.yml)。开发机的 Docker 快速验证可参考 [`prometheus.dev.yml.example`](../../deploy/prometheus/prometheus.dev.yml.example)；它使用 `insecure_skip_verify`，只允许本地临时测试，生产环境必须使用集群 CA 校验证书。

9 台测试 VPS 的 SSH 隧道验收配置见 [`prometheus.vps-acceptance.yml.example`](../../deploy/prometheus/prometheus.vps-acceptance.yml.example)。该配置只适合一次性验收，不是生产部署配置。

## 主要指标

- `dubi_node_up`、`dubi_node_ready`
- `dubi_root_observed`、`dubi_root_applied_index`、`dubi_root_commit_index`
- `dubi_root_term`、`dubi_root_leader_id`、`dubi_root_voters`
- `dubi_volume_health`、`dubi_volume_capacity_bytes`、`dubi_volume_available_bytes`
- `dubi_disk_pressured`、`dubi_disk_available_bytes`、`dubi_disk_reserve_bytes`
- `dubi_node_repair_debt`、`dubi_node_repair_last_check_unix`

指标是本地观察值。`dubi_node_ready=1` 不代表整个集群有 Root majority；集群级判断必须结合 `dubi_root_*`、节点可达性和 `dubi status`。

## 建议采集策略

- `scrape_interval`: 15 秒。
- `scrape_timeout`: 5 秒。
- 保留至少 14 天，以便分析节点故障、磁盘压力和迁移趋势。
- Prometheus 只读访问；禁止告警系统直接调用 DUBI 的破坏性管理命令。
- 每个 scrape target 添加 `cluster`、`region`、`node_name`、`node_id` 标签（可用 Prometheus 静态标签或 relabel 配置完成）。
