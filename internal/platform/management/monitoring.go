package management

import (
	"context"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/telemetry"
	"math"
	"sort"
	"sync"
	"time"
)

type MonitorProbe struct {
	Observation Observation
	Telemetry   telemetry.View
}
type MonitorSource interface {
	Probe(context.Context, Member, int64, int64) (MonitorProbe, error)
}
type MonitoringService struct {
	Directory *Service
	Source    MonitorSource
	Root      *rootstate.State
	Commit    func(context.Context, rootstate.Command) error
	slots     chan struct{}
}

func NewMonitoring(d *Service, s MonitorSource, r *rootstate.State, commit func(context.Context, rootstate.Command) error) *MonitoringService {
	return &MonitoringService{Directory: d, Source: s, Root: r, Commit: commit, slots: make(chan struct{}, 2)}
}
func (s *MonitoringService) Collect(ctx context.Context, region, node string, seconds int64) (v1.Monitoring, []MonitorProbe, error) {
	out := v1.Monitoring{Nodes: []v1.MonitoringNode{}, Points: []v1.MonitoringPoint{}, EndUnix: time.Now().Unix(), WindowSeconds: seconds, StepSeconds: int64(math.Ceil(float64(seconds)/120/10)) * 10, SampleSeconds: 10, RetentionSeconds: 21600}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return out, nil, tenantError(429, "busy", "监控采集繁忙，请稍后重试。")
	}
	ctx, cancel := context.WithTimeout(ctx, 9*time.Second)
	defer cancel()
	d, err := s.Directory.Directory(ctx)
	if err != nil {
		return out, nil, err
	}
	members := []Member{}
	for _, m := range d.Members {
		if (region == "" || m.RegionID == region) && (node == "" || m.NodeID == node) {
			members = append(members, m)
		}
	}
	if len(members) == 0 {
		return out, nil, tenantError(404, "not_found", "所选区域或节点不存在。")
	}
	if len(members) > 256 {
		return out, nil, tenantError(400, "scope_too_large", "请按区域筛选，一次最多观测 256 个节点。")
	}
	probes := make([]MonitorProbe, len(members))
	out.Nodes = make([]v1.MonitoringNode, len(members))
	errs := make([]error, len(members))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(8, len(members)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				m := members[i]
				call, cancel := context.WithTimeout(ctx, 3*time.Second)
				p, e := s.Source.Probe(call, m, out.EndUnix, seconds)
				cancel()
				probes[i], errs[i] = p, e
				n := v1.MonitoringNode{ManagedNode: v1.ManagedNode{CreatedIndex: m.CreatedIndex, NodeID: m.NodeID, Name: m.Name, RegionID: m.RegionID, Membership: "member", OperationalHealth: v1.OperationalHealth{Status: "unavailable", Reasons: []v1.ObservationError{{NodeID: m.NodeID, Code: "observation_unavailable", Message: "未能取得节点最新观测。"}}}}}
				if e == nil {
					o := p.Observation
					n.OperationalHealth = o.Health
					n.Observation = v1.Observation{ObservedUnix: &o.ObservedUnix, Complete: o.Complete}
					n.Capacity = o.Capacity
					count := uint32(len(o.Volumes))
					n.VolumeCount = &count
					n.Maintenance = &v1.NodeMaintenance{CreatedIndex: m.CreatedIndex, NodeID: m.NodeID, RegionID: m.RegionID, Observation: n.Observation, Items: o.Maintenance}
					if sample := p.Telemetry.Latest; sample != nil && sample.Unix <= out.EndUnix+2 && sample.Unix >= out.EndUnix-30 && sample.Seconds > 0 {
						read, write := float64(sample.Read.Count())/sample.Seconds, float64(sample.Write.Count())/sample.Seconds
						n.Metrics = v1.MonitoringMetrics{CPUCores: sample.CPUCores, MemoryTotal: sample.MemoryTotal, CPU: sample.CPU, RSS: sample.RSS, ReadRate: &read, WriteRate: &write, ReadP95: sample.Read.P95(), WriteP95: sample.Write.P95(), Ready: &sample.Ready, SampleUnix: &sample.Unix}
						if !sample.Ready {
							n.Status = "degraded"
							n.Reasons = append(n.Reasons, v1.ObservationError{Code: "runtime_not_ready", Message: "节点仍在启动或恢复。"})
						}
					} else {
						n.Complete = false
						n.Status = "unknown"
						n.Reasons = append(n.Reasons, v1.ObservationError{Code: "metrics_unavailable", Message: "采样尚未就绪或已过期。"})
					}
				}
				out.Nodes[i] = n
			}
		}()
	}
	for i := range members {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	type bucket struct {
		point       v1.MonitoringPoint
		read, write telemetry.Histogram
	}
	buckets := map[int64]*bucket{}
	for i, p := range probes {
		if errs[i] != nil {
			continue
		}
		for _, v := range p.Telemetry.Points {
			if v.Seconds <= 0 || v.Unix > out.EndUnix || v.Unix <= out.EndUnix-seconds {
				continue
			}
			b := buckets[v.Unix]
			if b == nil {
				b = &bucket{point: v1.MonitoringPoint{Unix: v.Unix}}
				buckets[v.Unix] = b
			}
			b.point.ObservedNodes++
			b.point.ReadRate += float64(v.Read.Count()) / v.Seconds
			b.point.WriteRate += float64(v.Write.Count()) / v.Seconds
			b.read.Add(v.Read)
			b.write.Add(v.Write)
		}
	}
	for _, b := range buckets {
		b.point.ReadP95 = b.read.P95()
		b.point.WriteP95 = b.write.P95()
		out.Points = append(out.Points, b.point)
	}
	sort.Slice(out.Points, func(i, j int) bool { return out.Points[i].Unix < out.Points[j].Unix })
	ns := make([]v1.ManagedNode, len(out.Nodes))
	for i, n := range out.Nodes {
		ns[i] = n.ManagedNode
	}
	out.Summary = Summary(ns)
	return out, probes, nil
}
func (s *MonitoringService) Reports(ctx context.Context) ([]rootstate.DiagnosticRecord, error) {
	if _, err := s.Directory.Directory(ctx); err != nil {
		return nil, err
	}
	return s.Root.Diagnostics()
}
func (s *MonitoringService) Run(ctx context.Context, id, region, node string, recheck func() error) (rootstate.DiagnosticRecord, error) {
	if rootstate.ValidateRequestID(id) != nil || len(id) > 80 {
		return rootstate.DiagnosticRecord{}, tenantError(400, "invalid_request", "请求标识无效。")
	}
	existing, err := s.Reports(ctx)
	if err != nil {
		return rootstate.DiagnosticRecord{}, err
	}
	for _, d := range existing {
		if d.ID == id {
			if d.RegionID != region || d.NodeID != node {
				return d, tenantError(409, "request_conflict", "此请求已用于其他检查范围。")
			}
			return d, nil
		}
	}
	// A pruned RequestID must not run again and pretend to persist a new record.
	if _, found, err := s.Root.Result(id); err != nil {
		return rootstate.DiagnosticRecord{}, err
	} else if found {
		return rootstate.DiagnosticRecord{}, tenantError(409, "request_used", "此请求已使用，请刷新列表。")
	}
	data, probes, err := s.Collect(ctx, region, node, 900)
	if err != nil {
		return rootstate.DiagnosticRecord{}, err
	}
	if len(data.Nodes) > 64 {
		return rootstate.DiagnosticRecord{}, tenantError(400, "scope_too_large", "一次检查最多 64 个节点，请缩小范围。")
	}
	d := rootstate.DiagnosticRecord{ID: id, RegionID: region, NodeID: node, CreatedUnix: time.Now().Unix(), Nodes: []rootstate.DiagnosticNode{}}
	for i, n := range data.Nodes {
		checks := []rootstate.DiagnosticCheck{{Kind: "reachability", State: "unknown", Message: "未取得节点响应。"}, {Kind: "readiness", State: "unknown", Message: "未取得 Runtime 就绪状态。"}, {Kind: "volumes", State: "unknown", Message: "未取得 Volume 状态。"}, {Kind: "maintenance", State: "unknown", Message: "未取得后台维护状态。"}}
		if n.ObservedUnix != nil {
			checks[0] = rootstate.DiagnosticCheck{Kind: "reachability", State: "passed", Message: "节点内部管理通道可达。"}
			if n.Metrics.Ready != nil {
				checks[1].State = "passed"
				checks[1].Message = "Runtime 已完成启动。"
				if !*n.Metrics.Ready {
					checks[1].State = "warning"
					checks[1].Message = "Runtime 尚未就绪。"
				}
			}
			vols := probes[i].Observation.Volumes
			if len(vols) > 0 {
				checks[2].State = "passed"
				checks[2].Message = "已观测 Volume 健康且未隔离写入。"
				for _, v := range vols {
					if v.Health != "healthy" && v.Health != "removed" || v.WriteIsolated {
						checks[2].State = "warning"
						checks[2].Message = "存在不健康或写入隔离的 Volume。"
					}
				}
			}
			if !probes[i].Observation.Complete && checks[2].State == "passed" {
				checks[2].State = "unknown"
				checks[2].Message = "Volume 观测不完整。"
			}
			if n.Maintenance != nil && len(n.Maintenance.Items) > 0 {
				checks[3].State = "passed"
				checks[3].Message = "后台维护无已知异常。"
				for _, a := range n.Maintenance.Items {
					if !a.Supported || a.Phase == "unknown" || a.Phase == "disabled" || a.Phase == "" {
						if checks[3].State != "warning" {
							checks[3].State = "unknown"
							checks[3].Message = "部分维护状态尚未提供。"
						}
					}
					if len(a.Errors) > 0 || a.Pending != nil && *a.Pending > 0 {
						checks[3].State = "warning"
						checks[3].Message = "后台维护仍有待处理数据或异常。"
					}
				}
			}
		}
		d.Nodes = append(d.Nodes, rootstate.DiagnosticNode{NodeID: n.NodeID, Name: n.Name, RegionID: n.RegionID, Checks: checks})
	}
	if err := recheck(); err != nil {
		return d, err
	}
	err = s.Commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: id, Type: rootstate.SaveDiagnostic, Diagnostic: &d})
	if err != nil {
		return d, tenantError(503, "result_unknown", "检查结果尚未确认，请保留请求标识并刷新列表；重试同一请求不会重复创建。")
	}
	all, err := s.Root.Diagnostics()
	if err != nil {
		return d, err
	}
	for _, v := range all {
		if v.ID == id {
			return v, nil
		}
	}
	return d, errors.New("diagnostic result missing")
}
