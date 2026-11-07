package management

import (
	"context"
	"errors"
	"fmt"
	v1 "github.com/dubi-project/dubi/api/v1"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

type Notifications struct {
	ObserveAllowed func() bool
	Root           *rootstate.State
	Barrier        func(context.Context) (uint64, error)
	Commit         func(context.Context, rootstate.Command) error
	Directory      *Service
	// Detector is called serially by the local Root leader only.
	misses map[string]int
}
type NotificationItem struct {
	rootstate.Notification
	Read bool `json:"read"`
}
type NotificationList struct {
	Items        []NotificationItem `json:"items"`
	Unread       int                `json:"unread"`
	ThroughIndex uint64             `json:"through_index"`
	Retention    int                `json:"retention"`
}

func (s *Notifications) Admin(ctx context.Context, id string) (string, error) {
	if _, e := s.Barrier(ctx); e != nil {
		return "", e
	}
	c, ok, e := s.Root.AccessCredential(id)
	if e != nil {
		return "", e
	}
	if ok && c.Role == "session" && c.AccountID == "" {
		c, ok, e = s.Root.AccessCredential(c.ParentID)
	}
	now := time.Now().Unix()
	if e != nil {
		return "", e
	}
	allowed := c.Role == "admin"
	if ok && c.Role == "session" && c.AccountID != "" {
		a, found, err := s.Root.AdminAccount(c.AccountID)
		if err != nil {
			return "", err
		}
		allowed = found && !a.Disabled && a.AuthVersion == c.AccountVersion
	}
	if !ok || !allowed || c.RevokedIndex != 0 || now < c.CreatedUnix || (c.ExpiresUnix != 0 && now >= c.ExpiresUnix) {
		return "", tenantError(403, "forbidden", "管理凭证已失效。")
	}
	return c.CredentialID, nil
}
func (s *Notifications) List(ctx context.Context, id string) (NotificationList, error) {
	out := NotificationList{Items: []NotificationItem{}, Retention: rootstate.MaxNotifications}
	admin, e := s.Admin(ctx, id)
	if e != nil {
		return out, e
	}
	feed, e := s.Root.NotificationFeed()
	if e != nil {
		return out, e
	}
	reads, e := s.Root.NotificationReads(admin)
	if e != nil {
		return out, e
	}
	set := map[string]bool{}
	for _, id := range reads.IDs {
		set[id] = true
	}
	out.ThroughIndex = feed.Revision
	for _, n := range feed.Items {
		read := set[n.ID]
		if !read {
			out.Unread++
		}
		out.Items = append(out.Items, NotificationItem{n, read})
	}
	return out, nil
}
func (s *Notifications) Mark(ctx context.Context, identity, request string, m rootstate.NotificationMark) error {
	admin, e := s.Admin(ctx, identity)
	if e != nil {
		return e
	}
	m.CredentialID = admin
	if rootstate.ValidateRequestID(request) != nil || m.Validate() != nil {
		return tenantError(400, "invalid_request", "消息操作参数无效。")
	}
	return (&Tenants{Root: s.Root, Commit: s.Commit}).commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.MarkNotifications, RequestID: request, NotificationMark: &m})
}
func event(kind, level, title, summary, source, region, id, target string) *rootstate.Notification {
	return &rootstate.Notification{Kind: kind, Level: level, Title: title, Summary: summary, Detail: summary, Source: source, Region: region, ResourceID: id, Target: target, Advice: "请前往对应管理页面查看当前状态与详细信息。"}
}
func (s *Notifications) save(ctx context.Context, feed rootstate.NotificationFeed, scope string, observations []rootstate.NotificationObservation) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.ObserveAllowed != nil && !s.ObserveAllowed() {
		return context.Canceled
	}
	next := map[string]string{}
	for _, o := range observations {
		next[o.Key] = o.State
	}
	if old, ok := feed.Sources[scope]; ok && maps.Equal(old, next) {
		return nil
	}
	for i := range observations {
		o := &observations[i]
		old, found := feed.Sources[scope][o.Key]
		_, initialized := feed.Sources[scope]
		if old == o.State || (!found && o.SuppressInitial) || (!initialized && scope == "backup") {
			o.Event = nil
		}
	}
	sort.Slice(observations, func(i, j int) bool { return observations[i].Key < observations[j].Key })
	u := rootstate.NotificationUpdate{Revision: feed.Revision, Scope: scope, ObservedUnix: time.Now().Unix(), Observations: observations}
	if e := u.Validate(); e != nil {
		return e
	}
	// A scope/revision pair identifies the same transition proposal. The durable
	// checkpoint, not transient process memory, suppresses duplicate events.
	request := fmt.Sprintf("notice-%s-%x-%x", scope, feed.Revision, u.ObservedUnix)
	return s.Commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.UpdateNotifications, RequestID: request, NotificationUpdate: &u})
}
func threshold(value, total uint64, previous string, high, low float64) string {
	if total == 0 {
		return previous
	}
	ratio := float64(value) / float64(total) * 100
	if ratio >= high || previous == "warning" && ratio >= low {
		return "warning"
	}
	return "healthy"
}
func observed(key, state string, n *rootstate.Notification) rootstate.NotificationObservation {
	return rootstate.NotificationObservation{Key: key, State: state, Event: n, SuppressInitial: state == "healthy"}
}

// Scan runs independently of HTTP readers. Only transitions enter Root; values
// that fluctuate within a state do not cause repeated messages or consensus writes.
func (s *Notifications) Reset() { s.misses = nil }
func (s *Notifications) Scan(ctx context.Context) error {
	if _, e := s.Barrier(ctx); e != nil {
		return e
	}
	if e := s.catalog(ctx); e != nil {
		return e
	}
	feed, e := s.Root.NotificationFeed()
	if e != nil {
		return e
	}
	d, e := s.Directory.Directory(ctx)
	if e != nil {
		return e
	}
	if len(d.Members) > 256 {
		return errors.New("notification observation scope exceeds 256 nodes")
	}
	ns := s.Directory.Nodes(ctx, d.Members)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.misses == nil {
		s.misses = map[string]int{}
	}
	nodeObs := []rootstate.NotificationObservation{}
	volumeObs := []rootstate.NotificationObservation{}
	members := map[string]bool{}
	for i, n := range ns {
		members[n.NodeID] = true
		name := n.Name
		if name == "" {
			name = n.NodeID
		}
		prior := feed.Sources["node"][n.NodeID]
		state := prior
		if n.Status == "unknown" {
			s.misses[n.NodeID] = 0
		}
		if n.Status == "unavailable" {
			s.misses[n.NodeID]++
			if s.misses[n.NodeID] >= 2 {
				state = "unavailable"
			}
		} else if n.Status != "unknown" && !n.Stale && n.ObservedUnix != nil {
			s.misses[n.NodeID] = 0
			state = "healthy"
		}
		if state != "" {
			level, title, detail := "success", name+" 已恢复连接", "节点已重新响应管理观测；数据恢复进展请查看节点详情。"
			if state == "unavailable" {
				level, title, detail = "error", name+" 暂时不可达", "连续两轮未取得节点响应。成员身份保留，请检查节点进程与网络。"
			}
			item := observed(n.NodeID, state, event("node", level, title, detail, name, n.RegionID, n.NodeID, "globe"))
			if _, initialized := feed.Sources["node"]; initialized && prior == "" && state == "healthy" {
				item.SuppressInitial = false
				item.Event = event("node", "info", name+" 已加入集群", "新节点已加入集群并取得响应，数据平衡进展可在节点详情查看。", name, n.RegionID, n.NodeID, "globe")
			}
			nodeObs = append(nodeObs, item)
		}
		var vols []v1.ManagedVolume
		// Keep checkpoints while a node cannot be observed. Never mistake missing
		// samples for recovery, and don't re-notify the same issue after reconnect.
		if n.Stale || n.Status == "unavailable" || n.Status == "unknown" || n.ObservedUnix == nil {
			for key, state := range feed.Sources["volume"] {
				if strings.HasPrefix(key, n.NodeID+"/") {
					volumeObs = append(volumeObs, observed(key, state, nil))
				}
			}
			continue
		}
		vols, e = s.Directory.Volumes(ctx, d.Members[i])
		if e != nil {
			return e
		}
		for _, v := range vols {
			if v.State == "removed" {
				continue
			}
			key := n.NodeID + "/" + v.VolumeID
			prior := feed.Sources["volume"][key]
			state := "healthy"
			if v.WriteIsolated || slices.Contains([]string{"failed", "unavailable", "missing", "invalid", "error"}, v.Health) {
				state = "failed"
			} else if v.Capacity != nil && v.Capacity.FilesystemTotalBytes > 0 {
				total := v.Capacity.FilesystemTotalBytes
				used := total - min(total, v.Capacity.FilesystemAvailableBytes)
				state = threshold(used, total, prior, 85, 80)
			} else {
				state = prior
			}
			if state == "" {
				continue
			}
			level, title, detail := "success", name+" 的 Volume 已恢复", "Volume 已恢复可写且容量低于提醒阈值。"
			if state == "warning" {
				level, title, detail = "warning", name+" 存储空间接近上限", "Volume 所在文件系统使用率达到 85% 提醒阈值；低于 80% 后解除提醒。"
			}
			if state == "failed" {
				level, title, detail = "error", name+" 的 Volume 不可写", "数据卷不可用或写入已隔离，请检查磁盘及 Volume 状态。"
			}
			volumeObs = append(volumeObs, observed(key, state, event("volume", level, title, detail, name+" · "+v.VolumeID, n.RegionID, v.VolumeID, "volume")))
		}
	}
	for id := range s.misses {
		if !members[id] {
			delete(s.misses, id)
		}
	}
	if e = s.save(ctx, feed, "node", nodeObs); e != nil {
		return e
	}
	feed, e = s.Root.NotificationFeed()
	if e != nil {
		return e
	}
	return s.save(ctx, feed, "volume", volumeObs)
}
func (s *Notifications) catalog(ctx context.Context) error {
	feed, e := s.Root.NotificationFeed()
	if e != nil {
		return e
	}
	tenants, credentials, e := s.Root.TenantDirectory()
	if e != nil {
		return e
	}
	obs := []rootstate.NotificationObservation{}
	for _, t := range tenants {
		if !t.Accounting || t.StorageLimit == 0 {
			continue
		}
		u, e := s.Root.TenantUsage(t.TenantID)
		if e != nil {
			return e
		}
		state := threshold(u.Used, t.StorageLimit, feed.Sources["tenant"][t.TenantID], 80, 75)
		name := t.Name
		if name == "" {
			name = t.TenantID
		}
		level, title, detail := "success", "租户 "+name+" 用量已回落", "租户存储用量已低于配额的 75%。"
		if state == "warning" {
			level, title, detail = "warning", "租户 "+name+" 用量接近配额", "租户存储用量达到配额的 80% 提醒阈值，请检查用量或调整配额。"
		}
		obs = append(obs, observed(t.TenantID, state, event("tenant", level, title, detail, name, "集群", t.TenantID, "tenant")))
	}
	if e = s.save(ctx, feed, "tenant", obs); e != nil {
		return e
	}
	feed, e = s.Root.NotificationFeed()
	if e != nil {
		return e
	}
	obs = []rootstate.NotificationObservation{}
	addCredential := func(id, name string, expires int64, revoked uint64) {
		if revoked != 0 {
			return
		}
		state := "healthy"
		remaining := expires - time.Now().Unix()
		level, title, detail := "warning", "访问凭证即将到期", "访问凭证将在 7 天内到期，请提前更新使用方。"
		if expires != 0 && remaining <= 0 {
			state = "expired"
			level, title, detail = "error", "访问凭证已到期", "访问凭证已到期，使用此凭证的请求将被拒绝。"
		} else if expires != 0 && remaining <= 7*86400 {
			state = "expiring"
		}
		if name == "" {
			name = id
		}
		item := observed(id, state, event("credential", level, title, detail, name, "集群", id, "key"))
		if _, initialized := feed.Sources["credential"]; initialized && feed.Sources["credential"][id] == "" && state == "healthy" {
			item.SuppressInitial = false
			item.Event = event("credential", "info", "访问凭证已创建", "新的访问凭证已生效，请确认该操作符合预期。", name, "集群", id, "key")
		}
		obs = append(obs, item)
	}
	access, e := s.Root.AccessCredentials()
	if e != nil {
		return e
	}
	for _, c := range access {
		if c.Role != "session" {
			addCredential(c.CredentialID, c.Name, c.ExpiresUnix, c.RevokedIndex)
		}
	}
	for _, c := range credentials {
		addCredential(c.CredentialID, c.Name, c.ExpiresUnix, c.RevokedIndex)
	}
	if e = s.save(ctx, feed, "credential", obs); e != nil {
		return e
	}
	feed, e = s.Root.NotificationFeed()
	if e != nil {
		return e
	}
	obs = []rootstate.NotificationObservation{}
	jobs, e := s.Root.BackupJobs()
	if e != nil {
		return e
	}
	for _, j := range jobs {
		var n *rootstate.Notification
		if j.State == rootstate.BackupJobCompleted {
			n = event("backup", "success", "集群备份已完成", "所有参与节点已完成备份，可在仓库与备份页面查看恢复点。", j.Repository, "集群", j.JobID, "archive")
		}
		if j.State == rootstate.BackupJobFailed {
			n = event("backup", "error", "集群备份失败", "有节点未能完成本次备份，请查看备份记录中的失败原因。", j.Repository, "集群", j.JobID, "archive")
		}
		obs = append(obs, observed(j.JobID, j.State, n))
	}
	return s.save(ctx, feed, "backup", obs)
}
