// Package management provides bounded, read-only Runtime observations. HTTP and
// CLI adapters can share it; it neither authorizes callers nor changes topology.
package management

import (
	"context"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"sort"
	"sync"
	"time"
)

var ErrNotFound = errors.New("management resource not found")
var ErrUnavailable = errors.New("management observation unavailable")

type Member struct {
	CreatedIndex, RegionCreatedIndex uint64
	NodeID, Name, RegionID, Address  string
	RaftID                           uint64
}
type Directory struct {
	ClusterID string
	Members   []Member
}
type Observation struct {
	Health       v1.OperationalHealth
	Capacity     *v1.FilesystemCapacity
	Volumes      []v1.ManagedVolume
	Maintenance  []v1.MaintenanceActivity
	ObservedUnix int64
	Complete     bool
}
type Source interface {
	// Must confirm the membership cut before returning. Unreachable members stay listed.
	Directory(context.Context) (Directory, error)
	Observe(context.Context, Member) (Observation, error)
}
type cached struct {
	value   Observation
	err     error
	checked time.Time
	flight  chan struct{}
	weight  int
}
type Service struct {
	source     Source
	mu         sync.Mutex
	cache      map[string]*cached
	slots      chan struct{}
	cacheBytes int
}

func New(source Source) *Service {
	return &Service{source: source, cache: make(map[string]*cached), slots: make(chan struct{}, 8)}
}
func (s *Service) Directory(ctx context.Context) (Directory, error) {
	d, err := s.source.Directory(ctx)
	if err != nil {
		return Directory{}, ErrUnavailable
	}
	if len(d.Members) > 4096 {
		return Directory{}, ErrUnavailable
	}
	sort.Slice(d.Members, func(i, j int) bool {
		return CreationKey(d.Members[i].CreatedIndex, d.Members[i].NodeID) < CreationKey(d.Members[j].CreatedIndex, d.Members[j].NodeID)
	})
	current := make(map[string]bool, len(d.Members))
	for _, m := range d.Members {
		if m.NodeID == "" || current[m.NodeID] {
			return Directory{}, ErrUnavailable
		}
		current[m.NodeID] = true
	}
	s.mu.Lock()
	for id, c := range s.cache {
		if !current[id] && c.flight == nil {
			s.cacheBytes -= c.weight
			delete(s.cache, id)
		}
	}
	s.mu.Unlock()
	return d, nil
}

// Node observations are immutable once published. A canceled initiating request
// releases its probes; concurrent waiters may retry after cancellation.
func (s *Service) observe(ctx context.Context, m Member) (Observation, error, bool) {
	for {
		if err := ctx.Err(); err != nil {
			return Observation{}, err, false
		}
		s.mu.Lock()
		c := s.cache[m.NodeID]
		if c == nil {
			c = &cached{}
			s.cache[m.NodeID] = c
		}
		if c.flight != nil {
			done := c.flight
			s.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return Observation{}, ctx.Err(), false
			}
		}
		if time.Since(c.checked) < 5*time.Second {
			v, err := c.value, c.err
			stale := err != nil && v.ObservedUnix != 0
			if stale && time.Since(time.Unix(v.ObservedUnix, 0)) > time.Minute {
				v = Observation{}
				stale = false
			}
			s.mu.Unlock()
			return v, err, stale
		}
		c.flight = make(chan struct{})
		s.mu.Unlock()
		var v Observation
		var err error
		select {
		case s.slots <- struct{}{}:
			call, cancel := context.WithTimeout(ctx, 3*time.Second)
			v, err = s.source.Observe(call, m)
			cancel()
			<-s.slots
		case <-ctx.Done():
			err = ctx.Err()
		}
		s.mu.Lock()
		if err == nil {
			s.cacheBytes -= c.weight
			c.value = v
			c.weight = observationWeight(v)
			s.cacheBytes += c.weight
		}
		c.err = err
		c.checked = time.Now()
		if ctx.Err() != nil {
			c.checked = time.Time{}
		}
		close(c.flight)
		c.flight = nil
		s.trimCache()
		old := c.value
		stale := err != nil && old.ObservedUnix != 0
		if stale && time.Since(time.Unix(old.ObservedUnix, 0)) > time.Minute {
			old = Observation{}
			stale = false
		}
		s.mu.Unlock()
		return old, err, stale
	}
}
func (s *Service) Nodes(ctx context.Context, members []Member) []v1.ManagedNode {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	out := make([]v1.ManagedNode, len(members))
	// A fixed worker set bounds goroutines, even for a large directory.
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(8, len(members)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				out[i] = s.node(ctx, members[i])
			}
		}()
	}
	for i := range members {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return out
}
func (s *Service) node(ctx context.Context, m Member) v1.ManagedNode {
	o, err, stale := s.observe(ctx, m)
	n := v1.ManagedNode{CreatedIndex: m.CreatedIndex, RegionCreatedIndex: m.RegionCreatedIndex, NodeID: m.NodeID, Name: m.Name, RegionID: m.RegionID, Membership: "member",
		OperationalHealth: v1.OperationalHealth{Status: "unknown", Reasons: []v1.ObservationError{}},
		Observation:       v1.Observation{Stale: stale}}
	if o.ObservedUnix != 0 {
		n.ObservedUnix = &o.ObservedUnix
		n.Capacity = o.Capacity
		n.Complete = o.Complete && err == nil
		n.OperationalHealth = o.Health
		n.Reasons = append([]v1.ObservationError{}, o.Health.Reasons...)
		count := uint32(len(o.Volumes))
		n.VolumeCount = &count
		var capacityCount uint32
		for _, v := range o.Volumes {
			if v.Capacity != nil {
				capacityCount++
			}
		}
		n.CapacityObservedVolumes = &capacityCount
		n.Maintenance = &v1.NodeMaintenance{NodeID: m.NodeID, RegionID: m.RegionID, Observation: n.Observation, Items: o.Maintenance}
	}
	if err != nil {
		n.Status = "unavailable"
		code, message := "node_unreachable", "Node observation could not be obtained; membership is unchanged"
		if ctx.Err() != nil {
			n.Status = "unknown"
			code = "observation_timeout"
			message = "Observation budget expired"
		}
		n.Reasons = append(n.Reasons, v1.ObservationError{NodeID: m.NodeID, Code: code, Message: message})
	}
	return n
}
func (s *Service) Volumes(ctx context.Context, m Member) ([]v1.ManagedVolume, error) {
	o, err, stale := s.observe(ctx, m)
	if o.ObservedUnix == 0 {
		return nil, ErrUnavailable
	}
	out := append([]v1.ManagedVolume{}, o.Volumes...)
	for i := range out {
		out[i].Observation = v1.Observation{ObservedUnix: &o.ObservedUnix, Stale: stale, Complete: o.Complete && err == nil}
	}
	sort.Slice(out, func(i, j int) bool { return LocalVolumeKey(out[i]) < LocalVolumeKey(out[j]) })
	return out, nil
}
func Summary(nodes []v1.ManagedNode) v1.ManagementSummary {
	s := v1.ManagementSummary{OperationalHealth: v1.OperationalHealth{Status: "healthy", Reasons: []v1.ObservationError{}}, NodeCount: len(nodes),
		NodeStates: map[string]int{"healthy": 0, "degraded": 0, "unavailable": 0, "unknown": 0}, CapacityScope: "sum_of_observed_node_filesystems", MaintenancePending: map[string]uint64{}, Observation: v1.Observation{Complete: true}}
	for _, n := range nodes {
		s.NodeStates[n.Status]++
		if n.Status != "healthy" {
			s.Status = "degraded"
		}
		if n.ObservedUnix != nil {
			s.ObservedNodes++
			if s.ObservedUnix == nil || *n.ObservedUnix < *s.ObservedUnix {
				v := *n.ObservedUnix
				s.ObservedUnix = &v
			}
		}
		s.Complete = s.Complete && n.Complete
		s.Stale = s.Stale || n.Stale
		if n.Capacity != nil {
			if s.Capacity == nil {
				s.Capacity = &v1.FilesystemCapacity{}
			}
			s.CapacityObservedNodes++
			s.Capacity.FilesystemTotalBytes += n.Capacity.FilesystemTotalBytes
			s.Capacity.FilesystemAvailableBytes += n.Capacity.FilesystemAvailableBytes
		}
		if n.Maintenance != nil {
			s.MaintenanceObservedNodes++
			for _, a := range n.Maintenance.Items {
				if a.Pending != nil {
					s.MaintenancePending[a.Kind] += *a.Pending
				}
			}
		}
	}
	if len(nodes) == 0 || s.NodeStates["unknown"] == len(nodes) {
		s.Status = "unknown"
	} else if s.NodeStates["unavailable"] == len(nodes) {
		s.Status = "unavailable"
	}
	if s.Status != "healthy" {
		s.Reasons = append(s.Reasons, v1.ObservationError{Code: "node_observations", Message: "One or more node observations are degraded, unavailable or unknown"})
	}
	return s
}
func Regions(nodes []v1.ManagedNode) []v1.ManagedRegion {
	groups := map[string][]v1.ManagedNode{}
	for _, n := range nodes {
		groups[n.RegionID] = append(groups[n.RegionID], n)
	}
	out := make([]v1.ManagedRegion, 0, len(groups))
	for region, nodes := range groups {
		out = append(out, v1.ManagedRegion{CreatedIndex: nodes[0].RegionCreatedIndex, RegionID: region, ManagementSummary: Summary(nodes)})
	}
	sort.Slice(out, func(i, j int) bool {
		return CreationKey(out[i].CreatedIndex, out[i].RegionID) < CreationKey(out[j].CreatedIndex, out[j].RegionID)
	})
	return out
}

// Conservative retained-size accounting avoids keeping a full volume inventory
// for thousands of nodes in a small VPS heap. In-flight probes are separately bounded.
func observationWeight(o Observation) int {
	n := 1024
	for _, r := range o.Health.Reasons {
		n += 128 + len(r.Code) + len(r.Message) + len(r.NodeID)
	}
	for _, v := range o.Volumes {
		n += 512 + len(v.VolumeID) + len(v.Path) + len(v.Error) + len(v.WriteError)
	}
	for _, a := range o.Maintenance {
		n += 512 + len(a.Phase) + len(a.SourceVolumeID) + len(a.TargetVolumeID)
		for _, e := range a.Errors {
			n += 32 + len(e)
		}
	}
	return n
}

// Caller holds mu. Evict only completed entries; waiters always get signaled.
func (s *Service) trimCache() {
	for len(s.cache) > 256 || s.cacheBytes > 16<<20 {
		id := ""
		var oldest time.Time
		for key, c := range s.cache {
			if c.flight == nil && (id == "" || c.checked.Before(oldest)) {
				id, oldest = key, c.checked
			}
		}
		if id == "" {
			return
		}
		s.cacheBytes -= s.cache[id].weight
		delete(s.cache, id)
	}
}
