package management

import (
	"context"
	"sort"
	"sync"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
)

func VolumeKey(v v1.ClusterVolume) string {
	return CreationKey(uint64(max(v.CreatedUnixNano, 0)), v.NodeID+"\x00"+v.VolumeID)
}
func LocalVolumeKey(v v1.ManagedVolume) string {
	return CreationKey(uint64(max(v.CreatedUnixNano, 0)), v.VolumeID)
}

func VolumeStatus(v v1.ManagedVolume) string {
	if v.Stale || v.ObservedUnix == nil {
		return "unknown"
	}
	switch v.Health {
	case "healthy", "missing", "invalid", "pending", "removed", "error":
		return v.Health
	default:
		return "unknown"
	}
}

// ClusterVolumes retains at most limit+1 rows for a cursor page. Collection uses
// the same shared probe budget/cache as node queries and has an overall deadline.
// Missing nodes are reported, never represented as invented volume rows.
func (s *Service) ClusterVolumes(ctx context.Context, members []Member, after string, limit int, status string) ([]v1.ClusterVolume, bool, []v1.ObservationError) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	rows := []v1.ClusterVolume{}
	problems := []v1.ObservationError{}
	complete := true
	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan Member)
	for range min(8, len(members)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range jobs {
				o, err, stale := s.observe(ctx, m)
				mu.Lock()
				if err != nil || !o.Complete || o.ObservedUnix == 0 {
					complete = false
					problems = append(problems, v1.ObservationError{NodeID: m.NodeID, Code: "incomplete_volume_observation", Message: "Volume information is unavailable, stale or incomplete"})
				}
				if o.ObservedUnix != 0 {
					for _, volume := range o.Volumes {
						volume.Observation = v1.Observation{ObservedUnix: &o.ObservedUnix, Stale: stale, Complete: o.Complete && err == nil}
						if status != "" && VolumeStatus(volume) != status {
							continue
						}
						v := v1.ClusterVolume{NodeID: m.NodeID, NodeName: m.Name, RegionID: m.RegionID, ManagedVolume: volume}
						key := VolumeKey(v)
						if key <= after {
							continue
						}
						i := sort.Search(len(rows), func(i int) bool { return VolumeKey(rows[i]) >= key })
						if i > limit {
							continue
						}
						rows = append(rows, v)
						copy(rows[i+1:], rows[i:len(rows)-1])
						rows[i] = v
						if len(rows) > limit+1 {
							rows = rows[:limit+1]
						}
					}
				}
				mu.Unlock()
			}
		}()
	}
	for _, m := range members {
		jobs <- m
	}
	close(jobs)
	wg.Wait()
	sort.Slice(problems, func(i, j int) bool { return problems[i].NodeID < problems[j].NodeID })
	return rows, complete, problems
}
