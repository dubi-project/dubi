package server

import (
	"context"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"sync"
)

type repairCapacitySample struct {
	bytes     uint64
	supported bool
	err       error
}

// A cache lives for one bounded worker page only. It grants no membership or
// durability authority, and stale capacity cannot bypass append admission.
type capacityRepairClient struct {
	*raftrpc.GroupClient
	runtime *node.Runtime
	samples map[uint64]repairCapacitySample
	mu      sync.Mutex
}

func (c *capacityRepairClient) RepairHasCapacity(ctx context.Context, node rootstate.Node, size uint64) (bool, error) {
	c.mu.Lock()
	sample, found := c.samples[node.RaftID]
	c.mu.Unlock()
	if !found {
		if node.RaftID == c.runtime.Identity.RaftID {
			report, err := c.runtime.Volumes.Status()
			sample = repairCapacitySample{bytes: volume.WritableCapacity(report, c.runtime.Packs.WriteErrors()), supported: true, err: err}
		} else {
			sample.bytes, sample.supported, sample.err = c.GroupClient.RepairWritableBytes(ctx, node)
		}
		c.mu.Lock()
		c.samples[node.RaftID] = sample
		c.mu.Unlock()
	}
	if sample.err != nil {
		return false, sample.err
	}
	// Older peers still enforce real append admission. Their missing telemetry
	// must not turn a rolling deployment into "no repair destination".
	return !sample.supported || sample.bytes >= size+(5<<20), nil
}
