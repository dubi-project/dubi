package routedstate

import (
	"encoding/hex"
	"errors"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// LocalConfirmation is an internal Node's claim that it freshly verified an
// exact published record on a majority of the confirmed regional cohort.
// Raft can validate its structure and current fences, but the authenticated
// physical observations themselves are not transferable signatures.
type LocalConfirmation struct {
	Target             router.Target       `json:"target"`
	Key                []byte              `json:"key"`
	ReferenceRequestID string              `json:"reference_request_id"`
	ReferenceIndex     uint64              `json:"reference_index"`
	RecordID           string              `json:"record_id"`
	RegionID           string              `json:"region_id"`
	Generation         uint64              `json:"generation"`
	GroupEpoch         uint64              `json:"group_epoch"`
	FenceIndex         uint64              `json:"fence_index"`
	Nodes              []uint64            `json:"nodes"`
	Holdings           []placement.Holding `json:"holdings"`
}

const MaxLocalConfirmationBytes = 1 << 20

func validPhysicalID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+32 {
		return false
	}
	b, err := hex.DecodeString(value[len(prefix):])
	return err == nil && len(b) == 16 && prefix+hex.EncodeToString(b) == value
}

func (c LocalConfirmation) Validate() error {
	if c.Target.Validate() != nil || len(c.Target.RoutingKey) > 4096 || len(c.Key) == 0 || len(c.Key) > 4096 ||
		rootstate.ValidateRequestID(c.ReferenceRequestID) != nil || c.ReferenceIndex == 0 ||
		rootstate.ValidateRangeID(c.RegionID) != nil || c.Generation == 0 || c.GroupEpoch < 2 ||
		c.FenceIndex == 0 || len(c.Nodes) < 3 || len(c.Nodes) > rootstate.MaxRegionCohortNodes ||
		len(c.Holdings) < len(c.Nodes)/2+1 || len(c.Holdings) > len(c.Nodes) {
		return errors.New("invalid local confirmation identity or majority")
	}
	if !strings.HasPrefix(c.RecordID, "record_") {
		return errors.New("invalid local confirmation RecordID")
	}
	id, err := hex.DecodeString(strings.TrimPrefix(c.RecordID, "record_"))
	if err != nil || len(id) != 32 || "record_"+hex.EncodeToString(id) != c.RecordID {
		return errors.New("invalid local confirmation RecordID")
	}
	known := make(map[uint64]bool, len(c.Nodes))
	for _, node := range c.Nodes {
		if node == 0 || known[node] {
			return errors.New("local confirmation repeats a cohort Node")
		}
		known[node] = true
	}
	seen := make(map[uint64]bool, len(c.Holdings))
	stores := make(map[string]bool, len(c.Holdings))
	identities := make(map[string]bool, len(c.Holdings))
	for _, h := range c.Holdings {
		if !known[h.RaftID] || seen[h.RaftID] || !validPhysicalID(h.NodeID, "node_") || identities[h.NodeID] ||
			!validPhysicalID(h.StoreID, "store_") || stores[h.StoreID] || h.RecordID != c.RecordID {
			return errors.New("local confirmation has inconsistent physical holdings")
		}
		seen[h.RaftID], stores[h.StoreID], identities[h.NodeID] = true, true, true
	}
	return nil
}
