package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RetentionScope binds local capacity hints and keeper promises to an exact
// confirmed cohort. Names/addresses are routing metadata, not holder identity.
func (p LocalPlan) RetentionScope() string {
	type nodeIdentity struct {
		RaftID uint64
		NodeID string
	}
	var identities []nodeIdentity
	for _, node := range p.nodes {
		identities = append(identities, nodeIdentity{node.RaftID, node.NodeID})
	}
	encoded, _ := json.Marshal(struct {
		Region, Group                 string
		Generation, Epoch, FenceIndex uint64
		Required                      int
		Nodes                         []nodeIdentity
	}{p.RegionID, p.GroupID, p.Generation, p.GroupEpoch, p.FenceIndex, p.Required, identities})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
