package rootjoin

import (
	"cmp"
	"errors"
	"net"
	"regexp"
	"slices"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
)

// MaxViewPeers bounds a discovery/cache document, not a replica count.
// Exceeding it must fail, never truncate membership or shrink quorum.
const MaxViewPeers = 4096

// View is protocol-independent. It can describe a validated RPC result or an
// identity-bound local routing cache, but is never itself a trust anchor or
// authority to bootstrap a new Raft group. AppliedIndex orders confirmed cuts.
type View struct {
	ClusterID      string           `json:"cluster_id"`
	AppliedIndex   uint64           `json:"applied_index"`
	Voters         []uint64         `json:"voters"`
	VotersOutgoing []uint64         `json:"voters_outgoing,omitempty"`
	Learners       []uint64         `json:"learners,omitempty"`
	LearnersNext   []uint64         `json:"learners_next,omitempty"`
	AutoLeave      bool             `json:"auto_leave,omitempty"`
	Peers          []rootstate.Node `json:"peers"`
	LeaderID       uint64           `json:"leader_id,omitempty"`
}

var nodeName = regexp.MustCompile(`^node_[0-9a-f]{32}$`)
var regionName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func (view View) Validate(clusterID string) error {
	if identity.ValidateClusterID(clusterID) != nil || view.ClusterID != clusterID || view.AppliedIndex == 0 ||
		len(view.Voters) == 0 || len(view.Peers) == 0 || len(view.Peers) > MaxViewPeers {
		return errors.New("invalid or oversized Root routing view")
	}
	sets := []map[uint64]bool{{}, {}, {}, {}}
	members := make(map[uint64]bool)
	for i, ids := range [][]uint64{view.Voters, view.VotersOutgoing, view.Learners, view.LearnersNext} {
		if len(ids) > MaxViewPeers {
			return errors.New("Root membership exceeds limit")
		}
		for _, id := range ids {
			if id == 0 || sets[i][id] {
				return errors.New("invalid or duplicate Root membership identity")
			}
			sets[i][id], members[id] = true, true
			if len(members) > MaxViewPeers {
				return errors.New("Root membership exceeds limit")
			}
		}
	}
	if len(view.Peers) != len(members) || len(view.VotersOutgoing) == 0 && (view.AutoLeave || len(view.LearnersNext) > 0) {
		return errors.New("incomplete Root membership")
	}
	for id := range sets[2] {
		if sets[0][id] || sets[1][id] || sets[3][id] {
			return errors.New("Root learner is also a voter or staged learner")
		}
	}
	for id := range sets[3] {
		if !sets[1][id] || sets[0][id] {
			return errors.New("invalid staged Root learner")
		}
	}
	if view.LeaderID != 0 && !sets[0][view.LeaderID] && !sets[1][view.LeaderID] {
		return errors.New("Root leader hint is not a voter")
	}
	seenIDs, seenNames, seenAddresses := make(map[uint64]bool), make(map[string]bool), make(map[string]bool)
	for _, peer := range view.Peers {
		if !members[peer.RaftID] || seenIDs[peer.RaftID] || !nodeName.MatchString(peer.NodeID) || seenNames[peer.NodeID] ||
			rootstate.ValidateNodeName(peer.Name) != nil ||
			!regionName.MatchString(peer.RegionID) || identityenrollment.ValidateNodeAddress(peer.Address) != nil {
			return errors.New("invalid Root peer descriptor")
		}
		address := EndpointKey(peer.Address)
		if seenAddresses[address] {
			return errors.New("duplicate Root peer endpoint")
		}
		seenIDs[peer.RaftID], seenNames[peer.NodeID], seenAddresses[address] = true, true, true
	}
	return nil
}

func (view View) Contains(id uint64) bool {
	return id != 0 && (slices.Contains(view.Voters, id) || slices.Contains(view.VotersOutgoing, id) || slices.Contains(view.Learners, id) || slices.Contains(view.LearnersNext, id))
}

// CloneCanonical owns all slices and gives equivalent unordered views stable
// serialization. Validate input before trusting it. LeaderID is preserved;
// persistent caches should deliberately discard this transient preference.
func (view View) CloneCanonical() View {
	view.Voters, view.VotersOutgoing = slices.Clone(view.Voters), slices.Clone(view.VotersOutgoing)
	view.Learners, view.LearnersNext = slices.Clone(view.Learners), slices.Clone(view.LearnersNext)
	for _, ids := range [][]uint64{view.Voters, view.VotersOutgoing, view.Learners, view.LearnersNext} {
		slices.Sort(ids)
	}
	view.Peers = slices.Clone(view.Peers)
	slices.SortFunc(view.Peers, func(a, b rootstate.Node) int { return cmp.Compare(a.RaftID, b.RaftID) })
	return view
}

// FromRootCut accepts one atomically captured applied-state cut. It does not
// obtain ReadIndex for the caller. A stale/non-voter hint is discarded.
func FromRootCut(clusterID string, cut rootstate.RootRouting, leader uint64) (View, error) {
	conf := cut.Membership
	view := View{ClusterID: clusterID, AppliedIndex: cut.AppliedIndex, Voters: conf.GetVoters(), VotersOutgoing: conf.GetVotersOutgoing(),
		Learners: conf.GetLearners(), LearnersNext: conf.GetLearnersNext(), AutoLeave: conf.GetAutoLeave(), Peers: cut.Nodes}
	if slices.Contains(view.Voters, leader) || slices.Contains(view.VotersOutgoing, leader) {
		view.LeaderID = leader
	}
	if err := view.Validate(clusterID); err != nil {
		return View{}, err
	}
	return view.CloneCanonical(), nil
}

// EndpointKey deduplicates already validated endpoints without DNS. Distinct
// DNS aliases still require operational detection; endpoint equivalence is
// never used as Node identity or quorum.
func EndpointKey(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	} else {
		host = strings.ToLower(strings.TrimSuffix(host, "."))
	}
	return net.JoinHostPort(host, port)
}
