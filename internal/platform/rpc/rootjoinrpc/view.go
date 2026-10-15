package rootjoinrpc

import (
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"google.golang.org/protobuf/proto"
)

// ValidateView applies common routing validation plus the protobuf budget.
// TLS establishes trust; neither the document nor a hint is a trust anchor.
func ValidateView(view *rootjoinwire.RootView, clusterID string) error {
	value, err := modelView(view)
	if err != nil {
		return err
	}
	return value.Validate(clusterID)
}

// DecodeView returns an owned, canonical core model, so subsequent protobuf
// mutation cannot change a validated view before persistence.
func DecodeView(view *rootjoinwire.RootView, clusterID string) (rootjoin.View, error) {
	value, err := modelView(view)
	if err != nil {
		return rootjoin.View{}, err
	}
	if err := value.Validate(clusterID); err != nil {
		return rootjoin.View{}, err
	}
	return value.CloneCanonical(), nil
}

func modelView(view *rootjoinwire.RootView) (rootjoin.View, error) {
	if view == nil || len(view.Peers) > MaxPeers || len(view.Voters) > MaxPeers || len(view.VotersOutgoing) > MaxPeers ||
		len(view.Learners) > MaxPeers || len(view.LearnersNext) > MaxPeers || proto.Size(view) > MaxResponseSize-128 {
		return rootjoin.View{}, errors.New("invalid or oversized Root routing view")
	}
	value := rootjoin.View{ClusterID: view.ClusterId, AppliedIndex: view.AppliedIndex, Voters: view.Voters, VotersOutgoing: view.VotersOutgoing,
		Learners: view.Learners, LearnersNext: view.LearnersNext, AutoLeave: view.AutoLeave, LeaderID: view.LeaderId,
		Peers: make([]rootstate.Node, 0, len(view.Peers))}
	for _, peer := range view.Peers {
		if peer == nil {
			return rootjoin.View{}, errors.New("missing Root peer descriptor")
		}
		value.Peers = append(value.Peers, rootstate.Node{NodeID: peer.NodeId, RaftID: peer.RaftId, Name: peer.Name, RegionID: peer.RegionId, Address: peer.Address})
	}
	return value, nil
}

func encodeView(value rootjoin.View) (*rootjoinwire.RootView, error) {
	if err := value.Validate(value.ClusterID); err != nil {
		return nil, err
	}
	value = value.CloneCanonical()
	view := &rootjoinwire.RootView{ClusterId: value.ClusterID, AppliedIndex: value.AppliedIndex, Voters: value.Voters,
		VotersOutgoing: value.VotersOutgoing, Learners: value.Learners, LearnersNext: value.LearnersNext,
		AutoLeave: value.AutoLeave, LeaderId: value.LeaderID}
	for _, node := range value.Peers {
		view.Peers = append(view.Peers, &rootjoinwire.RootPeer{NodeId: node.NodeID, RaftId: node.RaftID, Name: node.Name, RegionId: node.RegionID, Address: node.Address})
	}
	if proto.Size(view) > MaxResponseSize-128 {
		return nil, errors.New("Root routing response exceeds limit")
	}
	return view, nil
}

func endpointKey(address string) string { return rootjoin.EndpointKey(address) }

func viewContains(view *rootjoinwire.RootView, id uint64) bool {
	return view != nil && id != 0 && (slices.Contains(view.Voters, id) || slices.Contains(view.VotersOutgoing, id) || slices.Contains(view.Learners, id) || slices.Contains(view.LearnersNext, id))
}
