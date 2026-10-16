package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const regionObserveRequestBytes = 4 << 10
const regionObserveConfBytes = 256 << 10
const regionObserveResponseBytes = 2*regionfence.MaxRecordBytes + regionObserveConfBytes + 4096
const regionObserveTimeout = 10 * time.Second
const regionObserveConcurrency = 8

type regionReadBarrier interface {
	ReadIndex(context.Context, string) (uint64, error)
}

type RegionFenceServer struct {
	raftwire.UnimplementedRegionFenceServiceServer
	clusterID string
	raftID    uint64
	host      regionReadBarrier
	root      *rootstate.State
	states    EngineResolver
	authorize AuthorizePeer
	slots     chan struct{}
	timeout   time.Duration // Internal bound; shortened only by tests.
}

func NewRegionFenceServer(clusterID string, raftID uint64, host regionReadBarrier, root *rootstate.State, states EngineResolver, authorize AuthorizePeer) (*RegionFenceServer, error) {
	if clusterID == "" || raftID == 0 || host == nil || root == nil || states == nil || authorize == nil {
		return nil, errors.New("regional observation requires identity, Root, Host, states and peer authorization")
	}
	return &RegionFenceServer{clusterID: clusterID, raftID: raftID, host: host, root: root, states: states, authorize: authorize, slots: make(chan struct{}, regionObserveConcurrency), timeout: regionObserveTimeout}, nil
}

func (s *RegionFenceServer) Observe(ctx context.Context, req *raftwire.ObserveRegionFenceRequest) (*raftwire.ObserveRegionFenceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "regional observation request missing")
	}
	if proto.Size(req) > regionObserveRequestBytes {
		return nil, status.Error(codes.ResourceExhausted, "regional observation request too large")
	}
	if req.ClusterId != s.clusterID || req.FromRaftId == 0 || req.ToRaftId != s.raftID {
		return nil, status.Error(codes.PermissionDenied, "regional observation identity mismatch")
	}
	if req.GroupId == raft.RootGroupID || raft.ValidateGroupID(req.GroupId) != nil {
		return nil, status.Error(codes.InvalidArgument, "regional observation requires a data Group")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "regional observation is busy")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := authorizeSender(ctx, req.FromRaftId, s.authorize); err != nil {
		return nil, err
	}
	if _, err := s.confirmRoot(ctx, req.FromRaftId, req.GroupId); err != nil {
		return nil, err
	}
	if err := authorizeSender(ctx, req.FromRaftId, s.authorize); err != nil {
		return nil, err
	}
	state, found := s.states(req.GroupId)
	if !found || state == nil {
		return nil, status.Error(codes.NotFound, "regional Group is not hosted here")
	}
	barrier, err := s.host.ReadIndex(ctx, req.GroupId)
	if err != nil {
		return nil, observationWaitError(err)
	}
	view, err := state.RegionFence()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "regional state cannot provide a durable observation")
	}
	conf, err := proto.Marshal(view.Conf)
	if err != nil {
		return nil, status.Error(codes.Internal, "cannot encode regional membership")
	}
	response := &raftwire.ObserveRegionFenceResponse{ClusterId: s.clusterID, GroupId: req.GroupId, RaftId: s.raftID,
		GroupConfirmedIndex: barrier, AppliedIndex: view.AppliedIndex, MembershipIndex: view.MembershipIndex, ConfState: conf, Active: view.Active, StateFormat: enginestate.FormatVersion}
	if view.Found {
		response.FenceRecord, err = json.Marshal(view.Record)
		if err != nil {
			return nil, status.Error(codes.Internal, "cannot encode regional fence")
		}
	}
	if view.Handoff != nil {
		response.HandoffRecord, err = json.Marshal(view.Handoff)
		if err != nil {
			return nil, status.Error(codes.Internal, "cannot encode regional handoff")
		}
	}
	// Recheck Root membership AND current TLS validity after waits/state reads.
	// This is a point-in-time check, not a persistent global revocation fence.
	response.RootConfirmedIndex, err = s.confirmRoot(ctx, req.FromRaftId, req.GroupId)
	if err != nil {
		return nil, err
	}
	if err := authorizeSender(ctx, req.FromRaftId, s.authorize); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if _, err := decodeRegionObservation(s.clusterID, s.raftID, req.GroupId, response); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "regional Group has no stable valid observation")
	}
	return response, nil
}

func observationWaitError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, "regional observation quorum not confirmed")
}

func (s *RegionFenceServer) confirmRoot(ctx context.Context, caller uint64, group string) (uint64, error) {
	index, err := s.host.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return 0, observationWaitError(err)
	}
	if _, members, err := s.root.RootMembers(caller, s.raftID); err != nil {
		return 0, status.Error(codes.Unavailable, "Root membership unavailable")
	} else if !members {
		return 0, status.Error(codes.PermissionDenied, "regional observation requires current Root members")
	}
	descriptor, found, err := s.root.Group(group)
	if err != nil {
		return 0, status.Error(codes.Unavailable, "Root Group directory unavailable")
	}
	if !found || descriptor.StateKind != rootstate.GroupStateEngine {
		return 0, status.Error(codes.FailedPrecondition, "regional data Group is not registered")
	}
	return index, nil
}

type RegionObservation struct {
	RootConfirmedIndex  uint64
	GroupConfirmedIndex uint64
	View                enginestate.RegionFenceView
}

// ObserveRegionFence is direct-to-participant, never redirected to a leader:
// the caller needs THIS node's applied state, not an interchangeable read.
func (client *GroupClient) ObserveRegionFence(ctx context.Context, target uint64, group string) (RegionObservation, error) {
	if client == nil || target == 0 || target == client.options.RaftID || group == raft.RootGroupID || raft.ValidateGroupID(group) != nil {
		return RegionObservation{}, errors.New("invalid regional observation target")
	}
	ctx, cancel := context.WithTimeout(ctx, regionObserveTimeout)
	defer cancel()
	if _, err := client.peer(target); err != nil {
		return RegionObservation{}, err
	}
	client.mu.Lock()
	if client.closed || client.peers[target] == nil {
		client.mu.Unlock()
		return RegionObservation{}, errors.New("Group client is closed")
	}
	remote := raftwire.NewRegionFenceServiceClient(client.peers[target].conn)
	client.mu.Unlock()
	response, err := remote.Observe(ctx, &raftwire.ObserveRegionFenceRequest{ClusterId: client.options.ClusterID, GroupId: group, FromRaftId: client.options.RaftID, ToRaftId: target}, grpc.MaxCallSendMsgSize(regionObserveRequestBytes), grpc.MaxCallRecvMsgSize(regionObserveResponseBytes))
	if err != nil {
		return RegionObservation{}, err
	}
	return decodeRegionObservation(client.options.ClusterID, target, group, response)
}

func decodeRegionObservation(cluster string, target uint64, group string, r *raftwire.ObserveRegionFenceResponse) (RegionObservation, error) {
	bad := errors.New("invalid regional observation response")
	if r == nil || r.StateFormat != enginestate.FormatVersion || proto.Size(r) > regionObserveResponseBytes || r.ClusterId != cluster || r.RaftId != target || r.GroupId != group || r.RootConfirmedIndex == 0 || r.GroupConfirmedIndex == 0 || r.AppliedIndex < r.GroupConfirmedIndex || r.MembershipIndex == 0 || r.MembershipIndex > r.AppliedIndex || len(r.ConfState) > regionObserveConfBytes || len(r.FenceRecord) > regionfence.MaxRecordBytes || len(r.HandoffRecord) > regionfence.MaxRecordBytes {
		return RegionObservation{}, bad
	}
	conf := new(raftpb.ConfState)
	if proto.Unmarshal(r.ConfState, conf) != nil || !stableRegionalObservation(conf, target) {
		return RegionObservation{}, bad
	}
	v := enginestate.RegionFenceView{AppliedIndex: r.AppliedIndex, MembershipIndex: r.MembershipIndex, Conf: conf, Found: len(r.FenceRecord) != 0, Active: r.Active}
	if v.Found {
		d := json.NewDecoder(bytes.NewReader(r.FenceRecord))
		d.DisallowUnknownFields()
		if d.Decode(&v.Record) != nil || d.Decode(new(struct{})) != io.EOF || v.Record.Validate() != nil || v.Record.Fence.GroupID != group || v.Record.Index > v.AppliedIndex || v.MembershipIndex < v.Record.Fence.MembershipIndex || (v.MembershipIndex != v.Record.Fence.MembershipIndex && v.MembershipIndex <= v.Record.Index) {
			return RegionObservation{}, bad
		}
		if v.MembershipIndex == v.Record.Fence.MembershipIndex && !v.Record.Fence.Matches(conf) {
			return RegionObservation{}, bad
		}
	}
	if v.Active != (v.Found && v.MembershipIndex == v.Record.Fence.MembershipIndex && v.Record.Fence.Matches(conf)) {
		return RegionObservation{}, bad
	}
	if len(r.HandoffRecord) != 0 {
		v.Handoff = new(regionfence.Handoff)
		d := json.NewDecoder(bytes.NewReader(r.HandoffRecord))
		d.DisallowUnknownFields()
		if !v.Found || d.Decode(v.Handoff) != nil || d.Decode(new(struct{})) != io.EOF || v.Handoff.Validate() != nil || !v.Handoff.MatchesLatest(v.Record) || v.Handoff.SealIndex > v.AppliedIndex || v.Handoff.ActivationIndex > v.AppliedIndex {
			return RegionObservation{}, bad
		}
	}
	if v.Found && v.Record.Fence.Generation > 1 && !v.Record.Fence.Reformation && v.Handoff == nil {
		return RegionObservation{}, bad
	}
	return RegionObservation{RootConfirmedIndex: r.RootConfirmedIndex, GroupConfirmedIndex: r.GroupConfirmedIndex, View: v}, nil
}

func stableRegionalObservation(conf *raftpb.ConfState, target uint64) bool {
	// A three-Node Region may temporarily expose a stable two-voter subset
	// after an explicit maintenance removal. This is an observation-only
	// recovery window; Root growth still fixes the old three-Node denominator
	// and will not confirm a replacement until the new cohort is complete.
	if len(conf.GetVoters()) < 2 || len(conf.GetVoters())+len(conf.GetLearners()) > rootstate.MaxRegionCohortNodes || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return false
	}
	seen := map[uint64]bool{}
	for _, ids := range [][]uint64{conf.GetVoters(), conf.GetLearners()} {
		for _, id := range ids {
			if id == 0 || seen[id] {
				return false
			}
			seen[id] = true
		}
	}
	return seen[target]
}
