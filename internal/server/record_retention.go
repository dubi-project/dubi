package server

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func recordCapacityPlan(runtime *node.Runtime, index uint64, descriptor records.Descriptor, region string) (placement.LocalPlan, error) {
	route, found, err := runtime.Root.RouteKeyspace(descriptor.Keyspace, descriptor.Key)
	if err != nil || !found || route.OwnerGroupID != descriptor.GroupID || route.State != rootstate.RangeStateActive {
		return placement.LocalPlan{}, errors.Join(err, errors.New("capacity handoff record has no current originating Range"))
	}
	mapping, geo, err := placement.ReadRangeGeoPlanAtCut(index, runtime.Root, route.RangeID, 256, rootstate.MaxRegionCohortNodes)
	if err != nil || mapping.OwnerGroupID != descriptor.GroupID || mapping.RangeEpoch != route.Epoch {
		return placement.LocalPlan{}, errors.Join(err, errors.New("capacity handoff Range changed"))
	}
	for _, plan := range geo.Regions() {
		if plan.RegionID == region && len(plan.Nodes()) >= 3 {
			return plan, nil
		}
	}
	return placement.LocalPlan{}, errors.New("capacity handoff requires a stable Region with at least three Nodes")
}

func (s *recordRepairService) Retain(ctx context.Context, request *raftwire.RetainRecordRequest) (*raftwire.RepairRecordResponse, error) {
	if request == nil || proto.Size(request) > 20<<10 || len(request.ProtoReflect().GetUnknown()) != 0 ||
		request.ClusterId != s.runtime.Identity.ClusterID || request.FromRaftId == 0 || request.ToRaftId != s.runtime.Identity.RaftID ||
		request.FromRaftId == request.ToRaftId || request.Sequence == 0 || len(request.Scope) != 64 {
		return nil, status.Error(codes.InvalidArgument, "invalid record retention request")
	}
	descriptor, err := raftrpc.DecodeRecordDescriptor(request.RecordDescriptor)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid retained record descriptor")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-s.slots }()
	ctx = resource.WithMaintenance(ctx, s.io)
	source, index, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	plan, err := recordCapacityPlan(s.runtime, index, descriptor, source.RegionID)
	if err != nil || plan.RetentionScope() != request.Scope {
		return nil, status.Error(codes.FailedPrecondition, "retention differs from the current Region plan")
	}
	nodes := plan.Nodes()
	if !slices.ContainsFunc(nodes, func(n rootstate.Node) bool { return n.RaftID == source.RaftID && n.NodeID == source.NodeID }) ||
		!slices.ContainsFunc(nodes, func(n rootstate.Node) bool {
			return n.RaftID == s.runtime.Identity.RaftID && n.NodeID == s.runtime.Identity.NodeID
		}) {
		return nil, status.Error(codes.PermissionDenied, "retention source and keeper must belong to the same confirmed Region")
	}
	if _, err := s.runtime.Records.Verify(ctx, descriptor); errors.Is(err, records.ErrNotHeld) {
		// A fresh keeper allocation must fetch/verify the full immutable bytes;
		// Put atomically supersedes any old local capacity hint.
		if _, err := s.client.FetchRecord(ctx, source, descriptor, s.runtime.Records); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	holding, err := s.runtime.Records.Retain(ctx, source.NodeID, request.Sequence, descriptor, source.RaftID)
	if errors.Is(err, records.ErrRetentionReleased) {
		return nil, status.Error(codes.FailedPrecondition, "retention sequence was already released")
	}
	if err != nil {
		return nil, err
	}
	after, cut, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	current, err := recordCapacityPlan(s.runtime, cut, descriptor, source.RegionID)
	if err != nil || after != source || !plan.Matches(current) {
		return nil, status.Error(codes.FailedPrecondition, "retention authority changed while persisting bytes")
	}
	return &raftwire.RepairRecordResponse{ClusterId: s.runtime.Identity.ClusterID, RaftId: s.runtime.Identity.RaftID,
		NodeId: s.runtime.Identity.NodeID, RootConfirmedIndex: cut, RecordId: holding.RecordID, StoreId: holding.StoreID, VolumeId: holding.VolumeID}, nil
}

func (s *recordRepairService) ReleaseRetention(ctx context.Context, request *raftwire.ReleaseRecordRetentionRequest) (*raftwire.ReleaseRecordRetentionResponse, error) {
	if request == nil || proto.Size(request) > 4096 || len(request.ProtoReflect().GetUnknown()) != 0 ||
		request.ClusterId != s.runtime.Identity.ClusterID || request.FromRaftId == 0 || request.FromRaftId == request.ToRaftId ||
		request.ToRaftId != s.runtime.Identity.RaftID || request.Sequence == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid retention release request")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-s.slots }()
	source, _, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	if err := s.runtime.Records.ReleaseRetention(ctx, source.NodeID, request.Sequence); err != nil {
		return nil, err
	}
	after, index, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil || source != after {
		return nil, errors.Join(err, status.Error(codes.FailedPrecondition, "retention source authority changed"))
	}
	return &raftwire.ReleaseRecordRetentionResponse{RaftId: s.runtime.Identity.RaftID, NodeId: s.runtime.Identity.NodeID,
		RootConfirmedIndex: index, Sequence: request.Sequence}, nil
}
