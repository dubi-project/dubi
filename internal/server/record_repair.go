package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recordRepairService runs only on a newly added Node. Its verification mode
// checks the pending cohort after all new Nodes were filled. No user-facing
// operation or per-Node configuration is introduced by this internal RPC.
type recordRepairService struct {
	raftwire.UnimplementedRecordRepairServiceServer
	runtime *node.Runtime
	client  *raftrpc.GroupClient
	peer    raftrpc.AuthorizePeer
	slots   chan struct{}
	io      *resource.Maintenance
}

func newRecordRepairService(runtime *node.Runtime, client *raftrpc.GroupClient, peer raftrpc.AuthorizePeer) *recordRepairService {
	return &recordRepairService{runtime: runtime, client: client, peer: peer, slots: make(chan struct{}, 1)}
}

// acquire waits for the per-Node repair slot instead of rejecting otherwise
// valid repair work while another bounded Pack transfer is in progress. Geo
// repair is initiated independently by several Nodes; immediate rejection
// makes all of them retry the same busy destination and can keep durable repair
// debt alive indefinitely. The caller's deadline still bounds queueing.
func (s *recordRepairService) acquire(ctx context.Context) error {
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (s *recordRepairService) authorizeMember(ctx context.Context, caller uint64) (rootstate.Node, uint64, error) {
	var zero rootstate.Node
	if err := s.peer(ctx, caller); err != nil {
		return zero, 0, status.Error(codes.PermissionDenied, "record repair caller is not authorized")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return zero, 0, status.Error(codes.PermissionDenied, "verified Node certificate required")
	}
	peerID, err := identity.ParseNodeCertificate(leaf)
	if err != nil || peerID.ClusterID != s.runtime.Identity.ClusterID || peerID.RaftID != caller {
		return zero, 0, status.Error(codes.PermissionDenied, "record repair caller certificate mismatch")
	}
	index, err := s.runtime.ReadRootIndex(ctx)
	if err != nil {
		return zero, 0, status.Error(codes.Unavailable, "Root read barrier unavailable")
	}
	registered, member, err := s.runtime.Root.RootMember(caller)
	if err != nil || !member || registered.NodeID != peerID.NodeID {
		return zero, 0, status.Error(codes.PermissionDenied, "record repair caller is not a registered Root member")
	}
	local, member, err := s.runtime.Root.RootMember(s.runtime.Identity.RaftID)
	if err != nil || !member || local.NodeID != s.runtime.Identity.NodeID {
		return zero, 0, status.Error(codes.PermissionDenied, "record repair destination is not a registered Root member")
	}
	if err := s.peer(ctx, caller); err != nil {
		return zero, 0, status.Error(codes.PermissionDenied, "record repair caller is no longer authorized")
	}
	return registered, index, nil
}

// authorizeRootLeader is reserved for Fill, which participates in a Root-led
// membership handoff. Ordinary Repair only writes one destination Node's
// local immutable-record store and is authorized by current Root membership.
func (s *recordRepairService) authorizeRootLeader(ctx context.Context, caller uint64) (rootstate.Node, uint64, error) {
	registered, index, err := s.authorizeMember(ctx, caller)
	if err != nil {
		return rootstate.Node{}, 0, err
	}
	rootStatus, err := s.runtime.Host.Status(raft.RootGroupID)
	if err != nil || rootStatus.LeaderID != caller {
		return rootstate.Node{}, 0, status.Error(codes.FailedPrecondition, "record repair caller is not the current Root leader")
	}
	return registered, index, nil
}

func (s *recordRepairService) Fill(ctx context.Context, request *raftwire.FillRecordCutRequest) (*raftwire.FillRecordCutResponse, error) {
	if request == nil || proto.Size(request) > regionfence.MaxRecordBytes+4096 || len(request.ProtoReflect().GetUnknown()) != 0 ||
		request.ClusterId != s.runtime.Identity.ClusterID || request.FromRaftId == 0 || request.ToRaftId != s.runtime.Identity.RaftID {
		return nil, status.Error(codes.InvalidArgument, "invalid record repair request")
	}
	seal, err := raftrpc.DecodeRecordCutSeal(request.SealRecord)
	if err != nil || seal.Intent.Previous.Fence.GroupID != request.GroupId {
		return nil, status.Error(codes.InvalidArgument, "invalid record repair seal")
	}
	operation := "candidate-fill"
	if request.VerifyCandidate {
		operation = "candidate-verify"
	}
	// Two bounded timings under the existing opt-in switch distinguish waiting
	// for admission/authority from the sealed backfill or verification itself.
	// No tenant key, seal, request ID or payload is logged.
	ctx = withDataTrace(ctx, operation, "")
	started := time.Now()
	defer recordwrite.Trace(ctx, "repair.candidate_total", started)
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-s.slots }()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	before, _, err := s.authorizeRootLeader(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	var report recordrepair.Report
	// New data members may remain Root learners. All backfill/revalidation
	// barriers must use the Runtime's authenticated Root-voter read path;
	// a direct Host Root read can wait indefinitely on an admitted learner.
	started = time.Now()
	if request.VerifyCandidate {
		report, err = recordrepair.VerifyPendingCandidateLocal(ctx, s.runtime, s.runtime.Root, s.runtime.Identity.RaftID,
			seal, s.runtime.Snapshots, s.runtime.Records, s.client)
		report.AlreadyHeld = report.References // Verification only; no physical copy on this RPC.
	} else {
		report, err = recordrepair.FillPendingLocal(ctx, s.runtime, s.runtime.Root, s.runtime.Identity.RaftID,
			seal, s.runtime.Snapshots, s.runtime.Records, s.client)
	}
	recordwrite.Trace(ctx, "repair.candidate_work", started)
	if err != nil {
		return nil, err
	}
	after, index, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, status.Error(codes.FailedPrecondition, "record repair caller registration changed")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &raftwire.FillRecordCutResponse{ClusterId: s.runtime.Identity.ClusterID,
		RaftId: s.runtime.Identity.RaftID, NodeId: s.runtime.Identity.NodeID, RootConfirmedIndex: index,
		References: report.References, AlreadyHeld: report.AlreadyHeld, Fetched: report.Fetched,
		ReferencedBytes: report.ReferencedBytes, CandidateVerified: request.VerifyCandidate}, nil
}

// Repair asks this Node to repair one immutable record from a currently
// registered source Node. The destination owns the Pack write: the caller
// only selects the source and descriptor after Root has authorized the
// operation. This keeps storage placement and failure handling at the Node
// that owns the local volume, rather than allowing an administrator to write
// another Node's disk remotely.
func (s *recordRepairService) Repair(ctx context.Context, request *raftwire.RepairRecordRequest) (*raftwire.RepairRecordResponse, error) {
	if request == nil || proto.Size(request) > regionfence.MaxRecordBytes+4096 || len(request.ProtoReflect().GetUnknown()) != 0 ||
		request.ClusterId != s.runtime.Identity.ClusterID || request.GroupId == raft.RootGroupID ||
		request.FromRaftId == 0 || request.ToRaftId != s.runtime.Identity.RaftID || request.SourceRaftId == 0 ||
		request.SourceRaftId == s.runtime.Identity.RaftID {
		return nil, status.Error(codes.InvalidArgument, "invalid record repair request")
	}
	if err := raft.ValidateGroupID(request.GroupId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid record repair Group")
	}
	var descriptor records.Descriptor
	if len(request.RecordDescriptor) == 0 || len(request.RecordDescriptor) > 16<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid record repair descriptor")
	}
	decoder := json.NewDecoder(bytes.NewReader(request.RecordDescriptor))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&descriptor) != nil || decoder.Decode(new(struct{})) != io.EOF || descriptor.Validate() != nil || descriptor.GroupID != request.GroupId {
		return nil, status.Error(codes.InvalidArgument, "invalid record repair descriptor")
	}
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-s.slots }()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	caller, _, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	source, member, err := s.runtime.Root.RootMember(request.SourceRaftId)
	if err != nil || !member {
		return nil, status.Error(codes.FailedPrecondition, "record repair source is not a current Root member")
	}
	ctx = resource.WithMaintenance(ctx, s.io)
	var result raftrpc.RecordFetchResult
	if request.ReplaceExisting {
		result, err = s.client.RepairRecord(ctx, source, descriptor, s.runtime.Records)
	} else {
		result, err = s.client.FetchRecord(ctx, source, descriptor, s.runtime.Records)
	}
	if err != nil {
		return nil, err
	}
	// Physical record repair is a node-local storage operation initiated by
	// any authenticated Root member. Do not require the caller to remain the
	// Root leader here: unlike Fill, Repair does not change cluster membership
	// and Geo repair workers run on every Node. Reconfirm membership and identity
	// after the destination write so a removed/replaced caller cannot complete
	// an in-flight request.
	after, index, err := s.authorizeMember(ctx, request.FromRaftId)
	if err != nil {
		return nil, err
	}
	if caller != after {
		return nil, status.Error(codes.FailedPrecondition, "record repair caller registration changed")
	}
	want, err := descriptor.ID()
	if err != nil || result.Local.RecordID != want || result.Local.StoreID == "" || result.Local.VolumeID == "" {
		return nil, status.Error(codes.DataLoss, "record repair destination returned inconsistent holding")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &raftwire.RepairRecordResponse{ClusterId: s.runtime.Identity.ClusterID,
		RaftId: s.runtime.Identity.RaftID, NodeId: s.runtime.Identity.NodeID, RootConfirmedIndex: index,
		RecordId: result.Local.RecordID, StoreId: result.Local.StoreID, VolumeId: result.Local.VolumeID}, nil
}

// regionalRepairer is invoked by the Root leader before it confirms the next
// generation. The target is resolved from Root, not a CLI/server option.
type regionalRepairer struct {
	runtime *node.Runtime
	client  *raftrpc.GroupClient
}

func (r regionalRepairer) FillPending(ctx context.Context, target uint64, seal regionfence.Handoff) error {
	if _, err := r.runtime.ReadRootIndex(ctx); err != nil {
		return err
	}
	node, member, err := r.runtime.Root.RootMember(target)
	if err != nil {
		return err
	}
	if !member {
		return errors.New("record repair target is no longer a Root member")
	}
	_, err = r.client.FillPendingRecordCut(ctx, node, seal)
	return err
}

func (r regionalRepairer) VerifyPending(ctx context.Context, target uint64, seal regionfence.Handoff) error {
	if _, err := r.runtime.ReadRootIndex(ctx); err != nil {
		return err
	}
	node, member, err := r.runtime.Root.RootMember(target)
	if err != nil {
		return err
	}
	if !member {
		return errors.New("candidate verification target is no longer a Root member")
	}
	_, err = r.client.VerifyPendingRecordCut(ctx, node, seal)
	return err
}
