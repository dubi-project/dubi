package raftrpc

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RecordFillReport is a single authenticated Node's just-completed physical
// backfill or candidate-verification result. It is not a reusable retention
// receipt; the response does not encode per-Record observations.
type RecordFillReport struct {
	References, AlreadyHeld, Fetched, ReferencedBytes uint64
}

func (c *GroupClient) FillPendingRecordCut(ctx context.Context, target rootstate.Node, seal regionfence.Handoff) (RecordFillReport, error) {
	return c.pendingRecordCut(ctx, target, seal, false)
}

// VerifyPendingRecordCut asks the newly added Node to re-fetch the sealed
// reference cut and check every record against the pending physical majority.
// Its authenticated result is one-shot, not a durable retention receipt.
func (c *GroupClient) VerifyPendingRecordCut(ctx context.Context, target rootstate.Node, seal regionfence.Handoff) (RecordFillReport, error) {
	return c.pendingRecordCut(ctx, target, seal, true)
}

// RepairRecordOnNode asks target to pull one record from source and publish it
// into target's own Pack pool. The source and target are both authenticated
// Node identities; target, not the caller, owns the destination disk write.
func (c *GroupClient) RepairRecordOnNode(ctx context.Context, target, source rootstate.Node, group string, d records.Descriptor) (records.Holding, error) {
	return c.repairRecordOnNode(ctx, target, source, group, d, true)
}

// FetchRecordOnNode is the missing-index counterpart to RepairRecordOnNode.
// The target still performs the source fetch and local Pack write; unlike
// RepairRecordOnNode it will only publish a new local index entry.
func (c *GroupClient) FetchRecordOnNode(ctx context.Context, target, source rootstate.Node, group string, d records.Descriptor) (records.Holding, error) {
	return c.repairRecordOnNode(ctx, target, source, group, d, false)
}

func (c *GroupClient) repairRecordOnNode(ctx context.Context, target, source rootstate.Node, group string, d records.Descriptor, replaceExisting bool) (records.Holding, error) {
	var zero records.Holding
	if c == nil || ctx == nil || target.RaftID == 0 || source.RaftID == 0 || target.RaftID == source.RaftID ||
		!validHoldingID(target.NodeID, "node_") || !validHoldingID(source.NodeID, "node_") || group == raft.RootGroupID ||
		d.GroupID != group {
		return zero, errors.New("record repair requires distinct target and source Nodes and matching Group")
	}
	if err := d.Validate(); err != nil {
		return zero, err
	}
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > recordDescriptorBytes {
		return zero, errors.New("invalid record repair descriptor")
	}
	conn, err := c.recordConnection(target)
	if err != nil {
		return zero, err
	}
	ctx, cancel := context.WithTimeout(ctx, recordTransferTimeout)
	defer cancel()
	var remote peer.Peer
	response, err := raftwire.NewRecordRepairServiceClient(conn).Repair(ctx, &raftwire.RepairRecordRequest{
		ClusterId: c.options.ClusterID, GroupId: group, FromRaftId: c.options.RaftID,
		ToRaftId: target.RaftID, SourceRaftId: source.RaftID, RecordDescriptor: encoded, ReplaceExisting: replaceExisting,
	}, grpc.Peer(&remote), grpc.MaxCallSendMsgSize(recordHoldingMessageBytes), grpc.MaxCallRecvMsgSize(4096))
	if err != nil {
		return zero, err
	}
	want, err := d.ID()
	if err != nil {
		return zero, err
	}
	if response == nil || proto.Size(response) > 4096 || len(response.ProtoReflect().GetUnknown()) != 0 ||
		response.ClusterId != c.options.ClusterID || response.RaftId != target.RaftID || response.NodeId != target.NodeID ||
		response.RootConfirmedIndex == 0 || response.RecordId != want || !validHoldingID(response.StoreId, "store_") ||
		!validHoldingID(response.VolumeId, "volume_") {
		return zero, status.Error(codes.DataLoss, "record repair response identity or holding mismatch")
	}
	if err := recordCutPeer(ctx, &remote, c.options.ClusterID, target); err != nil {
		return zero, err
	}
	return records.Holding{RecordID: response.RecordId, Descriptor: d, StoreID: response.StoreId, VolumeID: response.VolumeId}, nil
}

func (c *GroupClient) pendingRecordCut(ctx context.Context, target rootstate.Node, seal regionfence.Handoff, verify bool) (RecordFillReport, error) {
	var zero RecordFillReport
	if c == nil || ctx == nil || seal.Validate() != nil || seal.Activation != nil ||
		slices.Contains(seal.Intent.Previous.Fence.Nodes(), target.RaftID) ||
		!slices.Contains(seal.Intent.Next.Nodes, target.RaftID) || target.RegionID != seal.Intent.Next.RegionID {
		return zero, errors.New("record repair requires a newly added target and exact pending seal")
	}
	encoded, err := json.Marshal(seal)
	if err != nil || len(encoded) > regionfence.MaxRecordBytes {
		return zero, errors.New("record repair seal exceeds request bound")
	}
	conn, err := c.recordConnection(target)
	if err != nil {
		return zero, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	var remote peer.Peer
	response, err := raftwire.NewRecordRepairServiceClient(conn).Fill(ctx, &raftwire.FillRecordCutRequest{
		ClusterId: c.options.ClusterID, GroupId: seal.Intent.Previous.Fence.GroupID,
		FromRaftId: c.options.RaftID, ToRaftId: target.RaftID, SealRecord: encoded, VerifyCandidate: verify,
	}, grpc.Peer(&remote), grpc.MaxCallSendMsgSize(recordCutHeaderBytes), grpc.MaxCallRecvMsgSize(4096))
	if err != nil {
		return zero, err
	}
	if response == nil || proto.Size(response) > 4096 || len(response.ProtoReflect().GetUnknown()) != 0 ||
		response.ClusterId != c.options.ClusterID || response.RaftId != target.RaftID || response.NodeId != target.NodeID ||
		response.CandidateVerified != verify || verify && (response.Fetched != 0 || response.AlreadyHeld != response.References) ||
		response.RootConfirmedIndex < seal.Intent.Next.PlannedIndex || response.AlreadyHeld > response.References ||
		response.Fetched != response.References-response.AlreadyHeld ||
		response.References == 0 && response.ReferencedBytes != 0 {
		return zero, status.Error(codes.DataLoss, "record repair response identity or counts mismatch")
	}
	if err := recordCutPeer(ctx, &remote, c.options.ClusterID, target); err != nil {
		return zero, err
	}
	return RecordFillReport{References: response.References, AlreadyHeld: response.AlreadyHeld,
		Fetched: response.Fetched, ReferencedBytes: response.ReferencedBytes}, nil
}
