package raftrpc

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (c *GroupClient) RetainRecordOnNode(ctx context.Context, target rootstate.Node, sequence uint64, scope string, descriptor records.Descriptor) (records.Holding, error) {
	if c == nil || ctx == nil || target.RaftID == 0 || sequence == 0 || len(scope) != 64 || descriptor.Validate() != nil {
		return records.Holding{}, errors.New("invalid record retention target, descriptor or sequence")
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil || len(encoded) > recordDescriptorBytes {
		return records.Holding{}, errors.New("invalid record retention descriptor")
	}
	conn, err := c.recordConnection(target)
	if err != nil {
		return records.Holding{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var remote peer.Peer
	response, err := raftwire.NewRecordRepairServiceClient(conn).Retain(ctx, &raftwire.RetainRecordRequest{
		ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID, ToRaftId: target.RaftID,
		Sequence: sequence, Scope: scope, RecordDescriptor: encoded},
		grpc.Peer(&remote), grpc.MaxCallSendMsgSize(recordHoldingMessageBytes), grpc.MaxCallRecvMsgSize(4096))
	if err != nil {
		return records.Holding{}, err
	}
	want, _ := descriptor.ID()
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 || proto.Size(response) > 4096 ||
		response.ClusterId != c.options.ClusterID || response.RaftId != target.RaftID || response.NodeId != target.NodeID ||
		response.RootConfirmedIndex == 0 || response.RecordId != want || !validHoldingID(response.StoreId, "store_") || !validHoldingID(response.VolumeId, "volume_") {
		return records.Holding{}, status.Error(codes.DataLoss, "retained holding differs from its exact Node/record identity")
	}
	if err := recordCutPeer(ctx, &remote, c.options.ClusterID, target); err != nil {
		return records.Holding{}, err
	}
	return records.Holding{RecordID: want, Descriptor: descriptor, StoreID: response.StoreId, VolumeID: response.VolumeId}, nil
}

func (c *GroupClient) ReleaseRecordRetention(ctx context.Context, target rootstate.Node, sequence uint64) error {
	if c == nil || ctx == nil || target.RaftID == 0 || sequence == 0 {
		return errors.New("invalid retention release target or sequence")
	}
	conn, err := c.recordConnection(target)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var remote peer.Peer
	response, err := raftwire.NewRecordRepairServiceClient(conn).ReleaseRetention(ctx, &raftwire.ReleaseRecordRetentionRequest{
		ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID, ToRaftId: target.RaftID, Sequence: sequence},
		grpc.Peer(&remote), grpc.MaxCallSendMsgSize(4096), grpc.MaxCallRecvMsgSize(4096))
	if err != nil {
		return err
	}
	if response == nil || proto.Size(response) > 4096 || len(response.ProtoReflect().GetUnknown()) != 0 || response.RaftId != target.RaftID ||
		response.NodeId != target.NodeID || response.RootConfirmedIndex == 0 || response.Sequence != sequence {
		return status.Error(codes.DataLoss, "retention release ACK identity or sequence differs")
	}
	return recordCutPeer(ctx, &remote, c.options.ClusterID, target)
}
