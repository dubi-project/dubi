package raftrpc

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RepairWritableBytes uses the exact-identity-pinned Node connection. Capacity
// is only a hint; the destination still reserves/revalidates the actual disk.
func (c *GroupClient) RepairWritableBytes(ctx context.Context, node rootstate.Node) (uint64, bool, error) {
	observation, err := c.RepairCapacity(ctx, node)
	return observation.WritableBytes, observation.Supported, err
}

type RepairCapacityObservation struct {
	WritableBytes, TotalBytes, AvailableBytes uint64
	Supported                                 bool
}

func (c *GroupClient) RepairCapacity(ctx context.Context, node rootstate.Node) (RepairCapacityObservation, error) {
	conn, err := c.NodeAdminConnection(node)
	if err != nil {
		return RepairCapacityObservation{}, err
	}
	call, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).ClusterRepairStatus(call, &datawire.LocalRepairStatusRequest{
		ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID}, grpc.MaxCallRecvMsgSize(512<<10), grpc.MaxCallSendMsgSize(4096))
	if status.Code(err) == codes.Unimplemented {
		return RepairCapacityObservation{}, nil
	}
	if err != nil {
		return RepairCapacityObservation{}, err
	}
	if response == nil || response.NodeId != node.NodeID || response.RaftId != node.RaftID {
		return RepairCapacityObservation{}, errors.New("repair capacity responder identity differs from plan")
	}
	if response.CapacityAvailableBytes > response.CapacityTotalBytes {
		return RepairCapacityObservation{}, errors.New("invalid repair capacity telemetry")
	}
	return RepairCapacityObservation{WritableBytes: response.WritableBytes, Supported: response.CapacitySupported,
		TotalBytes: response.CapacityTotalBytes, AvailableBytes: response.CapacityAvailableBytes}, nil
}
