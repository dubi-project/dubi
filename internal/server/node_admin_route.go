package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodeadminrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// nodeAdminRoute keeps the CLI/API independent of Root leadership. The
// request still enters through any live Node, then tries Root members until
// the current Root leader executes the normal removal coordinator.
type nodeAdminRoute struct {
	runtime     *node.Runtime
	directory   *raftrpc.RootDirectory
	credentials credentials.TransportCredentials
	clusterID   string
	localRaft   uint64
}

func (route *nodeAdminRoute) isLocalRootLeader() (bool, error) {
	if route == nil || route.runtime == nil {
		return false, errors.New("Node admin route is not configured")
	}
	status, err := route.runtime.Host.Status(raft.RootGroupID)
	if err != nil {
		return false, err
	}
	return status.Role == etcdraft.StateLeader && status.LeaderID == status.RaftID && status.RaftID == route.localRaft, nil
}

func (route *nodeAdminRoute) remove(ctx context.Context, target uint64) (bool, error) {
	leader, err := route.isLocalRootLeader()
	if err != nil {
		return false, err
	}
	if leader {
		return false, nil
	}
	return true, route.forward(ctx, func(callCtx context.Context, conn *grpc.ClientConn) error {
		return nodeadminrpc.RemoveNodeForwarded(callCtx, conn, route.clusterID, route.localRaft, target)
	})
}

func (route *nodeAdminRoute) removeByName(ctx context.Context, name string) (bool, uint64, error) {
	leader, err := route.isLocalRootLeader()
	if err != nil {
		return false, 0, err
	}
	if leader {
		return false, 0, nil
	}
	var target uint64
	err = route.forward(ctx, func(callCtx context.Context, conn *grpc.ClientConn) error {
		var callErr error
		target, callErr = nodeadminrpc.RemoveNodeByNameForwarded(callCtx, conn, route.clusterID, route.localRaft, name)
		return callErr
	})
	return true, target, err
}

func (route *nodeAdminRoute) forward(ctx context.Context, call func(context.Context, *grpc.ClientConn) error) error {
	if route == nil || route.directory == nil || route.credentials == nil {
		return errors.New("Node admin route is not configured")
	}
	candidates := route.directory.Candidates(raft.RootGroupID)
	if len(candidates) == 0 {
		return errors.New("Root has no management candidates")
	}
	var failures []error
	for _, raftID := range candidates {
		if raftID == 0 || raftID == route.localRaft {
			continue
		}
		address, found := route.directory.Resolve(raftID)
		if !found {
			continue
		}
		conn, err := grpc.NewClient("passthrough:///"+address,
			grpc.WithTransportCredentials(route.credentials),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(raftrpc.DefaultMaxMessageSize), grpc.MaxCallSendMsgSize(raftrpc.DefaultMaxMessageSize)))
		if err != nil {
			failures = append(failures, fmt.Errorf("Root Node %d: %w", raftID, err))
			continue
		}
		// A Root directory contains every Root member, including followers. A
		// follower may wait for its own ReadIndex/leadership transition, so do
		// not let one stale candidate consume the entire management request.
		// Move on to the next Root member and let the actual leader execute it.
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = call(attemptCtx, conn)
		cancel()
		_ = conn.Close()
		if err == nil {
			return nil
		}
		failures = append(failures, fmt.Errorf("Root Node %d: %w", raftID, err))
	}
	if len(failures) == 0 {
		return errors.New("Root has no reachable management candidate")
	}
	return errors.Join(failures...)
}
