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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// nodeAdminRoute keeps the CLI/API independent of Root leadership. The
// request still enters through any live Node, then tries Root members until
// the current Root leader executes the normal removal coordinator.
type managementInspector interface {
	ReadIndex(context.Context, uint64, string) (uint64, error)
}

type nodeAdminRoute struct {
	runtime     *node.Runtime
	directory   *raftrpc.RootDirectory
	inspector   managementInspector
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
	// Prefer resolving the label at the entry Node after a live Root barrier.
	// This turns the rest of the request into the stable Raft-ID form. A Node
	// can still have a lagging local Root state after ReadIndex (the status RPC
	// may have come from another member), so fall back to trying the name at
	// each Root candidate. The candidate loop then reaches the member whose
	// applied Root state contains the label and whose Root group is leader.
	if _, err := route.runtime.ReadRootIndex(ctx); err != nil {
		return true, 0, err
	}
	node, found, err := route.runtime.Root.NodeByName(name)
	if err != nil {
		return true, 0, err
	}
	if found {
		target := node.RaftID
		err = route.forward(ctx, func(callCtx context.Context, conn *grpc.ClientConn) error {
			return nodeadminrpc.RemoveNodeForwarded(callCtx, conn, route.clusterID, route.localRaft, target)
		})
		return true, target, err
	}
	target, err := route.forwardByName(ctx, name)
	return true, target, err
}

// Only read-only leadership discovery gets a short per-candidate timeout.
// Removal itself is a durable, multi-step operation and uses the caller's
// remaining deadline on the quorum-confirmed leader. Never replay a failed
// mutation on another candidate: it may already have committed some steps.
func (route *nodeAdminRoute) forward(ctx context.Context, call func(context.Context, *grpc.ClientConn) error) error {
	if ctx == nil || call == nil || route == nil || route.directory == nil || route.credentials == nil || route.inspector == nil {
		return errors.New("Node admin route is not configured")
	}
	candidates := route.directory.Candidates(raft.RootGroupID)
	seen := make(map[uint64]bool)
	var failures []error
	for len(candidates) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		raftID := candidates[0]
		candidates = candidates[1:]
		if raftID == 0 || raftID == route.localRaft || seen[raftID] {
			continue
		}
		seen[raftID] = true
		probe, cancel := context.WithTimeout(ctx, 3*time.Second)
		index, err := route.inspector.ReadIndex(probe, raftID, raft.RootGroupID)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			switch status.Code(err) {
			case codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument, codes.DataLoss:
				return err
			}
			failures = append(failures, fmt.Errorf("inspect Root Node %d: %w", raftID, err))
			// A follower response updates the existing directory leader hint.
			candidates = append(route.directory.Candidates(raft.RootGroupID), candidates...)
			continue
		}
		// Root ReadIndex RPC serves a non-zero cut only on the current
		// leader. Followers supply a routing hint, never confirmation.
		if index == 0 {
			return errors.New("management leader returned an empty Root confirmation")
		}
		address, found := route.directory.Resolve(raftID)
		if !found {
			return errors.New("management leader is outside the applied Root membership")
		}
		conn, err := grpc.NewClient("passthrough:///"+address,
			grpc.WithTransportCredentials(route.credentials),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(raftrpc.DefaultMaxMessageSize), grpc.MaxCallSendMsgSize(raftrpc.DefaultMaxMessageSize)))
		if err != nil {
			return err
		}
		err = call(ctx, conn)
		_ = conn.Close()
		if err != nil {
			return fmt.Errorf("Root leader %d: %w", raftID, err)
		}
		return nil
	}
	if len(failures) == 0 {
		return errors.New("Root has no confirmed management leader")
	}
	return errors.Join(failures...)
}

func (route *nodeAdminRoute) forwardByName(ctx context.Context, name string) (uint64, error) {
	var target uint64
	err := route.forward(ctx, func(callCtx context.Context, conn *grpc.ClientConn) error {
		var err error
		target, err = nodeadminrpc.RemoveNodeByNameForwarded(callCtx, conn, route.clusterID, route.localRaft, name)
		return err
	})
	return target, err
}
