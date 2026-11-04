package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type managementInspectorFunc func(context.Context, uint64, string) (uint64, error)

func (f managementInspectorFunc) ReadIndex(ctx context.Context, id uint64, group string) (uint64, error) {
	return f(ctx, id, group)
}

func managementRouteFixture(t *testing.T) *nodeAdminRoute {
	t.Helper()
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	for id := uint64(1); id <= 4; id++ {
		command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
			RequestID: fmt.Sprintf("node-%d", id), Type: rootstate.RegisterNode,
			Node: &rootstate.Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "west", Address: fmt.Sprintf("node-%d:9000", id)}})
		if err != nil {
			t.Fatal(err)
		}
		if err := root.Apply(id, command); err != nil {
			t.Fatal(err)
		}
		result, found, err := root.Result(fmt.Sprintf("node-%d", id))
		if err != nil || !found || result.Error != "" {
			t.Fatal(result, found, err)
		}
	}
	if err := root.ApplyMembership(5, &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	return &nodeAdminRoute{directory: raftrpc.NewRootDirectory(root), credentials: insecure.NewCredentials(), localRaft: 4}
}

func TestManagementRoutingDoesNotApplyProbeDeadlineToRemoval(t *testing.T) {
	route := managementRouteFixture(t)
	var probes []uint64
	route.inspector = managementInspectorFunc(func(_ context.Context, id uint64, group string) (uint64, error) {
		if group != raft.RootGroupID {
			t.Fatal("management discovery inspected another Group")
		}
		probes = append(probes, id)
		if id != 2 {
			route.directory.ObserveLeader(raft.RootGroupID, 2)
			return 0, status.Error(codes.Unavailable, "follower hint only")
		}
		return 5, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	calls := 0
	err := route.forward(ctx, func(call context.Context, conn *grpc.ClientConn) error {
		calls++
		if conn.Target() != "passthrough:///node-2:9000" {
			t.Fatal("mutation reached an unconfirmed leader", conn.Target())
		}
		// The real WAN removal exceeded the former three-second candidate
		// timeout after already beginning its durable Group changes.
		timer := time.NewTimer(3200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-call.Done():
			return call.Err()
		case <-timer.C:
			return nil
		}
	})
	if err != nil || calls != 1 || len(probes) != 2 || probes[1] != 2 {
		t.Fatalf("healthy leader operation was cut off or sent to a follower: err=%v, calls=%d, probes=%v", err, calls, probes)
	}
}

func TestManagementRoutingDoesNotReplayUncertainMutation(t *testing.T) {
	route := managementRouteFixture(t)
	route.directory.ObserveLeader(raft.RootGroupID, 2)
	probes, calls := 0, 0
	route.inspector = managementInspectorFunc(func(context.Context, uint64, string) (uint64, error) {
		probes++
		return 5, nil
	})
	lostReply := errors.New("reply lost after a durable removal step")
	err := route.forward(context.Background(), func(context.Context, *grpc.ClientConn) error { calls++; return lostReply })
	if !errors.Is(err, lostReply) || calls != 1 || probes != 1 {
		t.Fatal("uncertain operation was retried on another member", err, calls, probes)
	}
}

func TestManagementRoutingFallsBackOnlyDuringReadOnlyDiscovery(t *testing.T) {
	route := managementRouteFixture(t)
	route.directory.ObserveLeader(raft.RootGroupID, 2)
	var probes []uint64
	route.inspector = managementInspectorFunc(func(_ context.Context, id uint64, _ string) (uint64, error) {
		probes = append(probes, id)
		if id == 2 {
			return 0, status.Error(codes.Unavailable, "old leader is offline")
		}
		if id != 3 {
			route.directory.ObserveLeader(raft.RootGroupID, 3)
			return 0, status.Error(codes.Unavailable, "follower hint only")
		}
		return 5, nil
	})
	calls := 0
	err := route.forward(context.Background(), func(_ context.Context, conn *grpc.ClientConn) error {
		calls++
		if conn.Target() != "passthrough:///node-3:9000" {
			t.Fatal("mutation reached an unavailable or follower member")
		}
		return nil
	})
	if err != nil || calls != 1 || len(probes) != 3 || probes[0] != 2 || probes[2] != 3 {
		t.Fatal(err, calls, probes)
	}
}

func TestManagementRoutingRejectsUntrustedDiscovery(t *testing.T) {
	for _, scenario := range []string{"authorization", "empty confirmation", "unavailable", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			route := managementRouteFixture(t)
			probes, calls := 0, 0
			route.inspector = managementInspectorFunc(func(context.Context, uint64, string) (uint64, error) {
				probes++
				switch scenario {
				case "authorization":
					return 0, status.Error(codes.PermissionDenied, "denied")
				case "unavailable":
					return 0, status.Error(codes.Unavailable, "no confirmed leader")
				default:
					return 0, nil
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			err := route.forward(ctx, func(context.Context, *grpc.ClientConn) error { calls++; return nil })
			if err == nil || calls != 0 || (scenario == "canceled" && probes != 0) {
				t.Fatal("untrusted management discovery authorized mutation", err, calls, probes)
			}
		})
	}
}
