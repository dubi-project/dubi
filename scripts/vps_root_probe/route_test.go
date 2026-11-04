package main

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
)

func testProbe() *rootProbe {
	p := &rootProbe{targets: map[uint64]identity.CertificateIdentity{
		1: {ClusterID: "cluster", NodeID: "one", RaftID: 1},
		2: {ClusterID: "cluster", NodeID: "two", RaftID: 2},
	}}
	p.preferred.Store(1)
	return p
}

func TestAuthenticatedRedirectRequiresFreshConfirmation(t *testing.T) {
	p := testProbe()
	var called []uint64
	p.query = func(ctx context.Context, target uint64) (identity.CertificateIdentity, *raftwire.GroupReadIndexResponse, error) {
		called = append(called, target)
		if target == 1 {
			return p.targets[target], &raftwire.GroupReadIndexResponse{LeaderRaftId: 2}, nil
		}
		return p.targets[target], &raftwire.GroupReadIndexResponse{LeaderRaftId: 2, ConfirmedIndex: 99}, nil
	}
	index, attempts, err := p.confirm(context.Background())
	if err != nil || index != 99 || len(called) != 2 || called[0] != 1 || called[1] != 2 || len(attempts) != 2 || attempts[0].Outcome != "redirect" || attempts[0].Index != 0 || attempts[1].Outcome != "confirmed" {
		t.Fatalf("index=%d attempts=%+v calls=%v error=%v", index, attempts, called, err)
	}
	called = nil
	_, _, err = p.confirm(context.Background())
	if err != nil || len(called) != 1 || called[0] != 2 {
		t.Fatalf("confirmed leader was not reused: %v %v", called, err)
	}
}

func TestInvalidProofAndRPCFailureNeverPassOrRetry(t *testing.T) {
	for _, kind := range []string{"cluster", "node", "raft", "unknown-leader", "zero-index", "nil-response", "wrong-leader-proof", "timeout", "permission"} {
		t.Run(kind, func(t *testing.T) {
			p := testProbe()
			calls := 0
			p.query = func(context.Context, uint64) (identity.CertificateIdentity, *raftwire.GroupReadIndexResponse, error) {
				calls++
				cert := p.targets[1]
				r := &raftwire.GroupReadIndexResponse{LeaderRaftId: 1, ConfirmedIndex: 99}
				var err error
				switch kind {
				case "cluster":
					cert.ClusterID = "other"
				case "node":
					cert.NodeID = "other"
				case "raft":
					cert.RaftID = 2
				case "unknown-leader":
					r.LeaderRaftId = 3
					r.ConfirmedIndex = 0
				case "zero-index":
					r.ConfirmedIndex = 0
				case "nil-response":
					r = nil
				case "wrong-leader-proof":
					r.LeaderRaftId = 2
				case "timeout":
					err = context.DeadlineExceeded
				case "permission":
					err = errors.New("permission denied")
				}
				return cert, r, err
			}
			index, attempts, err := p.confirm(context.Background())
			if err == nil || index != 0 || calls != 1 || len(attempts) != 1 || attempts[0].Outcome != "failed" || attempts[0].Error == "" {
				t.Fatalf("accepted invalid %s: %d %+v %v", kind, index, attempts, err)
			}
			if kind == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("lost original timeout")
			}
		})
	}
}

func TestRedirectLoopAndCancellationAreBounded(t *testing.T) {
	p := testProbe()
	calls := 0
	p.query = func(context.Context, uint64) (identity.CertificateIdentity, *raftwire.GroupReadIndexResponse, error) {
		calls++
		target := uint64(calls)
		return p.targets[target], &raftwire.GroupReadIndexResponse{LeaderRaftId: 3 - target}, nil
	}
	index, attempts, err := p.confirm(context.Background())
	if err == nil || index != 0 || calls != 2 || len(attempts) != 2 {
		t.Fatalf("unbounded loop: %d %+v %v", index, attempts, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = p.confirm(ctx)
	if !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("cancellation ignored: %d %v", calls, err)
	}
}

func TestRedirectUsesOriginalDeadline(t *testing.T) {
	p := testProbe()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	p.query = func(received context.Context, target uint64) (identity.CertificateIdentity, *raftwire.GroupReadIndexResponse, error) {
		calls++
		if received != ctx {
			t.Fatal("replaced original context")
		}
		cancel()
		return p.targets[target], &raftwire.GroupReadIndexResponse{LeaderRaftId: 2}, nil
	}
	index, attempts, err := p.confirm(ctx)
	if !errors.Is(err, context.Canceled) || index != 0 || calls != 1 || len(attempts) != 1 {
		t.Fatalf("canceled redirect accepted: %d %+v %v", index, attempts, err)
	}
}
