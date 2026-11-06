package enginedata

import (
	"context"
	"errors"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"go.etcd.io/raft/v3/raftpb"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestUninitializedBindingRejectsOperations(t *testing.T) {
	for _, bound := range []*Bound{nil, {}} {
		if _, err := bound.Read(context.Background(), []byte("key")); err == nil {
			t.Fatal("uninitialized read accepted")
		}
		if _, err := bound.Write(context.Background(), BoundWrite{RequestID: "request", Key: []byte("key"),
			Publish: func(records.Descriptor) ([]byte, error) { return nil, nil }}); err == nil {
			t.Fatal("uninitialized write accepted")
		}
	}
}

// Reproduces the WAN first-write race without timing the background worker:
// Root has activated a Range but has not published its Region placement yet.
func TestActiveInitialRangeWaitsForPlacementAndHonorsCancellation(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	apply := func(c rootstate.Command) {
		t.Helper()
		c.Version = rootstate.FormatVersion
		c.RequestID = fmt.Sprintf("test-%d", root.AppliedIndex()+1)
		data, err := rootstate.EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		if err = root.Apply(root.AppliedIndex()+1, data); err != nil {
			t.Fatal(err)
		}
		result, found, err := root.Result(c.RequestID)
		if err != nil || !found || result.Error != "" {
			t.Fatal(result, found, err)
		}
	}
	apply(rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: "node_00000000000000000000000000000001", RaftID: 1, RegionID: "west", Address: "127.0.0.1:10001"}})
	apply(rootstate.Command{Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "regional-west", StateKind: rootstate.GroupStateEngine}})
	apply(rootstate.Command{Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: "regional-west", ExpectedEpoch: 1, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}}}})
	apply(rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: "regional-west", Nodes: []uint64{1}}})
	apply(rootstate.Command{Type: rootstate.ConfirmRegionCohort, RegionConfirmation: &rootstate.RegionCohortConfirmation{RegionID: "west", GroupID: "regional-west", Generation: 1, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 100}})
	apply(rootstate.Command{Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "tenant"}})
	space := rootstate.Keyspace{TenantID: "tenant", ModuleID: "config", EngineID: "record"}
	apply(rootstate.Command{Type: rootstate.PrepareRange, InitialRange: &rootstate.RangeSeed{TenantID: space.TenantID, ModuleID: space.ModuleID, EngineID: space.EngineID, RangeID: "range-a", OwnerGroupID: "regional-west"}})
	apply(rootstate.Command{Type: rootstate.ActivateRange, Activation: &rootstate.RangeActivation{RangeID: "range-a", OwnerGroupID: "regional-west", Epoch: 1, OwnerRequestID: "fence-range", OwnerAppliedIndex: 101}})
	// No Host/client: the ready fast path must not require any WAN barrier.
	service := &Service{runtime: &node.Runtime{Root: root}}
	prepares := 0
	service.initialPlacement = func(ctx context.Context, id string) error {
		prepares++
		if id != "range-a" {
			t.Fatal("wrong Range prepared", id)
		}
		return nil // An ACK without durable placement must not release the writer.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = service.ensureRange(ctx, space)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("active Range without placement accepted: %v", err)
	}
	if prepares != 1 {
		t.Fatal("first writer spun on placement preparation", prepares)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.ensureRange(ctx, space) }()
	select {
	case err := <-done:
		t.Fatalf("writer escaped before placement: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	apply(rootstate.Command{Type: rootstate.SetRangePlacement, RangePlacement: &rootstate.RangePlacement{RangeID: "range-a", RangeEpoch: 1, OwnerGroupID: "regional-west", Regions: []rootstate.RangeRegionPlacement{{RegionID: "west", GroupID: "regional-west", Generation: 1}}}})
	if err = <-done; err != nil {
		t.Fatal("placement did not release writer", err)
	}
	before := root.AppliedIndex()
	beforePrepares := prepares
	if err = service.ensureRange(ctx, space); err != nil || root.AppliedIndex() != before {
		t.Fatal("ready path touched consensus", err)
	}
	if prepares != beforePrepares {
		t.Fatal("established write triggered placement preparation")
	}
}
