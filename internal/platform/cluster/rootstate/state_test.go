package rootstate

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

func submit(t *testing.T, group *raft.Group, command Command) []byte {
	t.Helper()
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Submit(encoded); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestRootStateRaftApplyRestartAndSnapshot(t *testing.T) {
	root := t.TempDir()
	state, err := Open(filepath.Join(root, "state"))
	if err != nil {
		t.Fatal(err)
	}
	group, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: root, GroupID: raft.RootGroupID,
		RaftID: 11, BootstrapPeers: []uint64{11}, State: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Campaign(); err != nil {
		t.Fatal(err)
	}
	missing := Command{Version: FormatVersion, RequestID: "missing-tenant", Type: CreateDataset,
		Dataset: &Dataset{TenantID: "tenant-a", EngineID: "record", DatasetID: "items",
			RangeID: "range-a", OwnerGroupID: "data-1"}}
	submit(t, group, missing)
	if result, found, err := state.Result(missing.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("missing Tenant result = %+v, %v, %v", result, found, err)
	}
	node := Command{Version: FormatVersion, RequestID: "node-1", Type: RegisterNode,
		Node: &Node{NodeID: "node_00000000000000000000000000000001", RaftID: 11,
			RegionID: "us-west", Address: "127.0.0.1:10001"}}
	submit(t, group, node)
	rename := Command{Version: FormatVersion, RequestID: "node-rename", Type: RenameNode,
		NodeRename: &NodeRename{NodeID: "node_00000000000000000000000000000001", RaftID: 11, Name: "sh-01"}}
	submit(t, group, rename)
	if registered, found, err := state.Node(11); err != nil || !found || registered.Name != "sh-01" {
		t.Fatalf("renamed Node = %+v, %v, %v", registered, found, err)
	}
	duplicateName := Command{Version: FormatVersion, RequestID: "node-duplicate-name", Type: RegisterNode,
		Node: &Node{NodeID: "node_00000000000000000000000000000002", RaftID: 14, Name: "sh-01",
			RegionID: "us-west", Address: "127.0.0.1:10002"}}
	submit(t, group, duplicateName)
	if result, found, err := state.Result(duplicateName.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("duplicate Node name result = %+v, %v, %v", result, found, err)
	}
	second := duplicateName
	second.RequestID, second.Node.Name = "node-2", "sh-02"
	submit(t, group, second)
	conflictingRename := Command{Version: FormatVersion, RequestID: "node-rename-conflict", Type: RenameNode,
		NodeRename: &NodeRename{NodeID: node.Node.NodeID, RaftID: 11, Name: "sh-02"}}
	submit(t, group, conflictingRename)
	if result, found, err := state.Result(conflictingRename.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("conflicting Node rename result = %+v, %v, %v", result, found, err)
	}
	submit(t, group, Command{Version: FormatVersion, RequestID: "group-1", Type: RegisterGroup,
		Group: &Group{GroupID: "data-1", StateKind: GroupStateEngine}})
	if registered, found, err := state.Group("data-1"); err != nil || !found ||
		registered.StateKind != GroupStateEngine || registered.Epoch != 1 || len(registered.Members) != 0 {
		t.Fatalf("new Group descriptor = %+v, %v, %v", registered, found, err)
	}
	unknownMember := Command{Version: FormatVersion, RequestID: "members-unknown", Type: PublishGroupMembers,
		Membership: &GroupMembership{GroupID: "data-1", ExpectedEpoch: 1,
			Members: []GroupMember{{RaftID: 12, Role: GroupMemberVoter}}}}
	submit(t, group, unknownMember)
	if result, found, err := state.Result(unknownMember.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("unknown member result = %+v, %v, %v", result, found, err)
	}
	members := Command{Version: FormatVersion, RequestID: "members-1", Type: PublishGroupMembers,
		Membership: &GroupMembership{GroupID: "data-1", ExpectedEpoch: 1,
			Members: []GroupMember{{RaftID: 11, Role: GroupMemberVoter}}}}
	submit(t, group, members)
	if registered, found, err := state.Group("data-1"); err != nil || !found || registered.Epoch != 2 ||
		len(registered.Members) != 1 || registered.Members[0].RaftID != 11 {
		t.Fatalf("published Group descriptor = %+v, %v, %v", registered, found, err)
	}
	staleMembers := members
	staleMembers.RequestID = "members-stale"
	submit(t, group, staleMembers)
	if result, found, err := state.Result(staleMembers.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("stale membership result = %+v, %v, %v", result, found, err)
	}
	submit(t, group, Command{Version: FormatVersion, RequestID: "tenant-1", Type: RegisterTenant,
		Tenant: &Tenant{TenantID: "tenant-a"}})
	created := missing
	created.RequestID = "dataset-1"
	encoded := submit(t, group, created)
	// Replaying a RequestID does not create a second Dataset or overwrite the
	// original command's durable result.
	if err := group.Submit(encoded); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.Result(created.RequestID); err != nil || !found || result.Error != "" {
		t.Fatalf("create Dataset result = %+v, %v, %v", result, found, err)
	}
	if _, found, err := state.Route("tenant-a", "record", "items", nil); err != nil || found {
		t.Fatalf("pending Range was routable: found=%v err=%v", found, err)
	}
	if pending, found, err := state.Range("range-a"); err != nil || !found || pending.State != RangeStatePending {
		t.Fatalf("pending Range = %+v, %v, %v", pending, found, err)
	}
	if pending, err := state.PendingRanges(); err != nil || len(pending) != 1 || pending[0].RangeID != "range-a" {
		t.Fatalf("pending Range list = %+v, %v", pending, err)
	}
	wrongActivation := Command{Version: FormatVersion, RequestID: "activate-wrong", Type: ActivateRange,
		Activation: &RangeActivation{RangeID: "range-a", OwnerGroupID: "data-2", Epoch: 1,
			OwnerRequestID: "owner-wrong", OwnerAppliedIndex: 1}}
	submit(t, group, wrongActivation)
	if result, found, err := state.Result(wrongActivation.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("wrong activation result = %+v, %v, %v", result, found, err)
	}
	activation := Command{Version: FormatVersion, RequestID: "activate-a", Type: ActivateRange,
		Activation: &RangeActivation{RangeID: "range-a", OwnerGroupID: "data-1", Epoch: 1,
			OwnerRequestID: "owner-range-a", OwnerAppliedIndex: 7}}
	submit(t, group, activation)
	if result, found, err := state.Result(activation.RequestID); err != nil || !found || result.Error != "" {
		t.Fatalf("Range activation result = %+v, %v, %v", result, found, err)
	}
	reused := node
	reused.Node = &Node{NodeID: "node_00000000000000000000000000000002", RaftID: 12,
		RegionID: "us-west", Address: "127.0.0.1:10002"}
	reusedBytes := submit(t, group, reused)
	if _, found, err := state.ResultForCommand(reusedBytes); !found || !errors.Is(err, ErrRequestIDReused) {
		t.Fatalf("reused RequestID result = %v, %v", found, err)
	}
	if _, found, err := state.Node(12); err != nil || found {
		t.Fatalf("reused RequestID mutated Node directory: %v, %v", found, err)
	}
	submit(t, group, Command{Version: FormatVersion, RequestID: "tenant-2", Type: RegisterTenant,
		Tenant: &Tenant{TenantID: "tenant-b"}})
	submit(t, group, Command{Version: FormatVersion, RequestID: "dataset-2", Type: CreateDataset,
		Dataset: &Dataset{TenantID: "tenant-b", EngineID: "search", DatasetID: "items",
			RangeID: "range-b", OwnerGroupID: "data-1"}})
	submit(t, group, Command{Version: FormatVersion, RequestID: "dataset-3", Type: CreateDataset,
		Dataset: &Dataset{TenantID: "tenant-a", EngineID: "search", DatasetID: "items",
			RangeID: "range-c", OwnerGroupID: "data-1"}})
	for _, activation := range []RangeActivation{
		{RangeID: "range-b", OwnerGroupID: "data-1", Epoch: 1, OwnerRequestID: "owner-range-b", OwnerAppliedIndex: 8},
		{RangeID: "range-c", OwnerGroupID: "data-1", Epoch: 1, OwnerRequestID: "owner-range-c", OwnerAppliedIndex: 9},
	} {
		submit(t, group, Command{Version: FormatVersion, RequestID: "activate-" + activation.RangeID,
			Type: ActivateRange, Activation: &activation})
	}
	routed, found, err := state.Route("tenant-a", "record", "items", []byte("any-key"))
	if err != nil || !found || routed.RangeID != "range-a" || routed.OwnerGroupID != "data-1" || routed.Epoch != 1 ||
		routed.State != RangeStateActive || len(routed.Start) != 0 || len(routed.End) != 0 {
		t.Fatalf("initial full Range = %+v, %v, %v", routed, found, err)
	}
	for _, check := range []struct{ tenant, engine, rangeID string }{
		{"tenant-b", "search", "range-b"}, {"tenant-a", "search", "range-c"},
	} {
		keyspace := Keyspace{TenantID: check.tenant, EngineID: check.engine, DatasetID: "items"}
		value, found, err := state.RouteKeyspace(keyspace, []byte("same-key"))
		if err != nil || !found || value.RangeID != check.rangeID {
			t.Fatalf("isolated route %s/%s = %+v, %v, %v", check.tenant, check.engine, value, found, err)
		}
		if value.Keyspace() != keyspace {
			t.Fatalf("Range %s Keyspace = %+v, want %+v", value.RangeID, value.Keyspace(), keyspace)
		}
	}
	if registered, found, err := state.Node(11); err != nil || !found || registered.NodeID != node.Node.NodeID {
		t.Fatalf("registered Node = %+v, %v, %v", registered, found, err)
	}
	if registered, found, err := state.Group("data-1"); err != nil || !found || registered.GroupID != "data-1" ||
		registered.StateKind != GroupStateEngine || registered.Epoch != 2 ||
		len(registered.Members) != 1 || registered.Members[0].Role != GroupMemberVoter {
		t.Fatalf("registered Group = %+v, %v, %v", registered, found, err)
	}
	if registered, found, err := state.Tenant("tenant-a"); err != nil || !found || registered.TenantID != "tenant-a" {
		t.Fatalf("registered Tenant = %+v, %v, %v", registered, found, err)
	}
	if registered, found, err := state.Dataset("tenant-a", "record", "items"); err != nil || !found || registered.RangeID != "range-a" {
		t.Fatalf("registered Dataset = %+v, %v, %v", registered, found, err)
	}
	image, index, conf, err := state.Snapshot()
	if err != nil || index != state.AppliedIndex() {
		t.Fatalf("Root snapshot = %d, %v", index, err)
	}
	copy, err := Open(filepath.Join(t.TempDir(), "copy"))
	if err != nil {
		t.Fatal(err)
	}
	if err := copy.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	if value, found, err := copy.Route("tenant-a", "record", "items", nil); err != nil || !found || value.RangeID != "range-a" {
		t.Fatalf("snapshot-restored route = %+v, %v, %v", value, found, err)
	}
	if err := copy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := group.CreateSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(filepath.Join(root, "state"))
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: root, GroupID: raft.RootGroupID,
		RaftID: 11, State: reopened})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if value, found, err := reopened.Route("tenant-a", "record", "items", nil); err != nil || !found || value.RangeID != "range-a" {
		t.Fatalf("restarted route = %+v, %v, %v", value, found, err)
	}
}
