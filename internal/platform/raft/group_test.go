package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type testStateDisk struct {
	Applied  uint64   `json:"applied"`
	Conf     []byte   `json:"conf"`
	Commands [][]byte `json:"commands"`
}

type testState struct {
	root   string
	disk   testStateDisk
	fail   bool
	closed bool
}

type validatingTestState struct{ *testState }

func (s *validatingTestState) ValidateCommand(command []byte) error {
	if !bytes.HasPrefix(command, []byte("valid:")) {
		return errors.New("invalid test command")
	}
	return nil
}

func openTestState(t *testing.T, root string) *testState {
	t.Helper()
	state := &testState{root: root}
	encoded, err := os.ReadFile(filepath.Join(root, "TEST_STATE"))
	if errors.Is(err, os.ErrNotExist) {
		return state
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &state.disk); err != nil {
		t.Fatal(err)
	}
	return state
}

func (s *testState) AppliedIndex() uint64 { return s.disk.Applied }

func (s *testState) ConfState() *raftpb.ConfState {
	conf := new(raftpb.ConfState)
	if len(s.disk.Conf) != 0 {
		if err := proto.Unmarshal(s.disk.Conf, conf); err != nil {
			panic(err)
		}
	}
	return conf
}

func (s *testState) Apply(index uint64, command []byte) error {
	if s.fail {
		return errors.New("injected state-machine failure")
	}
	if index != s.disk.Applied+1 {
		return errors.New("state-machine index gap")
	}
	s.disk.Commands = append(s.disk.Commands, bytes.Clone(command))
	s.disk.Applied = index
	return s.persist()
}

func (s *testState) Advance(index uint64) error {
	if index != s.disk.Applied+1 {
		return errors.New("state-machine index gap")
	}
	s.disk.Applied = index
	return s.persist()
}

func (s *testState) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	if index != s.disk.Applied+1 {
		return errors.New("membership index gap")
	}
	encoded, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	s.disk.Conf = encoded
	s.disk.Applied = index
	return s.persist()
}

func (s *testState) Restore(index uint64, data []byte, conf *raftpb.ConfState) error {
	encoded, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	if len(data) != 0 {
		if err := json.Unmarshal(data, &s.disk); err != nil {
			return err
		}
		if s.disk.Applied != index || !bytes.Equal(s.disk.Conf, encoded) {
			return errors.New("snapshot state does not match Raft metadata")
		}
	} else {
		s.disk = testStateDisk{Applied: index, Conf: encoded}
	}
	return s.persist()
}

func (s *testState) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	data, err := json.Marshal(s.disk)
	return data, s.disk.Applied, s.ConfState(), err
}

func TestGroupValidatesCommandsBeforeProposal(t *testing.T) {
	root := t.TempDir()
	state := &validatingTestState{testState: openTestState(t, root)}
	group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "validated",
		RaftID: 61, BootstrapPeers: []uint64{61}, State: state})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	if err := group.Campaign(); err != nil {
		t.Fatal(err)
	}
	before, err := group.Status()
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Submit([]byte("bare")); err == nil {
		t.Fatal("invalid command entered Raft proposal path")
	}
	after, err := group.Status()
	if err != nil {
		t.Fatal(err)
	}
	if after.CommitIndex != before.CommitIndex || len(state.disk.Commands) != 0 {
		t.Fatalf("invalid command changed Group: before=%+v after=%+v commands=%q", before, after, state.disk.Commands)
	}
	if err := group.Submit([]byte("valid:mutation")); err != nil {
		t.Fatal(err)
	}
	if len(state.disk.Commands) != 1 || string(state.disk.Commands[0]) != "valid:mutation" {
		t.Fatalf("valid commands = %q", state.disk.Commands)
	}
}

func TestGroupCreatesDurableSnapshotAndRestoresState(t *testing.T) {
	root := t.TempDir()
	state := openTestState(t, root)
	group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "snapshot",
		RaftID: 51, BootstrapPeers: []uint64{51}, State: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Campaign(); err != nil {
		t.Fatal(err)
	}
	if err := group.Submit([]byte("durable-before-snapshot")); err != nil {
		t.Fatal(err)
	}
	index, err := group.CreateSnapshot()
	if err != nil || index == 0 {
		t.Fatalf("CreateSnapshot = %d, %v", index, err)
	}
	first, err := group.storage.FirstIndex()
	if err != nil || first != index+1 {
		t.Fatalf("compacted first index = %d, %v; want %d", first, err, index+1)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "TEST_STATE")); err != nil {
		t.Fatal(err)
	}
	restored := openTestState(t, root)
	restarted, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "snapshot",
		RaftID: 51, State: restored})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restored.AppliedIndex() != index || len(restored.disk.Commands) != 1 ||
		string(restored.disk.Commands[0]) != "durable-before-snapshot" {
		t.Fatalf("restored snapshot = %+v", restored.disk)
	}
}

func (s *testState) Close() error { s.closed = true; return nil }

func (s *testState) persist() error {
	encoded, err := json.Marshal(s.disk)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(s.root, "TEST_STATE-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(s.root, "TEST_STATE"))
}

func TestSingleGroupCommitAndRestartWithoutFileModel(t *testing.T) {
	root := t.TempDir()
	state := openTestState(t, root)
	group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: RootGroupID,
		RaftID: 11, BootstrapPeers: []uint64{11}, State: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Campaign(); err != nil {
		t.Fatal(err)
	}
	status, err := group.Status()
	if err != nil || status.Role != etcdraft.StateLeader || status.LeaderID != 11 {
		t.Fatalf("single Group status = %+v, %v", status, err)
	}
	if err := group.Submit([]byte("opaque-record-command")); err != nil {
		t.Fatal(err)
	}
	if len(state.disk.Commands) != 1 || string(state.disk.Commands[0]) != "opaque-record-command" {
		t.Fatalf("applied commands = %q", state.disk.Commands)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if !state.closed {
		t.Fatal("Group did not close its StateMachine")
	}
	wrongState := openTestState(t, root)
	if _, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: RootGroupID,
		RaftID: 12, State: wrongState}); err == nil || !wrongState.closed {
		t.Fatalf("wrong RaftID restart = %v, StateMachine closed = %v", err, wrongState.closed)
	}
	restartedState := openTestState(t, root)
	restarted, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: RootGroupID,
		RaftID: 11, State: restartedState})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if len(restartedState.disk.Commands) != 1 || string(restartedState.disk.Commands[0]) != "opaque-record-command" {
		t.Fatalf("recovered commands = %q", restartedState.disk.Commands)
	}
	if restartedState.AppliedIndex() == 0 || len(restartedState.ConfState().GetVoters()) != 1 {
		t.Fatalf("recovered state = %+v", restartedState.disk)
	}
}

func TestMultiNodeGroupRequiresTransport(t *testing.T) {
	root := t.TempDir()
	state := openTestState(t, root)
	if _, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional",
		RaftID: 31, BootstrapPeers: []uint64{31, 32, 33}, State: state}); err == nil || !state.closed {
		t.Fatalf("multi-node Group without Transport = %v, StateMachine closed = %v", err, state.closed)
	}
}

func TestGroupFailsClosedWhenStateMachineApplyFails(t *testing.T) {
	root := t.TempDir()
	state := openTestState(t, root)
	group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "test", RaftID: 21,
		BootstrapPeers: []uint64{21}, State: state})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	if err := group.Campaign(); err != nil {
		t.Fatal(err)
	}
	state.fail = true
	if err := group.Submit([]byte("must-not-apply")); err == nil {
		t.Fatal("failed state-machine Apply was accepted")
	}
	if err := group.Submit([]byte("later")); err == nil {
		t.Fatal("failed Group accepted another proposal")
	}
	if len(state.disk.Commands) != 0 {
		t.Fatalf("failed Group applied commands = %q", state.disk.Commands)
	}
}

type queuedMessage struct {
	groupID string
	message *raftpb.Message
}

type queuedTransport struct {
	mu      sync.Mutex
	groups  map[uint64]*Group
	queue   []queuedMessage
	blocked map[uint64]bool
}

func (q *queuedTransport) Send(groupID string, message *raftpb.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.blocked[message.GetFrom()] || q.blocked[message.GetTo()] {
		return errors.New("injected Raft link failure")
	}
	q.queue = append(q.queue, queuedMessage{groupID: groupID,
		message: proto.Clone(message).(*raftpb.Message)})
	return nil
}

func (q *queuedTransport) deliver(t *testing.T) {
	t.Helper()
	for turn := 0; turn < 1000; turn++ {
		q.mu.Lock()
		if len(q.queue) == 0 {
			q.mu.Unlock()
			return
		}
		item := q.queue[0]
		q.queue = q.queue[1:]
		target := q.groups[item.message.GetTo()]
		q.mu.Unlock()
		if target == nil || item.groupID != "regional" {
			t.Fatalf("misrouted Raft message: %+v", item)
		}
		if err := target.Step(item.message); err != nil && !errors.Is(err, etcdraft.ErrStepPeerNotFound) {
			t.Fatalf("deliver Raft message %d -> %d: %v", item.message.GetFrom(), item.message.GetTo(), err)
		}
	}
	t.Fatal("Raft message queue did not quiesce")
}

func TestThreeNodeGroupElectsAndReplicatesOpaqueCommand(t *testing.T) {
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	states := make(map[uint64]*testState)
	for _, id := range []uint64{31, 32, 33} {
		root := t.TempDir()
		state := openTestState(t, root)
		group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			BootstrapPeers: []uint64{31, 32, 33}, State: state, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
		states[id] = state
		t.Cleanup(func() { _ = group.Close() })
	}
	transport.deliver(t)
	if err := transport.groups[31].Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	status, err := transport.groups[31].Status()
	if err != nil || status.Role != etcdraft.StateLeader {
		t.Fatalf("three-node Group leader status = %+v, %v", status, err)
	}
	if err := transport.groups[31].Submit([]byte("opaque-message-command")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for id, state := range states {
		if len(state.disk.Commands) != 1 || string(state.disk.Commands[0]) != "opaque-message-command" {
			t.Fatalf("Node %d applied commands = %q", id, state.disk.Commands)
		}
	}
	transport.blocked[33] = true
	if err := transport.groups[31].Submit([]byte("after-one-node-failed")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for _, id := range []uint64{31, 32} {
		if len(states[id].disk.Commands) != 2 {
			t.Fatalf("quorum Node %d did not commit with Node 33 absent", id)
		}
	}
	if len(states[33].disk.Commands) != 1 {
		t.Fatal("isolated Node unexpectedly applied the second command")
	}
	delete(transport.blocked, 33)
	for turn := 0; turn < 5 && len(states[33].disk.Commands) < 2; turn++ {
		if err := transport.groups[31].Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	if len(states[33].disk.Commands) != 2 {
		t.Fatal("rejoined Node did not catch up")
	}
}

func TestGroupReadIndexRequiresQuorumAndAppliedState(t *testing.T) {
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	states := make(map[uint64]*testState)
	for _, id := range []uint64{41, 42, 43} {
		root := t.TempDir()
		state := openTestState(t, root)
		group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			BootstrapPeers: []uint64{41, 42, 43}, State: state, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
		states[id] = state
		t.Cleanup(func() { _ = group.Close() })
	}
	transport.deliver(t)
	leader := transport.groups[41]
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	// An idle Group has no committed entry in the freshly elected term. The
	// first ReadIndex must establish that barrier itself instead of waiting for
	// an unrelated client proposal.
	freshCtx, freshCancel := context.WithTimeout(context.Background(), time.Second)
	freshDone := make(chan error, 1)
	go func() {
		_, err := leader.ReadIndex(freshCtx)
		freshDone <- err
	}()
	for {
		transport.deliver(t)
		select {
		case err := <-freshDone:
			freshCancel()
			if err != nil {
				t.Fatalf("fresh-term ReadIndex = %v", err)
			}
			goto freshReadComplete
		case <-freshCtx.Done():
			t.Fatalf("fresh-term ReadIndex did not establish a barrier: %v", freshCtx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}
freshReadComplete:
	if err := leader.Submit([]byte("read-visible")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	transport.blocked[42] = true
	transport.blocked[43] = true
	isolated, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := leader.ReadIndex(isolated); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("isolated ReadIndex = %v, want deadline exceeded", err)
	}
	delete(transport.blocked, 42)
	delete(transport.blocked, 43)
	type result struct {
		index uint64
		err   error
	}
	read := func(group *Group) result {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan result, 1)
		go func() {
			index, err := group.ReadIndex(ctx)
			done <- result{index: index, err: err}
		}()
		for {
			transport.deliver(t)
			select {
			case value := <-done:
				return value
			case <-ctx.Done():
				return result{err: ctx.Err()}
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
	for _, id := range []uint64{41, 42} {
		value := read(transport.groups[id])
		if value.err != nil || value.index == 0 || value.index > states[id].AppliedIndex() {
			t.Fatalf("Node %d ReadIndex = %+v, applied = %d", id, value, states[id].AppliedIndex())
		}
	}
	transport.blocked[42] = true
	transport.blocked[43] = true
	stopping, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { _, err := leader.ReadIndex(stopping); stopped <- err }()
	time.Sleep(10 * time.Millisecond)
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("pending ReadIndex after Close = %v", err)
		}
	case <-stopping.Done():
		t.Fatal("pending ReadIndex did not terminate when Group closed")
	}
}

func TestGroupTransfersLeadershipToLaggingVoter(t *testing.T) {
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	states := make(map[uint64]*testState)
	for _, id := range []uint64{51, 52, 53} {
		root := t.TempDir()
		state := openTestState(t, root)
		group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			BootstrapPeers: []uint64{51, 52, 53}, State: state, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
		states[id] = state
		t.Cleanup(func() { _ = group.Close() })
	}
	transport.deliver(t)
	oldLeader := transport.groups[51]
	if err := oldLeader.Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := oldLeader.TransferLeadership(ctx, 51); err == nil {
		t.Fatal("leadership transferred to self")
	}
	if _, err := oldLeader.TransferLeadership(ctx, 54); err == nil {
		t.Fatal("leadership transferred to non-voter")
	}
	transport.blocked[52] = true
	if err := oldLeader.Submit([]byte("before-transfer")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if len(states[52].disk.Commands) != 0 {
		t.Fatal("isolated target unexpectedly caught up")
	}
	delete(transport.blocked, 52)
	type result struct {
		status GroupStatus
		err    error
	}
	done := make(chan result, 1)
	go func() {
		status, err := oldLeader.TransferLeadership(ctx, 52)
		done <- result{status: status, err: err}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		transport.deliver(t)
		select {
		case value := <-done:
			if value.err != nil || value.status.LeaderID != 52 {
				t.Fatalf("leadership transfer = %+v", value)
			}
			goto transferred
		case <-ctx.Done():
			t.Fatalf("leadership transfer timed out: %v", ctx.Err())
		case <-ticker.C:
			for _, id := range []uint64{51, 52, 53} {
				if err := transport.groups[id].Tick(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
transferred:
	if len(states[52].disk.Commands) != 1 {
		t.Fatalf("new Leader missed pre-transfer command: %q", states[52].disk.Commands)
	}
	if err := transport.groups[52].Submit([]byte("after-transfer")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for _, id := range []uint64{51, 52, 53} {
		if len(states[id].disk.Commands) != 2 {
			t.Fatalf("Node %d commands after transfer = %q", id, states[id].disk.Commands)
		}
	}
}

func TestGroupLearnerJoinsPromotesAndMemberIsRemoved(t *testing.T) {
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	states := make(map[uint64]*testState)
	roots := make(map[uint64]string)
	for _, id := range []uint64{31, 32} {
		root := t.TempDir()
		roots[id] = root
		state := openTestState(t, root)
		group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			BootstrapPeers: []uint64{31, 32}, State: state, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
		states[id] = state
		t.Cleanup(func() { _ = group.Close() })
	}
	transport.deliver(t)
	leader := transport.groups[31]
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if status, err := leader.Status(); err != nil || status.Role != etcdraft.StateLeader {
		t.Fatalf("Group leader = %+v, %v", status, err)
	}
	if err := leader.PromoteLearner(33); err == nil {
		t.Fatal("unknown member promoted without first joining as learner")
	}
	if err := leader.RemoveMember(31); err == nil {
		t.Fatal("local leader removal was accepted")
	}
	root33 := t.TempDir()
	state33 := openTestState(t, root33)
	joining, err := OpenGroup(GroupOptions{StoreRoot: root33, GroupID: "regional", RaftID: 33,
		Joining: true, State: state33, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	transport.groups[33] = joining
	states[33] = state33
	t.Cleanup(func() { _ = joining.Close() })
	transport.blocked[33] = true
	if err := leader.AddLearner(33); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	conf, err := leader.Membership()
	if err != nil || !containsLearner(conf, 33) || containsVoter(conf, 33) {
		t.Fatalf("learner membership = %+v, %v", conf, err)
	}
	if _, err := leader.TransferLeadership(context.Background(), 33); err == nil {
		t.Fatal("leadership transferred to a learner")
	}
	if err := leader.PromoteLearner(33); err == nil {
		t.Fatal("learner was promoted before catching up")
	}
	delete(transport.blocked, 33)
	for turn := 0; turn < 10 && !containsLearner(state33.ConfState(), 33); turn++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	if !containsLearner(state33.ConfState(), 33) {
		t.Fatal("learner did not catch up after reconnecting")
	}
	if err := leader.Submit([]byte("learner-caught-up")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if len(state33.disk.Commands) != 1 || string(state33.disk.Commands[0]) != "learner-caught-up" {
		t.Fatalf("learner commands = %q", state33.disk.Commands)
	}
	if err := leader.PromoteLearner(33); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for _, id := range []uint64{31, 32, 33} {
		conf, err := transport.groups[id].Membership()
		if err != nil || !containsVoter(conf, 33) || containsLearner(conf, 33) {
			t.Fatalf("Node %d promoted membership = %+v, %v", id, conf, err)
		}
	}
	if err := leader.RemoveMember(32); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for _, id := range []uint64{31, 33} {
		conf, err := transport.groups[id].Membership()
		if err != nil || containsMember(conf, 32) {
			t.Fatalf("Node %d membership after removal = %+v, %v", id, conf, err)
		}
	}
	if err := leader.Submit([]byte("after-removal")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for _, id := range []uint64{31, 33} {
		if len(states[id].disk.Commands) != 2 {
			t.Fatalf("Node %d did not commit after removal: %q", id, states[id].disk.Commands)
		}
	}
	if err := transport.groups[32].Close(); err != nil {
		t.Fatal(err)
	}
	restartedState := openTestState(t, roots[32])
	if _, err := OpenGroup(GroupOptions{StoreRoot: roots[32], GroupID: "regional", RaftID: 32,
		State: restartedState, Transport: transport}); err == nil || !restartedState.closed {
		t.Fatalf("removed member restart = %v, StateMachine closed = %v", err, restartedState.closed)
	}
}
