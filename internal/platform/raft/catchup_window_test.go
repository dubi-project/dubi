package raft

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Persist hashes so the state-machine fixture does not rewrite all the large
// payloads on every application. Raft still persists and replicates each byte.
type catchupHashState struct{ *testState }

func (s *catchupHashState) Apply(index uint64, command []byte) error {
	digest := sha256.Sum256(command)
	return s.testState.Apply(index, digest[:])
}

func TestHistoricalCatchupBoundsUnacknowledgedBytesAndStillCompletes(t *testing.T) {
	for _, groupID := range []string{RootGroupID, "regional"} {
		t.Run(groupID, func(t *testing.T) {
			transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool), groupID: groupID}
			states := make(map[uint64]*catchupHashState)
			for _, id := range []uint64{71, 72} {
				root := t.TempDir()
				state := &catchupHashState{openTestState(t, root)}
				options := GroupOptions{StoreRoot: root, GroupID: groupID, RaftID: id, State: state, Transport: transport, Joining: id == 72}
				if id == 71 {
					options.BootstrapPeers = []uint64{71}
				}
				group, err := OpenGroup(options)
				if err != nil {
					t.Fatal(err)
				}
				states[id], transport.groups[id] = state, group
				t.Cleanup(func() { _ = group.Close() })
			}
			leader := transport.groups[71]
			if err := leader.Campaign(); err != nil {
				t.Fatal(err)
			}
			transport.deliver(t)
			for i := 0; i < 48; i++ {
				payload := bytes.Repeat([]byte{byte(i + 1)}, 256<<10)
				if err := leader.Submit(payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := leader.AddLearner(72); err != nil {
				t.Fatal(err)
			}
			var maxBurst uint64
			var windowFilled bool
			for turn := 0; ; turn++ {
				if turn > 1000 {
					t.Fatal("catchup did not quiesce")
				}
				if len(transport.queue) == 0 {
					break
				}
				item := transport.queue[0]
				transport.queue = transport.queue[1:]
				before := len(transport.queue)
				if err := transport.groups[item.message.GetTo()].Step(item.message); err != nil {
					t.Fatal(err)
				}
				var burst uint64
				for _, sent := range transport.queue[before:] {
					if sent.message.GetFrom() == 71 && sent.message.GetType() == raftpb.MessageType_MsgApp {
						for _, entry := range sent.message.GetEntries() {
							burst += uint64(proto.Size(entry))
						}
					}
				}
				maxBurst = max(maxBurst, burst)
				if maxBurst > 9<<20 {
					t.Fatalf("one acknowledgment generated %d bytes before dispatch", maxBurst)
				}
				status := leader.raw.Status()
				if p, ok := status.Progress[72]; ok && p.Inflights.Full() && p.Match < status.GetCommit() {
					windowFilled = true
					// The original voter quorum remains writable while learner
					// acknowledgements are waiting in the transport queue.
					if err := leader.Submit([]byte("write-during-catchup")); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !windowFilled || maxBurst < 4<<20 {
				t.Fatal("large backlog did not exercise byte flow control", windowFilled, maxBurst)
			}
			left, right := states[71].disk, states[72].disk
			if left.Applied != right.Applied || len(left.Commands) != len(right.Commands) {
				t.Fatal("learner did not reach committed state")
			}
			for i := range left.Commands {
				if !bytes.Equal(left.Commands[i], right.Commands[i]) {
					t.Fatalf("replicated payload %d differs", i)
				}
			}
			if err := leader.PromoteLearner(72); err != nil {
				t.Fatal(err)
			}
			transport.deliver(t)
		})
	}
}
