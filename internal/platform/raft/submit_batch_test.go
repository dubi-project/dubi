package raft

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
)

func batchTestGroup(t *testing.T) (*Group, *testState, string) {
	t.Helper()
	root := t.TempDir()
	s := openTestState(t, root)
	g, e := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "batch", RaftID: 61, BootstrapPeers: []uint64{61}, State: &validatingTestState{s}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = g.Close() })
	if e = g.Campaign(); e != nil {
		t.Fatal(e)
	}
	return g, s, root
}
func TestSubmitBatchIndependentRowsAndRestart(t *testing.T) {
	g, s, root := batchTestGroup(t)
	denied := errors.New("pressure")
	calls := 0
	g.proposalGuard = func() error {
		calls++
		if calls == 2 {
			return denied
		}
		return nil
	}
	rows, e := g.SubmitBatch([][]byte{[]byte("valid:first"), []byte("bad"), nil, []byte("valid:denied"), []byte("valid:last")})
	if e != nil || len(rows) != 5 || rows[0] != nil || rows[1] == nil || rows[2] == nil || !errors.Is(rows[3], denied) || rows[4] != nil {
		t.Fatal(rows, e)
	}
	if calls != 3 || len(s.disk.Commands) != 2 || string(s.disk.Commands[1]) != "valid:last" {
		t.Fatal(calls, s.disk.Commands)
	}
	if e := g.Close(); e != nil {
		t.Fatal(e)
	}
	restored := openTestState(t, root)
	again, e := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "batch", RaftID: 61, State: &validatingTestState{restored}})
	if e != nil {
		t.Fatal(e)
	}
	defer again.Close()
	if len(restored.disk.Commands) != 2 || !bytes.Equal(restored.disk.Commands[0], []byte("valid:first")) {
		t.Fatal(restored.disk.Commands)
	}
}
func TestSubmitBatchBoundsAndFailedGroup(t *testing.T) {
	g, s, _ := batchTestGroup(t)
	for _, cmds := range [][][]byte{nil, make([][]byte, 33), {bytes.Repeat([]byte("x"), SubmitBatchBytes+1)}, {bytes.Repeat([]byte("x"), SubmitBatchBytes), []byte("x")}} {
		if _, e := g.SubmitBatch(cmds); e == nil {
			t.Fatal("oversized batch admitted")
		}
	}
	if len(s.disk.Commands) != 0 {
		t.Fatal("invalid envelope changed state")
	}
	s.fail = true
	if _, e := g.SubmitBatch([][]byte{[]byte("valid:a"), []byte("valid:b")}); e == nil {
		t.Fatal("failed Apply reported success")
	}
	if _, e := g.SubmitBatch([][]byte{[]byte("valid:c")}); e == nil {
		t.Fatal("failed Group accepted later batch")
	}
}
func TestSubmitBatchFollowerQuorumAndRecovery(t *testing.T) {
	q := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	states := map[uint64]*testState{}
	for _, id := range []uint64{31, 32, 33} {
		dir := t.TempDir()
		s := openTestState(t, dir)
		g, e := OpenGroup(GroupOptions{StoreRoot: dir, GroupID: "regional", RaftID: id, BootstrapPeers: []uint64{31, 32, 33}, State: s, Transport: q})
		if e != nil {
			t.Fatal(e)
		}
		q.groups[id] = g
		states[id] = s
		t.Cleanup(func() { _ = g.Close() })
	}
	q.deliver(t)
	if e := q.groups[31].Campaign(); e != nil {
		t.Fatal(e)
	}
	q.deliver(t)
	cmds := [][]byte{[]byte("first"), []byte("second")}
	rows, e := q.groups[32].SubmitBatch(cmds)
	if e != nil || len(rows) != 2 || rows[0] != nil || rows[1] != nil {
		t.Fatal(rows, e)
	}
	if len(states[32].disk.Commands) != 0 {
		t.Fatal("follower admission claimed application before quorum")
	}
	q.deliver(t)
	for _, s := range states {
		if len(s.disk.Commands) != 2 {
			t.Fatal(s.disk.Commands)
		}
	}
	q.blocked[32], q.blocked[33] = true, true
	rows, e = q.groups[31].SubmitBatch([][]byte{[]byte("pending-a"), []byte("pending-b")})
	if e != nil || rows[0] != nil || rows[1] != nil {
		t.Fatal(rows, e)
	}
	if len(states[31].disk.Commands) != 2 {
		t.Fatal("batch committed without quorum")
	}
	delete(q.blocked, 32)
	delete(q.blocked, 33)
	for i := 0; i < 20 && len(states[33].disk.Commands) < 4; i++ {
		if e := q.groups[31].Tick(); e != nil {
			t.Fatal(e)
		}
		q.deliver(t)
	}
	for _, s := range states {
		if len(s.disk.Commands) != 4 || string(s.disk.Commands[3]) != "pending-b" {
			t.Fatal(s.disk.Commands)
		}
	}
}
func TestSubmitBatchDiskDiagnostic(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		g, _, _ := batchTestGroup(t)
		cmds := make([][]byte, 32)
		for i := range cmds {
			cmds[i] = []byte(fmt.Sprintf("valid:%d", i))
		}
		start := time.Now()
		if mode == "single" {
			for _, c := range cmds {
				if e := g.Submit(c); e != nil {
					t.Fatal(e)
				}
			}
		} else {
			rows, e := g.SubmitBatch(cmds)
			if e != nil {
				t.Fatal(e)
			}
			for _, e := range rows {
				if e != nil {
					t.Fatal(e)
				}
			}
		}
		t.Logf("mode=%s commands=32 elapsed_us=%d", mode, time.Since(start).Microseconds())
	}
}
