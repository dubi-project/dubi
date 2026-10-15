package rootstate

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRootUncertainCommitBlocksDirectoryReadsUntilReopen(t *testing.T) {
	command, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "tenant", Type: RegisterTenant, Tenant: &Tenant{TenantID: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Apply(1, command); err != nil {
		t.Fatal(err)
	}
	conf := &raftpb.ConfState{Voters: []uint64{1}}
	if err := source.ApplyMembership(2, conf); err != nil {
		t.Fatal(err)
	}
	image, index, snapshotConf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, persisted := range []bool{false, true} {
		for _, operation := range []string{"apply", "membership", "restore"} {
			t.Run(fmt.Sprintf("%s/persisted=%v", operation, persisted), func(t *testing.T) {
				root := t.TempDir()
				state, err := Open(root)
				if err != nil {
					t.Fatal(err)
				}
				defer state.Close()
				if err := state.Advance(1); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("injected uncertain Root commit")
				state.commit = func(batch *pebble.Batch) error {
					if persisted {
						if err := batch.Commit(pebble.Sync); err != nil {
							return err
						}
					}
					return injected
				}
				switch operation {
				case "apply":
					err = state.Apply(2, command)
				case "membership":
					err = state.ApplyMembership(2, conf)
				case "restore":
					err = state.Restore(index, image, snapshotConf)
				}
				if !errors.Is(err, injected) {
					t.Fatal("commit error not preserved", err)
				}
				if state.AppliedIndex() != 1 || len(state.ConfState().GetVoters()) != 0 {
					t.Fatal("uncertain in-memory state advanced")
				}
				checks := map[string]func() error{
					"group-initial-cohort": func() error { _, _, err := state.GroupInitialCohort("regional"); return err },
					"region-cohort":        func() error { _, _, err := state.RegionCohort("west", 1); return err },
					"region-cohort-head":   func() error { _, _, err := state.RegionCohortHead("west"); return err },
					"root-position":        func() error { _, _, err := state.RootPosition(); return err },
					"root-routing":         func() error { _, err := state.RootRoutingView(4096); return err },
					"root-member":          func() error { _, _, err := state.RootMember(1); return err },
					"root-members":         func() error { _, _, err := state.RootMembers(1, 2); return err },
					"root-join":            func() error { _, _, err := state.RootJoin("join_00000000000000000000000000000001"); return err },
					"pending-root-joins":   func() error { _, err := state.PendingRootJoins(); return err },
					"root-membership":      func() error { _, err := state.RootMembership(); return err },
					"result":               func() error { _, _, err := state.Result("tenant"); return err },
					"command-result":       func() error { _, _, err := state.ResultForCommand(command); return err },
					"node":                 func() error { _, _, err := state.Node(1); return err },
					"group":                func() error { _, _, err := state.Group("data-1"); return err },
					"groups":               func() error { _, err := state.Groups(); return err },
					"change":               func() error { _, _, err := state.GroupChange("change"); return err },
					"active-change":        func() error { _, _, err := state.ActiveGroupChange("data-1"); return err },
					"pending-changes":      func() error { _, err := state.PendingGroupChanges(); return err },
					"tenant":               func() error { _, _, err := state.Tenant("a"); return err },
					"dataset":              func() error { _, _, err := state.Dataset("a", "record", "items"); return err },
					"range":                func() error { _, _, err := state.Range("range"); return err },
					"route":                func() error { _, _, err := state.Route("a", "record", "items", nil); return err },
					"pending-ranges":       func() error { _, err := state.PendingRanges(); return err },
					"snapshot":             func() error { _, _, _, err := state.Snapshot(); return err },
					"restore":              func() error { return state.Restore(index, image, snapshotConf) },
					"advance":              func() error { return state.Advance(2) },
					"apply":                func() error { return state.Apply(2, command) },
					"membership":           func() error { return state.ApplyMembership(2, conf) },
				}
				for name, check := range checks {
					if err := check(); !errors.Is(err, injected) {
						t.Errorf("%s ignored uncertain commit: %v", name, err)
					}
				}
				if err := state.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(root)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				wantIndex := uint64(1)
				if persisted {
					wantIndex = 2
				}
				if reopened.AppliedIndex() != wantIndex {
					t.Fatal("recovery did not use persistent applied index")
				}
				_, found, err := reopened.Tenant("a")
				if err != nil || found != (persisted && operation != "membership") {
					t.Fatal("wrong recovered directory", found, err)
				}
				if persisted && operation != "apply" && !proto.Equal(reopened.ConfState(), conf) {
					t.Fatal("lost persisted membership")
				}
				if !persisted {
					if err := reopened.Apply(2, command); err != nil {
						t.Fatal("reopen did not clear uncertainty", err)
					}
				}
			})
		}
	}
}

func TestRootRestoreRejectsOversizedInlineImageBeforeDecode(t *testing.T) {
	state, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.Restore(1, make([]byte, maxSnapshotBytes+1), &raftpb.ConfState{}); err == nil || !strings.Contains(err.Error(), "inline limit") {
		t.Fatal(err)
	}
	if state.AppliedIndex() != 0 {
		t.Fatal("oversized image changed state")
	}
}
