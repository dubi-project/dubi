package rootstate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestRegionGrowthProgressOwnedCutAndEligibility(t *testing.T) {
	s, _, p := growthFixture(t)
	check := func() RegionProgress {
		t.Helper()
		v, found, err := s.RegionProgress(p.GroupID)
		if err != nil || !found || !v.Eligible || v.AppliedIndex != s.AppliedIndex() {
			t.Fatal(v, found, err)
		}
		return v
	}
	v := check()
	if v.Current.Generation != 1 || v.Pending.Generation != 2 || v.PendingGrowth != nil || v.PendingTask != nil {
		t.Fatal(v)
	}
	v.Current.Nodes[0], v.Pending.Nodes[0], v.Group.Members[0].RaftID = 99, 99, 99
	v = check()
	if v.Current.Nodes[0] != 1 || v.Pending.Nodes[0] != 1 || v.Group.Members[0].RaftID != 1 {
		t.Fatal("aliased progress", v)
	}
	applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &p})
	v = check()
	if v.PendingGrowth == nil || v.PendingTask == nil || v.PendingTask.State != GroupChangePending {
		t.Fatal(v)
	}
	v.PendingGrowth.Seal.Members[0].RaftID, v.PendingTask.Members[0].RaftID = 99, 99
	v = check()
	if v.PendingGrowth.Seal.Members[0].RaftID != 1 || v.PendingTask.Members[0].RaftID != 1 {
		t.Fatal("aliased task")
	}
	stable := s.ConfState()
	joint := s.ConfState()
	joint.VotersOutgoing = append(joint.VotersOutgoing, 1)
	if err := s.ApplyMembership(s.AppliedIndex()+1, joint); err != nil {
		t.Fatal(err)
	}
	if v, found, err := s.RegionProgress(p.GroupID); err != nil || !found || v.Eligible {
		t.Fatal(v, found, err)
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, stable); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: p.GroupID, ChangeID: p.ChangeID}})
	v = check()
	if v.Group.Epoch != 3 || v.PendingTask.State != GroupChangeCompleted {
		t.Fatal(v)
	}
	applyRootFileCommand(t, s, cohortConfirm(2, 3, 100003))
	v = check()
	if v.Pending != nil || v.PendingTask != nil || v.CurrentGrowth == nil || !reflect.DeepEqual(v.CurrentGrowth.Seal, p) {
		t.Fatal(v)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.RegionProgress(p.GroupID); err == nil {
		t.Fatal("closed progress readable")
	}
}

func TestRegionGrowthProgressRejectsCorruptLinksAndUncertainty(t *testing.T) {
	for _, mode := range []string{"missing-task", "missing-record", "active-task", "group-epoch", "uncertain"} {
		t.Run(mode, func(t *testing.T) {
			s, _, p := growthFixture(t)
			applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &p})
			var err error
			switch mode {
			case "missing-task":
				err = s.db.Delete(groupChangeKey(p.ChangeID), pebble.Sync)
			case "missing-record":
				err = s.db.Delete(regionGrowthKey(p.RegionID, p.Generation), pebble.Sync)
			case "active-task":
				err = s.db.Set(activeGroupChangeKey(p.GroupID), []byte("other"), pebble.Sync)
			case "group-epoch":
				group, _, _ := s.Group(p.GroupID)
				group.Epoch++
				batch := s.db.NewBatch()
				defer batch.Close()
				if err := setJSON(batch, groupKey(group.GroupID), group); err != nil {
					t.Fatal(err)
				}
				err = batch.Commit(pebble.Sync)
			case "uncertain":
				s.commit = func(*pebble.Batch) error { return errors.New("uncertain") }
				err = s.Advance(s.AppliedIndex() + 1)
				if err == nil {
					t.Fatal("missing injected failure")
				}
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := s.RegionProgress(p.GroupID); err == nil || found {
				t.Fatal("invalid progress admitted", found, err)
			}
		})
	}
}
