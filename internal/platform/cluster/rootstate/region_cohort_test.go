package rootstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

func cohortFixture(t *testing.T) (*State, string) {
	t.Helper()
	root := t.TempDir()
	s := rootFileState(t, root)
	if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2, 3, 4, 5, 6, 7, 8}}); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 8; id++ {
		region := "west"
		if id >= 6 {
			region = "east"
		}
		applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: region, Address: fmt.Sprintf("127.0.0.1:%d", 10000+id)}})
	}
	for _, group := range []string{"regional-west", "other"} {
		applyRootFileCommand(t, s, Command{Type: RegisterGroup, Group: &Group{GroupID: group, StateKind: GroupStateCapability}})
	}
	cohortPublish(t, s, 1, []uint64{1, 2, 3})
	return s, root
}

func cohortPublish(t *testing.T, s *State, epoch uint64, ids []uint64) {
	t.Helper()
	members := make([]GroupMember, 0, len(ids))
	for i, id := range ids {
		role := GroupMemberVoter
		if i >= 3 {
			role = GroupMemberLearner
		}
		members = append(members, GroupMember{RaftID: id, Role: role})
	}
	if head, confirmed, err := s.confirmedRegion("regional-west"); err != nil {
		t.Fatal(err)
	} else if confirmed {
		previous, _, _ := s.RegionCohort("west", head.CurrentGeneration)
		next, _, _ := s.RegionCohort("west", head.PendingGeneration)
		seal := RegionGrowthSeal{RegionID: "west", GroupID: "regional-west", Generation: next.Generation, PlannedIndex: next.PlannedIndex, ExpectedEpoch: epoch,
			PreviousFenceRequestID: previous.GroupFenceRequestID, PreviousFenceIndex: previous.GroupFenceIndex, PreviousMembershipIndex: 1,
			SealRequestID: fmt.Sprintf("seal-%d", next.Generation), SealIndex: previous.GroupFenceIndex + 1, ChangeID: fmt.Sprintf("grow-%d", next.Generation), Members: members}
		applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &seal})
		applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: seal.GroupID, ChangeID: seal.ChangeID}})
		return
	}
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "regional-west", ExpectedEpoch: epoch, Members: members}})
}

func cohortPlan(generation uint64, nodes ...uint64) Command {
	return Command{Type: PlanRegionCohort, RegionPlan: &RegionCohortPlan{RegionID: "west", ExpectedGeneration: generation, GroupID: "regional-west", Nodes: nodes}}
}

func cohortConfirm(generation, epoch, fence uint64) Command {
	return Command{Type: ConfirmRegionCohort, RegionConfirmation: &RegionCohortConfirmation{RegionID: "west", Generation: generation, GroupID: "regional-west", GroupEpoch: epoch, GroupFenceRequestID: fmt.Sprintf("fence-%d", generation), GroupFenceIndex: fence}}
}

func cohortReject(t *testing.T, s *State, cmd Command) {
	t.Helper()
	cmd.Version, cmd.RequestID = FormatVersion, fmt.Sprintf("reject-%d", s.AppliedIndex()+1)
	index := s.AppliedIndex()
	if result := applyDirect(t, s, &index, cmd); result.Error == "" {
		t.Fatal("unexpectedly accepted", cmd.Type)
	}
}

func TestRegionCohortGenerationsPersistWithoutChangingConfirmedDenominator(t *testing.T) {
	s, root := cohortFixture(t)
	plan := cohortPlan(0, 1, 2, 3)
	plan.Version, plan.RequestID = FormatVersion, "stable-plan"
	index := s.AppliedIndex()
	first := applyDirect(t, s, &index, plan)
	if first.Error != "" {
		t.Fatal(first)
	}
	if retry := applyDirect(t, s, &index, plan); retry != first {
		t.Fatal("lost original idempotent result", retry, first)
	}
	h, found, err := s.RegionCohortHead("west")
	if err != nil || !found || h.CurrentGeneration != 0 || h.PendingGeneration != 1 {
		t.Fatal(h, found, err)
	}
	c, _, err := s.RegionCohort("west", 1)
	if err != nil || c.State != RegionCohortPlanned || c.PlannedIndex != first.Index {
		t.Fatal(c, err)
	}
	c.Nodes[0] = 900 // Returned slices must not alias persistent state.
	cohortReject(t, s, cohortPlan(0, 1, 2, 3, 4))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = rootFileState(t, root)
	c, _, err = s.RegionCohort("west", 1)
	if err != nil || !slices.Equal(c.Nodes, []uint64{1, 2, 3}) || c.State != RegionCohortPlanned {
		t.Fatal(c, err)
	}
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000)) // Group index is not a Root index.
	old, _, _ := s.RegionCohort("west", 1)
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000)) // Different RequestID, same immutable assertion.
	cohortReject(t, s, cohortConfirm(1, 2, 100001))
	cohortReject(t, s, cohortPlan(0, 1, 2, 3, 4))
	cohortReject(t, s, cohortPlan(1, 1, 2, 3))
	cohortReject(t, s, cohortPlan(1, 1, 2, 4)) // Replacement is not an addition.
	move := cohortPlan(1, 1, 2, 3, 4)
	move.RegionPlan.GroupID = "other"
	cohortReject(t, s, move)
	applyRootFileCommand(t, s, cohortPlan(1, 1, 2, 3, 4))
	h, _, _ = s.RegionCohortHead("west")
	if h.CurrentGeneration != 1 || h.PendingGeneration != 2 {
		t.Fatal("plan changed confirmed denominator", h)
	}
	cohortReject(t, s, cohortConfirm(2, 2, 100001)) // New Node not yet published.
	cohortPublish(t, s, 2, []uint64{1, 2, 3, 4})
	cohortReject(t, s, cohortConfirm(2, 2, 100001)) // Stale Group epoch.
	cohortReject(t, s, cohortConfirm(2, 3, 100000)) // Nonadvancing Group fence.
	applyRootFileCommand(t, s, cohortConfirm(2, 3, 100003))
	h, _, _ = s.RegionCohortHead("west")
	if h.CurrentGeneration != 2 || h.PendingGeneration != 0 {
		t.Fatal(h)
	}
	after, _, _ := s.RegionCohort("west", 1)
	if !reflect.DeepEqual(old, after) {
		t.Fatal("historical denominator changed", old, after)
	}
	// Late retries of older confirmations neither roll back nor overwrite head.
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))
	h, _, _ = s.RegionCohortHead("west")
	if h.CurrentGeneration != 2 {
		t.Fatal("late retry regressed head", h)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = rootFileState(t, root)
	h, _, err = s.RegionCohortHead("west")
	if err != nil || h.CurrentGeneration != 2 || h.PendingGeneration != 0 {
		t.Fatal(h, err)
	}
}

func TestRegionCohortRejectsUnboundParticipantsAndUnstableGroup(t *testing.T) {
	s, _ := cohortFixture(t)
	cohortReject(t, s, cohortPlan(0, 1, 2, 9))
	cohortReject(t, s, cohortPlan(0, 1, 2, 6))
	missing := cohortPlan(0, 1, 2, 3)
	missing.RegionPlan.GroupID = "missing"
	cohortReject(t, s, missing)
	cohortReject(t, s, cohortConfirm(1, 2, 50))
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	east := cohortPlan(0, 6, 7, 8)
	east.RegionPlan.RegionID = "east"
	cohortReject(t, s, east) // A Group cannot be assigned to two Regions.
	bad := cohortConfirm(1, 2, 50)
	bad.RegionConfirmation.GroupID = "other"
	cohortReject(t, s, bad)
	// A pending membership task fences publication even if the descriptor matches.
	members := []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}}
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "change", GroupID: "regional-west", ExpectedEpoch: 2, Members: members}})
	cohortReject(t, s, cohortConfirm(1, 2, 50))
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{ChangeID: "change", GroupID: "regional-west"}})
	cohortReject(t, s, cohortConfirm(1, 2, 50))
	applyRootFileCommand(t, s, cohortConfirm(1, 3, 50))
}

func TestRegionCohortAllowsOneNodeRegion(t *testing.T) {
	s, _ := cohortFixture(t)
	plan := cohortPlan(0, 6)
	plan.RegionPlan.RegionID, plan.RegionPlan.GroupID = "east", "other"
	applyRootFileCommand(t, s, plan)
	members := []GroupMember{{RaftID: 6, Role: GroupMemberVoter}}
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "other", ExpectedEpoch: 1, Members: members}})
	applyRootFileCommand(t, s, Command{Type: ConfirmRegionCohort, RegionConfirmation: &RegionCohortConfirmation{RegionID: "east", Generation: 1, GroupID: "other", GroupEpoch: 2, GroupFenceRequestID: "east-fence", GroupFenceIndex: 50}})
	c, _, err := s.RegionCohort("east", 1)
	if err != nil || len(c.Nodes) != 1 || c.State != RegionCohortConfirmed {
		t.Fatal("one-Node Region was not confirmed", c, err)
	}
}

func TestRegionCohortRequiresThreeLocalVotersButCountsLearnerDataNodes(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3, 4))
	members := []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberLearner}, {RaftID: 4, Role: GroupMemberLearner}}
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "regional-west", ExpectedEpoch: 2, Members: members}})
	cohortReject(t, s, cohortConfirm(1, 3, 50))
	cohortPublish(t, s, 3, []uint64{1, 2, 3, 4})
	applyRootFileCommand(t, s, cohortConfirm(1, 4, 51))
	c, _, err := s.RegionCohort("west", 1)
	if err != nil || len(c.Nodes) != 4 {
		t.Fatal("learner omitted from data denominator", c, err)
	}
}

func TestRegionCohortCommandBounds(t *testing.T) {
	nodes := make([]uint64, MaxRegionCohortNodes)
	for i := range nodes {
		nodes[i] = uint64(i + 1)
	}
	max := cohortPlan(0, nodes...)
	max.Version, max.RequestID = FormatVersion, "maximum"
	if _, err := EncodeCommand(max); err != nil {
		t.Fatal("valid bounded maximum rejected", err)
	}
	for name, nodes := range map[string][]uint64{"empty": nil, "zero": {0, 1}, "duplicate": {1, 1}, "unsorted": {2, 1}, "large": make([]uint64, MaxRegionCohortNodes+1)} {
		t.Run(name, func(t *testing.T) {
			cmd := cohortPlan(0, nodes...)
			cmd.Version, cmd.RequestID = FormatVersion, "plan"
			if _, err := EncodeCommand(cmd); err == nil {
				t.Fatal("invalid nodes accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*Command){
		"overflow":      func(c *Command) { c.RegionPlan.ExpectedGeneration = math.MaxUint64 },
		"root":          func(c *Command) { c.RegionPlan.GroupID = "root" },
		"bad-region":    func(c *Command) { c.RegionPlan.RegionID = "west/escape" },
		"extra-payload": func(c *Command) { c.Tenant = &Tenant{TenantID: "x"} },
		"wrong-type":    func(c *Command) { c.Type = ConfirmRegionCohort },
	} {
		t.Run(name, func(t *testing.T) {
			c := cohortPlan(0, 1, 2, 3)
			c.Version, c.RequestID = FormatVersion, "plan"
			mutate(&c)
			if _, err := EncodeCommand(c); err == nil {
				t.Fatal("invalid plan accepted")
			}
		})
	}
}

// Real Root Raft persistence, not a claim about regional Group/data replication.
type cohortDiscardTransport struct{}

func (cohortDiscardTransport) Send(string, *raftpb.Message) error { return nil }

func TestRegionCohortRootRaftReplay(t *testing.T) {
	root := t.TempDir()
	open := func() (*State, *raft.Group) {
		s, err := Open(filepath.Join(root, "state"))
		if err != nil {
			t.Fatal(err)
		}
		g, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: root, GroupID: raft.RootGroupID, RaftID: 1, BootstrapPeers: []uint64{1}, State: s, Transport: cohortDiscardTransport{}})
		if err != nil {
			s.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { g.Close(); s.Close() })
		if err := g.Campaign(); err != nil {
			t.Fatal(err)
		}
		return s, g
	}
	s, g := open()
	propose := func(command Command) Result {
		command.Version, command.RequestID = FormatVersion, fmt.Sprintf("raft-%d", s.AppliedIndex()+1)
		submit(t, g, command)
		result, found, err := s.Result(command.RequestID)
		if err != nil || !found || result.Error != "" {
			t.Fatal(result, found, err)
		}
		return result
	}
	for id := uint64(1); id <= 3; id++ {
		propose(Command{Type: RegisterNode, Node: &Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "west", Address: fmt.Sprintf("127.0.0.1:%d", 10000+id)}})
	}
	propose(Command{Type: RegisterGroup, Group: &Group{GroupID: "regional-west", StateKind: GroupStateCapability}})
	for _, id := range []uint64{2, 3} {
		if err := g.AddLearner(id); err != nil {
			t.Fatal(err)
		}
	}
	propose(cohortPlan(0, 1, 2, 3)) // Plan may precede initial Group publication.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, g = open()
	h, found, err := s.RegionCohortHead("west")
	if err != nil || !found || h.CurrentGeneration != 0 || h.PendingGeneration != 1 {
		t.Fatal(h, found, err)
	}
	propose(Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "regional-west", ExpectedEpoch: 1, Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}}}})
	result := propose(cohortConfirm(1, 2, 100000))
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, g = open()
	c, found, err := s.RegionCohort("west", 1)
	if err != nil || !found || c.State != RegionCohortConfirmed || c.ConfirmedIndex != result.Index {
		t.Fatal(c, found, err)
	}
	propose(Command{Type: RegisterNode, Node: &Node{NodeID: fmt.Sprintf("node_%032x", 4), RaftID: 4, RegionID: "west", Address: "127.0.0.1:10004"}})
	if err := g.AddLearner(4); err != nil {
		t.Fatal(err)
	}
	propose(cohortPlan(1, 1, 2, 3, 4))
	next, _, _ := s.RegionCohort("west", 2)
	seal := RegionGrowthSeal{RegionID: "west", GroupID: "regional-west", Generation: 2, PlannedIndex: next.PlannedIndex, ExpectedEpoch: 2,
		PreviousFenceRequestID: c.GroupFenceRequestID, PreviousFenceIndex: c.GroupFenceIndex, PreviousMembershipIndex: 1, SealRequestID: "seal-2", SealIndex: 100001, ChangeID: "growth-2",
		Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}, {RaftID: 4, Role: GroupMemberLearner}}}
	propose(Command{Type: AuthorizeRegionGrowth, RegionGrowth: &seal})
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, g = open()
	if growth, found, err := s.RegionGrowth("west", 2); err != nil || !found || !reflect.DeepEqual(growth.Seal, seal) {
		t.Fatal(growth, found, err)
	}
	if task, found, err := s.ActiveGroupChange("regional-west"); err != nil || !found || task.ChangeID != "growth-2" {
		t.Fatal(task, found, err)
	}
	propose(Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: "regional-west", ChangeID: "growth-2"}})
	propose(cohortConfirm(2, 3, 100003))
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, g = open()
	if h, found, err := s.RegionCohortHead("west"); err != nil || !found || h.CurrentGeneration != 2 || h.PendingGeneration != 0 {
		t.Fatal(h, found, err)
	}
	if _, found, err := s.ActiveGroupChange("regional-west"); err != nil || found {
		t.Fatal(found, err)
	}
}

func TestRegionCohortUncertainCommitIsAtomicOnReopen(t *testing.T) {
	for _, operation := range []string{"plan", "confirm"} {
		for _, persisted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", operation, persisted), func(t *testing.T) {
				s, root := cohortFixture(t)
				cmd := cohortPlan(0, 1, 2, 3)
				if operation == "confirm" {
					applyRootFileCommand(t, s, cmd)
					cmd = cohortConfirm(1, 2, 90)
				}
				cmd.Version, cmd.RequestID = FormatVersion, "uncertain"
				data, err := EncodeCommand(cmd)
				if err != nil {
					t.Fatal(err)
				}
				before := s.AppliedIndex()
				injected := errors.New("uncertain cohort commit")
				s.commit = func(b *pebble.Batch) error {
					if persisted {
						if err := b.Commit(pebble.Sync); err != nil {
							return err
						}
					}
					return injected
				}
				if err := s.Apply(before+1, data); !errors.Is(err, injected) {
					t.Fatal(err)
				}
				if _, _, err := s.RegionCohortHead("west"); !errors.Is(err, injected) {
					t.Fatal("head not fenced", err)
				}
				if _, _, err := s.RegionCohort("west", 1); !errors.Is(err, injected) {
					t.Fatal("generation not fenced", err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = rootFileState(t, root)
				result, found, err := s.ResultForCommand(data)
				if err != nil || found != persisted || result.Error != "" {
					t.Fatal(result, found, err)
				}
				h, found, err := s.RegionCohortHead("west")
				if err != nil {
					t.Fatal(err)
				}
				if operation == "plan" && found != persisted {
					t.Fatal("plan head partially committed", h, found)
				}
				if operation == "confirm" && (!found || (h.CurrentGeneration == 1) != persisted || (h.PendingGeneration == 0) != persisted) {
					t.Fatal("confirmation partially committed", h, found)
				}
				if !persisted {
					if err := s.Apply(before+1, data); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestRegionCohortSnapshotRoundtripAndCorruption(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))
	applyRootFileCommand(t, s, cohortPlan(1, 1, 2, 3, 4))
	cohortPublish(t, s, 2, []uint64{1, 2, 3, 4})
	applyRootFileCommand(t, s, cohortConfirm(2, 3, 100003))
	applyRootFileCommand(t, s, cohortPlan(2, 1, 2, 3, 4, 5))
	inline, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var original snapshotImage
	if err := json.Unmarshal(inline, &original); err != nil {
		t.Fatal(err)
	}
	store := rootArtifacts(t, t.TempDir())
	file, _, _ := buildRootFile(t, s, store)
	for _, format := range []string{"inline", "file"} {
		t.Run(format, func(t *testing.T) {
			dest := rootFileState(t, t.TempDir())
			if format == "inline" {
				err = dest.Restore(index, inline, conf)
			} else {
				err = dest.RestoreFrom(context.Background(), store, index, file, conf)
			}
			if err != nil {
				t.Fatal(err)
			}
			h, found, err := dest.RegionCohortHead("west")
			if err != nil || !found || h.CurrentGeneration != 2 || h.PendingGeneration != 3 {
				t.Fatal(h, found, err)
			}
			for g := uint64(1); g <= 3; g++ {
				a, _, _ := s.RegionCohort("west", g)
				b, found, err := dest.RegionCohort("west", g)
				if err != nil || !found || !reflect.DeepEqual(a, b) {
					t.Fatal(a, b, found, err)
				}
			}
		})
	}
	mutateCohort := func(m inlineRootView, g uint64, f func(*RegionCohort)) {
		key := string(regionCohortKey("west", g))
		var c RegionCohort
		json.Unmarshal(m[key], &c)
		f(&c)
		m[key], _ = json.Marshal(c)
	}
	mutateGrowth := func(m inlineRootView, f func(*RegionGrowth)) {
		key := string(regionGrowthKey("west", 2))
		var g RegionGrowth
		json.Unmarshal(m[key], &g)
		f(&g)
		m[key], _ = json.Marshal(g)
	}
	cases := map[string]func(inlineRootView){
		"missing-growth":              func(m inlineRootView) { delete(m, string(regionGrowthKey("west", 2))) },
		"missing-growth-task":         func(m inlineRootView) { delete(m, string(groupChangeKey("grow-2"))) },
		"growth-future-authorization": func(m inlineRootView) { mutateGrowth(m, func(g *RegionGrowth) { g.AuthorizedIndex = index + 1 }) },
		"growth-wrong-plan":           func(m inlineRootView) { mutateGrowth(m, func(g *RegionGrowth) { g.Seal.PlannedIndex++ }) },
		"growth-seal-after-candidate": func(m inlineRootView) { mutateGrowth(m, func(g *RegionGrowth) { g.Seal.SealIndex = 100003 }) },
		"growth-changed-old-role": func(m inlineRootView) {
			mutateGrowth(m, func(g *RegionGrowth) { g.BaseMembers[0].Role = GroupMemberLearner })
		},
		"growth-missing-result": func(m inlineRootView) {
			mutateGrowth(m, func(g *RegionGrowth) { delete(m, string(resultKey(g.RequestID))) })
		},
		"growth-bad-result-hash": func(m inlineRootView) {
			mutateGrowth(m, func(g *RegionGrowth) { g.ResultSHA256 = fmt.Sprintf("%064x", 0) })
		},
		"growth-task-annotations-removed": func(m inlineRootView) {
			key := string(groupChangeKey("grow-2"))
			var task GroupChange
			json.Unmarshal(m[key], &task)
			task.RegionID, task.GrowthGeneration = "", 0
			m[key], _ = json.Marshal(task)
		},
		"later-generation-formation": func(m inlineRootView) { mutateCohort(m, 2, func(c *RegionCohort) { c.Formation = true }) },
		"missing-history":            func(m inlineRootView) { delete(m, string(regionCohortKey("west", 1))) },
		"missing-head":               func(m inlineRootView) { delete(m, string(regionCohortHeadKey("west"))) },
		"missing-group-owner":        func(m inlineRootView) { delete(m, string(regionCohortGroupKey("regional-west"))) },
		"wrong-group-owner":          func(m inlineRootView) { m[string(regionCohortGroupKey("regional-west"))] = []byte("east") },
		"missing-node":               func(m inlineRootView) { delete(m, string(nodeKey(4))) },
		"foreign-region": func(m inlineRootView) {
			mutateCohort(m, 3, func(c *RegionCohort) { c.Nodes = []uint64{1, 2, 3, 4, 6} })
		},
		"shrunk-history": func(m inlineRootView) { mutateCohort(m, 2, func(c *RegionCohort) { c.Nodes = []uint64{1, 2, 4} }) },
		"future-index":   func(m inlineRootView) { mutateCohort(m, 3, func(c *RegionCohort) { c.PlannedIndex = index + 1 }) },
		"stale-fence":    func(m inlineRootView) { mutateCohort(m, 2, func(c *RegionCohort) { c.GroupFenceIndex = 99999 }) },
		"same-fence-request": func(m inlineRootView) {
			mutateCohort(m, 2, func(c *RegionCohort) { c.GroupFenceRequestID = "fence-1" })
		},
		"future-group-epoch": func(m inlineRootView) { mutateCohort(m, 2, func(c *RegionCohort) { c.GroupEpoch = 4 }) },
		"pending-has-proof":  func(m inlineRootView) { mutateCohort(m, 3, func(c *RegionCohort) { c.GroupFenceIndex = 1 }) },
		"bad-head": func(m inlineRootView) {
			m[string(regionCohortHeadKey("west"))] = []byte(`{"region_id":"west","current_generation":3}`)
		},
		"unknown-field": func(m inlineRootView) {
			key := string(regionCohortHeadKey("west"))
			m[key] = []byte(`{"region_id":"west","current_generation":2,"pending_generation":3,"extra":1}`)
		},
	}
	ref, _, _ := snapshot.DecodeReference(file)
	r, err := store.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	d, err := snapshot.NewDecoder(r, ref.Size)
	if err != nil {
		t.Fatal(err)
	}
	header := d.Header()
	r.Close()
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			entries := inlineRootView{}
			for _, e := range original.Entries {
				entries[string(e.Key)] = slices.Clone(e.Value)
			}
			mutate(entries)
			keys := make([]string, 0, len(entries))
			for key := range entries {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			bad := snapshotImage{Version: original.Version, Applied: index, Conf: original.Conf}
			for _, key := range keys {
				bad.Entries = append(bad.Entries, snapshotEntry{Key: []byte(key), Value: entries[key]})
			}
			badInline, err := json.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			badRef, err := store.Create(context.Background(), header, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, key := range keys {
					if err := e.Add([]byte(key), entries[key]); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			badFile, err := badRef.Encode()
			if err != nil {
				t.Fatal(err)
			}
			for _, format := range []string{"inline", "file"} {
				t.Run(format, func(t *testing.T) {
					dest := rootFileState(t, t.TempDir())
					applyRootFileCommand(t, dest, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "old"}})
					if format == "inline" {
						err = dest.Restore(index, badInline, conf)
					} else {
						err = dest.RestoreFrom(context.Background(), store, index, badFile, conf)
					}
					if err == nil {
						t.Fatal("corrupt snapshot accepted")
					}
					if dest.AppliedIndex() != 1 {
						t.Fatal("rejected snapshot advanced applied index")
					}
					if _, found, err := dest.Tenant("old"); err != nil || !found {
						t.Fatal("rejected snapshot changed live state", found, err)
					}
				})
			}
		})
	}
}
