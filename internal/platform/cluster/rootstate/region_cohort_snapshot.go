package rootstate

import (
	"bytes"
	"errors"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

// Use the same fixed view for every link, retaining bounded per-record memory.
func validateRegionCohortSnapshot(view rootSnapshotReader, index uint64, key, value []byte) error {
	bad := errors.New("invalid Region cohort or linked record")
	name := string(key)
	if strings.HasPrefix(name, "data/region-cohort-group/") {
		groupID := strings.TrimPrefix(name, "data/region-cohort-group/")
		var first RegionCohort
		if raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID || !validID(string(value)) ||
			streamJSON(view, regionCohortKey(string(value), 1), &first) != nil || first.RegionID != string(value) || first.Generation != 1 || first.GroupID != groupID {
			return bad
		}
		return nil
	}
	if strings.HasPrefix(name, "data/region-cohort-head/") {
		var head RegionCohortHead
		if decodeStreamJSON(value, &head) != nil || head.validate() != nil || !bytes.Equal(key, regionCohortHeadKey(head.RegionID)) {
			return bad
		}
		for generation, state := range map[uint64]string{head.CurrentGeneration: RegionCohortConfirmed, head.PendingGeneration: RegionCohortPlanned} {
			if generation == 0 {
				continue
			}
			var cohort RegionCohort
			if streamJSON(view, regionCohortKey(head.RegionID, generation), &cohort) != nil || cohort.Validate() != nil || cohort.RegionID != head.RegionID || cohort.Generation != generation || cohort.State != state {
				return bad
			}
		}
		return nil
	}
	var c RegionCohort
	if decodeStreamJSON(value, &c) != nil || c.Validate() != nil || !bytes.Equal(key, regionCohortKey(c.RegionID, c.Generation)) || c.PlannedIndex > index || c.ConfirmedIndex > index {
		return bad
	}
	var head RegionCohortHead
	var group Group
	if streamJSON(view, regionCohortHeadKey(c.RegionID), &head) != nil || head.validate() != nil || head.RegionID != c.RegionID ||
		streamJSON(view, groupKey(c.GroupID), &group) != nil || group.GroupID != c.GroupID || group.StateKind != GroupStateEngine || group.Epoch == 0 {
		return bad
	}
	owner, err := streamValue(view, regionCohortGroupKey(c.GroupID))
	if err != nil || string(owner) != c.RegionID {
		return bad
	}
	if (c.Formation || c.Reformation) && group.Epoch == 2 && !formationMatchesMembers(c, group.Members) {
		return bad
	}
	if c.Reformation {
		var retirement RegionGroupRetirement
		var rebuild RegionGroupRebuild
		if streamJSON(view, regionGroupRetirementKey(c.GroupID), &retirement) != nil || retirement.validateStored() != nil ||
			streamJSON(view, regionGroupRebuildKey(c.GroupID), &rebuild) != nil || rebuild.validateStored() != nil ||
			retirement.RegionID != c.RegionID || retirement.Generation+1 != c.Generation ||
			!slices.Equal(retirement.Nodes, rebuild.PreviousNodes) ||
			rebuild.TargetGeneration != c.Generation || !slices.Equal(rebuild.TargetNodes, c.Nodes) {
			return bad
		}
	}
	if c.State == RegionCohortPlanned {
		if head.PendingGeneration != c.Generation {
			return bad
		}
	} else if head.CurrentGeneration < c.Generation || group.Epoch < c.GroupEpoch {
		return bad
	} else if group.Epoch == c.GroupEpoch && !cohortMatchesMembers(c, group.Members) {
		return bad
	}
	if c.State == RegionCohortConfirmed && c.Generation == head.CurrentGeneration && group.Epoch != c.GroupEpoch {
		if cohortContainsMembers(c, group.Members) {
			// Explicit removals advance Group membership before an ordinary
			// new-member growth can replace the immutable confirmed cohort.
			if err := validateSnapshotMaintenance(view, c, group); err != nil {
				return err
			}
		} else {
			var growth RegionGrowth
			var task GroupChange
			if head.PendingGeneration == 0 || streamJSON(view, regionGrowthKey(c.RegionID, head.PendingGeneration), &growth) != nil || growth.Validate() != nil || growth.Seal.ExpectedEpoch < c.GroupEpoch ||
				streamJSON(view, groupChangeKey(growth.Seal.ChangeID), &task) != nil || !growthMatchesTask(growth, task) || task.State != GroupChangeCompleted || task.CompletedEpoch != group.Epoch || group.Epoch != growth.Seal.ExpectedEpoch+1 || !slices.Equal(group.Members, growth.Seal.Members) {
				return bad
			}
			if growth.Seal.ExpectedEpoch > c.GroupEpoch {
				base := Group{GroupID: c.GroupID, StateKind: GroupStateEngine, Epoch: growth.Seal.ExpectedEpoch, Members: growth.BaseMembers}
				if err := validateSnapshotMaintenance(view, c, base); err != nil {
					return err
				}
			}
		}
	}
	for _, id := range c.Nodes {
		var node Node
		if streamJSON(view, nodeKey(id), &node) != nil || node.RaftID != id || node.RegionID != c.RegionID {
			return bad
		}
	}
	if c.Generation > 1 && !c.Reformation {
		var previous RegionCohort
		if streamJSON(view, regionCohortKey(c.RegionID, c.Generation-1), &previous) != nil || previous.Validate() != nil || previous.Generation != c.Generation-1 || previous.State != RegionCohortConfirmed || previous.ConfirmedIndex >= c.PlannedIndex || !cohortExtends(previous, c, cohortExistingMembers(previous, group.Members)) {
			return bad
		}
		if c.State == RegionCohortConfirmed && (c.GroupEpoch <= previous.GroupEpoch || c.GroupFenceIndex <= previous.GroupFenceIndex || c.GroupFenceRequestID == previous.GroupFenceRequestID) {
			return bad
		}
		if c.State == RegionCohortConfirmed {
			var growth RegionGrowth
			if streamJSON(view, regionGrowthKey(c.RegionID, c.Generation), &growth) != nil || growth.Validate() != nil || growth.Seal.GroupID != c.GroupID || growth.Seal.PlannedIndex != c.PlannedIndex || c.GroupEpoch != growth.Seal.ExpectedEpoch+1 || c.GroupFenceIndex <= growth.Seal.SealIndex || c.GroupFenceRequestID == growth.Seal.SealRequestID {
				return bad
			}
		}
	}
	return nil
}

// The history is already Root-committed, but every epoch gap must still have
// its exact completed maintenance task. Iterate the SAME fixed view with
// bounded per-row memory; do not invent a new persistent history index.
func validateSnapshotMaintenance(view rootSnapshotReader, cohort RegionCohort, group Group) error {
	bad := errors.New("regional snapshot lacks completed maintenance history")
	if group.Epoch <= cohort.GroupEpoch || group.Epoch-cohort.GroupEpoch > uint64(len(cohort.Nodes)) || !cohortContainsMembers(cohort, group.Members) {
		return bad
	}
	var previous []GroupMember
	if cohort.Generation > 1 && !cohort.Reformation {
		var growth RegionGrowth
		if streamJSON(view, regionGrowthKey(cohort.RegionID, cohort.Generation), &growth) != nil || growth.Validate() != nil || !cohortMatchesMembers(cohort, growth.Seal.Members) {
			return bad
		}
		previous = growth.Seal.Members
	} else if len(cohort.Nodes) <= 3 {
		// Cohort confirmation requires min(3, N) voters, so these roles
		// are exact even for an initial cohort without a growth record.
		for _, id := range cohort.Nodes {
			previous = append(previous, GroupMember{RaftID: id, Role: GroupMemberVoter})
		}
	}
	for epoch := cohort.GroupEpoch; epoch < group.Epoch; epoch++ {
		var task GroupChange
		found := false
		err := walkSnapshotGroupChanges(view, func(key, value []byte) error {
			var candidate GroupChange
			if len(value) > snapshot.MaxValueBytes || decodeStreamJSON(value, &candidate) != nil {
				return bad
			}
			if candidate.GroupID != group.GroupID || candidate.ExpectedEpoch != epoch || candidate.State != GroupChangeCompleted {
				return nil
			}
			if found || !candidate.Maintenance || candidate.RegionID != "" || candidate.GrowthGeneration != 0 || candidate.CompletedEpoch != epoch+1 || !validID(candidate.ChangeID) || !bytes.Equal(key, groupChangeKey(candidate.ChangeID)) || validateGroupMembers(candidate.Members) != nil {
				return bad
			}
			task, found = candidate, true
			return nil
		})
		if err != nil {
			return err
		}
		if !found || !cohortContainsMembers(cohort, task.Members) || len(task.Members) >= len(cohort.Nodes) {
			return bad
		}
		if previous != nil && validateMaintenanceRemoval(Group{Members: previous}, task.Members) != nil {
			return bad
		}
		previous = task.Members
	}
	if !slices.Equal(previous, group.Members) {
		return bad
	}
	return nil
}

func walkSnapshotGroupChanges(view rootSnapshotReader, visit func([]byte, []byte) error) (returnErr error) {
	if inline, ok := view.(inlineRootView); ok {
		for key, value := range inline {
			if strings.HasPrefix(key, "data/group-change/") {
				if err := visit([]byte(key), value); err != nil {
					return err
				}
			}
		}
		return nil
	}
	reader, ok := view.(interface {
		NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
	})
	if !ok {
		return errors.New("Root snapshot view cannot verify maintenance tasks")
	}
	it, err := reader.NewIter(&pebble.IterOptions{LowerBound: []byte("data/group-change/"), UpperBound: []byte("data/group-change0")})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, it.Close()) }()
	for valid := it.First(); valid; valid = it.Next() {
		if err := visit(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Error()
}
