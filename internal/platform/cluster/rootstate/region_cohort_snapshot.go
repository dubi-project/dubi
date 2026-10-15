package rootstate

import (
	"bytes"
	"errors"
	"slices"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/raft"
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
		streamJSON(view, groupKey(c.GroupID), &group) != nil || group.GroupID != c.GroupID || group.StateKind != GroupStateCapability || group.Epoch == 0 {
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
		var growth RegionGrowth
		var task GroupChange
		if head.PendingGeneration == 0 || streamJSON(view, regionGrowthKey(c.RegionID, head.PendingGeneration), &growth) != nil || growth.Validate() != nil || growth.Seal.ExpectedEpoch != c.GroupEpoch ||
			streamJSON(view, groupChangeKey(growth.Seal.ChangeID), &task) != nil || !growthMatchesTask(growth, task) || task.State != GroupChangeCompleted || task.CompletedEpoch != group.Epoch || group.Epoch != c.GroupEpoch+1 {
			return bad
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
