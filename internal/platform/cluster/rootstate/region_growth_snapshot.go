package rootstate

import (
	"bytes"
	"errors"
	"slices"
)

func validateRegionGrowthSnapshot(view rootSnapshotReader, index uint64, key, value []byte) error {
	bad := errors.New("invalid regional growth authorization or task linkage")
	var g RegionGrowth
	if decodeStreamJSON(value, &g) != nil || g.Validate() != nil || !bytes.Equal(key, regionGrowthKey(g.Seal.RegionID, g.Seal.Generation)) || g.AuthorizedIndex > index {
		return bad
	}
	p := g.Seal
	var previous, next RegionCohort
	var group Group
	var task GroupChange
	var result Result
	if streamJSON(view, regionCohortKey(p.RegionID, p.Generation-1), &previous) != nil || previous.Validate() != nil || previous.State != RegionCohortConfirmed || previous.GroupID != p.GroupID || previous.GroupEpoch > p.ExpectedEpoch || previous.GroupFenceIndex != p.PreviousFenceIndex || previous.GroupFenceRequestID != p.PreviousFenceRequestID || !cohortContainsMembers(previous, g.BaseMembers) ||
		streamJSON(view, regionCohortKey(p.RegionID, p.Generation), &next) != nil || next.Validate() != nil || next.PlannedIndex != p.PlannedIndex || !cohortExtends(previous, next, g.BaseMembers) || !cohortMatchesMembers(next, p.Members) ||
		streamJSON(view, groupKey(p.GroupID), &group) != nil || group.GroupID != p.GroupID || group.StateKind != GroupStateEngine ||
		streamJSON(view, groupChangeKey(p.ChangeID), &task) != nil || !growthMatchesTask(g, task) ||
		streamJSON(view, resultKey(g.RequestID), &result) != nil || result.RequestID != g.RequestID || result.Index != g.AuthorizedIndex || result.Hash != g.ResultSHA256 || result.Error != "" {
		return bad
	}
	if task.State == GroupChangePending {
		if task.CompletedEpoch != 0 || group.Epoch != p.ExpectedEpoch || !slices.Equal(group.Members, g.BaseMembers) || next.State != RegionCohortPlanned {
			return bad
		}
	} else if task.State != GroupChangeCompleted || task.CompletedEpoch != p.ExpectedEpoch+1 || group.Epoch < task.CompletedEpoch {
		return bad
	}
	if group.Epoch == p.ExpectedEpoch+1 && !slices.Equal(group.Members, p.Members) {
		return bad
	}
	if next.State == RegionCohortConfirmed && (task.State != GroupChangeCompleted || next.ConfirmedIndex <= g.AuthorizedIndex || next.GroupEpoch != p.ExpectedEpoch+1 || next.GroupFenceIndex <= p.SealIndex || next.GroupFenceRequestID == p.SealRequestID) {
		return bad
	}
	return nil
}
