package rootstate

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

func decodeStreamJSON(encoded []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing Root snapshot JSON")
	}
	return nil
}

type rootSnapshotReader interface {
	Get([]byte) ([]byte, io.Closer, error)
}

func streamValue(view rootSnapshotReader, key []byte) ([]byte, error) {
	value, closer, err := view.Get(key)
	if err != nil {
		return nil, fmt.Errorf("Root snapshot reference %q: %w", key, err)
	}
	if len(value) == 0 || len(value) > snapshot.MaxValueBytes {
		return nil, errors.Join(errors.New("invalid Root snapshot referenced record size"), closer.Close())
	}
	copied := bytes.Clone(value)
	return copied, closer.Close()
}

func streamJSON(view rootSnapshotReader, key []byte, destination any) error {
	value, err := streamValue(view, key)
	if err != nil {
		return err
	}
	return decodeStreamJSON(value, destination)
}

// Every record is visited independently; cross references use the SAME fixed
// view. No directory-sized map or image-sized batch is constructed.
func validateStreamEntry(view rootSnapshotReader, index uint64, key, value []byte) error {
	bad := func() error { return fmt.Errorf("invalid Root snapshot record %q", key) }
	if len(key) > snapshot.MaxKeyBytes || len(value) == 0 || len(value) > snapshot.MaxValueBytes {
		return bad()
	}
	name := string(key)
	membersValid := func(members []GroupMember) bool {
		if validateGroupMembers(members) != nil {
			return false
		}
		for _, member := range members {
			var node Node
			if streamJSON(view, nodeKey(member.RaftID), &node) != nil || node.RaftID != member.RaftID {
				return false
			}
		}
		return true
	}
	switch {
	case bytes.Equal(key, enrollmentAuthorityKey):
		var authority EnrollmentAuthority
		if decodeStreamJSON(value, &authority) != nil || authority.validate(index) != nil {
			return bad()
		}
		for _, member := range authority.Members {
			var node Node
			if streamJSON(view, nodeKey(member), &node) != nil || node.RaftID != member {
				return bad()
			}
			// This is the last published delegation cut. Root membership and
			// fences may have advanced while a new cut is being prepared;
			// live authorization intersects those facts on every operation.
		}
	case strings.HasPrefix(name, "data/region-growth/"):
		return validateRegionGrowthSnapshot(view, index, key, value)
	case strings.HasPrefix(name, "data/region-group-retirement/"):
		var retirement RegionGroupRetirement
		if decodeStreamJSON(value, &retirement) != nil || retirement.validateStored() != nil ||
			!bytes.Equal(key, regionGroupRetirementKey(retirement.GroupID)) || retirement.RetiredAt > index {
			return bad()
		}
	case strings.HasPrefix(name, "data/region-group-rebuild/"):
		var rebuild RegionGroupRebuild
		if decodeStreamJSON(value, &rebuild) != nil || rebuild.validateStored() != nil ||
			!bytes.Equal(key, regionGroupRebuildKey(rebuild.GroupID)) || rebuild.PlannedAt > index {
			return bad()
		}
	case strings.HasPrefix(name, "data/region-cohort/"), strings.HasPrefix(name, "data/region-cohort-head/"), strings.HasPrefix(name, "data/region-cohort-group/"):
		if err := validateRegionCohortSnapshot(view, index, key, value); err != nil {
			return fmt.Errorf("%w: %v", bad(), err)
		}
	case strings.HasPrefix(name, "data/root-join/"):
		var task RootJoin
		if decodeStreamJSON(value, &task) != nil || task.Validate() != nil || !bytes.Equal(key, rootJoinKey(task.GrantID)) || task.AdmittedIndex > index {
			return bad()
		}
		var grant JoinGrant
		var node Node
		var registration Result
		if streamJSON(view, joinGrantKey(task.GrantID), &grant) != nil || grant.State != JoinGrantConsumed || grant.BoundRequestSHA256 != task.RequestSHA256 ||
			streamJSON(view, nodeKey(task.RaftID), &node) != nil || node.RaftID != task.RaftID || node.NodeID != task.NodeID ||
			streamJSON(view, resultKey("enroll-node-"+task.RequestSHA256), &registration) != nil || registration.RequestID != "enroll-node-"+task.RequestSHA256 ||
			registration.Error != "" || registration.Hash != task.RegistrationSHA256 {
			return bad()
		}
	case strings.HasPrefix(name, "data/node-fence/"):
		var fence NodeFenceRecord
		var node Node
		if decodeStreamJSON(value, &fence) != nil || !nodeIdentity.MatchString(fence.NodeID) || fence.RaftID == 0 ||
			fence.FencedIndex == 0 || fence.FencedIndex > index || !bytes.Equal(key, nodeFenceKey(fence.RaftID)) ||
			streamJSON(view, nodeKey(fence.RaftID), &node) != nil || node.NodeID != fence.NodeID || node.RaftID != fence.RaftID {
			return bad()
		}
	case strings.HasPrefix(name, "data/join-grant/"):
		var grant JoinGrant
		if decodeStreamJSON(value, &grant) != nil || grant.Validate() != nil || !bytes.Equal(key, joinGrantKey(grant.GrantID)) {
			return bad()
		}
		var issuer Node
		if streamJSON(view, nodeKey(grant.IssuerRaftID), &issuer) != nil || issuer.RaftID != grant.IssuerRaftID {
			return bad()
		}
	case strings.HasPrefix(name, "data/node/"):
		var node Node
		if decodeStreamJSON(value, &node) != nil || !bytes.Equal(key, nodeKey(node.RaftID)) ||
			(Command{Version: FormatVersion, RequestID: "snapshot", Type: RegisterNode, Node: &node}).validate() != nil {
			return bad()
		}
		reverse, err := streamValue(view, nodeIDKey(node.NodeID))
		if err != nil || string(reverse) != strconv.FormatUint(node.RaftID, 10) {
			return bad()
		}
	case strings.HasPrefix(name, "data/node-id/"):
		id := strings.TrimPrefix(name, "data/node-id/")
		raftID, err := strconv.ParseUint(string(value), 10, 64)
		var node Node
		if err != nil || raftID == 0 || !nodeIdentity.MatchString(id) ||
			strconv.FormatUint(raftID, 10) != string(value) || streamJSON(view, nodeKey(raftID), &node) != nil ||
			node.NodeID != id || node.RaftID != raftID {
			return bad()
		}
	case strings.HasPrefix(name, "data/group/"):
		var group Group
		if decodeStreamJSON(value, &group) != nil || !bytes.Equal(key, groupKey(group.GroupID)) ||
			raft.ValidateGroupID(group.GroupID) != nil || !validID(group.StateKind) || group.Epoch == 0 {
			return bad()
		}
		if len(group.Members) == 0 {
			if group.Epoch != 1 {
				return bad()
			}
		} else if !membersValid(group.Members) {
			return bad()
		}
	case strings.HasPrefix(name, "data/group-change/"):
		var change GroupChange
		if decodeStreamJSON(value, &change) != nil || !validID(change.ChangeID) ||
			!bytes.Equal(key, groupChangeKey(change.ChangeID)) || raft.ValidateGroupID(change.GroupID) != nil ||
			change.ExpectedEpoch == 0 || change.ExpectedEpoch == math.MaxUint64 ||
			(change.Maintenance && (change.RegionID != "" || change.GrowthGeneration != 0)) || !membersValid(change.Members) {
			return bad()
		}
		var group Group
		if streamJSON(view, groupKey(change.GroupID), &group) != nil || group.GroupID != change.GroupID {
			return bad()
		}
		if change.RegionID != "" || change.GrowthGeneration != 0 {
			var growth RegionGrowth
			if !validID(change.RegionID) || change.GrowthGeneration < 2 || streamJSON(view, regionGrowthKey(change.RegionID, change.GrowthGeneration), &growth) != nil || growth.Validate() != nil || !growthMatchesTask(growth, change) {
				return bad()
			}
		} else if owner, closer, err := view.Get(regionCohortGroupKey(change.GroupID)); err == nil {
			region := string(owner)
			if err := closer.Close(); err != nil {
				return err
			}
			var first RegionCohort
			if streamJSON(view, regionCohortKey(region, 1), &first) != nil {
				return bad()
			}
			// Only generic tasks completed before the first confirmation are
			// historical. Removing growth annotations cannot turn a later task
			// into an unguarded generic task during restore.
			if first.State == RegionCohortConfirmed && !change.Maintenance && (change.State != GroupChangeCompleted || change.CompletedEpoch > first.GroupEpoch) {
				return bad()
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		switch change.State {
		case GroupChangePending:
			active, err := streamValue(view, activeGroupChangeKey(change.GroupID))
			if err != nil || string(active) != change.ChangeID || change.CompletedEpoch != 0 || group.Epoch != change.ExpectedEpoch {
				return bad()
			}
		case GroupChangeCompleted:
			if change.CompletedEpoch != change.ExpectedEpoch+1 || group.Epoch < change.CompletedEpoch {
				return bad()
			}
		default:
			return bad()
		}
	case strings.HasPrefix(name, "data/group-active/"):
		groupID := strings.TrimPrefix(name, "data/group-active/")
		var change GroupChange
		if raft.ValidateGroupID(groupID) != nil || !validID(string(value)) ||
			streamJSON(view, groupChangeKey(string(value)), &change) != nil || change.ChangeID != string(value) ||
			change.GroupID != groupID || change.State != GroupChangePending {
			return bad()
		}
	case strings.HasPrefix(name, "data/tenant/"):
		var tenant Tenant
		if decodeStreamJSON(value, &tenant) != nil || !validID(tenant.TenantID) || !bytes.Equal(key, tenantKey(tenant.TenantID)) {
			return bad()
		}
	case strings.HasPrefix(name, "data/tenant-credential/"):
		var credential TenantCredential
		if decodeStreamJSON(value, &credential) != nil || credential.Validate() != nil ||
			credential.RevokedIndex > index ||
			!bytes.Equal(key, tenantCredentialKey(credential.CredentialID)) {
			return bad()
		}
		var tenant Tenant
		if streamJSON(view, tenantKey(credential.TenantID), &tenant) != nil || tenant.TenantID != credential.TenantID {
			return bad()
		}
	case strings.HasPrefix(name, "data/dataset/"):
		var dataset Dataset
		if decodeStreamJSON(value, &dataset) != nil || !bytes.Equal(key, datasetKey(dataset)) ||
			(Command{Version: FormatVersion, RequestID: "snapshot", Type: CreateDataset, Dataset: &dataset}).validate() != nil {
			return bad()
		}
		var tenant Tenant
		var group Group
		var descriptor Range
		if streamJSON(view, tenantKey(dataset.TenantID), &tenant) != nil || tenant.TenantID != dataset.TenantID ||
			streamJSON(view, groupKey(dataset.OwnerGroupID), &group) != nil || group.GroupID != dataset.OwnerGroupID ||
			streamJSON(view, rangeKey(dataset), &descriptor) != nil || descriptor.Keyspace() != dataset.Keyspace() ||
			descriptor.RangeID != dataset.RangeID || descriptor.OwnerGroupID != dataset.OwnerGroupID {
			return bad()
		}
	case strings.HasPrefix(name, "data/range/"):
		var descriptor Range
		if decodeStreamJSON(value, &descriptor) != nil || descriptor.Validate() != nil ||
			!bytes.Equal(key, keyspaceRangeKey(descriptor.Keyspace())) || descriptor.Epoch != 1 ||
			len(descriptor.Start) != 0 || len(descriptor.End) != 0 {
			return bad()
		}
		if descriptor.State == RangeStatePending {
			if descriptor.OwnerFenceRequestID != "" || descriptor.OwnerFenceIndex != 0 {
				return bad()
			}
		} else if !validID(descriptor.OwnerFenceRequestID) || descriptor.OwnerFenceIndex == 0 {
			return bad()
		}
		// OwnerFenceIndex belongs to the OWNER Group, not this Root cut.
		var dataset Dataset
		if streamJSON(view, []byte("data/dataset/"+descriptor.Keyspace().path()), &dataset) != nil ||
			dataset.Keyspace() != descriptor.Keyspace() || dataset.RangeID != descriptor.RangeID ||
			dataset.OwnerGroupID != descriptor.OwnerGroupID {
			return bad()
		}
		reverse, err := streamValue(view, rangeIDKey(descriptor.RangeID))
		if err != nil || !bytes.Equal(reverse, key) {
			return bad()
		}
	case strings.HasPrefix(name, "data/range-id/"):
		id := strings.TrimPrefix(name, "data/range-id/")
		var descriptor Range
		if !validID(id) || !bytes.HasPrefix(value, []byte("data/range/")) ||
			streamJSON(view, value, &descriptor) != nil || descriptor.RangeID != id ||
			!bytes.Equal(value, keyspaceRangeKey(descriptor.Keyspace())) {
			return bad()
		}
	case strings.HasPrefix(name, "data/range-placement/"):
		var placement RangePlacement
		if decodeStreamJSON(value, &placement) != nil || placement.Validate() != nil ||
			!bytes.Equal(key, rangePlacementKey(placement.RangeID)) {
			return bad()
		}
		var descriptor Range
		if streamJSON(view, rangeIDKey(placement.RangeID), &descriptor) != nil || descriptor.State != RangeStateActive ||
			descriptor.Epoch != placement.RangeEpoch || descriptor.OwnerGroupID != placement.OwnerGroupID {
			return bad()
		}
		for _, region := range placement.Regions {
			var group Group
			var cohort RegionCohort
			var head RegionCohortHead
			if streamJSON(view, groupKey(region.GroupID), &group) != nil || group.StateKind != GroupStateEngine ||
				streamJSON(view, regionCohortKey(region.RegionID, region.Generation), &cohort) != nil || cohort.State != RegionCohortConfirmed ||
				cohort.GroupID != region.GroupID || streamJSON(view, regionCohortHeadKey(region.RegionID), &head) != nil ||
				head.CurrentGeneration != region.Generation {
				return bad()
			}
		}
	case strings.HasPrefix(name, "result/"):
		var result Result
		if decodeStreamJSON(value, &result) != nil || !validID(result.RequestID) ||
			!bytes.Equal(key, resultKey(result.RequestID)) || result.Index == 0 || result.Index > index {
			return bad()
		}
		digest, err := hex.DecodeString(result.Hash)
		if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != result.Hash {
			return bad()
		}
	default:
		return bad()
	}
	return nil
}
