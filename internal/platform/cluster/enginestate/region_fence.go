package enginestate

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var regionFenceKey = []byte("data/fence/region")
var membershipIndexKey = []byte("data/fence/membership-index")

// MembershipCut is independent of regional cohort size and works while adding
// the first learner to a one-voter data Group. Its position is preserved in v5
// snapshots and changes on every applied ConfChange, including membership ABA.
func (s *State) MembershipCut() (raft.MembershipCut, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return raft.MembershipCut{}, err
	}
	_, _, index, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil {
		return raft.MembershipCut{}, err
	}
	return raft.MembershipCut{Index: index, Conf: proto.Clone(s.conf).(*raftpb.ConfState)}, nil
}

func (s *State) validateRegionSnapshot(view *pebble.Snapshot, index uint64, conf *raftpb.ConfState) error {
	_, _, _, err := s.validateRegionMetadata(index, conf, func(key []byte) ([]byte, bool, error) {
		value, closer, err := view.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if len(value) > regionfence.MaxRecordBytes {
			return nil, false, errors.Join(errors.New("regional snapshot metadata exceeds bound"), closer.Close())
		}
		copied := bytes.Clone(value)
		return copied, true, closer.Close()
	})
	return err
}

// RegionFenceView is one local durable cut, NOT a linearizable RPC or proof
// about another participant. Callers must obtain their own Group read barrier.
type RegionFenceView struct {
	Record          regionfence.Record
	Handoff         *regionfence.Handoff // Same durable cut; nil before the first seal.
	Found           bool
	Active          bool
	AppliedIndex    uint64
	MembershipIndex uint64
	Conf            *raftpb.ConfState
}

func (s *State) RegionFence() (RegionFenceView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RegionFenceView{}, err
	}
	r, found, membership, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil {
		return RegionFenceView{}, err
	}
	h, hasHandoff, err := s.validateHandoff(s.applied, r, found, s.get)
	if err != nil {
		return RegionFenceView{}, err
	}
	var handoff *regionfence.Handoff
	if hasHandoff {
		handoff = &h
	}
	return RegionFenceView{Record: r, Found: found, Active: found && membership == r.Fence.MembershipIndex && r.Fence.Matches(s.conf),
		Handoff: handoff, AppliedIndex: s.applied, MembershipIndex: membership, Conf: proto.Clone(s.conf).(*raftpb.ConfState)}, nil
}

func (s *State) ApplyRegionFence(index uint64, requestID string, fence regionfence.Descriptor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if _, found, err := s.result(requestID); err != nil {
		return err
	} else if found {
		return s.commitIndex(batch, index)
	}
	old, found, membership, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil {
		return err
	}
	handoff, hasHandoff, err := s.validateHandoff(s.applied, old, found, s.get)
	if err != nil {
		return err
	}
	candidate := regionfence.Record{Fence: fence, RequestID: requestID, Index: index}
	result := Result{RequestID: requestID, Index: index, Hash: commandHash("region_fence", requestID, fence, nil)}
	switch {
	case rootstate.ValidateRequestID(requestID) != nil || fence.Validate() != nil || fence.GroupID != s.groupID:
		result.Error = "invalid regional fence"
	case membership != fence.MembershipIndex || !fence.Matches(s.conf):
		result.Error = "regional fence no longer matches applied membership"
	case !found && fence.Generation != 1 && !fence.Reformation:
		result.Error = "initial regional fence must be generation one"
	case found && !fence.Advances(old.Fence):
		result.Error = "regional fence must advance without shrinking its cohort"
	case hasHandoff && (handoff.Activation != nil || !handoff.Intent.Accepts(candidate, handoff.SealIndex)):
		result.Error = "regional candidate differs from sealed handoff"
	case found && fence.Generation != old.Fence.Generation && !hasHandoff:
		result.Error = "regional growth requires a sealed handoff"
	default:
		result.Applied = true
		if err := setJSON(batch, regionFenceKey, regionfence.Record{Fence: fence, RequestID: requestID, Index: index}); err != nil {
			return err
		}
	}
	if err := setJSON(batch, resultKey(requestID), result); err != nil {
		return err
	}
	return s.commitIndex(batch, index)
}

func (s *State) ResultForRegionFence(requestID string, fence regionfence.Descriptor) (Result, bool, error) {
	r, found, err := s.Result(requestID)
	if err != nil || !found {
		return r, found, err
	}
	if r.Hash != commandHash("region_fence", requestID, fence, nil) {
		return Result{}, true, ErrRequestIDReused
	}
	return r, true, nil
}

// Used by live reads and BOTH snapshot formats against one fixed view. The
// membership position survives even when the configuration later looks equal.
func (s *State) validateRegionMetadata(index uint64, conf *raftpb.ConfState, get func([]byte) ([]byte, bool, error)) (regionfence.Record, bool, uint64, error) {
	var r regionfence.Record
	position, hasPosition, err := get(membershipIndexKey)
	if err != nil {
		return r, false, 0, err
	}
	var membership uint64
	if hasPosition {
		if len(position) != 8 {
			return r, false, 0, errors.New("invalid Group membership position")
		}
		membership = binary.BigEndian.Uint64(position)
		if membership == 0 || membership > index {
			return r, false, 0, errors.New("Group membership position exceeds applied cut")
		}
	}
	encoded, found, err := get(regionFenceKey)
	if err != nil || !found {
		if err == nil {
			_, _, err = s.validateHandoff(index, r, false, get)
		}
		return r, found, membership, err
	}
	if len(encoded) > regionfence.MaxRecordBytes || decodePackJSON(encoded, &r) != nil || r.Validate() != nil || r.Fence.GroupID != s.groupID || r.Index > index || membership < r.Fence.MembershipIndex ||
		(membership != r.Fence.MembershipIndex && membership <= r.Index) ||
		(membership == r.Fence.MembershipIndex && !r.Fence.Matches(conf)) {
		return r, false, membership, errors.New("invalid regional fence or membership linkage")
	}
	encoded, found, err = get(resultKey(r.RequestID))
	if err != nil {
		return r, false, membership, err
	}
	var result Result
	if !found || len(encoded) > regionfence.MaxRecordBytes || decodePackJSON(encoded, &result) != nil || result.RequestID != r.RequestID || result.Index != r.Index || !result.Applied || result.Error != "" || len(result.Value) != 0 || result.Hash != commandHash("region_fence", r.RequestID, r.Fence, nil) {
		return r, false, membership, errors.New("regional fence has no matching durable result")
	}
	if _, handoff, err := s.validateHandoff(index, r, true, get); err != nil {
		return r, false, membership, err
	} else if r.Fence.Generation > 1 && !r.Fence.Reformation && !handoff {
		return r, false, membership, errors.New("grown regional fence has no handoff")
	}
	return r, true, membership, nil
}
