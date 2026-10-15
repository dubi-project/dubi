package capstate

import (
	"errors"
	"reflect"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
)

var regionHandoffKey = []byte("data/fence/region-handoff")

// RegionHandoff returns durable progress, never permission to publish Root.
func (s *State) RegionHandoff() (regionfence.Handoff, bool, error) {
	v, err := s.RegionFence()
	if err != nil || v.Handoff == nil {
		return regionfence.Handoff{}, false, err
	}
	return *v.Handoff, true, nil
}

func (s *State) ApplyRegionHandoff(index uint64, requestID string, command regionfence.HandoffCommand) error {
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
	r, found, memberIndex, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil {
		return err
	}
	h, hasHandoff, err := s.validateHandoff(s.applied, r, found, s.get)
	if err != nil {
		return err
	}
	result := Result{RequestID: requestID, Index: index, Hash: commandHash("region_handoff", requestID, command, nil)}
	switch {
	case rootstate.ValidateRequestID(requestID) != nil || command.Validate() != nil:
		result.Error = "invalid regional handoff"
	case !found || !handoffMembershipAllowed(r, memberIndex, s.conf, command):
		result.Error = "regional handoff requires current exact membership fence"
	case command.Action == regionfence.Seal:
		if !reflect.DeepEqual(command.Intent.Previous, r) || hasHandoff && h.Activation == nil {
			result.Error = "regional seal conflicts with existing generation or pending handoff"
		} else {
			h = regionfence.Handoff{Intent: *command.Intent, SealRequestID: requestID, SealIndex: index}
			result.Applied = true
		}
	case command.Action == regionfence.Activate:
		if !hasHandoff || h.Activation != nil || !reflect.DeepEqual(command.Activation.Candidate, r) || !h.Intent.Accepts(r, h.SealIndex) {
			result.Error = "regional activation differs from sealed candidate"
		} else {
			h.Activation, h.ActivationRequestID, h.ActivationIndex = command.Activation, requestID, index
			result.Applied = true
		}
	}
	if result.Applied {
		if err := setJSON(batch, regionHandoffKey, h); err != nil {
			return err
		}
	}
	if err := setJSON(batch, resultKey(requestID), result); err != nil {
		return err
	}
	return s.commitIndex(batch, index)
}

func handoffMembershipAllowed(record regionfence.Record, membership uint64, conf *raftpb.ConfState, command regionfence.HandoffCommand) bool {
	if membership == record.Fence.MembershipIndex && record.Fence.Matches(conf) {
		return true
	}
	if command.Action != regionfence.Seal || membership <= record.Fence.MembershipIndex || conf == nil ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return false
	}
	oldIDs := make(map[uint64]struct{}, len(record.Fence.Members))
	oldVoters := 0
	for _, member := range record.Fence.Members {
		oldIDs[member.RaftID] = struct{}{}
		if member.Role == rootstate.GroupMemberVoter {
			oldVoters++
		}
	}
	overlap := 0
	for _, id := range append(append([]uint64{}, conf.GetVoters()...), conf.GetLearners()...) {
		if _, found := oldIDs[id]; !found {
			return false
		}
		overlap++
	}
	if overlap < oldVoters/2+1 {
		return false
	}
	for _, id := range append(append([]uint64{}, conf.GetVoters()...), conf.GetLearners()...) {
		if !containsID(command.Intent.Next.Nodes, id) {
			return false
		}
	}
	return true
}

func containsID(ids []uint64, target uint64) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func (s *State) ResultForRegionHandoff(requestID string, c regionfence.HandoffCommand) (Result, bool, error) {
	r, found, err := s.Result(requestID)
	if err == nil && found && r.Hash != commandHash("region_handoff", requestID, c, nil) {
		return Result{}, true, ErrRequestIDReused
	}
	return r, found, err
}

// Called under the data Apply lock BEFORE invoking any module or Pack write.
// Non-regional Groups remain unchanged. This is a local write fence, not a
// LocalReady/GeoReady receipt or a substitute for public authorization.
func (s *State) regionalWriteRejection() (string, error) {
	r, found, memberIndex, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	h, exists, err := s.validateHandoff(s.applied, r, true, s.get)
	if err != nil {
		return "", err
	}
	if memberIndex != r.Fence.MembershipIndex || !r.Fence.Matches(s.conf) || exists && h.Activation == nil {
		return "regional writes are fenced pending generation activation", nil
	}
	return "", nil
}

func (s *State) validateHandoff(index uint64, r regionfence.Record, found bool, get func([]byte) ([]byte, bool, error)) (regionfence.Handoff, bool, error) {
	var h regionfence.Handoff
	encoded, exists, err := get(regionHandoffKey)
	if err != nil || !exists {
		return h, exists, err
	}
	if !found || len(encoded) > regionfence.MaxRecordBytes || decodePackJSON(encoded, &h) != nil || h.Validate() != nil || h.Intent.Previous.Fence.GroupID != s.groupID || h.SealIndex > index || h.ActivationIndex > index || !h.MatchesLatest(r) {
		return h, false, errors.New("invalid regional handoff metadata")
	}
	checkResult := func(id string, applied uint64, hash string) error {
		encoded, exists, err := get(resultKey(id))
		if err != nil {
			return err
		}
		var result Result
		if !exists || len(encoded) > regionfence.MaxRecordBytes || decodePackJSON(encoded, &result) != nil || result.RequestID != id || result.Index != applied || !result.Applied || result.Error != "" || len(result.Value) != 0 || result.Hash != hash {
			return errors.New("regional handoff has no matching durable result")
		}
		return nil
	}
	previous := h.Intent.Previous
	if err := checkResult(previous.RequestID, previous.Index, commandHash("region_fence", previous.RequestID, previous.Fence, nil)); err != nil {
		return h, false, err
	}
	seal := regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &h.Intent}
	if err := checkResult(h.SealRequestID, h.SealIndex, commandHash("region_handoff", h.SealRequestID, seal, nil)); err != nil {
		return h, false, err
	}
	if h.Activation != nil {
		activate := regionfence.HandoffCommand{Action: regionfence.Activate, Activation: h.Activation}
		if err := checkResult(h.ActivationRequestID, h.ActivationIndex, commandHash("region_handoff", h.ActivationRequestID, activate, nil)); err != nil {
			return h, false, err
		}
	}
	return h, true, nil
}
