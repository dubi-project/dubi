package rootstate

import (
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
)

var enrollmentAuthorityKey = []byte("data/enrollment-authority")

var ErrEnrollmentAuthorityDenied = errors.New("Node is not a current enrollment authority")

// AuthorizeEnrollmentSigner requires an applied Root read barrier at the
// caller. A prepared next-epoch envelope is kept sealed until Root publishes
// that epoch as the active cut.
func (s *State) AuthorizeEnrollmentSigner(local Node, signerEpoch uint64, bootstrap bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return err
	}
	node, member, err := s.rootMember(local.RaftID)
	if err != nil {
		return err
	}
	if !member || node.NodeID != local.NodeID {
		return ErrEnrollmentAuthorityDenied
	}
	encoded, found, err := s.get(enrollmentAuthorityKey)
	if err != nil {
		return err
	}
	if !found {
		if bootstrap && signerEpoch == 0 {
			return nil
		}
		return ErrEnrollmentAuthorityDenied
	}
	var authority EnrollmentAuthority
	if err := decodeStreamJSON(encoded, &authority); err != nil {
		return err
	}
	if err := authority.validate(s.applied); err != nil {
		return err
	}
	if authority.State != AuthorityStateActive || !slices.Contains(authority.Members, local.RaftID) {
		return ErrEnrollmentAuthorityDenied
	}
	if bootstrap && signerEpoch == 0 {
		return nil
	}
	// A membership-only cut may advance the Root epoch without changing the
	// CA key. Keep an existing envelope usable for that one cut; the node must
	// still be in the new authority member set.
	if signerEpoch != authority.Epoch && !(signerEpoch > 0 && signerEpoch+1 == authority.Epoch) {
		return ErrEnrollmentAuthorityDenied
	}
	return nil
}

// AuthorizeEnrollmentTransfer permits current-epoch repair or preparation of
// exactly the next epoch. The destination must be a current stable Root voter;
// the source must still be a live-authorized member of the old authority cut.
func (s *State) AuthorizeEnrollmentTransfer(from, to, epoch uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return err
	}
	if epoch == 0 || len(s.conf.GetVotersOutgoing()) != 0 || len(s.conf.GetLearnersNext()) != 0 ||
		!slices.Contains(s.conf.GetVoters(), to) {
		return ErrEnrollmentAuthorityDenied
	}
	for _, id := range []uint64{from, to} {
		if _, member, err := s.rootMember(id); err != nil {
			return err
		} else if !member {
			return ErrEnrollmentAuthorityDenied
		}
	}
	encoded, found, err := s.get(enrollmentAuthorityKey)
	if err != nil {
		return err
	}
	if !found {
		return ErrEnrollmentAuthorityDenied
	}
	var authority EnrollmentAuthority
	if err := decodeStreamJSON(encoded, &authority); err != nil {
		return err
	}
	if err := authority.validate(s.applied); err != nil {
		return err
	}
	if authority.State != AuthorityStateActive ||
		(!slices.Contains(authority.Members, from) && !(epoch == authority.Epoch+1 && slices.Contains(s.conf.GetVoters(), from))) ||
		epoch < authority.Epoch || epoch-authority.Epoch > 1 ||
		(epoch == authority.Epoch && !slices.Contains(authority.Members, to)) {
		return ErrEnrollmentAuthorityDenied
	}
	return nil
}

func (value EnrollmentAuthorityPlan) validate() error {
	if value.ExpectedEpoch >= value.Epoch || value.Epoch == 0 {
		return errors.New("invalid enrollment authority epoch")
	}
	if len(value.Members) == 0 || !slices.IsSorted(value.Members) {
		return errors.New("enrollment authority members must be sorted and non-empty")
	}
	for index, member := range value.Members {
		if member == 0 || (index > 0 && value.Members[index-1] == member) {
			return errors.New("enrollment authority has invalid or duplicate member")
		}
	}
	if value.State != AuthorityStateActive && value.State != AuthorityStateDegraded {
		return errors.New("invalid enrollment authority state")
	}
	return validateAuthorityReady(value.Members, value.ReadyMembers)
}

func (value EnrollmentAuthority) validate(index uint64) error {
	if value.Epoch == 0 || value.ConfirmedIndex == 0 || value.ConfirmedIndex > index {
		return errors.New("invalid enrollment authority record index or epoch")
	}
	if len(value.Members) == 0 || !slices.IsSorted(value.Members) {
		return errors.New("enrollment authority members must be sorted and non-empty")
	}
	for position, member := range value.Members {
		if member == 0 || (position > 0 && value.Members[position-1] == member) {
			return errors.New("enrollment authority has invalid or duplicate member")
		}
	}
	if value.State != AuthorityStateActive && value.State != AuthorityStateDegraded {
		return errors.New("invalid enrollment authority state")
	}
	return validateAuthorityReady(value.Members, value.ReadyMembers)
}

func validateAuthorityReady(members, ready []uint64) error {
	if !slices.IsSorted(ready) {
		return errors.New("authority readiness must be sorted")
	}
	for i, id := range ready {
		if !slices.Contains(members, id) || (i > 0 && ready[i-1] == id) {
			return errors.New("invalid authority readiness member")
		}
	}
	return nil
}

// EnrollmentAuthority returns the latest Root-authorized signer delegation
// cut. The returned record is a durable Root view; callers needing a
// linearizable decision must pair it with a Root ReadIndex as they do for
// membership and routing.
func (s *State) EnrollmentAuthority() (EnrollmentAuthority, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return EnrollmentAuthority{}, false, err
	}
	encoded, found, err := s.get(enrollmentAuthorityKey)
	if err != nil || !found {
		return EnrollmentAuthority{}, found, err
	}
	var value EnrollmentAuthority
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return EnrollmentAuthority{}, false, err
	}
	if err := value.validate(s.applied); err != nil {
		return EnrollmentAuthority{}, false, err
	}
	return value, true, nil
}

func (s *State) applyEnrollmentAuthority(batch *pebble.Batch, plan EnrollmentAuthorityPlan) (string, error) {
	if err := plan.validate(); err != nil {
		return "", err
	}
	voters := s.conf.GetVoters()
	if len(s.conf.GetVotersOutgoing()) != 0 || len(s.conf.GetLearnersNext()) != 0 || s.conf.GetAutoLeave() {
		return "enrollment authority requires stable Root membership", nil
	}
	majority := len(voters)/2 + 1
	if len(voters) == 0 || len(plan.Members) < majority {
		return "enrollment authority requires a Root voter majority", nil
	}
	if len(plan.ReadyMembers) < majority {
		return "enrollment authority requires durable signer installation on a Root majority", nil
	}
	for _, member := range plan.Members {
		if !containsUint64(voters, member) {
			return "enrollment authority member is not a current Root voter", nil
		}
		if _, found, err := s.get(nodeKey(member)); err != nil {
			return "", err
		} else if !found {
			return "enrollment authority member is not registered", nil
		}
		if _, fenced, err := s.nodeFence(member); err != nil {
			return "", err
		} else if fenced {
			return "enrollment authority member is fenced", nil
		}
	}
	current, found, err := s.get(enrollmentAuthorityKey)
	if err != nil {
		return "", err
	}
	if found {
		var previous EnrollmentAuthority
		if err := decodeStreamJSON(current, &previous); err != nil {
			return "", fmt.Errorf("decode prior enrollment authority: %w", err)
		}
		if err := previous.validate(s.applied); err != nil {
			return "", err
		}
		if previous.Epoch != plan.ExpectedEpoch {
			return "enrollment authority Epoch does not match", nil
		}
	} else if plan.ExpectedEpoch != 0 {
		return "initial enrollment authority must start at Epoch 1", nil
	}
	if plan.Epoch != plan.ExpectedEpoch+1 {
		return "enrollment authority Epoch must advance by one", nil
	}
	value := EnrollmentAuthority{Epoch: plan.Epoch, Members: slices.Clone(plan.Members), ReadyMembers: slices.Clone(plan.ReadyMembers), State: plan.State, ConfirmedIndex: s.applied + 1}
	return "", setJSON(batch, enrollmentAuthorityKey, value)
}

func containsUint64(values []uint64, needle uint64) bool {
	return slices.Contains(values, needle)
}
