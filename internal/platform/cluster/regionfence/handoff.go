package regionfence

import (
	"errors"
	"math"
	"reflect"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const Seal = "seal"
const Activate = "activate"

// HandoffIntent names both immutable generations. Next is the pending Root
// plan, never a health-filtered set. Sealing stops new data mutations before
// membership changes; it does not itself publish the new denominator.
type HandoffIntent struct {
	Previous Record                 `json:"previous"`
	Next     rootstate.RegionCohort `json:"next"`
	Recovery bool                   `json:"recovery,omitempty"`
}

func (i HandoffIntent) Validate() error {
	p, n := i.Previous.Fence, i.Next
	if i.Previous.Validate() != nil || n.Validate() != nil || n.Formation || n.Reformation || n.State != rootstate.RegionCohortPlanned || p.Generation == math.MaxUint64 || n.Generation != p.Generation+1 || n.GroupID != p.GroupID || n.RegionID != p.RegionID || n.PlannedIndex <= p.PlannedIndex {
		return errors.New("invalid regional handoff generations")
	}
	if !overlapsOldVoterMajority(p, n.Nodes) {
		return errors.New("regional handoff must retain an old voter majority")
	}
	return nil
}

func (i HandoffIntent) Accepts(r Record, sealIndex uint64) bool {
	f := r.Fence
	return i.Validate() == nil && r.Validate() == nil && sealIndex > i.Previous.Index && f.Advances(i.Previous.Fence) &&
		f.Generation == i.Next.Generation && f.PlannedIndex == i.Next.PlannedIndex && f.MembershipIndex > sealIndex && slices.Equal(f.Nodes(), i.Next.Nodes)
}

// Activation is a trusted coordinator assertion of the exact Root publication.
// Deterministic data Apply cannot read another Group. The coordinator must
// authenticate/recheck Root before proposing this; this is not a public proof.
type Activation struct {
	Candidate Record                 `json:"candidate"`
	Confirmed rootstate.RegionCohort `json:"confirmed"`
}

func (a Activation) Validate() error {
	r, c := a.Candidate, a.Confirmed
	if r.Validate() != nil || c.Validate() != nil || c.State != rootstate.RegionCohortConfirmed || c.Formation || c.Reformation ||
		c.RegionID != r.Fence.RegionID || c.GroupID != r.Fence.GroupID || c.Generation != r.Fence.Generation || c.PlannedIndex != r.Fence.PlannedIndex || c.GroupEpoch != r.Fence.GroupEpoch || c.GroupFenceIndex != r.Index || c.GroupFenceRequestID != r.RequestID || !slices.Equal(c.Nodes, r.Fence.Nodes()) {
		return errors.New("regional activation differs from Root publication")
	}
	return nil
}

type HandoffCommand struct {
	Action     string         `json:"action"`
	Intent     *HandoffIntent `json:"intent,omitempty"`
	Activation *Activation    `json:"activation,omitempty"`
}

func (c HandoffCommand) Validate() error {
	switch c.Action {
	case Seal:
		if c.Intent != nil && c.Activation == nil {
			return c.Intent.Validate()
		}
	case Activate:
		if c.Intent == nil && c.Activation != nil {
			return c.Activation.Validate()
		}
	}
	return errors.New("invalid regional handoff command")
}

type Handoff struct {
	Intent              HandoffIntent `json:"intent"`
	SealRequestID       string        `json:"seal_request_id"`
	SealIndex           uint64        `json:"seal_index"`
	Activation          *Activation   `json:"activation,omitempty"`
	ActivationRequestID string        `json:"activation_request_id,omitempty"`
	ActivationIndex     uint64        `json:"activation_index,omitempty"`
}

func (h Handoff) Validate() error {
	if h.Intent.Validate() != nil || rootstate.ValidateRequestID(h.SealRequestID) != nil || h.SealIndex <= h.Intent.Previous.Index {
		return errors.New("invalid regional seal")
	}
	if h.Activation == nil {
		if h.ActivationIndex == 0 && h.ActivationRequestID == "" {
			return nil
		}
	} else if h.Activation.Validate() == nil && h.Intent.Accepts(h.Activation.Candidate, h.SealIndex) && h.ActivationIndex > h.Activation.Candidate.Index && rootstate.ValidateRequestID(h.ActivationRequestID) == nil && h.ActivationRequestID != h.SealRequestID {
		return nil
	}
	return errors.New("invalid regional activation linkage")
}

func (h Handoff) MatchesLatest(r Record) bool {
	if h.Validate() != nil {
		return false
	}
	if h.Activation != nil {
		return reflect.DeepEqual(r, h.Activation.Candidate)
	}
	return reflect.DeepEqual(r, h.Intent.Previous) || h.Intent.Accepts(r, h.SealIndex)
}
