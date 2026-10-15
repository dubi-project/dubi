package rootjoin

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	LocalCatchingUp = "catching_up"
	LocalActive     = "active"
)

var (
	ErrOlderView       = errors.New("Root routing view is older than the persisted cut")
	ErrConflictingView = errors.New("Root routing view conflicts at the same applied index")
	ErrRecoveryBehind  = errors.New("local Root recovery is behind its durable activation fence")
	storeName          = regexp.MustCompile(`^store_[0-9a-f]{32}$`)
)

// LocalState is the identity-bound recovery model for an enrolled Node. It
// contains no secrets and does not perform IO. A caller must durably publish
// transitions before using them. Peer routes never initialize Raft voters.
type LocalState struct {
	Version          int    `json:"version"`
	ClusterID        string `json:"cluster_id"`
	NodeID           string `json:"node_id"`
	RaftID           uint64 `json:"raft_id"`
	StoreID          string `json:"store_id"`
	GrantID          string `json:"grant_id"`
	RequestSHA256    string `json:"request_sha256"`
	AdmittedIndex    uint64 `json:"admitted_index"`
	InitialViewIndex uint64 `json:"initial_view_index"`
	ActivatedIndex   uint64 `json:"activated_index,omitempty"`
	Phase            string `json:"phase"`
	View             View   `json:"view"`
}

// NewLocalState requires a previously authenticated/loaded receipt and the
// matching authenticated Root admission response. It is not a second admission
// authority: validation of these public values cannot replace mTLS or Raft.
func NewLocalState(id identity.Identity, storeID string, receipt identityenrollment.Receipt, view View, admittedIndex uint64) (LocalState, error) {
	if receipt.Version != 1 || receipt.Request.NodeID != id.NodeID || receipt.Request.RaftID != id.RaftID ||
		receipt.Bundle.ClusterID != id.ClusterID || receipt.Bundle.NodeID != id.NodeID || receipt.Bundle.RaftID != id.RaftID {
		return LocalState{}, errors.New("Root admission receipt belongs to another Node")
	}
	encoded, err := identityenrollment.EncodeRequest(receipt.Request)
	if err != nil {
		return LocalState{}, err
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != receipt.RequestSHA256 {
		return LocalState{}, errors.New("Root admission receipt request digest differs")
	}
	if err := view.Validate(id.ClusterID); err != nil {
		return LocalState{}, err
	}
	view = view.CloneCanonical()
	view.LeaderID = 0
	state := LocalState{Version: 1, ClusterID: id.ClusterID, NodeID: id.NodeID, RaftID: id.RaftID, StoreID: storeID,
		GrantID: receipt.GrantID, RequestSHA256: receipt.RequestSHA256, AdmittedIndex: admittedIndex, InitialViewIndex: view.AppliedIndex,
		Phase: LocalCatchingUp, View: view}
	if err := state.Validate(id, storeID); err != nil {
		return LocalState{}, err
	}
	for _, peer := range view.Peers {
		if peer.RaftID == id.RaftID && (peer.RegionID != receipt.Request.RegionID || peer.Address != receipt.Request.Address) {
			return LocalState{}, errors.New("Root admission changed enrolled Node metadata")
		}
	}
	return state, nil
}

func (state LocalState) Validate(id identity.Identity, storeID string) error {
	if _, err := identity.NodeIdentityURI(id); err != nil {
		return err
	}
	if id.Origin != identity.OriginJoin || state.ClusterID != id.ClusterID || state.NodeID != id.NodeID || state.RaftID != id.RaftID || state.StoreID != storeID {
		return errors.New("local Root join belongs to a different Node or Store")
	}
	return state.validateContent()
}

func (state LocalState) validateContent() error {
	digest, err := hex.DecodeString(state.RequestSHA256)
	if state.Version != 1 || !storeName.MatchString(state.StoreID) || identity.ValidateJoinGrantID(state.GrantID) != nil ||
		err != nil || len(digest) != 32 || hex.EncodeToString(digest) != state.RequestSHA256 {
		return errors.New("invalid local Root join identity or request binding")
	}
	if state.AdmittedIndex == 0 || state.InitialViewIndex < state.AdmittedIndex || state.View.AppliedIndex < state.InitialViewIndex || state.View.LeaderID != 0 {
		return errors.New("invalid local Root join position or transient leader persisted")
	}
	if err := state.validateView(state.View); err != nil {
		return err
	}
	switch state.Phase {
	case LocalCatchingUp:
		if state.ActivatedIndex != 0 {
			return errors.New("catching-up Node has activation fence")
		}
	case LocalActive:
		if state.ActivatedIndex < state.InitialViewIndex || state.ActivatedIndex > state.View.AppliedIndex {
			return errors.New("invalid local Root activation fence")
		}
	default:
		return errors.New("unknown local Root join phase")
	}
	return nil
}

func (state LocalState) validateView(view View) error {
	if err := view.Validate(state.ClusterID); err != nil {
		return err
	}
	if !view.Contains(state.RaftID) {
		return ErrMemberRemoved
	}
	for _, peer := range view.Peers {
		if peer.RaftID == state.RaftID && peer.NodeID != state.NodeID {
			return errors.New("Root route changed local Node identity")
		}
	}
	return nil
}

// Observe advances only the routing cut, not the local catch-up phase. The
// caller establishes a confirmed view, then persists the returned value.
// A changed Node address is allowed at a newer cut, but identity is immutable.
func (state LocalState) Observe(view View) (LocalState, error) {
	if err := state.validateContent(); err != nil {
		return LocalState{}, err
	}
	if err := state.validateView(view); err != nil {
		return LocalState{}, err
	}
	if view.AppliedIndex < state.View.AppliedIndex {
		return LocalState{}, ErrOlderView
	}
	view = view.CloneCanonical()
	view.LeaderID = 0
	if view.AppliedIndex == state.View.AppliedIndex {
		old, err := json.Marshal(state.View.CloneCanonical())
		if err != nil {
			return LocalState{}, err
		}
		current, err := json.Marshal(view)
		if err != nil {
			return LocalState{}, err
		}
		if !bytes.Equal(old, current) {
			return LocalState{}, ErrConflictingView
		}
	}
	state.View = view
	return state, nil
}

// Activate is allowed only after the caller's LOCAL Root ReadIndex and a
// matching applied task/membership cut. A remote Join response alone is not
// local catch-up evidence. Repeated activation preserves the first fence.
func (state LocalState) Activate(local View, task rootstate.RootJoin) (LocalState, error) {
	if err := task.Validate(); err != nil {
		return LocalState{}, err
	}
	if task.State != rootstate.RootJoinAdmitted || task.GrantID != state.GrantID || task.NodeID != state.NodeID || task.RaftID != state.RaftID ||
		task.RequestSHA256 != state.RequestSHA256 || task.AdmittedIndex != state.AdmittedIndex {
		return LocalState{}, errors.New("local Root task does not match admission")
	}
	if state.Phase != LocalCatchingUp && state.Phase != LocalActive {
		return LocalState{}, errors.New("unknown local Root join phase")
	}
	updated, err := state.Observe(local)
	if err != nil {
		return LocalState{}, err
	}
	if updated.ActivatedIndex == 0 {
		updated.ActivatedIndex = local.AppliedIndex
	}
	updated.Phase = LocalActive
	return updated, nil
}

// CheckRecovered runs AFTER persisted snapshot/log replay and BEFORE opening
// the listener. Even an interrupted catching-up marker cannot excuse removal
// once the admitted position has been replayed. Active state never re-enters
// joining mode after losing local consensus resources.
func (state LocalState) CheckRecovered(applied uint64, conf *raftpb.ConfState) error {
	if err := state.validateContent(); err != nil {
		return err
	}
	if state.Phase == LocalActive && applied < state.ActivatedIndex {
		return ErrRecoveryBehind
	}
	if state.Phase != LocalCatchingUp && state.Phase != LocalActive {
		return errors.New("unknown local Root join phase")
	}
	if (state.Phase == LocalActive || applied >= state.AdmittedIndex) && !Contains(conf, state.RaftID) {
		return ErrMemberRemoved
	}
	return nil
}
