package raft

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft/logstore"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// StateMachine owns one Group's application state. Each Apply/Advance call
// must durably and atomically persist its effects and AppliedIndex. Replaying
// an already-applied index must be harmless. Group owns and closes it.
type StateMachine interface {
	AppliedIndex() uint64
	ConfState() *raftpb.ConfState
	Apply(index uint64, command []byte) error
	Advance(index uint64) error
	ApplyMembership(index uint64, conf *raftpb.ConfState) error
	Restore(index uint64, snapshot []byte, conf *raftpb.ConfState) error
	Close() error
}

// bootstrapMembershipState persists the initial ConfState that is created by
// Raft storage Bootstrap. Bootstrap does not emit a ConfChange entry, so a
// fresh one-node or forming regional Group otherwise leaves the application
// state with an empty membership envelope and cannot publish its first fence.
// It is optional so existing StateMachine implementations keep the same core
// contract; the built-in Root and Engine states implement it.
type bootstrapMembershipState interface {
	BootstrapMembership(*raftpb.ConfState) error
}

// CommandValidator rejects malformed application commands before a leader
// proposes them. Apply must still validate committed bytes deterministically;
// this hook prevents an invalid client command from poisoning a whole Group.
type CommandValidator interface {
	ValidateCommand(command []byte) error
}

// Snapshotter captures the application state and its exact Raft cut. The
// returned bytes must restore that state on another Node without engine-
// specific knowledge in the Raft layer.
type Snapshotter interface {
	Snapshot() (data []byte, index uint64, conf *raftpb.ConfState, err error)
}

// SnapshotPreparer captures an immutable cut quickly under the Group lock.
// Build runs outside that lock and must honor cancellation; Close releases the
// captured resources on success, failure or shutdown. The returned bytes may
// be a durable artifact reference once the state/transport support that format.
type SnapshotPreparer interface {
	PrepareSnapshot() (*PreparedSnapshot, error)
}

type PreparedSnapshot struct {
	Index uint64
	Conf  *raftpb.ConfState
	Build func(context.Context) ([]byte, error)
	Close func() error
}

var ErrSnapshotBusy = errors.New("Raft Group already has a snapshot build in progress")

var ErrNoLeader = errors.New("Raft Group has no known leader for ReadIndex")
var ErrReadIndexBusy = errors.New("Raft Group has too many pending ReadIndex requests")

const (
	defaultReadIndexTimeout = 10 * time.Second
	maxPendingReadIndexes   = 1024
)

// Transport enqueues a durable Raft message for another Node. Send must not
// synchronously call back into this Group; Group emits messages after unlocking.
type Transport interface {
	Send(groupID string, message *raftpb.Message) error
}

type GroupOptions struct {
	StoreRoot string
	GroupID   string
	RaftID    uint64
	// BootstrapPeers is fixed durably on first creation, before any Raft Ready.
	// On restart it may be omitted or must match the original set exactly.
	// Leave empty when joining an existing Group as a learner.
	BootstrapPeers []uint64
	Joining        bool
	State          StateMachine
	Transport      Transport
	ProposalGuard  func() error
}

type Group struct {
	mu           sync.Mutex
	id           uint64
	groupID      string
	storage      *logstore.Storage
	state        StateMachine
	transport    Transport
	raw          *etcdraft.RawNode
	closed       bool
	fatal        error
	pendingReads map[string]*pendingRead
	// readBarrierProposalTerm records the leader term for which a Raft
	// no-op has already been proposed when ReadIndex needed to establish a
	// current-term commit. It prevents concurrent readers from appending an
	// unbounded stream of identical barrier entries.
	readBarrierProposalTerm uint64
	proposalGuard           func() error
	snapshotting            bool
	snapshotCancel          context.CancelFunc
	snapshotWG              sync.WaitGroup
	closeDone               chan struct{}
	closeErr                error
}

type pendingRead struct {
	index  uint64
	ready  bool
	result chan readResult
}

type readResult struct {
	index uint64
	err   error
}

type GroupStatus struct {
	RaftID         uint64
	LeaderID       uint64
	Role           etcdraft.StateType
	Term           uint64
	CommitIndex    uint64
	AppliedIndex   uint64
	LeadTransferee uint64
}

// OpenGroup starts a serialized, caller-driven Raft Group. The surrounding
// Node host will supply ticks, transport, membership operations and shutdown.
func OpenGroup(options GroupOptions) (*Group, error) {
	return openGroup(options, nil)
}

func openGroup(options GroupOptions, hook func(string) error) (*Group, error) {
	if options.RaftID == 0 || options.State == nil {
		return nil, errors.New("Raft Group requires a non-zero RaftID and StateMachine")
	}
	root, err := PrepareGroupRoot(options.StoreRoot, options.GroupID)
	if err != nil {
		return nil, errors.Join(err, options.State.Close())
	}
	expectedLog, err := groupLogBinding(options.StoreRoot, options.GroupID, "")
	if err != nil {
		return nil, errors.Join(err, options.State.Close())
	}
	storage, err := logstore.OpenForGroup(root, expectedLog)
	if err != nil {
		_ = options.State.Close()
		return nil, err
	}
	closeOnError := func(openErr error) (*Group, error) {
		return nil, errors.Join(openErr, options.State.Close(), storage.Close())
	}
	stage := func(at string) error {
		if hook != nil {
			return hook(at)
		}
		return nil
	}
	if err := stage("after-log-open"); err != nil {
		return closeOnError(err)
	}
	if _, err := groupLogBinding(options.StoreRoot, options.GroupID, storage.Identity()); err != nil {
		return closeOnError(err)
	}
	if err := stage("after-log-binding"); err != nil {
		return closeOnError(err)
	}
	initialPeers, err := storage.BindGroup(options.GroupID, options.RaftID, options.BootstrapPeers, options.Joining)
	if err != nil {
		return closeOnError(err)
	}
	if err := stage("after-formation"); err != nil {
		return closeOnError(err)
	}
	if err := reconcileGroup(storage, options.State); err != nil {
		return closeOnError(err)
	}
	hard, storedConf, err := storage.InitialState()
	if err != nil {
		return closeOnError(err)
	}
	if options.State.AppliedIndex() > hard.GetCommit() {
		return closeOnError(fmt.Errorf("StateMachine applied index %d exceeds Raft commit %d", options.State.AppliedIndex(), hard.GetCommit()))
	}
	// A historical snapshot may predate this Node's AddLearner. Membership
	// eligibility is checked after replaying the committed tail below, never
	// from that cut alone. Joining=true is supplied only by an authorized join
	// workflow; normal recovery still rejects absent/removed Nodes after replay.
	if options.Transport == nil && len(initialPeers) > 1 {
		return closeOnError(errors.New("multi-node Raft Group requires a Transport"))
	}
	if options.Transport == nil && membershipSize(storedConf) > 1 {
		return closeOnError(errors.New("persisted multi-node Raft Group requires a Transport"))
	}
	last, err := storage.LastIndex()
	if err != nil {
		return closeOnError(err)
	}
	// Root is the only global Raft Group. Its messages may cross regions with
	// 150--300ms RTT, so a local 100ms heartbeat and one-second election lease
	// causes healthy voters to repeatedly challenge one another. Keep data
	// Groups responsive locally, but give Root a 500ms heartbeat and a 15s
	// election window. The wider lease prevents a busy cross-region control
	// stream or a short VPS scheduling pause from needlessly changing the Root
	// leader; management availability is more important than a few seconds of
	// failover latency.
	heartbeatTick, electionTick := 1, 10
	if options.GroupID == RootGroupID {
		heartbeatTick, electionTick = 5, 150
	}
	raw, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID: options.RaftID, ElectionTick: electionTick, HeartbeatTick: heartbeatTick,
		Storage: storage, Applied: options.State.AppliedIndex(),
		MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 4 << 20,
		MaxUncommittedEntriesSize: 16 << 20, MaxInflightMsgs: 256,
		CheckQuorum: true, PreVote: true,
	})
	if err != nil {
		return closeOnError(err)
	}
	group := &Group{id: options.RaftID, groupID: options.GroupID, storage: storage,
		state: options.State, transport: options.Transport, raw: raw,
		pendingReads: make(map[string]*pendingRead), proposalGuard: options.ProposalGuard, closeDone: make(chan struct{})}
	if last == 0 && len(initialPeers) != 0 {
		peers, err := bootstrapPeers(initialPeers, options.RaftID)
		if err != nil {
			return closeOnError(err)
		}
		if err := raw.Bootstrap(peers); err != nil {
			return closeOnError(err)
		}
		if bootstrap, ok := options.State.(bootstrapMembershipState); ok {
			conf := &raftpb.ConfState{Voters: append([]uint64(nil), initialPeers...)}
			if err := bootstrap.BootstrapMembership(conf); err != nil {
				return closeOnError(err)
			}
		}
	}
	if err := group.process(); err != nil {
		return closeOnError(err)
	}
	if err := stage("after-initial-ready"); err != nil {
		return closeOnError(err)
	}
	if !options.Joining && !containsMember(options.State.ConfState(), options.RaftID) {
		return closeOnError(fmt.Errorf("RaftID %d is not in Group %q membership", options.RaftID, options.GroupID))
	}
	return group, nil
}

func containsMember(conf *raftpb.ConfState, id uint64) bool {
	for _, member := range conf.GetVoters() {
		if member == id {
			return true
		}
	}
	for _, member := range conf.GetVotersOutgoing() {
		if member == id {
			return true
		}
	}
	for _, member := range conf.GetLearners() {
		if member == id {
			return true
		}
	}
	for _, member := range conf.GetLearnersNext() {
		if member == id {
			return true
		}
	}
	return false
}

func membershipSize(conf *raftpb.ConfState) int {
	return len(conf.GetVoters()) + len(conf.GetLearners()) + len(conf.GetVotersOutgoing()) + len(conf.GetLearnersNext())
}

func containsVoter(conf *raftpb.ConfState, id uint64) bool {
	for _, voter := range conf.GetVoters() {
		if voter == id {
			return true
		}
	}
	return false
}

func containsLearner(conf *raftpb.ConfState, id uint64) bool {
	for _, learner := range conf.GetLearners() {
		if learner == id {
			return true
		}
	}
	return false
}

func bootstrapPeers(ids []uint64, local uint64) ([]etcdraft.Peer, error) {
	peers := make([]etcdraft.Peer, 0, len(ids))
	seen := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		if id == 0 || seen[id] {
			return nil, errors.New("bootstrap Raft IDs must be non-zero and unique")
		}
		seen[id] = true
		peers = append(peers, etcdraft.Peer{ID: id})
	}
	if !seen[local] {
		return nil, errors.New("bootstrap Raft peers must include the local RaftID")
	}
	return peers, nil
}

func reconcileGroup(storage *logstore.Storage, state StateMachine) error {
	snapshot, err := storage.Snapshot()
	if err != nil {
		return err
	}
	if snapshot.GetMetadata().GetIndex() > state.AppliedIndex() {
		if err := state.Restore(snapshot.GetMetadata().GetIndex(), snapshot.GetData(), snapshot.GetMetadata().GetConfState()); err != nil {
			return fmt.Errorf("restore unapplied Raft snapshot: %w", err)
		}
	}
	_, storedConf, err := storage.InitialState()
	if err != nil {
		return err
	}
	if state.AppliedIndex() > 0 && !proto.Equal(storedConf, state.ConfState()) {
		if err := storage.SaveConfState(state.ConfState()); err != nil {
			return fmt.Errorf("reconcile persisted ConfState: %w", err)
		}
	}
	return nil
}

// Campaign requests leadership; success means only that the request was
// processed, not that this Group has won an election.
func (g *Group) Campaign() error {
	return g.mutate(func() error { return g.raw.Campaign() })
}

// Submit proposes an opaque command. It does not imply the command committed;
// callers must observe their durable request result in the StateMachine.
func (g *Group) Submit(command []byte) error {
	if len(command) == 0 {
		return errors.New("empty command is reserved for Raft no-op entries")
	}
	if validator, ok := g.state.(CommandValidator); ok {
		if err := validator.ValidateCommand(command); err != nil {
			return fmt.Errorf("validate Raft command: %w", err)
		}
	}
	return g.mutate(func() error {
		if err := g.admitProposal(); err != nil {
			return err
		}
		return g.raw.Propose(command)
	})
}

// ReadIndex returns only after a quorum-confirmed read barrier is applied by
// this local StateMachine. The caller can then read its local durable view.
func (g *Group) ReadIndex(ctx context.Context) (uint64, error) {
	if ctx == nil {
		return 0, errors.New("Raft ReadIndex requires context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultReadIndexTimeout)
		defer cancel()
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return 0, fmt.Errorf("create Raft read token: %w", err)
	}
	key := string(token[:])
	request := &pendingRead{result: make(chan readResult, 1)}
	g.mu.Lock()
	if err := g.checkLocked(); err != nil {
		g.mu.Unlock()
		return 0, err
	}
	if g.raw.BasicStatus().SoftState.Lead == 0 {
		g.mu.Unlock()
		return 0, ErrNoLeader
	}
	if len(g.pendingReads) >= maxPendingReadIndexes {
		g.mu.Unlock()
		return 0, ErrReadIndexBusy
	}
	status := g.raw.Status()
	// A current-term committed entry is required before etcd/raft can release
	// ReadIndex. Propose that entry once after leadership changes, then use the
	// native quorum-confirmed ReadIndex path for each read. A per-read no-op
	// would be correct but creates unbounded Root log traffic from every Node;
	// learners can then fall behind the very Root cut they must observe.
	singleVoter := len(status.Config.Voters[0]) == 1 && len(status.Config.Voters[1]) == 0
	avoidLeaderBarrier := singleVoter && g.snapshotting && g.groupID != RootGroupID
	// etcd/raft intentionally holds ReadIndex requests until the leader has
	// committed an entry in its current term. Regional Groups can be idle for
	// long periods, so a freshly elected leader otherwise makes every read
	// (including repair/status observations) wait until its context expires.
	// Commit one empty normal entry once per term to establish that barrier.
	if !avoidLeaderBarrier {
		committedInTerm := false
		if commit := status.GetCommit(); commit != 0 {
			term, termErr := g.storage.Term(commit)
			if termErr != nil {
				g.mu.Unlock()
				return 0, fmt.Errorf("read current-term barrier: %w", termErr)
			}
			committedInTerm = term == status.GetTerm()
		}
		if committedInTerm {
			g.readBarrierProposalTerm = status.GetTerm()
		} else if g.readBarrierProposalTerm != status.GetTerm() {
			if g.admitProposal() == nil {
				if err := g.raw.Propose(nil); err != nil {
					g.mu.Unlock()
					return 0, fmt.Errorf("propose current-term read barrier: %w", err)
				}
				g.readBarrierProposalTerm = status.GetTerm()
			}
		}
	}
	g.pendingReads[key] = request
	g.raw.ReadIndex(token[:])
	messages, err := g.processLocked()
	g.mu.Unlock()
	if err != nil {
		return 0, err
	}
	g.dispatch(messages)
	select {
	case result := <-request.result:
		return result.index, result.err
	case <-ctx.Done():
		g.mu.Lock()
		status := g.raw.BasicStatus()
		pending := len(g.pendingReads)
		applied := g.state.AppliedIndex()
		delete(g.pendingReads, key)
		g.mu.Unlock()
		return 0, fmt.Errorf("%w (leader=%d term=%d commit=%d applied=%d pending_reads=%d)",
			ctx.Err(), status.SoftState.Lead, status.HardState.GetTerm(), status.HardState.GetCommit(), applied, pending)
	}
}

func (g *Group) waitApplied(ctx context.Context, index uint64) (uint64, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		g.mu.Lock()
		if err := g.checkLocked(); err != nil {
			g.mu.Unlock()
			return 0, err
		}
		applied := g.state.AppliedIndex()
		g.mu.Unlock()
		if applied >= index {
			return index, nil
		}
		select {
		case <-ctx.Done():
			g.mu.Lock()
			status := g.raw.BasicStatus()
			applied = g.state.AppliedIndex()
			g.mu.Unlock()
			return 0, fmt.Errorf("%w (barrier_index=%d leader=%d term=%d commit=%d applied=%d)",
				ctx.Err(), index, status.SoftState.Lead, status.HardState.GetTerm(), status.HardState.GetCommit(), applied)
		case <-ticker.C:
		}
	}
}

// Membership returns the durably applied Group member set, not a placement
// policy or a user-configured number of copies.
func (g *Group) Membership() (*raftpb.ConfState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return nil, err
	}
	conf := g.state.ConfState()
	if conf == nil {
		return &raftpb.ConfState{}, nil
	}
	return proto.Clone(conf).(*raftpb.ConfState), nil
}

// AddLearner proposes a non-voting peer. The caller must first open its Group
// and make its address resolvable. Success means proposed, not committed.
func (g *Group) AddLearner(raftID uint64) error {
	return g.proposeMembership(raftID, raftpb.ConfChangeType_ConfChangeAddLearnerNode)
}

// PromoteLearner requires recent Raft activity and a log matched through the
// current committed index. Historical Match alone survives disconnection and
// must not qualify an offline peer for a larger voting quorum. Recent activity
// is not a guarantee against subsequent failure; the change still commits via
// Raft and does not establish regional placement/durability qualification.
func (g *Group) PromoteLearner(raftID uint64) error {
	return g.proposeMembership(raftID, raftpb.ConfChangeType_ConfChangeAddNode)
}

// RemoveMember proposes removing a learner or voter. Removing the local
// leader requires a separate leadership-transfer and drain workflow.
func (g *Group) RemoveMember(raftID uint64) error {
	return g.proposeMembership(raftID, raftpb.ConfChangeType_ConfChangeRemoveNode)
}

// TransferLeadership waits until a different voting peer is actually Leader.
// The caller should set a deadline. Raft itself catches a lagging voter up
// before sending MsgTimeoutNow; a temporary transfer abort is retried.
func (g *Group) TransferLeadership(ctx context.Context, target uint64) (GroupStatus, error) {
	return g.transferLeadership(ctx, target, nil, 0)
}

func (g *Group) transferLeadership(ctx context.Context, target uint64, expected *MembershipCut, term uint64) (GroupStatus, error) {
	if target == 0 || target == g.id {
		return GroupStatus{}, errors.New("leadership transfer target must be another RaftID")
	}
	if err := ctx.Err(); err != nil {
		return GroupStatus{}, err
	}
	if err := g.requestLeadershipTransferExpected(target, expected, term); err != nil {
		return GroupStatus{}, err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := g.Status()
		if err != nil {
			return GroupStatus{}, err
		}
		if status.LeaderID == target && status.Role != etcdraft.StateLeader {
			return status, nil
		}
		if status.LeaderID != 0 && status.LeaderID != g.id && status.LeaderID != target {
			return GroupStatus{}, fmt.Errorf("leadership moved to RaftID %d instead of %d", status.LeaderID, target)
		}
		if status.Role == etcdraft.StateLeader && status.LeadTransferee == 0 {
			if err := g.requestLeadershipTransferExpected(target, expected, term); err != nil {
				return GroupStatus{}, err
			}
		}
		select {
		case <-ctx.Done():
			return GroupStatus{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (g *Group) requestLeadershipTransfer(target uint64) error {
	return g.requestLeadershipTransferExpected(target, nil, 0)
}

func (g *Group) requestLeadershipTransferExpected(target uint64, expected *MembershipCut, term uint64) error {
	return g.mutate(func() error {
		status := g.raw.Status()
		if status.RaftState != etcdraft.StateLeader {
			return fmt.Errorf("leadership transfer requires Group leader; current leader is %d", status.Lead)
		}
		if expected != nil {
			cut, err := g.membershipCutLocked()
			if err != nil {
				return err
			}
			if status.GetTerm() != term || cut.Index != expected.Index || !proto.Equal(cut.Conf, expected.Conf) {
				return ErrMembershipChanged
			}
			if len(cut.Conf.GetVotersOutgoing()) != 0 || len(cut.Conf.GetLearnersNext()) != 0 || cut.Conf.GetAutoLeave() {
				return errors.New("fenced transfer requires stable membership")
			}
		}
		if !containsVoter(g.state.ConfState(), target) {
			return fmt.Errorf("RaftID %d is not a voting Group member", target)
		}
		g.raw.TransferLeader(target)
		return nil
	})
}

func (g *Group) proposeMembership(raftID uint64, changeType raftpb.ConfChangeType) error {
	return g.proposeMembershipExpected(raftID, changeType, nil)
}

func (g *Group) proposeMembershipExpected(raftID uint64, changeType raftpb.ConfChangeType, expected *MembershipCut) error {
	if raftID == 0 {
		return errors.New("member RaftID must not be zero")
	}
	return g.mutate(func() error {
		if err := g.admitProposal(); err != nil {
			return err
		}
		status := g.raw.Status()
		if status.RaftState != etcdraft.StateLeader {
			return fmt.Errorf("member change requires Group leader; current leader is %d", status.Lead)
		}
		conf := g.state.ConfState()
		if expected != nil {
			cut, err := g.membershipCutLocked()
			if err != nil {
				return err
			}
			if cut.Index != expected.Index || !proto.Equal(cut.Conf, expected.Conf) {
				return ErrMembershipChanged
			}
		}
		if len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 {
			return errors.New("member change is already in joint consensus")
		}
		switch changeType {
		case raftpb.ConfChangeType_ConfChangeAddLearnerNode:
			if containsMember(conf, raftID) {
				return fmt.Errorf("RaftID %d is already a Group member", raftID)
			}
		case raftpb.ConfChangeType_ConfChangeAddNode:
			if !containsLearner(conf, raftID) {
				return fmt.Errorf("RaftID %d must first join as a learner", raftID)
			}
			progress, found := status.Progress[raftID]
			if !found || !progress.RecentActive {
				return fmt.Errorf("learner %d has no recent Raft activity", raftID)
			}
			if progress.Match < status.GetCommit() {
				return fmt.Errorf("learner %d has not caught up to committed index %d", raftID, status.GetCommit())
			}
		case raftpb.ConfChangeType_ConfChangeRemoveNode:
			if !containsMember(conf, raftID) {
				return fmt.Errorf("RaftID %d is not a Group member", raftID)
			}
			if raftID == g.id {
				return errors.New("removing local Group leader requires leadership transfer")
			}
			if containsVoter(conf, raftID) && len(conf.GetVoters()) <= 1 {
				return errors.New("cannot remove the last Group voter")
			}
		default:
			return errors.New("unsupported Group member change")
		}
		change := &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{
			Type: changeType.Enum(), NodeId: proto.Uint64(raftID)}}}
		return g.raw.ProposeConfChange(change)
	})
}

func (g *Group) Tick() error {
	return g.mutate(func() error { g.raw.Tick(); return nil })
}

func (g *Group) Step(message *raftpb.Message) error {
	if message == nil {
		return errors.New("Raft message must not be nil")
	}
	return g.mutate(func() error {
		// Forwarded proposals must pass the leader's own admission. Replication,
		// ReadIndex, election traffic and committed snapshot installation must
		// still make progress using the reserved headroom.
		if message.GetType() == raftpb.MessageType_MsgProp {
			if err := g.admitProposal(); err != nil {
				return err
			}
		}
		return g.raw.Step(message)
	})
}

// ReportUnreachable informs Raft that an asynchronous peer delivery failed.
func (g *Group) ReportUnreachable(raftID uint64) error {
	if raftID == 0 || raftID == g.id {
		return errors.New("unreachable peer must be a remote RaftID")
	}
	return g.mutate(func() error { g.raw.ReportUnreachable(raftID); return nil })
}

func (g *Group) ReportSnapshot(raftID uint64, success bool) error {
	if raftID == 0 || raftID == g.id {
		return errors.New("snapshot peer must be a remote RaftID")
	}
	status := etcdraft.SnapshotFailure
	if success {
		status = etcdraft.SnapshotFinish
	}
	return g.mutate(func() error { g.raw.ReportSnapshot(raftID, status); return nil })
}

// InstallSnapshot returns only after the snapshot and restored state machine
// are durable, so an RPC caller may use the returned index as an ACK.
func (g *Group) InstallSnapshot(message *raftpb.Message) (uint64, error) {
	if message == nil || message.GetType() != raftpb.MessageType_MsgSnap || etcdraft.IsEmptySnap(message.GetSnapshot()) {
		return 0, errors.New("non-empty Raft MsgSnap is required")
	}
	if err := g.Step(message); err != nil {
		return 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return 0, err
	}
	index := g.state.AppliedIndex()
	if index < message.GetSnapshot().GetMetadata().GetIndex() {
		return 0, fmt.Errorf("snapshot index %d was not applied; current index %d", message.GetSnapshot().GetMetadata().GetIndex(), index)
	}
	return index, nil
}

// CreateSnapshot writes a engine-opaque state image into Raft storage and
// compacts the log through the included index in one durable storage batch.
func (g *Group) CreateSnapshot() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return 0, err
	}
	if err := g.admitProposal(); err != nil {
		return 0, err
	}
	if g.snapshotting {
		return 0, ErrSnapshotBusy
	}
	if preparer, ok := g.state.(SnapshotPreparer); ok {
		return g.createPreparedSnapshotLocked(preparer)
	}
	snapshotter, ok := g.state.(Snapshotter)
	if !ok {
		return 0, errors.New("Raft StateMachine does not implement Snapshotter")
	}
	data, index, conf, err := snapshotter.Snapshot()
	if err != nil {
		return 0, err
	}
	if index == 0 || index != g.state.AppliedIndex() || conf == nil || !proto.Equal(conf, g.state.ConfState()) {
		return 0, errors.New("StateMachine snapshot does not match the current applied index and membership")
	}
	current, err := g.storage.Snapshot()
	if err != nil {
		return 0, err
	}
	if index <= current.GetMetadata().GetIndex() {
		return current.GetMetadata().GetIndex(), nil
	}
	term, err := g.storage.Term(index)
	if err != nil {
		return 0, err
	}
	snapshot := &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(index), Term: proto.Uint64(term), ConfState: conf}}
	if err := g.storage.SaveLocalSnapshot(snapshot); err != nil {
		return 0, g.failLocked(err)
	}
	return index, nil
}

// Entered and returned with g.mu held. Exactly one builder per Group holds a
// shutdown lease; any concurrent Apply/ReadIndex/tick may run during Build.
func (g *Group) createPreparedSnapshotLocked(preparer SnapshotPreparer) (uint64, error) {
	prepared, err := preparer.PrepareSnapshot()
	if err != nil {
		if prepared != nil && prepared.Close != nil {
			err = errors.Join(err, prepared.Close())
		}
		return 0, err
	}
	if prepared == nil || prepared.Close == nil {
		return 0, errors.New("invalid snapshot preparation")
	}
	if prepared.Build == nil || prepared.Index == 0 || prepared.Index != g.state.AppliedIndex() ||
		prepared.Conf == nil || !proto.Equal(prepared.Conf, g.state.ConfState()) {
		return 0, errors.Join(errors.New("prepared snapshot does not match captured Raft state"), prepared.Close())
	}
	index, conf := prepared.Index, proto.Clone(prepared.Conf).(*raftpb.ConfState)
	current, err := g.storage.Snapshot()
	if err != nil {
		return 0, errors.Join(err, prepared.Close())
	}
	if index <= current.GetMetadata().GetIndex() {
		return current.GetMetadata().GetIndex(), prepared.Close()
	}
	term, err := g.storage.Term(index)
	if err != nil {
		return 0, errors.Join(err, prepared.Close())
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.snapshotting, g.snapshotCancel = true, cancel
	g.snapshotWG.Add(1)
	// Reacquire before the outer deferred unlock even on unexpected panics.
	data, buildErr := func() ([]byte, error) {
		g.mu.Unlock()
		defer g.mu.Lock()
		defer cancel()
		defer g.snapshotWG.Done()
		var data []byte
		var err error
		func() {
			defer func() { err = errors.Join(err, prepared.Close()) }()
			data, err = prepared.Build(ctx)
		}()
		return data, err
	}()
	g.snapshotting, g.snapshotCancel = false, nil
	if buildErr != nil {
		return 0, buildErr
	}
	if err := g.checkLocked(); err != nil {
		return 0, err
	}
	current, err = g.storage.Snapshot()
	if err != nil {
		return 0, err
	}
	// A received newer snapshot can supersede this local immutable cut while
	// its file was being built. Retain the unused artifact for later safe GC.
	if index <= current.GetMetadata().GetIndex() {
		return current.GetMetadata().GetIndex(), nil
	}
	if index > g.state.AppliedIndex() {
		return 0, errors.New("prepared snapshot is ahead of applied state")
	}
	snapshot := &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(index), Term: proto.Uint64(term), ConfState: conf}}
	if err := g.storage.SaveLocalSnapshot(snapshot); err != nil {
		return 0, g.failLocked(err)
	}
	return index, nil
}

func (g *Group) Status() (GroupStatus, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return GroupStatus{}, err
	}
	status := g.raw.BasicStatus()
	return GroupStatus{RaftID: g.id, LeaderID: status.SoftState.Lead,
		Role: status.SoftState.RaftState, Term: status.HardState.GetTerm(),
		CommitIndex: status.HardState.GetCommit(), AppliedIndex: g.state.AppliedIndex(),
		LeadTransferee: status.LeadTransferee}, nil
}

// TermAt returns the Raft term for a persisted applied/snapshot index. Restore
// snapshots must carry the term at the cut, not merely the current term.
func (g *Group) TermAt(index uint64) (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return 0, err
	}
	if index == 0 {
		return 0, errors.New("snapshot index is required")
	}
	return g.storage.Term(index)
}

func (g *Group) mutate(operation func() error) error {
	g.mu.Lock()
	if err := g.checkLocked(); err != nil {
		g.mu.Unlock()
		return err
	}
	if err := operation(); err != nil {
		g.mu.Unlock()
		return err
	}
	messages, err := g.processLocked()
	g.mu.Unlock()
	if err != nil {
		return err
	}
	g.dispatch(messages)
	return nil
}

func (g *Group) admitProposal() error {
	if g.proposalGuard != nil {
		return g.proposalGuard()
	}
	return nil
}

func (g *Group) process() error {
	g.mu.Lock()
	messages, err := g.processLocked()
	g.mu.Unlock()
	if err != nil {
		return err
	}
	g.dispatch(messages)
	return nil
}

func (g *Group) processLocked() ([]*raftpb.Message, error) {
	var outbound []*raftpb.Message
	for pass := 0; g.raw.HasReady(); pass++ {
		if pass >= 1024 {
			return nil, g.failLocked(errors.New("Raft Ready did not quiesce after 1024 batches"))
		}
		ready := g.raw.Ready()
		if err := g.storage.Save(ready.HardState, ready.Entries, ready.Snapshot); err != nil {
			return nil, g.failLocked(err)
		}
		if !etcdraft.IsEmptySnap(ready.Snapshot) && ready.Snapshot.GetMetadata().GetIndex() > g.state.AppliedIndex() {
			if err := g.state.Restore(ready.Snapshot.GetMetadata().GetIndex(), ready.Snapshot.GetData(), ready.Snapshot.GetMetadata().GetConfState()); err != nil {
				return nil, g.failLocked(err)
			}
		}
		for _, entry := range ready.CommittedEntries {
			if err := g.applyEntry(entry); err != nil {
				return nil, g.failLocked(err)
			}
		}
		for _, readState := range ready.ReadStates {
			if request := g.pendingReads[string(readState.RequestCtx)]; request != nil {
				request.index = readState.Index
				request.ready = true
			}
		}
		g.completeReadsLocked()
		var rootProgress map[uint64]tracker.Progress
		if g.groupID == RootGroupID {
			rootProgress = g.raw.Status().Progress
		}
		for _, message := range ready.Messages {
			if message.GetTo() == g.id {
				if err := g.raw.Step(message); err != nil && !errors.Is(err, etcdraft.ErrStepLocalMsg) {
					return nil, g.failLocked(err)
				}
			} else {
				// ReadIndex heartbeats only need acknowledgements from voting
				// members. Root has many non-voting Nodes; broadcasting every
				// linearizable read to all of them saturates the per-peer control
				// queues and delays the voter quorum itself.
				if isRootReadHeartbeatToLearner(g.groupID, message, rootProgress) {
					continue
				}
				outbound = append(outbound, proto.Clone(message).(*raftpb.Message))
			}
		}
		g.raw.Advance(ready)
	}
	return outbound, nil
}

func isRootReadHeartbeatToLearner(groupID string, message *raftpb.Message, progress map[uint64]tracker.Progress) bool {
	if groupID != RootGroupID || message == nil || message.GetType() != raftpb.MessageType_MsgHeartbeat || len(message.GetContext()) == 0 {
		return false
	}
	return progress[message.GetTo()].IsLearner
}

func (g *Group) completeReadsLocked() {
	for key, request := range g.pendingReads {
		if request.ready && g.state.AppliedIndex() >= request.index {
			request.result <- readResult{index: request.index}
			delete(g.pendingReads, key)
		}
	}
}

func (g *Group) failReadsLocked(err error) {
	for key, request := range g.pendingReads {
		request.result <- readResult{err: err}
		delete(g.pendingReads, key)
	}
}

func (g *Group) applyEntry(entry *raftpb.Entry) error {
	index := entry.GetIndex()
	if index <= g.state.AppliedIndex() {
		return nil
	}
	switch entry.GetType() {
	case raftpb.EntryType_EntryNormal:
		if len(entry.GetData()) == 0 {
			return g.state.Advance(index)
		}
		return g.state.Apply(index, entry.GetData())
	case raftpb.EntryType_EntryConfChange:
		change := new(raftpb.ConfChange)
		if err := proto.Unmarshal(entry.GetData(), change); err != nil {
			return fmt.Errorf("decode ConfChange at index %d: %w", index, err)
		}
		return g.applyConfChange(index, change)
	case raftpb.EntryType_EntryConfChangeV2:
		change := new(raftpb.ConfChangeV2)
		if err := proto.Unmarshal(entry.GetData(), change); err != nil {
			return fmt.Errorf("decode ConfChangeV2 at index %d: %w", index, err)
		}
		return g.applyConfChange(index, change)
	default:
		return fmt.Errorf("unsupported Raft entry type %s", entry.GetType())
	}
}

func (g *Group) applyConfChange(index uint64, change raftpb.ConfChangeI) error {
	conf := g.raw.ApplyConfChange(change)
	if err := g.state.ApplyMembership(index, conf); err != nil {
		return err
	}
	return g.storage.SaveConfState(conf)
}

func (g *Group) dispatch(messages []*raftpb.Message) {
	for _, message := range messages {
		if g.transport == nil {
			g.mu.Lock()
			if !g.closed && g.fatal == nil {
				if message.GetType() == raftpb.MessageType_MsgSnap {
					g.raw.ReportSnapshot(message.GetTo(), etcdraft.SnapshotFailure)
				} else {
					g.raw.ReportUnreachable(message.GetTo())
				}
			}
			g.mu.Unlock()
			continue
		}
		if err := g.transport.Send(g.groupID, message); err != nil {
			g.mu.Lock()
			if !g.closed && g.fatal == nil {
				if message.GetType() == raftpb.MessageType_MsgSnap {
					g.raw.ReportSnapshot(message.GetTo(), etcdraft.SnapshotFailure)
				} else {
					g.raw.ReportUnreachable(message.GetTo())
				}
			}
			g.mu.Unlock()
		}
	}
}

func (g *Group) checkLocked() error {
	if g.closed {
		return errors.New("Raft Group is closed")
	}
	return g.fatal
}

func (g *Group) failLocked(err error) error {
	g.fatal = err
	g.failReadsLocked(err)
	return err
}

func (g *Group) Close() error {
	g.mu.Lock()
	if g.closed {
		done := g.closeDone
		g.mu.Unlock()
		<-done
		return g.closeErr
	}
	g.closed = true
	g.failReadsLocked(errors.New("Raft Group is closed"))
	if g.snapshotCancel != nil {
		g.snapshotCancel()
	}
	g.mu.Unlock()
	g.snapshotWG.Wait()
	err := errors.Join(g.state.Close(), g.storage.Close())
	g.mu.Lock()
	g.closeErr = err
	close(g.closeDone)
	g.mu.Unlock()
	return err
}
