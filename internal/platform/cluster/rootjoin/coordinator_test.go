package rootjoin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type discardTransport struct{}

func TestRootJoinRejectedCSRDoesNotPoisonAuthorizedRequest(t *testing.T) {
	host, root, start, _ := coordinatorFixture(t)
	c, err := New(host, root)
	if err != nil {
		t.Fatal(err)
	}
	_, wrong, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "r1", "wrong:7443")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Ensure(ctx, rootstate.RootJoinStart{GrantID: start.GrantID, Request: wrong}); !errors.Is(err, ErrJoinRejected) {
		t.Fatal("wrong CSR not rejected", err)
	}
	if task, err := c.Ensure(ctx, start); err != nil || task.State != rootstate.RootJoinAdmitted {
		t.Fatal("rejected attempt poisoned authorized join", task, err)
	}
}

func (discardTransport) Send(string, *raftpb.Message) error { return nil }

// A real single-voter Root commits membership for an offline learner. This
// proves durable orchestration only, not learner catch-up or voter promotion.
func coordinatorFixture(t *testing.T) (*raft.Host, *rootstate.State, rootstate.RootJoinStart, string) {
	t.Helper()
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(filepath.Join(store, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: store, TickInterval: time.Hour, Transport: discardTransport{}})
	if err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := host.AddGroup(raft.RootGroupID, []uint64{id.RaftID}, false, root); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	apply := func(command rootstate.Command) {
		t.Helper()
		command.Version = 1
		if command.RequestID == "" {
			command.RequestID = fmt.Sprintf("fixture-%d", root.AppliedIndex()+1)
		}
		encoded, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Submit(raft.RootGroupID, encoded); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		result, found, err := root.ResultForCommand(encoded)
		if err != nil || !found || result.Error != "" {
			t.Fatal(result, found, err)
		}
	}
	apply(rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "local", Address: "issuer:7443"}})
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "local", "learner:7443")
	if err != nil {
		t.Fatal(err)
	}
	registration, err := rootstate.NodeEnrollmentCommand(request)
	if err != nil {
		t.Fatal(err)
	}
	apply(registration)
	grant := rootstate.JoinGrant{GrantID: "join_00000000000000000000000000000001", SecretSHA256: strings.Repeat("a", 64), EnrollmentKeySHA256: strings.Repeat("b", 64),
		IssuerRaftID: id.RaftID, CreatedUnix: 1, ExpiresUnix: 100, State: rootstate.JoinGrantActive}
	apply(rootstate.Command{Type: rootstate.CreateJoinGrant, JoinGrant: &grant})
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	apply(rootstate.Command{Type: rootstate.ConsumeJoinGrant, JoinConsumption: &rootstate.JoinGrantConsumption{GrantID: grant.GrantID, RequestSHA256: hex.EncodeToString(digest[:]), AuthorizedUnix: 2}})
	return host, root, rootstate.RootJoinStart{GrantID: grant.GrantID, Request: request}, store
}

func TestRootJoinCoordinatorConcurrentEnsureAndNoReadmissionAfterRemoval(t *testing.T) {
	host, root, start, _ := coordinatorFixture(t)
	c, err := New(host, root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := c.Ensure(ctx, start)
			if err != nil || task.State != rootstate.RootJoinAdmitted {
				t.Error(task, err)
			}
		}()
	}
	wg.Wait()
	conf, err := host.Membership(raft.RootGroupID)
	if err != nil || len(conf.GetVoters()) != 1 || len(conf.GetLearners()) != 1 || conf.GetLearners()[0] != start.Request.RaftID {
		t.Fatal("join changed voter quorum or lost learner", conf, err)
	}
	if err := host.RemoveMember(raft.RootGroupID, start.Request.RaftID); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ensure(ctx, start); !errors.Is(err, ErrMemberRemoved) {
		t.Fatal("old admission re-added removed Node", err)
	}
	if err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	conf, err = host.Membership(raft.RootGroupID)
	if err != nil || Contains(conf, start.Request.RaftID) {
		t.Fatal("reconcile resurrected member", conf, err)
	}
}

type faultHost struct {
	Host
	failBegin, failAdd, failComplete bool
	loseResponses                    bool
	follower                         bool
	adds                             atomic.Int32
}

func (h *faultHost) Submit(group string, data []byte) error {
	var command rootstate.Command
	if err := json.Unmarshal(data, &command); err != nil {
		return err
	}
	if h.failBegin && command.Type == rootstate.BeginRootJoin || h.failComplete && command.Type == rootstate.CompleteRootJoin {
		return errors.New("injected pre-proposal failure")
	}
	if err := h.Host.Submit(group, data); err != nil {
		return err
	}
	if h.loseResponses {
		return errors.New("injected response loss")
	}
	return nil
}
func (h *faultHost) AddLearner(group string, id uint64) error {
	h.adds.Add(1)
	if h.failAdd {
		return errors.New("injected learner proposal failure")
	}
	if err := h.Host.AddLearner(group, id); err != nil {
		return err
	}
	if h.loseResponses {
		return errors.New("injected membership response loss")
	}
	return nil
}
func (h *faultHost) Status(group string) (raft.GroupStatus, error) {
	status, err := h.Host.Status(group)
	if h.follower {
		status.Role, status.LeaderID = etcdraft.StateFollower, 0
	}
	return status, err
}

func TestRootJoinCoordinatorResumesDurableFailureBoundaries(t *testing.T) {
	for _, phase := range []string{"before-intent", "before-learner", "before-completion", "lost-responses"} {
		t.Run(phase, func(t *testing.T) {
			host, root, start, _ := coordinatorFixture(t)
			fault := &faultHost{Host: host, failBegin: phase == "before-intent", failAdd: phase == "before-learner",
				failComplete: phase == "before-completion", loseResponses: phase == "lost-responses"}
			c, err := New(fault, root)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err = c.Ensure(ctx, start)
			if (err == nil) != (phase == "lost-responses") {
				t.Fatal("unexpected first attempt", err)
			}
			task, found, err := root.RootJoin(start.GrantID)
			if err != nil || found != (phase != "before-intent") {
				t.Fatal("wrong durable task boundary", found, err)
			}
			if found && phase != "lost-responses" && task.State != rootstate.RootJoinPending {
				t.Fatal("failure marked task complete", task)
			}
			fault.failBegin, fault.failAdd, fault.failComplete, fault.loseResponses = false, false, false, false
			resumed, err := New(fault, root)
			if err != nil {
				t.Fatal(err)
			}
			beforeAdds := fault.adds.Load()
			if phase == "before-intent" {
				if _, err := resumed.Ensure(ctx, start); err != nil {
					t.Fatal("retry failed", err)
				}
			} else if err := resumed.Reconcile(ctx); err != nil {
				t.Fatal("durable reconciliation failed", err)
			}
			if phase == "before-completion" && fault.adds.Load() != beforeAdds {
				t.Fatal("already applied learner was proposed again")
			}
			task, found, err = root.RootJoin(start.GrantID)
			if err != nil || !found || task.State != rootstate.RootJoinAdmitted {
				t.Fatal("recovery did not finish", task, err)
			}
		})
	}
}

func TestRootJoinCoordinatorRejectsFollowerAndCancelledWork(t *testing.T) {
	host, root, start, _ := coordinatorFixture(t)
	fault := &faultHost{Host: host, follower: true}
	c, err := New(fault, root)
	if err != nil {
		t.Fatal(err)
	}
	before := root.AppliedIndex()
	if _, err := c.Ensure(context.Background(), start); !errors.Is(err, ErrNotLeader) {
		t.Fatal(err)
	}
	if err := c.Reconcile(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatal(err)
	}
	fault.follower = false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Ensure(ctx, start); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if root.AppliedIndex() != before || fault.adds.Load() != 0 {
		t.Fatal("unauthorized work changed Root")
	}
}

func TestRootJoinCoordinatorReopensRaftAndPendingTask(t *testing.T) {
	for _, afterLearner := range []bool{false, true} {
		t.Run(fmt.Sprint(afterLearner), func(t *testing.T) {
			host, root, start, store := coordinatorFixture(t)
			fault := &faultHost{Host: host, failAdd: !afterLearner, failComplete: afterLearner}
			c, err := New(fault, root)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := c.Ensure(ctx, start); err == nil {
				t.Fatal("injected failure not reached")
			}
			before, found, err := root.RootJoin(start.GrantID)
			if err != nil || !found || before.State != rootstate.RootJoinPending {
				t.Fatal(before, err)
			}
			if err := host.Close(); err != nil {
				t.Fatal(err)
			}
			reopenedRoot, err := rootstate.Open(filepath.Join(store, "root-state"))
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := raft.OpenHost(raft.HostOptions{StoreRoot: store, TickInterval: time.Hour, Transport: discardTransport{}})
			if err != nil {
				_ = reopenedRoot.Close()
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := reopened.AddGroup(raft.RootGroupID, nil, false, reopenedRoot); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Campaign(raft.RootGroupID); err != nil {
				t.Fatal(err)
			}
			resumed, err := New(reopened, reopenedRoot)
			if err != nil {
				t.Fatal(err)
			}
			if err := resumed.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			after, found, err := reopenedRoot.RootJoin(start.GrantID)
			if err != nil || !found || after.State != rootstate.RootJoinAdmitted || after.RequestSHA256 != before.RequestSHA256 || after.RegistrationSHA256 != before.RegistrationSHA256 {
				t.Fatal("reopened task binding changed", after, err)
			}
			conf, err := reopened.Membership(raft.RootGroupID)
			if err != nil || !Contains(conf, start.Request.RaftID) || len(conf.GetVoters()) != 1 {
				t.Fatal("recovered membership is not one voter plus learner", conf, err)
			}
		})
	}
}
