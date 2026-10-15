package command

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/server"
	etcdraft "go.etcd.io/raft/v3"
)

// The bootstrap Node is deliberately stopped and removed before the fourth
// Node requests enrollment. The surviving authority envelope must be enough;
// no bootstrap endpoint or private CA file is reused.
func TestManagedEnrollmentAuthoritySurvivesBootstrapRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	type process struct {
		store, address string
		id             identity.Identity
		server         *server.Server
		stop, wait     func()
	}
	nodes := make([]process, 3)
	nodes[0].store = t.TempDir()
	if err := runInit([]string{"--store", nodes[0].store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	regions := []string{"west", "east", "north"}
	open := func(store, address, region string) *server.Server {
		t.Helper()
		args := []string{"--store", store, "--listen", address, "--region", region}
		p, err := openServer(args, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	start := func(p *server.Server) (func(), func()) {
		t.Helper()
		stop, wait := startJoinTestServer(t, p)
		wait()
		return stop, wait
	}
	nodes[0].server = open(nodes[0].store, "127.0.0.1:0", regions[0])
	nodes[0].stop, nodes[0].wait = start(nodes[0].server)
	for i := range nodes {
		if i == 0 {
			continue
		}
		if nodes[i-1].address == "" {
			node, found, err := nodes[0].server.Runtime.Root.Node(nodes[0].server.Runtime.Identity.RaftID)
			if err != nil || !found {
				t.Fatal(node, found, err)
			}
			nodes[0].id = nodes[0].server.Runtime.Identity
			nodes[0].address = node.Address
		}
		draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		attempt, stopAttempt := context.WithTimeout(ctx, 20*time.Second)
		ticket, err := enrollmentrpc.CreateTicket(attempt, nodes[0].store, nodes[0].address, draft)
		stopAttempt()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := identityenrollment.EncodeTicket(ticket, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		ticketPath := filepath.Join(t.TempDir(), "ticket")
		if err := os.WriteFile(ticketPath, []byte(encoded), 0o600); err != nil {
			t.Fatal(err)
		}
		nodes[i].store = t.TempDir()
		p, err := openJoiningServer(ctx, []string{"--store", nodes[i].store, "--listen", "127.0.0.1:0", "--region", regions[i], "--ticket-file", ticketPath}, nil, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].server = p
		nodes[i].stop, nodes[i].wait = start(p)
		nodes[i].id = p.Runtime.Identity
		node, found, err := p.Runtime.Root.Node(p.Runtime.Identity.RaftID)
		if err != nil || !found {
			t.Fatal(node, found, err)
		}
		nodes[i].address = node.Address
	}
	poll := func(label string, check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				for i, node := range nodes {
					if node.server == nil {
						continue
					}
					conf, confErr := node.server.Runtime.Host.Membership(raft.RootGroupID)
					authority, found, authorityErr := node.server.Runtime.Root.EnrollmentAuthority()
					t.Logf("node %d root voters=%v learners=%v authority=%+v found=%v confErr=%v authorityErr=%v", i, conf.GetVoters(), conf.GetLearners(), authority, found, confErr, authorityErr)
				}
				t.Fatal(label, ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	poll("three stable Root voters", func() bool {
		conf, err := nodes[1].server.Runtime.Host.Membership(raft.RootGroupID)
		if err != nil || len(conf.GetVoters()) != 3 || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearners()) != 0 {
			return false
		}
		authority, found, err := nodes[1].server.Runtime.Root.EnrollmentAuthority()
		return err == nil && found && len(authority.Members) == 3 && len(authority.ReadyMembers) == 3
	})
	authority, found, err := nodes[1].server.Runtime.Root.EnrollmentAuthority()
	if err != nil || !found || len(authority.Members) != 3 || len(authority.ReadyMembers) != 3 {
		t.Fatalf("initial authority = %+v, found=%v, err=%v", authority, found, err)
	}
	for i := 1; i < 3; i++ {
		signer, err := nodepki.OpenSigner(nodes[i].store, nodes[i].id, time.Now().UTC())
		if err != nil || signer.AuthorityEpoch() != authority.Epoch {
			t.Fatalf("Node %d signer epoch=%d err=%v", i, signer.AuthorityEpoch(), err)
		}
	}
	poll("surviving Root leader before removal", func() bool {
		if status, err := nodes[0].server.Runtime.Host.Status(raft.RootGroupID); err == nil && status.LeaderID == nodes[0].id.RaftID {
			_, _ = nodes[0].server.Runtime.Host.TransferLeadership(ctx, raft.RootGroupID, nodes[1].id.RaftID)
		}
		for _, i := range []int{1, 2} {
			status, err := nodes[i].server.Runtime.Host.Status(raft.RootGroupID)
			if err == nil && status.LeaderID == nodes[i].id.RaftID && status.Role == etcdraft.StateLeader {
				return true
			}
		}
		return false
	})
	remover := 1
	if status, err := nodes[1].server.Runtime.Host.Status(raft.RootGroupID); err == nil && status.LeaderID != nodes[1].id.RaftID {
		remover = 2
	}
	for {
		if _, err := nodes[remover].server.RemoveNode(ctx, nodes[0].id.RaftID); err == nil {
			break
		} else if ctx.Err() != nil {
			t.Fatal("remove bootstrap", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("remove bootstrap", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	nodes[0].stop()
	poll("bootstrap fenced and authority advanced", func() bool {
		a, ok, e := nodes[1].server.Runtime.Root.EnrollmentAuthority()
		if e != nil || !ok || a.Epoch <= authority.Epoch || slices.Contains(a.Members, nodes[0].id.RaftID) || len(a.ReadyMembers) < 2 {
			return false
		}
		_, fenced, e := nodes[1].server.Runtime.Root.NodeFence(nodes[0].id.RaftID)
		return e == nil && fenced
	})
	current, _, _ := nodes[1].server.Runtime.Root.EnrollmentAuthority()
	for i := 1; i < 3; i++ {
		signer, err := nodepki.OpenSigner(nodes[i].store, nodes[i].id, time.Now().UTC())
		if err != nil || (signer.AuthorityEpoch() != current.Epoch && signer.AuthorityEpoch()+1 != current.Epoch) {
			t.Fatalf("survivor %d signer epoch=%d want=%d err=%v", i, signer.AuthorityEpoch(), current.Epoch, err)
		}
	}
	draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := enrollmentrpc.CreateTicket(ctx, nodes[1].store, nodes[1].address, draft)
	if err != nil {
		t.Fatal("surviving authority could not create ticket", err)
	}
	encoded, err := identityenrollment.EncodeTicket(ticket, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	ticketPath := filepath.Join(t.TempDir(), "ticket")
	if err := os.WriteFile(ticketPath, []byte(encoded), 0o600); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()
	joining, err := openJoiningServer(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--region", "west", "--ticket-file", ticketPath}, nil, io.Discard)
	if err != nil {
		t.Fatal("new Node enrollment after bootstrap removal", err)
	}
	stop, wait := start(joining)
	defer stop()
	wait()
	if joining.Runtime.Identity.ClusterID != nodes[1].server.Runtime.Identity.ClusterID {
		t.Fatal("new Node joined another Cluster")
	}
}
