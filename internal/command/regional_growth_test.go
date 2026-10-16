package command

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type regionalGrowthLog struct{ t *testing.T }

func (w regionalGrowthLog) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

// Actual CLI/managed servers, TCP/mTLS and Raft. No test-issued membership,
// seal, candidate, Root growth authorization or activation commands. All stores
// are on one host; this is not an independent-hardware durability qualification.
func TestManagedRegionalGrowthAutomaticThreeToFiveAndColdRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	type process struct {
		store, address string
		id             identity.Identity
		server         *server.Server
		stop, wait     func()
	}
	var nodes []process
	log := regionalGrowthLog{t}
	firstStore := t.TempDir()
	if err := runInit([]string{"--store", firstStore}, io.Discard, log); err != nil {
		t.Fatal(err)
	}
	first, err := openServer([]string{"--store", firstStore, "--listen", "127.0.0.1:0"}, log)
	if err != nil {
		t.Fatal(err)
	}
	start := func(store string, s *server.Server) process {
		stop, wait := startJoinTestServer(t, s)
		wait()
		n, found, err := s.Runtime.Root.Node(s.Runtime.Identity.RaftID)
		if err != nil || !found {
			t.Fatal(n, found, err)
		}
		return process{store: store, address: n.Address, id: s.Runtime.Identity, server: s, stop: stop, wait: wait}
	}
	nodes = append(nodes, start(firstStore, first))
	restartAll := func() {
		t.Helper()
		// Start every persisted member before waiting on either majority.
		for i := range nodes {
			s, err := openServer([]string{"--store", nodes[i].store, "--listen", nodes[i].address}, log)
			if err != nil {
				t.Fatal(err)
			}
			if s.Runtime.Identity != nodes[i].id {
				s.Close()
				t.Fatal("cold restart changed identity")
			}
			nodes[i].server = s
			nodes[i].stop, nodes[i].wait = startJoinTestServer(t, s)
		}
		for _, n := range nodes {
			n.wait()
		}
	}
	join := func() {
		t.Helper()
		draft, err := enrollment.NewGrantDraft(0, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		defer done()
		var ticket enrollment.Ticket
		for {
			ticket, err = enrollmentrpc.CreateTicket(attempt, nodes[0].store, nodes[0].address, draft)
			if err == nil {
				break
			}
			if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
				t.Fatal(err)
			}
			select {
			case <-attempt.Done():
				t.Fatal("ticket retry", err)
			case <-time.After(50 * time.Millisecond):
			}
		}
		encoded, err := enrollment.EncodeTicket(ticket, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "ticket")
		if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
			t.Fatal(err)
		}
		store := t.TempDir()
		s, err := openJoiningServer(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--ticket-file", path}, nil, log)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, start(store, s))
	}
	poll := func(label string, check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				for i, n := range nodes {
					v, _, err := n.server.Runtime.Root.RegionProgress(regiongroups.GroupID("default"))
					t.Logf("node %d progress=%+v err=%v", i, v, err)
				}
				t.Fatal(label, ctx.Err())
			case <-time.After(30 * time.Millisecond):
			}
		}
	}
	join()
	join()
	group := regiongroups.GroupID("default")
	check := func(indices []int, generation uint64, expected *regionfence.Record) bool {
		ids := make([]uint64, len(nodes))
		for i, n := range nodes {
			ids[i] = n.id.RaftID
		}
		slices.Sort(ids)
		for _, i := range indices {
			r := nodes[i].server.Runtime
			head, found, err := r.Root.RegionCohortHead("default")
			if err != nil || !found || head.CurrentGeneration != generation || head.PendingGeneration != 0 {
				return false
			}
			c, found, err := r.Root.RegionCohort("default", generation)
			if err != nil || !found || !slices.Equal(c.Nodes, ids) {
				return false
			}
			s, found := r.Engines.Resolve(group)
			if !found {
				return false
			}
			v, err := s.RegionFence()
			if err != nil || !v.Active || v.Record.Index != c.GroupFenceIndex || v.Record.RequestID != c.GroupFenceRequestID {
				return false
			}
			voters := slices.Clone(v.Conf.Voters)
			slices.Sort(voters)
			if !slices.Equal(voters, ids) || len(v.Conf.Learners) != 0 {
				return false
			}
			if generation > 1 && (v.Handoff == nil || v.Handoff.Activation == nil || !reflect.DeepEqual(v.Handoff.Activation.Confirmed, c)) {
				return false
			}
			if expected != nil && !reflect.DeepEqual(v.Record, *expected) {
				return false
			}
			rootConf, err := r.Host.Membership(raft.RootGroupID)
			wantVoters := len(nodes)
			if wantVoters%2 == 0 {
				wantVoters--
			}
			if err != nil || len(rootConf.GetVoters()) != wantVoters || len(rootConf.GetVotersOutgoing()) != 0 || rootConf.GetAutoLeave() {
				return false
			}
			attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			_, err = r.Host.ReadIndex(attempt, group)
			stop()
			if err != nil {
				return false
			}
			// Membership alone does not prove a live Root quorum after failures.
			attempt, stop = context.WithTimeout(ctx, 500*time.Millisecond)
			_, err = r.Host.ReadIndex(attempt, raft.RootGroupID)
			stop()
			if err != nil {
				return false
			}
		}
		return true
	}
	all := []int{0, 1, 2}
	poll("initial confirmed region", func() bool { return check(all, 1, nil) })
	for generation := uint64(2); generation <= 3; generation++ {
		join()
		all = append(all, len(nodes)-1)
		if generation == 2 {
			poll("in-flight sealed growth", func() bool {
				for _, n := range nodes {
					s, found := n.server.Runtime.Engines.Resolve(group)
					if !found {
						continue
					}
					v, err := s.RegionFence()
					h, found, e := n.server.Runtime.Root.RegionCohortHead("default")
					if err == nil && e == nil && found && h.CurrentGeneration == 1 && h.PendingGeneration == 2 && v.Handoff != nil && v.Handoff.Intent.Next.Generation == 2 && v.Handoff.Activation == nil {
						return true
					}
				}
				return false
			})
			for _, n := range nodes {
				n.stop()
			}
			restartAll()
		}
		poll("automatic sealed growth and activation", func() bool { return check(all, generation, nil) })
	}
	state, _ := nodes[0].server.Runtime.Engines.Resolve(group)
	view, err := state.RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	expected := view.Record
	// Five automatic voters retain quorum with two arbitrary Nodes stopped.
	nodes[0].stop()
	nodes[1].stop()
	poll("three surviving regional and Root voters", func() bool { return check([]int{2, 3, 4}, 3, &expected) })
	for _, i := range []int{2, 3, 4} {
		nodes[i].stop()
	}
	restartAll()
	poll("cold regional growth recovery", func() bool { return check(all, 3, &expected) })
	for _, n := range nodes {
		for generation := uint64(2); generation <= 3; generation++ {
			g, found, err := n.server.Runtime.Root.RegionGrowth("default", generation)
			if err != nil || !found {
				t.Fatal(g, found, err)
			}
			if _, found, err := n.server.Runtime.Root.ActiveGroupChange(group); err != nil || found {
				t.Fatal("reactivated historical task", found, err)
			}
		}
	}
}
