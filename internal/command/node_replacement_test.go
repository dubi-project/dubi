package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/server"
)

// This is the operator path: remove one current member, then enroll a fresh
// identity. No replacement workflow or special rejoin command is involved.
func TestManagedRegionalGrowthAfterExplicitRemovalAndFreshJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	log := regionalGrowthLog{t}
	firstStore := t.TempDir()
	if err := runInit([]string{"--store", firstStore}, io.Discard, log); err != nil {
		t.Fatal(err)
	}
	first, err := openServer([]string{"--store", firstStore, "--listen", "127.0.0.1:0"}, log)
	if err != nil {
		t.Fatal(err)
	}
	stopFirst, waitFirst := startJoinTestServer(t, first)
	waitFirst()
	rootID := first.Runtime.Identity.RaftID
	addresses := []string{first.LocalAddress()}
	ids := []uint64{rootID}
	servers := []*server.Server{first}
	stops := []func(){stopFirst}
	groupID := regiongroups.GroupID("default")
	join := func() {
		t.Helper()
		draft, err := enrollment.NewGrantDraft(0, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		defer done()
		ticket, err := enrollmentrpc.CreateTicket(attempt, firstStore, addresses[0], draft)
		if err != nil {
			t.Fatal(err)
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
		stop, wait := startJoinTestServer(t, s)
		wait()
		n, found, err := s.Runtime.Root.Node(s.Runtime.Identity.RaftID)
		if err != nil || !found {
			t.Fatal(n, found, err)
		}
		addresses = append(addresses, n.Address)
		ids = append(ids, s.Runtime.Identity.RaftID)
		servers = append(servers, s)
		stops = append(stops, stop)
	}
	join()
	join()
	poll := func(label string, check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				for i, s := range servers {
					progress, found, perr := s.Runtime.Root.RegionProgress(groupID)
					group, gfound, gerr := s.Runtime.Root.Group(groupID)
					fence := "closed"
					if state, ok := s.Runtime.Engines.Resolve(groupID); ok {
						if view, ferr := state.RegionFence(); ferr == nil {
							fence = fmt.Sprintf("active=%v membership=%d handoff=%+v", view.Active, view.MembershipIndex, view.Handoff)
						}
					}
					t.Logf("node %d progress=%+v found=%v err=%v group=%+v found=%v err=%v fence=%s", i, progress, found, perr, group, gfound, gerr, fence)
				}
				t.Fatal(label, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	poll("initial region", func() bool {
		for _, s := range servers {
			h, found, err := s.Runtime.Root.RegionCohortHead("default")
			if err != nil || !found || h.CurrentGeneration != 1 || h.PendingGeneration != 0 {
				return false
			}
		}
		return true
	})
	removeCtx, removeCancel := context.WithTimeout(ctx, 30*time.Second)
	for {
		removed := false
		for _, s := range servers {
			_, err = s.RemoveNode(removeCtx, ids[2])
			if err == nil {
				removed = true
				break
			}
			if !strings.Contains(err.Error(), "current Root leader") && !strings.Contains(err.Error(), "Group leadership transferred") && !strings.Contains(err.Error(), "not yet applied") && !strings.Contains(err.Error(), "membership command failed") && !strings.Contains(err.Error(), "still a Root member") && !strings.Contains(err.Error(), "Fence not confirmed") {
				removeCancel()
				t.Fatal("remove", err)
			}
		}
		if removed {
			break
		}
		select {
		case <-removeCtx.Done():
			removeCancel()
			t.Fatal("remove", removeCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	removeCancel()
	if err != nil {
		t.Fatal("remove", err)
	}
	stops[2]()
	join()
	poll("replacement region", func() bool {
		for _, i := range []int{0, 1, 3} {
			s := servers[i]
			h, found, err := s.Runtime.Root.RegionCohortHead("default")
			if err != nil || !found || h.CurrentGeneration != 2 || h.PendingGeneration != 0 {
				return false
			}
		}
		return true
	})
	for i, stop := range stops {
		if i != 2 {
			stop()
		}
	}
}
