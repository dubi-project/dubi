package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/server"
)

// This is the operator path: remove one current member, then enroll a fresh
// identity through the ordinary membership flow.
func TestManagedRegionalGrowthAfterExplicitRemovalAndFreshJoin(t *testing.T) {
	t.Run("single-region", func(t *testing.T) { testManagedRemovalAndFreshJoin(t, false) })
	t.Run("root-learner", func(t *testing.T) { testManagedRemovalAndFreshJoin(t, true) })
}

func testManagedRemovalAndFreshJoin(t *testing.T, multiRegion bool) {
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	log := regionalGrowthLog{t}
	mux := referenceTestMux(t)
	firstStore := t.TempDir()
	if err := runInit([]string{"--store", firstStore}, io.Discard, log); err != nil {
		t.Fatal(err)
	}
	first, err := openServerWithMux([]string{"--store", firstStore, "--listen", "127.0.0.1:0"}, log, mux)
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
	join := func(region string) {
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
		s, err := openJoiningServerWithMux(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--region", region, "--ticket-file", path}, nil, log, mux)
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
	join("default")
	join("default")
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
					t.Logf("node %d progress=%+v found=%v err=%v group=%+v found=%v err=%v fence=%s pendingTask=%+v growth=%+v", i, progress, found, perr, group, gfound, gerr, fence, progress.PendingTask, progress.PendingGrowth)
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
	space := rootstate.Keyspace{TenantID: "member-flow", EngineID: "data-alpha", ModuleID: "shared"}
	tenant, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "member-flow-tenant", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: space.TenantID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(raft.RootGroupID, tenant); err != nil {
		t.Fatal(err)
	}
	activateEngineTestRange(t, ctx, first, groupID, space)
	key, body := []byte("survives-member-removal"), bytes.Repeat([]byte("member-data"), 24<<10)
	written, err := first.EngineData.WriteLocal(ctx, enginedata.WriteRequest{RequestID: "before-member-removal",
		Keyspace: space, Key: key, Body: body, Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) }})
	if err != nil || written.State != enginedata.WriteLocalReady {
		t.Fatal("initial physical majority write", written, err)
	}
	descriptor, err := records.Describe(space, groupID, key, body)
	if err != nil {
		t.Fatal(err)
	}
	var originalPlacement rootstate.RangePlacement
	poll("initial fixed Region placement", func() bool {
		placement, found, err := first.Runtime.Root.RangePlacement("range-" + space.EngineID)
		if err != nil || !found {
			return false
		}
		originalPlacement = placement
		return true
	})
	retired := 2
	if multiRegion {
		// Keep three Root voters in different Regions. The fresh data member
		// must converge while remaining a Root learner permanently.
		join("east")
		join("north")
		poll("stable cross-Region Root roles", func() bool {
			for _, s := range servers {
				conf, err := s.Runtime.Host.Membership(raft.RootGroupID)
				if err != nil || len(conf.GetVoters()) != 3 || len(conf.GetLearners()) != 2 || len(conf.GetVotersOutgoing()) != 0 || conf.GetAutoLeave() {
					return false
				}
			}
			return true
		})
		conf, _ := first.Runtime.Host.Membership(raft.RootGroupID)
		for _, voter := range conf.GetVoters() {
			if voter == ids[retired] {
				retired = 1
			}
		}
	}
	removeCtx, removeCancel := context.WithTimeout(ctx, 30*time.Second)
	for {
		removed := false
		for _, s := range servers {
			_, err = s.RemoveNode(removeCtx, ids[retired])
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
	stops[retired]()
	fresh := len(servers)
	join("default")
	participants := []int{fresh}
	for i := 0; i < 3; i++ {
		if i != retired {
			participants = append(participants, i)
		}
	}
	poll("fresh-join region", func() bool {
		for _, i := range participants {
			s := servers[i]
			h, found, err := s.Runtime.Root.RegionCohortHead("default")
			if err != nil || !found || h.CurrentGeneration != 2 || h.PendingGeneration != 0 {
				return false
			}
			state, found := s.Runtime.Engines.Resolve(groupID)
			if !found {
				return false
			}
			fence, err := state.RegionFence()
			if err != nil || !fence.Active || fence.Handoff == nil || fence.Handoff.Activation == nil {
				return false
			}
		}
		return true
	})
	poll("fixed Region placement refreshes after fresh Join", func() bool {
		for _, i := range participants {
			placement, view, err := servers[i].Runtime.Root.RangeRegionPlacementView(originalPlacement.RangeID, len(originalPlacement.Regions), rootstate.MaxRegionCohortNodes)
			if err != nil || placement.RangeEpoch != originalPlacement.RangeEpoch || placement.OwnerGroupID != originalPlacement.OwnerGroupID || len(placement.Regions) != len(originalPlacement.Regions) {
				return false
			}
			for j, prior := range originalPlacement.Regions {
				current := placement.Regions[j]
				if current.RegionID != prior.RegionID || current.GroupID != prior.GroupID || current.Generation != view.Regions[j].Progress.Current.Generation {
					return false
				}
			}
		}
		return true
	})
	if _, err := servers[fresh].Runtime.Records.Verify(ctx, descriptor); err != nil {
		t.Fatal("fresh Node did not receive the existing physical record", err)
	}
	poll("background confirmation permits the pre-removal read", func() bool {
		read, err := servers[fresh].EngineData.Read(ctx, space, key)
		if err != nil {
			if strings.Contains(err.Error(), "obsolete Region plan or publication") || strings.Contains(err.Error(), "Range placement Region generation is not current") {
				return false // Placement and per-record confirmation converge separately.
			}
			t.Fatal("fresh Node could not confirm and read the pre-removal record", err)
		}
		if !bytes.Equal(read.Body, body) || read.Reference.Index != written.Local.Reference.Index {
			t.Fatal("fresh Node returned a different pre-removal publication")
		}
		return true
	})
	if _, member, err := servers[fresh].Runtime.Root.RootMember(ids[retired]); err != nil || member {
		t.Fatal("removed identity regained membership", member, err)
	}
	if multiRegion {
		conf, err := servers[fresh].Runtime.Host.Membership(raft.RootGroupID)
		if err != nil || !slices.Contains(conf.GetLearners(), ids[fresh]) {
			t.Fatal("fresh data Node required Root promotion", conf, err)
		}
	}
	for i, stop := range stops {
		if i != retired {
			stop()
		}
	}
}
