package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/server"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Real managed CLI construction, TCP/mTLS, independent Node identities and
// persistent stores on ONE host. This proves automatic Root voter operation,
// not data Placement, independent Region hardware, or physical disk failure.
func TestManagedRootTopologyAutomaticQuorumFailoverAndColdRestart(t *testing.T) {
	for _, regions := range [][]string{{"default", "default", "default"}, {"west", "east", "north"}, {"west", "west", "west", "east", "east", "east", "north", "north", "north"}} {
		t.Run(fmt.Sprint(regions), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			type process struct {
				store, address string
				id             identity.Identity
				server         *server.Server
				stop, wait     func()
			}
			nodes := make([]process, len(regions))
			nodes[0].store = t.TempDir()
			if err := runInit([]string{"--store", nodes[0].store}, io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
			first, err := openServer([]string{"--store", nodes[0].store, "--listen", "127.0.0.1:0", "--region", regions[0]}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			nodes[0].server = first
			nodes[0].stop, nodes[0].wait = startJoinTestServer(t, first)
			nodes[0].wait()
			describe := func(i int) {
				t.Helper()
				nodes[i].id = nodes[i].server.Runtime.Identity
				node, found, err := nodes[i].server.Runtime.Root.Node(nodes[i].id.RaftID)
				if err != nil || !found {
					t.Fatal("missing registered Node", err)
				}
				nodes[i].address = node.Address
			}
			describe(0)
			issueTicket := func() string {
				t.Helper()
				draft, err := enrollment.NewGrantDraft(0, time.Time{})
				if err != nil {
					t.Fatal(err)
				}
				attempt, stop := context.WithTimeout(ctx, 15*time.Second)
				defer stop()
				for {
					ticket, err := enrollmentrpc.CreateTicket(attempt, nodes[0].store, nodes[0].address, draft)
					if err == nil {
						encoded, err := enrollment.EncodeTicket(ticket, time.Time{})
						if err != nil {
							t.Fatal(err)
						}
						path := filepath.Join(t.TempDir(), "ticket")
						if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
							t.Fatal(err)
						}
						return path
					}
					// An automatic leadership transfer can outlive one bounded
					// management call. Retry the SAME public draft, never create a
					// second grant or hide authorization/validation failures.
					if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
						t.Fatal(err)
					}
					select {
					case <-attempt.Done():
						t.Fatal("ticket did not recover after topology change", err)
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
			for i := 1; i < len(nodes); i++ {
				ticket := issueTicket()
				nodes[i].store = t.TempDir()
				joined, err := openJoiningServer(ctx, []string{"--store", nodes[i].store, "--listen", "127.0.0.1:0", "--region", regions[i], "--ticket-file", ticket}, nil, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				nodes[i].server = joined
				nodes[i].stop, nodes[i].wait = startJoinTestServer(t, joined)
				nodes[i].wait()
				describe(i)
			}
			var ids []uint64
			var all []int
			regionByID := make(map[uint64]string)
			for i, node := range nodes {
				ids = append(ids, node.id.RaftID)
				all = append(all, i)
				regionByID[node.id.RaftID] = regions[i]
			}
			slices.Sort(ids)
			poll := func(label string, check func() bool) {
				t.Helper()
				for !check() {
					select {
					case <-ctx.Done():
						t.Fatal(label, ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			var stableVoters []uint64
			stable := func(indices []int) bool {
				var observed []uint64
				for _, i := range indices {
					conf, err := nodes[i].server.Runtime.Host.Membership(raft.RootGroupID)
					if err != nil || len(conf.GetVotersOutgoing()) != 0 || conf.GetAutoLeave() || len(conf.GetVoters()) != 3 || len(conf.GetLearners()) != len(nodes)-3 {
						return false
					}
					voters := slices.Clone(conf.GetVoters())
					slices.Sort(voters)
					members := append(slices.Clone(voters), conf.GetLearners()...)
					slices.Sort(members)
					if !slices.Equal(members, ids) || stableVoters != nil && !slices.Equal(voters, stableVoters) || observed != nil && !slices.Equal(voters, observed) {
						return false
					}
					if regions[0] != regions[len(regions)-1] {
						seen := make(map[string]bool)
						for _, id := range voters {
							seen[regionByID[id]] = true
						}
						if len(seen) != 3 {
							return false
						}
					}
					observed = voters
				}
				stableVoters = observed
				return true
			}
			poll("automatic voter formation", func() bool { return stable(all) })
			regionNodes := map[string][]uint64{}
			for _, id := range ids {
				region := regionByID[id]
				regionNodes[region] = append(regionNodes[region], id)
			}
			fences := map[string]regionfence.Record{}
			checkRegional := func(indices []int) bool {
				for region, participants := range regionNodes {
					id := regiongroups.GroupID(region)
					for _, i := range indices {
						runtime := nodes[i].server.Runtime
						group, found, err := runtime.Root.Group(id)
						if err != nil {
							return false
						}
						if !found || group.Epoch != 2 || len(group.Members) != len(participants) {
							return false
						}
						cohort, found, err := runtime.Root.GroupInitialCohort(id)
						if err != nil || !found || !cohort.Formation || !slices.Equal(cohort.Nodes, participants) {
							return false
						}
						head, found, err := runtime.Root.RegionCohortHead(region)
						if err != nil || !found || head.CurrentGeneration != 1 || head.PendingGeneration != 0 || cohort.State != rootstate.RegionCohortConfirmed {
							return false
						}
						if regions[i] != region {
							if runtime.Host.HasGroup(id) {
								t.Fatal("foreign Region created local Group", region, i)
							}
							continue
						}
						if !runtime.Host.HasGroup(id) {
							return false
						}
						conf, err := runtime.Host.Membership(id)
						voters := slices.Clone(conf.GetVoters())
						slices.Sort(voters)
						if err != nil || !slices.Equal(voters, participants) || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearners()) != 0 {
							return false
						}
						attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
						_, err = runtime.Host.ReadIndex(attempt, id)
						stop()
						if err != nil {
							return false
						}
						{
							state, found := runtime.Capabilities.Resolve(id)
							if !found {
								return false
							}
							view, err := state.RegionFence()
							if err != nil || !view.Active || view.Record.RequestID != cohort.GroupFenceRequestID || view.Record.Index != cohort.GroupFenceIndex || view.Record.Fence.PlannedIndex != cohort.PlannedIndex || view.Record.Fence.GroupEpoch != cohort.GroupEpoch {
								return false
							}
							if expected, ok := fences[id]; ok && !reflect.DeepEqual(view.Record, expected) {
								return false
							}
							fences[id] = view.Record
							for _, observer := range indices {
								if observer == i {
									continue
								}
								attempt, stop := context.WithTimeout(ctx, time.Second)
								remote, err := nodes[observer].server.GroupClient.ObserveRegionFence(attempt, nodes[i].id.RaftID, id)
								stop()
								if err != nil || !remote.View.Active || !reflect.DeepEqual(remote.View.Record, view.Record) {
									return false
								}
								break
							}
						}
					}
				}
				return true
			}
			// No test proposal: the production worker must persist the fence,
			// observe every original participant and confirm Root automatically.
			poll("automatic regional formation and all-participant confirmation", func() bool { return checkRegional(all) })
			leader := func(indices []int) int {
				for _, i := range indices {
					status, err := nodes[i].server.Runtime.Host.Status(raft.RootGroupID)
					if err != nil || status.Role != etcdraft.StateLeader || status.LeaderID != nodes[i].id.RaftID {
						continue
					}
					attempt, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
					_, err = nodes[i].server.Runtime.Host.ReadIndex(attempt, raft.RootGroupID)
					cancel()
					if err == nil {
						return i
					}
				}
				return -1
			}
			current := -1
			poll("initial quorum", func() bool { current = leader(all); return current >= 0 })
			nodes[current].stop()
			var survivors []int
			for _, i := range all {
				if i != current {
					survivors = append(survivors, i)
				}
			}
			next := -1
			poll("leader failure election", func() bool { next = leader(survivors); return next >= 0 })
			if !stable(survivors) {
				t.Fatal("failure changed durable voter denominator")
			}
			poll("regional quorum after Node loss", func() bool { return checkRegional(survivors) })
			command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "after-root-failover", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "survived"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := nodes[next].server.Runtime.Host.Submit(raft.RootGroupID, command); err != nil {
				t.Fatal(err)
			}
			poll("surviving quorum commit", func() bool {
				result, found, err := nodes[next].server.Runtime.Root.ResultForCommand(command)
				return err == nil && found && result.Error == ""
			})
			restart := func(i int) {
				t.Helper()
				p, err := openServer([]string{"--store", nodes[i].store, "--listen", nodes[i].address}, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				if p.Runtime.Identity != nodes[i].id {
					_ = p.Close()
					t.Fatal("restart changed Node identity")
				}
				nodes[i].server = p
				nodes[i].stop, nodes[i].wait = startJoinTestServer(t, p)
			}
			restart(current)
			nodes[current].wait()
			poll("former leader catch-up", func() bool {
				_, found, err := nodes[current].server.Runtime.Root.Tenant("survived")
				return err == nil && found && stable(all)
			})
			for _, i := range all {
				nodes[i].stop()
			}
			// Open/run every persisted member before waiting for quorum readiness.
			for _, i := range all {
				restart(i)
			}
			for _, i := range all {
				nodes[i].wait()
			}
			poll("cold quorum recovery", func() bool { return stable(all) && leader(all) >= 0 })
			poll("cold regional Group recovery", func() bool { return checkRegional(all) })
			for _, i := range all {
				if _, found, err := nodes[i].server.Runtime.Root.Tenant("survived"); err != nil || !found {
					t.Fatal("cold restart lost committed Root data", i, err)
				}
			}
			if len(regions) == 9 {
				// Full failure-domain exercise: stop one complete Region, remove
				// its old identities from a surviving Region through the managed
				// CLI, then join three fresh Nodes with the same Region name.
				failedRegion := "east"
				var failed, survivors []int
				for i, region := range regions {
					if region == failedRegion {
						failed = append(failed, i)
					} else {
						survivors = append(survivors, i)
					}
				}
				for _, i := range failed {
					nodes[i].stop()
				}
				removeFailed := func(target uint64) {
					t.Helper()
					lastDiagnostics := ""
					removeCtx, cancelRemove := context.WithTimeout(ctx, 30*time.Second)
					defer cancelRemove()
					for {
						removed := false
						for _, i := range survivors {
							var out, diagnostics bytes.Buffer
							code := Run(removeCtx, []string{"node", "remove", "--store", nodes[i].store, "--server", nodes[i].address, strconv.FormatUint(target, 10)}, &out, &diagnostics)
							lastDiagnostics = diagnostics.String()
							if code == 0 {
								removed = true
								break
							}
						}
						if removed {
							return
						}
						select {
						case <-removeCtx.Done():
							t.Fatalf("remove failed Region Node %d: %v diagnostics=%s", target, removeCtx.Err(), lastDiagnostics)
						case <-time.After(100 * time.Millisecond):
						}
					}
				}
				for _, i := range failed {
					removeFailed(nodes[i].id.RaftID)
				}
				poll("failed Region removed from Root", func() bool {
					for _, failedIndex := range failed {
						id := nodes[failedIndex].id.RaftID
						for _, survivor := range survivors {
							_, member, err := nodes[survivor].server.Runtime.Root.RootMember(id)
							if err != nil || member {
								return false
							}
							_, fenced, err := nodes[survivor].server.Runtime.Root.NodeFence(id)
							if err != nil || !fenced {
								return false
							}
						}
					}
					return true
				})
				newIndices := make([]int, 0, len(failed))
				for range failed {
					ticket := issueTicket()
					store := t.TempDir()
					joined, err := openJoiningServer(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--region", failedRegion, "--ticket-file", ticket}, nil, io.Discard)
					if err != nil {
						authority, authorityFound, authorityErr := nodes[0].server.Runtime.Root.EnrollmentAuthority()
						member, memberFound, memberErr := nodes[0].server.Runtime.Root.RootMember(nodes[0].id.RaftID)
						t.Fatalf("join failed: %v authority=%+v found=%v err=%v local=%+v member=%v found=%v err=%v conf=%+v", err, authority, authorityFound, authorityErr, nodes[0].id, member, memberFound, memberErr, nodes[0].server.Runtime.Root.ConfState())
					}
					stop, wait := startJoinTestServer(t, joined)
					wait()
					node, found, err := joined.Runtime.Root.Node(joined.Runtime.Identity.RaftID)
					if err != nil || !found || node.RegionID != failedRegion {
						t.Fatal("fresh same-name Region Node was not registered", node, found, err)
					}
					nodes = append(nodes, process{store: store, address: node.Address, id: joined.Runtime.Identity, server: joined, stop: stop, wait: wait})
					newIndices = append(newIndices, len(nodes)-1)
				}
				var newIDs []uint64
				for _, i := range newIndices {
					newIDs = append(newIDs, nodes[i].id.RaftID)
				}
				slices.Sort(newIDs)
				reformationDeadline, cancelReformation := context.WithTimeout(ctx, 30*time.Second)
				for {
					ready := true
					for _, i := range survivors {
						head, found, err := nodes[i].server.Runtime.Root.RegionCohortHead(failedRegion)
						if err != nil || !found || head.CurrentGeneration != 2 || head.PendingGeneration != 0 {
							ready = false
							break
						}
						group, found, err := nodes[i].server.Runtime.Root.Group(regiongroups.GroupID(failedRegion))
						if err != nil || !found || group.Epoch != 2 || len(group.Members) != len(newIDs) {
							ready = false
							break
						}
						members := make([]uint64, 0, len(group.Members))
						for _, member := range group.Members {
							members = append(members, member.RaftID)
						}
						slices.Sort(members)
						if !slices.Equal(members, newIDs) {
							ready = false
							break
						}
					}
					if ready {
						break
					}
					select {
					case <-reformationDeadline.Done():
						for _, i := range append(survivors, newIndices...) {
							runtime := nodes[i].server.Runtime
							head, _, headErr := runtime.Root.RegionCohortHead(failedRegion)
							retired, retiredErr := runtime.Root.RegionGroupIsRetired(regiongroups.GroupID(failedRegion))
							rebuild, rebuildFound, rebuildErr := runtime.Root.RegionGroupRebuild(regiongroups.GroupID(failedRegion))
							group, groupFound, groupErr := runtime.Root.Group(regiongroups.GroupID(failedRegion))
							view, viewFound, viewErr := runtime.Root.PendingRegionConfirmation(regiongroups.GroupID(failedRegion))
							localGroup := runtime.Host.HasGroup(regiongroups.GroupID(failedRegion))
							status, statusErr := runtime.Host.Status(regiongroups.GroupID(failedRegion))
							t.Logf("reformation node=%d head=%+v headErr=%v retired=%v retiredErr=%v rebuild=%+v found=%v err=%v group=%+v found=%v err=%v pending=%+v pendingFound=%v pendingErr=%v local=%v status=%+v statusErr=%v root=%+v", i, head, headErr, retired, retiredErr, rebuild, rebuildFound, rebuildErr, group, groupFound, groupErr, view, viewFound, viewErr, localGroup, status, statusErr, runtime.Root.ConfState())
						}
						t.Fatalf("same-name Region reformation %v", reformationDeadline.Err())
					case <-time.After(100 * time.Millisecond):
					}
				}
				cancelReformation()
				for _, i := range newIndices {
					if _, found, err := nodes[i].server.Runtime.Root.RegionCohortHead(failedRegion); err != nil || !found {
						t.Fatal("fresh Node did not observe reformed Region", i, err)
					}
				}
			}
		})
	}
}
