package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"go.etcd.io/raft/v3/raftpb"
)

type restoreAcceptanceDiscardTransport struct{}

func (restoreAcceptanceDiscardTransport) Send(string, *raftpb.Message) error { return nil }

// TestRestoreClusterRealRuntimeAcceptance installs a real multi-node backup
// into nine freshly bootstrapped local Runtimes. It is opt-in because the
// Repository is an external VPS export and the target stores are intentionally
// created outside the source cluster. The test exercises the complete runtime
// path, not only Repository preflight.
func TestRestoreClusterRealRuntimeAcceptance(t *testing.T) {
	repoRoot := strings.TrimSpace(os.Getenv("DUBI_RESTORE_ACCEPTANCE_ROOT"))
	if repoRoot == "" {
		t.Skip("set DUBI_RESTORE_ACCEPTANCE_ROOT to run the external Runtime restore acceptance")
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("DUBI_RESTORE_ACCEPTANCE_EPOCH")), 10, 64)
	if err != nil || epoch == 0 {
		t.Fatal("DUBI_RESTORE_ACCEPTANCE_EPOCH is required")
	}
	targetRoot := strings.TrimSpace(os.Getenv("DUBI_RESTORE_ACCEPTANCE_TARGET"))
	if targetRoot == "" {
		targetRoot = filepath.Join(t.TempDir(), "targets")
	} else if err := os.MkdirAll(targetRoot, 0o750); err != nil {
		t.Fatal(err)
	}

	point, stores, rootImage, sourceNodes, sourceGroups := loadRealRestoreInput(t, repoRoot, epoch)
	destinationNodes, runtimes := openRestoreTargets(t, targetRoot, rootImage, sourceNodes, sourceGroups)
	defer func() {
		for _, runtime := range runtimes {
			if err := runtime.Close(); err != nil {
				t.Errorf("close restored Runtime: %v", err)
			}
		}
	}()

	if _, _, err := rootstate.SnapshotMetadata(rootImage); err != nil {
		t.Fatal(err)
	}
	targets := make([]RuntimeRestoreTarget, 0, len(runtimes))
	for _, runtime := range runtimes {
		targets = append(targets, RuntimeRestoreTarget{NodeID: runtime.Identity.NodeID, Runtime: runtime})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := RestoreClusterToRuntimes(ctx, point, func(_ context.Context, nodeID string) (repository.ObjectStore, error) {
		store, found := stores[nodeID]
		if !found {
			return nil, fmt.Errorf("missing source Repository for Node %s", nodeID)
		}
		return store, nil
	}, targets); err != nil {
		t.Fatal("complete Runtime restore: ", err)
	}

	if got := len(runtimes); got != len(point.Nodes) {
		t.Fatalf("target Runtime count = %d, want %d", got, len(point.Nodes))
	}
	for _, runtime := range runtimes {
		nodes, err := runtime.Root.Nodes()
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != len(destinationNodes) {
			t.Fatalf("restored Root on %s has %d Nodes, want %d", runtime.Identity.NodeID, len(nodes), len(destinationNodes))
		}
		report, err := runtime.Volumes.Status()
		if err != nil {
			t.Fatal(err)
		}
		for _, volume := range report.Volumes {
			if volume.State != "active" || volume.Health != "healthy" {
				t.Fatalf("restored target %s Volume %s is %s/%s", runtime.Identity.NodeID, volume.ID, volume.State, volume.Health)
			}
		}
	}
}

func loadRealRestoreInput(t *testing.T, repoRoot string, epoch uint64) (repository.RecoveryPoint, map[string]repository.ObjectStore, []byte, []rootstate.Node, []rootstate.Group) {
	t.Helper()
	entries, err := os.ReadDir(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	stores := make(map[string]repository.ObjectStore)
	var point repository.RecoveryPoint
	var rootImage []byte
	var sourceNodes []rootstate.Node
	var sourceGroups []rootstate.Group
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		store, err := repository.OpenFile(repository.Repository{Name: "runtime-acceptance-" + entry.Name(), Type: repository.TypeFile, Path: filepath.Join(repoRoot, entry.Name())})
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := store.ReadManifest(epoch)
		if err != nil {
			t.Fatal(err)
		}
		if len(manifest.Artifacts) == 0 {
			t.Fatal("source Manifest has no artifacts")
		}
		nodeID := manifest.Artifacts[0].NodeID
		if _, found := stores[nodeID]; found {
			t.Fatalf("duplicate source Node %s", nodeID)
		}
		if len(stores) == 0 {
			candidate, readPointErr := store.ReadRecoveryPoint(epoch)
			if readPointErr != nil {
				t.Fatal(readPointErr)
			}
			point = candidate
			for _, artifact := range manifest.Artifacts {
				if artifact.Kind != "root-state" {
					continue
				}
				reader, openErr := store.OpenObject(artifact.Object.ID)
				if openErr != nil {
					t.Fatal(openErr)
				}
				rootImage, err = io.ReadAll(reader)
				if closeErr := reader.Close(); err == nil {
					err = closeErr
				}
				if err != nil {
					t.Fatal(err)
				}
				break
			}
			if len(rootImage) == 0 {
				t.Fatal("source backup has no Root image")
			}
			sourceNodes, sourceGroups = decodeSourceRoot(t, rootImage)
		}
		stores[nodeID] = store
	}
	if len(stores) != len(point.Nodes) || len(sourceNodes) != len(point.Nodes) {
		t.Fatalf("source backup has %d stores, %d indexed Nodes, %d Root Nodes", len(stores), len(point.Nodes), len(sourceNodes))
	}
	return point, stores, rootImage, sourceNodes, sourceGroups
}

func decodeSourceRoot(t *testing.T, image []byte) ([]rootstate.Node, []rootstate.Group) {
	t.Helper()
	index, conf, err := rootstate.SnapshotMetadata(image)
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(filepath.Join(t.TempDir(), "source-root"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.ApplyMembership(1, conf); err != nil {
		t.Fatal(err)
	}
	if err := root.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	nodes, err := root.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	groups, err := root.Groups()
	if err != nil {
		t.Fatal(err)
	}
	return nodes, groups
}

func openRestoreTargets(t *testing.T, root string, rootImage []byte, sourceNodes []rootstate.Node, sourceGroups []rootstate.Group) ([]rootstate.Node, []*node.Runtime) {
	t.Helper()
	if len(sourceNodes) == 0 {
		t.Fatal("source Root has no Nodes")
	}
	firstDir := filepath.Join(root, sourceNodes[0].NodeID)
	first, err := identity.Bootstrap(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	destination := make([]rootstate.Node, len(sourceNodes))
	runtimes := make([]*node.Runtime, 0, len(sourceNodes))
	for index, source := range sourceNodes {
		store := filepath.Join(root, fmt.Sprintf("target-%02d", index+1))
		var id identity.Identity
		if index == 0 {
			id = first
			store = firstDir
		} else {
			id, err = identity.PrepareJoin(store)
			if err != nil {
				t.Fatal(err)
			}
			id, err = identity.BindCluster(store, first.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := node.InitializeRoot(store); err != nil {
			t.Fatal(err)
		}
		destination[index] = rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: source.RegionID, Address: fmt.Sprintf("restore-%02d:7443", index+1)}
		mux := serverTestMux(t)
		runtime, err := node.Open(node.Options{StoreRoot: store, TickInterval: time.Hour, Transport: restoreAcceptanceDiscardTransport{}, RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime)
	}
	// Every target Root starts with the complete destination Node catalog. This
	// is the normal pre-restore bootstrap state; the restore later replaces the
	// catalog and logical descriptors with the source snapshot.
	for index, runtime := range runtimes {
		if err := runtime.Host.Campaign(raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		for nodeIndex, destinationNode := range destination {
			restoreAcceptanceApplyRoot(t, runtime, rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprintf("restore-node-%02d-%02d", index, nodeIndex), Type: rootstate.RegisterNode, Node: &destinationNode})
		}
		mapping, err := rootstate.BuildRestoreIdentityMap(rootImage, destination)
		if err != nil {
			t.Fatal(err)
		}
		for groupIndex, sourceGroup := range sourceGroups {
			if sourceGroup.GroupID == raft.RootGroupID {
				continue
			}
			restoreAcceptanceApplyRoot(t, runtime, rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprintf("restore-group-%02d-%02d", index, groupIndex), Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: sourceGroup.GroupID, StateKind: sourceGroup.StateKind}})
			if len(sourceGroup.Members) == 0 {
				continue
			}
			members := make([]rootstate.GroupMember, len(sourceGroup.Members))
			for memberIndex, member := range sourceGroup.Members {
				mapped, found := mapping.RaftIDs[member.RaftID]
				if !found {
					t.Fatalf("source Group %s member %d has no target", sourceGroup.GroupID, member.RaftID)
				}
				members[memberIndex] = rootstate.GroupMember{RaftID: mapped, Role: member.Role}
			}
			sort.Slice(members, func(i, j int) bool { return members[i].RaftID < members[j].RaftID })
			restoreAcceptanceApplyRoot(t, runtime, rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprintf("restore-members-%02d-%02d", index, groupIndex), Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: sourceGroup.GroupID, ExpectedEpoch: 1, Members: members}})
		}
	}
	// The bootstrap writes above are only to make each target's persisted Root
	// catalog complete enough to open its local Groups. Reopen every Runtime
	// without bootstrap peers so the restore starts from a follower state.
	for _, runtime := range runtimes {
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for index := range runtimes {
		store := filepath.Join(root, fmt.Sprintf("target-%02d", index+1))
		if index == 0 {
			store = firstDir
		}
		mux := serverTestMux(t)
		runtime, err := node.Open(node.Options{StoreRoot: store, TickInterval: time.Hour, Transport: restoreAcceptanceDiscardTransport{}, EngineMux: mux})
		if err != nil {
			t.Fatal(err)
		}
		runtimes[index] = runtime
	}
	// Open every local Engine Group named by the source Root catalog. This is
	// the same post-bootstrap shape a normal joined Node has before snapshot
	// installation, and lets the restore path install Engine snapshots without
	// requiring a live network quorum in this offline acceptance harness.
	mapping, err := rootstate.BuildRestoreIdentityMap(rootImage, destination)
	if err != nil {
		t.Fatal(err)
	}
	for _, runtime := range runtimes {
		for _, sourceGroup := range sourceGroups {
			if sourceGroup.GroupID == raft.RootGroupID || len(sourceGroup.Members) == 0 {
				continue
			}
			local := false
			peers := make([]uint64, 0, len(sourceGroup.Members))
			for _, member := range sourceGroup.Members {
				mapped, found := mapping.RaftIDs[member.RaftID]
				if !found {
					t.Fatalf("source Group %s member %d has no target", sourceGroup.GroupID, member.RaftID)
				}
				peers = append(peers, mapped)
				local = local || mapped == runtime.Identity.RaftID
			}
			if !local {
				continue
			}
			descriptor, found, err := runtime.Root.Group(sourceGroup.GroupID)
			if err != nil || !found {
				t.Fatalf("target Group %s missing: %v", sourceGroup.GroupID, err)
			}
			state, err := runtime.Groups.NewStateFromDescriptor(context.Background(), descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Host.AddGroup(sourceGroup.GroupID, peers, false, state); err != nil {
				t.Fatal(err)
			}
		}
	}
	return destination, runtimes
}

func restoreAcceptanceApplyRoot(t *testing.T, runtime *node.Runtime, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Host.ReadIndex(context.Background(), raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	result, found, err := runtime.Root.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatalf("Root command result = %+v, found %v, err %v", result, found, err)
	}
}
