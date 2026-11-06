package server

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/engine/record"
	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/recovery"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// Exercises the actual backup worker artifact builder and new-cluster restore,
// including mixed-Pack bytes, scoped references and regenerated physical IDs.
// This is an isolated single-node recovery test, not a WAN availability test.
func TestModuleIsolationThroughClusterBackupRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	storeRoot := t.TempDir()
	id, err := identity.Bootstrap(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	registry := engineruntime.NewRegistry()
	if err := registry.Register(record.EngineID, record.Handler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	source, err := node.Open(node.Options{StoreRoot: storeRoot, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	apply := func(request, typ string, edit func(*rootstate.Command)) {
		c := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request, Type: typ}
		edit(&c)
		restoreAcceptanceApplyRoot(t, source, c)
	}
	apply("node", rootstate.RegisterNode, func(c *rootstate.Command) {
		c.Node = &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "west", Address: "source:7443"}
	})
	const group = "data-1"
	apply("group", rootstate.RegisterGroup, func(c *rootstate.Command) {
		c.Group = &rootstate.Group{GroupID: group, StateKind: rootstate.GroupStateEngine}
	})
	state, err := source.Groups.NewState(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Host.AddGroup(group, []uint64{id.RaftID}, false, state); err != nil {
		t.Fatal(err)
	}
	apply("members", rootstate.PublishGroupMembers, func(c *rootstate.Command) {
		c.Membership = &rootstate.GroupMembership{GroupID: group, ExpectedEpoch: 1, Members: []rootstate.GroupMember{{RaftID: id.RaftID, Role: rootstate.GroupMemberVoter}}}
	})
	if err := source.Host.Campaign(group); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"a", "b"} {
		apply("tenant-"+tenant, rootstate.RegisterTenant, func(c *rootstate.Command) { c.Tenant = &rootstate.Tenant{TenantID: tenant} })
	}
	engine, ok := source.Engines.Resolve(group)
	if !ok {
		t.Fatal("missing source Engine state")
	}
	bootstrap, err := rangebootstrap.New(source.Host, source.Root, func(string) (rangebootstrap.OwnerControl, bool) {
		owner, err := rangebootstrap.NewLocalOwner(source.Host, engine)
		return owner, err == nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scopes := []rootstate.Keyspace{
		{TenantID: "a", ModuleID: "config", EngineID: record.EngineID},
		{TenantID: "a", ModuleID: "session", EngineID: record.EngineID},
		{TenantID: "b", ModuleID: "config", EngineID: record.EngineID},
	}
	descriptors := make([]records.Descriptor, len(scopes))
	values := make([][]byte, len(scopes))
	for i, scope := range scopes {
		seed := rootstate.RangeSeed{TenantID: scope.TenantID, ModuleID: scope.ModuleID, EngineID: scope.EngineID, RangeID: fmt.Sprintf("range-%d", i), OwnerGroupID: group}
		apply(fmt.Sprintf("prepare-%d", i), rootstate.PrepareRange, func(c *rootstate.Command) { c.InitialRange = &seed })
		if err := bootstrap.Reconcile(ctx, seed.RangeID); err != nil {
			t.Fatal(err)
		}
		values[i] = bytes.Repeat([]byte{byte('a' + i)}, 32<<10)
		descriptors[i], err = records.Describe(scope, group, []byte("same-key"), values[i])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.Records.Put(ctx, descriptors[i], values[i]); err != nil {
			t.Fatal(err)
		}
		payload, err := record.ReferencePutPayload(descriptors[i])
		if err != nil {
			t.Fatal(err)
		}
		target := router.Target{Keyspace: scope, RangeID: seed.RangeID, OwnerGroupID: group, Epoch: 1, RoutingKey: []byte("same-key")}
		requestID := fmt.Sprintf("write-%d", i)
		command, err := routedstate.EncodeMutation(requestID, target, payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := source.Host.Submit(group, command); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.ReadRouted(ctx, source.Host, target, func(view enginestate.ReadView) error {
			ref, found, err := view.RecordReference(target.RoutingKey)
			if err != nil || !found || ref.Descriptor.Keyspace != scope {
				return fmt.Errorf("source reference missing or crossed scope: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	worker := backupWorker{runtime: source, storeRoot: storeRoot}
	sources, release, err := worker.sources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	repo := repository.Repository{Name: "isolation", Type: repository.TypeFile, Path: filepath.Join(t.TempDir(), "backup")}
	backend, err := repository.OpenFile(repo)
	if err != nil {
		t.Fatal(err)
	}
	first, err := recovery.RunFileBackup(ctx, backend, recovery.Plan{Repository: repo, ClusterID: id.ClusterID, Epoch: 1}, sources)
	if err != nil {
		t.Fatal(err)
	}
	second, err := recovery.RunFileBackup(ctx, backend, recovery.Plan{Repository: repo, ClusterID: id.ClusterID, Epoch: 2}, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Manifest.Objects) != len(second.Manifest.Objects) {
		t.Fatal("incremental backup changed unchanged objects")
	}
	for i := range first.Manifest.Objects {
		if first.Manifest.Objects[i] != second.Manifest.Objects[i] {
			t.Fatal("incremental backup lost content identity")
		}
	}
	point := repository.RecoveryPoint{Version: 1, Repository: repo.Name, ClusterID: id.ClusterID, Epoch: 2, CreatedAt: time.Now().UTC(), Nodes: []repository.RecoveryPointNode{{NodeID: id.NodeID}}}
	if err := backend.CommitRecoveryPoint(ctx, point); err != nil {
		t.Fatal(err)
	}
	rootImage, _, _, err := source.Root.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sourceNodes, sourceGroups := decodeSourceRoot(t, rootImage)
	_, targets := openRestoreTargets(t, t.TempDir(), rootImage, sourceNodes, sourceGroups)
	defer func() {
		for _, target := range targets {
			target.Close()
		}
	}()
	destination := targets[0]
	if destination.Identity.NodeID == id.NodeID || destination.Packs.StoreID() == source.Packs.StoreID() {
		t.Fatal("restore reused physical identity")
	}
	if err := RestoreClusterToRuntimes(ctx, point, func(context.Context, string) (repository.ObjectStore, error) { return backend, nil }, []RuntimeRestoreTarget{{NodeID: destination.Identity.NodeID, Runtime: destination}}); err != nil {
		t.Fatal(err)
	}
	restored, ok := destination.Engines.Resolve(group)
	if !ok {
		t.Fatal("restored Engine not open")
	}
	if err := destination.Host.Campaign(group); err != nil {
		t.Fatal(err)
	}
	for i, scope := range scopes {
		r, found, err := destination.Root.InitialRange(scope)
		if err != nil || !found || r.Keyspace() != scope {
			t.Fatal("restored Range lost scope", r, err)
		}
		target := router.Target{Keyspace: scope, RangeID: r.RangeID, OwnerGroupID: group, Epoch: r.Epoch, RoutingKey: []byte("same-key")}
		if _, err := restored.ReadRouted(ctx, destination.Host, target, func(view enginestate.ReadView) error {
			ref, found, err := view.RecordReference(target.RoutingKey)
			if err != nil || !found || ref.Descriptor.Keyspace != scope {
				return fmt.Errorf("restored reference crossed scope: %v", err)
			}
			body, _, err := destination.Records.Read(ctx, ref.Descriptor)
			if err != nil || !bytes.Equal(body, values[i]) {
				return fmt.Errorf("restored bytes differ: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
