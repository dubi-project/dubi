package node

import (
	"context"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func registrationRuntime(t *testing.T) *Runtime {
	t.Helper()
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	registry := engineruntime.NewRegistry()
	if err := registry.Register("record", &nodeTestHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := runtime.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestBootstrapNodeSelfRegistrationIsConfirmedAndImmutable(t *testing.T) {
	runtime := registrationRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.EnsureLocalNode(ctx, "west", "127.0.0.1:7443"); err != nil {
		t.Fatal(err)
	}
	first, found, err := runtime.Root.Node(runtime.Identity.RaftID)
	if err != nil || !found || first.NodeID != runtime.Identity.NodeID || first.RegionID != "west" || first.Address != "127.0.0.1:7443" {
		t.Fatal(first, found, err)
	}
	index, _, err := runtime.Root.RootPosition()
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.EnsureLocalNode(ctx, "west", "127.0.0.1:7443"); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][2]string{{"east", first.Address}, {"west", "127.0.0.1:7444"}, {"west", "0.0.0.0:7443"}, {"../bad", first.Address}} {
		if err := runtime.EnsureLocalNode(ctx, changed[0], changed[1]); err == nil {
			t.Fatal("silently changed network registration", changed)
		}
	}
	after, _, err := runtime.Root.RootPosition()
	if err != nil || after != index {
		t.Fatal("retry/rejection wrote another Root entry", index, after, err)
	}
	current, _, err := runtime.Root.Node(runtime.Identity.RaftID)
	if err != nil || current != first {
		t.Fatal("registration changed", current, err)
	}
}

func TestBootstrapNodeCannotRepairMissingEnrollmentOrExpandedCatalog(t *testing.T) {
	for _, mode := range []string{"joined-role", "expanded-membership"} {
		t.Run(mode, func(t *testing.T) {
			runtime := registrationRuntime(t)
			if mode == "joined-role" {
				// Exercise the role gate independently of the mTLS enrollment tests.
				runtime.Identity.Origin = identity.OriginJoin
			} else if err := runtime.Host.AddLearner(raft.RootGroupID, 42); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := runtime.EnsureLocalNode(ctx, "west", "127.0.0.1:7443"); err == nil {
				t.Fatal("invented missing registration", mode)
			}
			if _, found, err := runtime.Root.Node(runtime.Identity.RaftID); err != nil || found {
				t.Fatal("rejected startup mutated Root", found, err)
			}
		})
	}
}
