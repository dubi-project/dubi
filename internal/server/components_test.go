package server

import (
	"fmt"
	"testing"

	stateengine "github.com/dubi-project/dubi/internal/engine/state"
	configmodule "github.com/dubi-project/dubi/internal/module/config"
)

func TestPublicComponentCatalogMatchesBoundServices(t *testing.T) {
	catalog := publicComponentCatalog("node_a")
	if catalog.Scope != "entry_node" || catalog.NodeID != "node_a" || len(catalog.Items) != 2 {
		t.Fatalf("unexpected catalog: %+v", catalog)
	}
	engine, module := catalog.Items[0], catalog.Items[1]
	if engine.ID != stateengine.EngineID || engine.Kind != "engine" || module.ID != configmodule.ModuleID || module.Kind != "module" || len(module.Engines) != 1 || module.Engines[0] != engine.ID {
		t.Fatal("registered dependency mismatch")
	}
	if engine.Status != "registered" || module.Status != "registered" {
		t.Fatal("registration must not claim runtime health")
	}
	if engine.Settings[0].Value != fmt.Sprintf("%d KiB", stateengine.MaxValueBytes/1024) || engine.Settings[1].Value != fmt.Sprintf("%d KiB", stateengine.MaxKeyBytes/1024) {
		t.Fatal("limits differ from State")
	}
}
