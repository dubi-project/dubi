package server

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
)

func TestLocalNetworkResolutionPreservesRegistrationAndRejectsFlagChanges(t *testing.T) {
	id, err := identity.Bootstrap(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	runtime := &node.Runtime{Identity: id, Root: root}
	initial, err := resolveLocalNetwork(runtime, "127.0.0.1:17443", NetworkOptions{Name: "sh-01"})
	if err != nil || initial.Address != "127.0.0.1:17443" || initial.RegionID != "default" || initial.NodeID != id.NodeID || initial.RaftID != id.RaftID || initial.Name != "sh-01" {
		t.Fatal(initial, err)
	}
	if _, found, err := root.Node(id.RaftID); err != nil || found {
		t.Fatal("resolution prematurely published registration", found, err)
	}
	initial.RegionID, initial.Address = "west", "stable.example:17443"
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "registered-node", Type: rootstate.RegisterNode, Node: &initial})
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Apply(1, command); err != nil {
		t.Fatal(err)
	}
	for _, options := range []NetworkOptions{{}, {Advertise: initial.Address}, {Region: "west"}} {
		got, err := resolveLocalNetwork(runtime, "0.0.0.0:17443", options)
		if err != nil || got != initial {
			t.Fatal("restart rederived persisted metadata", got, err)
		}
	}
	for _, options := range []NetworkOptions{{Advertise: "changed.example:17443"}, {Region: "east"}} {
		if _, err := resolveLocalNetwork(runtime, "0.0.0.0:17443", options); err == nil {
			t.Fatal("flag changed registration", options)
		}
	}
	if _, err := resolveLocalNetwork(runtime, "127.0.0.1:17444", NetworkOptions{}); err == nil {
		t.Fatal("ephemeral port silently broke registered route")
	}
	if got, err := resolveLocalNetwork(runtime, "127.0.0.1:17444", NetworkOptions{Advertise: initial.Address}); err != nil || got != initial {
		t.Fatal("intentional port mapping rejected", got, err)
	}
	runtime.Identity.NodeID = "node_00000000000000000000000000000001"
	if _, err := resolveLocalNetwork(runtime, "127.0.0.1:17443", NetworkOptions{}); err == nil {
		t.Fatal("another identity reused registration")
	}
}
