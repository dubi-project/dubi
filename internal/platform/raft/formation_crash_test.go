package raft

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestGroupFormationProcessCrash(t *testing.T) {
	if stage := os.Getenv("DUBI_TEST_GROUP_FORMATION_STAGE"); stage != "" {
		root := os.Getenv("DUBI_TEST_GROUP_FORMATION_ROOT")
		_, err := openGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: 1, BootstrapPeers: []uint64{1}, State: openTestState(t, root)}, func(at string) error {
			if at == stage {
				os.Exit(75)
			}
			return nil
		})
		t.Fatalf("formation stage not reached: %v", err)
	}
	for _, stage := range []string{"after-log-open", "after-log-binding", "after-formation", "after-initial-ready"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestGroupFormationProcessCrash$")
			command.Env = append(os.Environ(), "DUBI_TEST_GROUP_FORMATION_STAGE="+stage, "DUBI_TEST_GROUP_FORMATION_ROOT="+root)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 75 {
				t.Fatalf("child: %v\n%s", err, output)
			}
			peers := []uint64{1}
			if stage == "after-formation" || stage == "after-initial-ready" {
				peers = nil
			} // Recover original intent without external bootstrap input.
			g, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: 1, BootstrapPeers: peers, State: openTestState(t, root)})
			if err != nil {
				t.Fatal("resume formation", err)
			}
			if err := g.Campaign(); err != nil {
				t.Fatal(err)
			}
			if err := g.Submit([]byte("after-crash")); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			state := openTestState(t, root)
			g, err = OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: 1, State: state})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			if len(state.disk.Commands) != 1 || string(state.disk.Commands[0]) != "after-crash" {
				t.Fatal("recovery changed committed commands", state.disk)
			}
		})
	}
}
