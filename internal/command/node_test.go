package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func TestNodeRemoveByNameUsesManagedRPCAndRetainsMajorityCheck(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0", "--name", "leader"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, wait := startJoinTestServer(t, process)
	wait()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	address := process.LocalAddress()
	invoke := func(args ...string) (string, error) {
		var output bytes.Buffer
		err := runNode(ctx, append([]string{"remove", "--store", store, "--server", address}, args...), &output, io.Discard)
		return output.String(), err
	}
	if _, err := invoke("missing"); err == nil {
		t.Fatal("unknown name was accepted")
	}
	if _, err := invoke("leader"); err == nil || !strings.Contains(err.Error(), "majority") {
		t.Fatal("named removal bypassed majority check", err)
	}
	// Register unused identities to exercise successful fencing without
	// pretending a descriptor edit is a Raft membership change.
	for i, name := range []string{"sh-01", "123"} {
		raftID := process.Runtime.Identity.RaftID + uint64(i) + 1
		node := rootstate.Node{NodeID: fmt.Sprintf("node_%032x", i+1), RaftID: raftID, Name: name, RegionID: "default", Address: fmt.Sprintf("n%d:7443", i+1)}
		command := rootstate.Command{Version: 1, RequestID: fmt.Sprintf("register-test-%d", i), Type: rootstate.RegisterNode, Node: &node}
		encoded, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := process.Runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := process.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
				t.Fatal(err)
			}
			result, found, err := process.Runtime.Root.ResultForCommand(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
		args := []string{name}
		if name == "123" {
			args = []string{"--name", name}
		}
		output, err := invoke(args...)
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			TargetRaftID uint64 `json:"target_raft_id"`
			TargetName   string `json:"target_name"`
			Completed    bool   `json:"completed"`
		}
		if err := json.Unmarshal([]byte(output), &result); err != nil || !result.Completed || result.TargetRaftID != raftID || result.TargetName != name {
			t.Fatal(output, err)
		}
		if _, fenced, err := process.Runtime.Root.NodeFence(raftID); err != nil || !fenced {
			t.Fatal("named target was not fenced", err)
		}
		if _, found, err := process.Runtime.Root.NodeByName(name); err != nil || found {
			t.Fatal("removed Node remained addressable by name", err)
		}
	}
}
