package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestGroupLostLogAndStateDoesNotBootstrapAgain(t *testing.T) {
	for _, groupID := range []string{RootGroupID, "regional-west"} {
		for _, damage := range []string{"log", "log-and-registration", "whole-group"} {
			t.Run(groupID+"/"+damage, func(t *testing.T) {
				root := t.TempDir()
				g, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: groupID, RaftID: 1, BootstrapPeers: []uint64{1}, State: openTestState(t, root)})
				if err != nil {
					t.Fatal(err)
				}
				if err := g.Campaign(); err != nil {
					t.Fatal(err)
				}
				if err := g.Submit([]byte("must-not-be-lost")); err != nil {
					t.Fatal(err)
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				logPath := filepath.Join(root, groupDirectory, groupID, "dubi-raft")
				lostPath := logPath
				if damage == "whole-group" {
					lostPath = filepath.Dir(logPath)
				}
				if err := os.Rename(lostPath, lostPath+".retained"); err != nil {
					t.Fatal(err)
				}
				if damage == "log-and-registration" {
					marker := filepath.Join(filepath.Dir(logPath), "DUBI_RAFT_STORAGE")
					if err := os.Rename(marker, marker+".retained"); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Rename(filepath.Join(root, "TEST_STATE"), filepath.Join(root, "TEST_STATE.retained")); err != nil {
					t.Fatal(err)
				}
				state := openTestState(t, root)
				g, err = OpenGroup(GroupOptions{StoreRoot: root, GroupID: groupID, RaftID: 1, BootstrapPeers: []uint64{1}, State: state})
				if err == nil {
					g.Close()
					t.Fatal("lost Raft history was silently bootstrapped")
				}
				if !state.closed {
					t.Fatal("failed open leaked StateMachine")
				}
				if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("lost log directory was recreated", err)
				}
			})
		}
	}
}
