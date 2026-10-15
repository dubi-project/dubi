package capstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestHandlerResultEncodingExactBounds(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 127, 4096} {
		for _, rejection := range []string{"", "quoted \"<&>\n", string([]byte{0xff, 0, 'x'}), "\u2028\u2029"} {
			t.Run(fmt.Sprintf("%d-%x", n, rejection), func(t *testing.T) {
				r := Result{RequestID: "request", Hash: strings.Repeat("a", 64), Index: 27, Applied: rejection == "", Error: rejection, Value: bytes.Repeat([]byte{0xff}, n)}
				want, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				for _, limit := range []int{len(want) - 1, len(want), len(want) + 1} {
					got, err := encodeHandlerResult(r, limit)
					if limit < len(want) {
						if !errors.Is(err, ErrOutcomeTooLarge) || got != nil {
							t.Fatal("accepted oversized encoding", limit, err)
						}
						continue
					}
					if err != nil || !bytes.Equal(got, want) {
						t.Fatal("encoding changed or exact boundary rejected", limit, err)
					}
				}
			})
		}
	}
	r := Result{RequestID: "r", Hash: strings.Repeat("0", 64), Index: 1, Error: strings.Repeat("\x00", MaxOutcomeRejectionBytes)}
	if got, err := encodeHandlerResult(r, MaxResultBytes); err != nil || len(got) <= MaxOutcomeRejectionBytes {
		t.Fatal("escaped rejection boundary", len(got), err)
	}
	r.Error += "x"
	if _, err := encodeHandlerResult(r, MaxResultBytes); !errors.Is(err, ErrOutcomeTooLarge) {
		t.Fatal("unbounded rejection", err)
	}
}

func TestBoundedStateReadChecksBorrowedRowBeforeCopy(t *testing.T) {
	s, err := Open(t.TempDir(), "data-1", &packedHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	key := []byte("test/row")
	if err := s.db.Set(key, []byte("12345678"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if value, found, err := s.getBounded(key, 7); err == nil || found || value != nil {
		t.Fatal("oversized row returned", found, err)
	}
	value, found, err := s.getBounded(key, 8)
	if err != nil || !found || string(value) != "12345678" {
		t.Fatal(found, err)
	}
	value[0] = 'x'
	if value, _, err := s.getBounded(key, 8); err != nil || string(value) != "12345678" {
		t.Fatal("borrowed bytes leaked", err)
	}
	if value, found, err := s.getBounded([]byte("missing"), 8); err != nil || found || value != nil {
		t.Fatal(found, err)
	}
}

func TestOversizedPersistedResultFailsClosed(t *testing.T) {
	s, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := packedTarget(t, s, "record", "t")
	if err := s.ApplyRouted(2, "publish", target, []byte("data")); err != nil {
		t.Fatal(err)
	}
	result, found, err := s.Result("publish")
	if err != nil || !found {
		t.Fatal(found, err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	// Valid JSON with huge whitespace: a parser-only check would accept it.
	oversized := bytes.Repeat([]byte(" "), MaxResultBytes+1)
	copy(oversized, encoded)
	if err := s.db.Set(resultKey("publish"), oversized, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.Result("publish"); err == nil || found || !strings.Contains(err.Error(), "read limit") {
		t.Fatal("unbounded persisted Result read", found, err)
	}
	if _, found, err := readRecordReference(t, s, target); err == nil || found || !strings.Contains(err.Error(), "read limit") {
		t.Fatal("unbounded reference backlink read", found, err)
	}
}

func TestMutationOutcomeLimitRealRaftKeepsApplyingAfterRejection(t *testing.T) {
	root := t.TempDir()
	h := &packedHandler{action: func(tx *Transaction, payload []byte) (Outcome, error) {
		if string(payload) == "oversize" {
			if err := tx.Set([]byte("item"), []byte("must-not-publish")); err != nil {
				return Outcome{}, err
			}
			return Outcome{Rejection: strings.Repeat("x", MaxOutcomeRejectionBytes+1)}, nil
		}
		return referenceHandler().action(tx, payload)
	}}
	openGroup := func() (*State, *raft.Group) {
		s, err := Open(filepath.Join(root, "state"), "data-1", h)
		if err != nil {
			t.Fatal(err)
		}
		a, err := routedstate.New(s)
		if err != nil {
			s.Close()
			t.Fatal(err)
		}
		g, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: root, GroupID: "data-1", RaftID: 71, BootstrapPeers: []uint64{71}, State: a, Transport: offlineMembershipTransport{}})
		if err != nil {
			s.Close()
			t.Fatal(err)
		}
		if err := g.Campaign(); err != nil {
			g.Close()
			t.Fatal(err)
		}
		return s, g
	}
	s, g := openGroup()
	defer func() { g.Close() }()
	r := rootstate.Range{RangeID: "r", TenantID: "t", CapabilityID: "record", DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	activate, err := routedstate.EncodeActivateRange("activate", r)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(activate); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: r.Keyspace(), RangeID: "r", OwnerGroupID: "data-1", Epoch: 1, RoutingKey: []byte("item")}
	command, err := routedstate.EncodeMutation("oversize", target, []byte("oversize"))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(command); err != nil {
		t.Fatal("oversized outcome stopped Raft", err)
	}
	first, found, err := s.Result("oversize")
	if err != nil || !found || first.Applied || first.Error != ErrOutcomeTooLarge.Error() {
		t.Fatal(first, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	s, g = openGroup()
	if err := g.Submit(command); err != nil || h.calls != 1 {
		t.Fatal("replay recomputed a rejected outcome", h.calls, err)
	}
	after, found, err := s.Result("oversize")
	if err != nil || !found || after.Index != first.Index {
		t.Fatal(after, err)
	}
	if _, found, err := s.Value(target.Keyspace, []byte("item")); err != nil || found {
		t.Fatal("rejected value survived restart", found, err)
	}
	command, err = routedstate.EncodeMutation("next", target, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(command); err != nil {
		t.Fatal("Raft cannot apply after rejection", err)
	}
	if _, found, err := readRecordReference(t, s, target); err != nil || !found {
		t.Fatal(found, err)
	}
}

func TestMutationOutcomeLimitRejectsBeforePhysicalPublication(t *testing.T) {
	for _, kind := range []string{"value", "rejection"} {
		t.Run(kind, func(t *testing.T) {
			packs := &memoryPacks{id: "store"}
			handler := &packedHandler{action: func(tx *Transaction, _ []byte) (Outcome, error) {
				if err := tx.Set([]byte("item"), bytes.Repeat([]byte("v"), PackValueThreshold)); err != nil {
					return Outcome{}, err
				}
				d, err := records.Describe(tx.keyspace, "origin", []byte("item"), []byte("body"))
				if err != nil {
					return Outcome{}, err
				}
				if err := tx.SetRecordReference([]byte("item"), d); err != nil {
					return Outcome{}, err
				}
				if kind == "value" {
					return Outcome{Value: make([]byte, (64<<20)/4*3)}, nil
				}
				return Outcome{Rejection: strings.Repeat("x", (64<<10)+1)}, nil
			}}
			root := t.TempDir()
			state, err := open(root, "data-1", handler, packs)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { state.Close() }()
			target := packedTarget(t, state, "record", "t")
			if err := state.ApplyRouted(2, "large", target, []byte("command")); err != nil {
				t.Fatal("resource rejection poisoned Apply", err)
			}
			result, found, err := state.Result("large")
			if err != nil || !found || result.Applied || result.Error != "capability outcome exceeds result limit" || len(result.Value) != 0 || state.AppliedIndex() != 2 {
				t.Fatal("oversize outcome not durably rejected", result.Applied, len(result.Value), len(result.Error), err)
			}
			if packs.calls != 0 {
				t.Fatal("rejected outcome appended Pack data", packs.calls)
			}
			if _, found, err := state.Value(target.Keyspace, []byte("item")); err != nil || found {
				t.Fatal("partial KV publication", found, err)
			}
			if _, found, err := readRecordReference(t, state, target); err != nil || found {
				t.Fatal("partial reference publication", found, err)
			}
			if err := state.ApplyRouted(3, "large", target, []byte("command")); err != nil || handler.calls != 1 {
				t.Fatal("replay reran rejected handler", err)
			}
			if _, _, _, err := state.Snapshot(); err != nil {
				t.Fatal("rejection broke inline snapshot", err)
			}
			artifacts, _ := fileArtifacts(t)
			prepared, err := state.PrepareSnapshotTo(artifacts)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if _, err := prepared.Build(context.Background()); err != nil {
				t.Fatal("rejection broke file snapshot", err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			state, err = open(root, "data-1", handler, packs)
			if err != nil {
				t.Fatal(err)
			}
			result, found, err = state.Result("large")
			if err != nil || !found || result.Applied || result.Error != ErrOutcomeTooLarge.Error() || result.Index != 2 || state.AppliedIndex() != 3 {
				t.Fatal("reopened rejection changed", result, err)
			}
			handler.action = referenceHandler().action
			if err := state.ApplyRouted(4, "next", target, []byte("next")); err != nil {
				t.Fatal("group unusable after rejection", err)
			}
			if _, found, err := readRecordReference(t, state, target); err != nil || !found {
				t.Fatal(found, err)
			}
		})
	}
}
