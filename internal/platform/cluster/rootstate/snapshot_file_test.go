package rootstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func rootArtifacts(t *testing.T, root string) *snapshot.Store {
	t.Helper()
	store, err := snapshot.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func rootFileState(t *testing.T, root string) *State {
	t.Helper()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func applyRootFileCommand(t *testing.T, s *State, command Command) {
	t.Helper()
	command.Version = FormatVersion
	command.RequestID = fmt.Sprintf("request-%d", s.AppliedIndex()+1)
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	if result, found, err := s.Result(command.RequestID); err != nil || !found || result.Error != "" {
		t.Fatal(result, found, err)
	}
}

func rootFileFixture(t *testing.T) *State {
	t.Helper()
	s := rootFileState(t, t.TempDir())
	if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{11}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: "node_00000000000000000000000000000001", RaftID: 11, RegionID: "west", Address: "127.0.0.1:10001"}})
	for _, group := range []string{"data", "empty"} {
		applyRootFileCommand(t, s, Command{Type: RegisterGroup, Group: &Group{GroupID: group, StateKind: GroupStateEngine}})
	}
	members := []GroupMember{{RaftID: 11, Role: GroupMemberVoter}}
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "data", ExpectedEpoch: 1, Members: members}})
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "done", GroupID: "data", ExpectedEpoch: 2, Members: members}})
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{ChangeID: "done", GroupID: "data"}})
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "pending", GroupID: "data", ExpectedEpoch: 3, Members: members}})
	applyRootFileCommand(t, s, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "tenant"}})
	for _, engine := range []string{"record", "search"} {
		applyRootFileCommand(t, s, Command{Type: CreateDataset, Dataset: &Dataset{TenantID: "tenant", EngineID: engine, DatasetID: "items", RangeID: engine, OwnerGroupID: "data"}})
	}
	// Owner index is deliberately greater than Root cut.
	applyRootFileCommand(t, s, Command{Type: ActivateRange, Activation: &RangeActivation{RangeID: "record", OwnerGroupID: "data", Epoch: 1, OwnerRequestID: "owner-record", OwnerAppliedIndex: 100000}})
	return s
}

func buildRootFile(t *testing.T, s *State, store *snapshot.Store) ([]byte, uint64, *raftpb.ConfState) {
	t.Helper()
	prepared, err := s.PrepareSnapshotTo(store)
	if err != nil {
		t.Fatal(err)
	}
	data, err := prepared.Build(context.Background())
	if err := errors.Join(err, prepared.Close()); err != nil {
		t.Fatal(err)
	}
	return data, prepared.Index, prepared.Conf
}

func assertRootFileRestored(t *testing.T, s *State, index uint64) {
	t.Helper()
	if s.AppliedIndex() != index {
		t.Fatal("applied", s.AppliedIndex(), index)
	}
	if route, found, err := s.Route("tenant", "record", "items", nil); err != nil || !found || route.OwnerFenceIndex != 100000 {
		t.Fatal(route, found, err)
	}
	if _, found, err := s.Route("tenant", "search", "items", nil); err != nil || found {
		t.Fatal("pending Range routable", found, err)
	}
	if node, found, err := s.Node(11); err != nil || !found || node.RaftID != 11 {
		t.Fatal(node, found, err)
	}
	if groups, err := s.Groups(); err != nil || len(groups) != 2 {
		t.Fatal(groups, err)
	}
}

func assertRootFileBlocked(t *testing.T, s *State, store *snapshot.Store) {
	t.Helper()
	checks := []func() error{
		func() error { _, _, e := s.Tenant("tenant"); return e },
		func() error { _, _, e := s.Result("request-2"); return e },
		func() error { _, _, e := s.Route("tenant", "record", "items", nil); return e },
		func() error { _, _, e := s.Node(11); return e },
		func() error { _, _, e := s.RootMember(11); return e },
		func() error { _, _, e := s.RootMembers(11); return e },
		func() error { _, e := s.Groups(); return e },
		func() error { _, e := s.PendingRanges(); return e },
		func() error { return s.Advance(s.AppliedIndex() + 1) },
		func() error { _, _, _, e := s.Snapshot(); return e },
		func() error {
			p, e := s.PrepareSnapshotTo(store)
			if p != nil {
				p.Close()
			}
			return e
		},
	}
	for n, check := range checks {
		if check() == nil {
			t.Fatalf("partial Root restore operation %d succeeded", n)
		}
	}
}

func TestRootFileSnapshotFixedCutRoundtripAndClose(t *testing.T) {
	store := rootArtifacts(t, t.TempDir())
	source := rootFileFixture(t)
	prepared, err := source.PrepareSnapshotTo(store)
	if err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, source, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "after-cut"}})
	closeDone := make(chan error, 1)
	go func() { closeDone <- source.Close() }()
	select {
	case err := <-closeDone:
		t.Fatal("Close abandoned fixed view", err)
	case <-time.After(20 * time.Millisecond):
	}
	encoded, err := prepared.Build(context.Background())
	if err != nil {
		prepared.Close()
		t.Fatal(err)
	}
	if _, err := prepared.Build(context.Background()); err == nil {
		t.Fatal("double Build accepted")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	destination := rootFileState(t, root)
	applyRootFileCommand(t, destination, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "old"}})
	if err := destination.RestoreFrom(context.Background(), store, prepared.Index, encoded, prepared.Conf); err != nil {
		t.Fatal(err)
	}
	assertRootFileRestored(t, destination, prepared.Index)
	for _, id := range []string{"old", "after-cut"} {
		if _, found, err := destination.Tenant(id); err != nil || found {
			t.Fatal(id, found, err)
		}
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	destination = rootFileState(t, root)
	again, index, conf := buildRootFile(t, destination, store)
	if !bytes.Equal(again, encoded) || index != prepared.Index || !proto.Equal(conf, prepared.Conf) {
		t.Fatal("logical snapshot changed on reopen")
	}
	if err := destination.RestoreFrom(context.Background(), store, index, encoded, conf); err == nil {
		t.Fatal("stale restore accepted")
	}
}

func TestRootFileSnapshotRestoreFailureAndResume(t *testing.T) {
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, rootFileFixture(t), store)
	for _, stage := range []string{"after-validation", "after-fence", "after-batch", "before-final", "after-final", "canceled-after-fence", "uncertain-clear", "uncertain-final"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			s := rootFileState(t, root)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.restoreHook = func(at string) error {
				if stage == "canceled-after-fence" && at == "after-fence" {
					cancel()
				}
				if at == stage {
					return errors.New("injected restore fault")
				}
				return nil
			}
			commits := 0
			s.commit = func(batch *pebble.Batch) error {
				commits++
				if err := batch.Commit(pebble.Sync); err != nil {
					return err
				}
				if stage == "uncertain-clear" && commits == 1 || stage == "uncertain-final" && commits == 2 {
					return errors.New("uncertain persisted commit")
				}
				return nil
			}
			if err := s.RestoreFrom(ctx, store, index, encoded, conf); err == nil {
				t.Fatal("failure ignored")
			}
			if stage == "after-validation" {
				if s.AppliedIndex() != 0 {
					t.Fatal("preflight mutated applied")
				}
				if _, _, err := s.Tenant("tenant"); err != nil {
					t.Fatal("preflight poisoned state", err)
				}
			} else {
				assertRootFileBlocked(t, s, store)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = rootFileState(t, root)
			if stage != "after-final" && stage != "uncertain-final" {
				if stage != "after-validation" {
					if s.AppliedIndex() != 0 {
						t.Fatal("pending restore not advertised to Raft")
					}
					assertRootFileBlocked(t, s, store)
					wrongConf := &raftpb.ConfState{Voters: []uint64{12}}
					if err := s.RestoreFrom(context.Background(), store, index, encoded, wrongConf); err == nil {
						t.Fatal("changed recovery membership accepted")
					}
					changed, _, _ := snapshot.DecodeReference(encoded)
					changed.SHA256 = strings.Repeat("b", 64)
					other, _ := changed.Encode()
					if err := s.RestoreFrom(context.Background(), store, index, other, conf); err == nil {
						t.Fatal("changed recovery reference accepted")
					}
					empty := rootArtifacts(t, t.TempDir())
					if err := s.RestoreFrom(context.Background(), empty, index, encoded, conf); err == nil {
						t.Fatal("missing artifact accepted")
					}
					assertRootFileBlocked(t, s, store)
				}
				if err := s.RestoreFrom(context.Background(), store, index, encoded, conf); err != nil {
					t.Fatal(err)
				}
			}
			assertRootFileRestored(t, s, index)
		})
	}
}

func TestRootFileSnapshotCrashRecovery(t *testing.T) {
	if stage := os.Getenv("DUBI_ROOT_RESTORE_CRASH"); stage != "" {
		store, err := snapshot.OpenStore(os.Getenv("DUBI_ROOT_RESTORE_ARTIFACTS"))
		if err != nil {
			t.Fatal(err)
		}
		s, err := Open(os.Getenv("DUBI_ROOT_RESTORE_STATE"))
		if err != nil {
			t.Fatal(err)
		}
		encoded := []byte(os.Getenv("DUBI_ROOT_RESTORE_REFERENCE"))
		ref, _, err := snapshot.DecodeReference(encoded)
		if err != nil {
			t.Fatal(err)
		}
		s.restoreHook = func(at string) error {
			if at == stage {
				os.Exit(75)
			}
			return nil
		}
		if err := s.RestoreFrom(context.Background(), store, ref.Index, encoded, &raftpb.ConfState{Voters: []uint64{11}}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash hook missed")
	}
	for _, stage := range []string{"after-validation", "after-fence", "after-batch", "before-final", "after-final"} {
		t.Run(stage, func(t *testing.T) {
			artifacts := t.TempDir()
			store := rootArtifacts(t, artifacts)
			encoded, index, conf := buildRootFile(t, rootFileFixture(t), store)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			stateRoot := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestRootFileSnapshotCrashRecovery$")
			command.Env = append(os.Environ(), "DUBI_ROOT_RESTORE_CRASH="+stage, "DUBI_ROOT_RESTORE_ARTIFACTS="+artifacts,
				"DUBI_ROOT_RESTORE_STATE="+stateRoot, "DUBI_ROOT_RESTORE_REFERENCE="+string(encoded))
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 75 {
				t.Fatalf("child did not crash: %v %s", err, output)
			}
			store = rootArtifacts(t, artifacts)
			s := rootFileState(t, stateRoot)
			if stage != "after-final" {
				if stage != "after-validation" {
					assertRootFileBlocked(t, s, store)
				}
				if err := s.RestoreFrom(context.Background(), store, index, encoded, conf); err != nil {
					t.Fatal(err)
				}
			}
			assertRootFileRestored(t, s, index)
		})
	}
}

func TestRootFileSnapshotInvalidDirectoryLeavesStateUntouched(t *testing.T) {
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, rootFileFixture(t), store)
	ref, _, _ := snapshot.DecodeReference(encoded)
	reader, err := store.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := snapshot.NewDecoder(reader, ref.Size)
	if err != nil {
		t.Fatal(err)
	}
	original := map[string][]byte{}
	for {
		key, value, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		original[string(key)] = bytes.Clone(value)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(map[string][]byte){
		"missing-node-reverse":   func(m map[string][]byte) { delete(m, string(nodeIDKey("node_00000000000000000000000000000001"))) },
		"missing-node":           func(m map[string][]byte) { delete(m, string(nodeKey(11))) },
		"missing-tenant":         func(m map[string][]byte) { delete(m, string(tenantKey("tenant"))) },
		"missing-owner":          func(m map[string][]byte) { delete(m, string(groupKey("data"))) },
		"missing-range":          func(m map[string][]byte) { delete(m, "data/range/tenant/record/items") },
		"missing-dataset":        func(m map[string][]byte) { delete(m, "data/dataset/tenant/record/items") },
		"missing-range-reverse":  func(m map[string][]byte) { delete(m, string(rangeIDKey("record"))) },
		"missing-active-change":  func(m map[string][]byte) { delete(m, string(activeGroupChangeKey("data"))) },
		"missing-pending-change": func(m map[string][]byte) { delete(m, string(groupChangeKey("pending"))) },
		"unknown-key":            func(m map[string][]byte) { m["data/unsupported"] = []byte("{}") },
		"metadata-in-stream":     func(m map[string][]byte) { m["meta/applied"] = []byte("bad") },
		"bad-node-reverse": func(m map[string][]byte) {
			m[string(nodeIDKey("node_00000000000000000000000000000001"))] = []byte("011")
		},
		"bad-result":    func(m map[string][]byte) { m["result/bad"] = []byte("{}") },
		"trailing-json": func(m map[string][]byte) { m[string(tenantKey("tenant"))] = []byte(`{"tenant_id":"tenant"} {}`) },
		"unknown-field": func(m map[string][]byte) {
			m[string(tenantKey("tenant"))] = []byte(`{"tenant_id":"tenant","secret":1}`)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			entries := map[string][]byte{}
			for k, v := range original {
				entries[k] = bytes.Clone(v)
			}
			mutate(entries)
			keys := make([]string, 0, len(entries))
			for key := range entries {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			invalid, err := store.Create(context.Background(), decoder.Header(), snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, key := range keys {
					if err := e.Add([]byte(key), entries[key]); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			data, err := invalid.Encode()
			if err != nil {
				t.Fatal(err)
			}
			s := rootFileState(t, t.TempDir())
			applyRootFileCommand(t, s, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "old"}})
			if err := s.RestoreFrom(context.Background(), store, index, data, conf); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
			if s.AppliedIndex() != 1 {
				t.Fatal("invalid snapshot changed index")
			}
			if _, found, err := s.Tenant("old"); err != nil || !found {
				t.Fatal("invalid snapshot changed state", found, err)
			}
			matches, err := filepath.Glob(filepath.Join(s.root, ".restore-stage-*"))
			if err != nil || len(matches) != 0 {
				t.Fatal("failed stage leaked", matches, err)
			}
		})
	}
}

func TestRootFileSnapshotBeyondInlineLimit(t *testing.T) {
	store := rootArtifacts(t, t.TempDir())
	source := rootFileFixture(t)
	// Synthetic but schema-valid durable results exercise size, not claims
	// about a production directory workload or real-machine throughput.
	payload := strings.Repeat("x", 1<<20)
	for i := range 72 {
		batch := source.db.NewBatch()
		result := Result{RequestID: fmt.Sprintf("large-%03d", i), Hash: strings.Repeat("a", 64), Index: source.AppliedIndex(), Error: payload}
		if err := setJSON(batch, resultKey(result.RequestID), result); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(batch.Commit(pebble.Sync), batch.Close()); err != nil {
			t.Fatal(err)
		}
	}
	encoded, index, conf := buildRootFile(t, source, store)
	ref, _, err := snapshot.DecodeReference(encoded)
	if err != nil || ref.Size <= maxSnapshotBytes || len(encoded) > 1024 {
		t.Fatal("not a large file reference", ref, err)
	}
	destination := rootFileState(t, t.TempDir())
	if err := destination.RestoreFrom(context.Background(), store, index, encoded, conf); err != nil {
		t.Fatal(err)
	}
	assertRootFileRestored(t, destination, index)
	result, found, err := destination.Result("large-071")
	if err != nil || !found || result.Error != payload {
		t.Fatal("large result differs", found, err)
	}
}
