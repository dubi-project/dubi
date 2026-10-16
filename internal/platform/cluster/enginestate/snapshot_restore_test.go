package enginestate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func fileRestoreFixture(t *testing.T, artifacts *snapshot.Store) ([]byte, router.Target, []byte) {
	t.Helper()
	source, err := open(t.TempDir(), "data-1", &packedHandler{}, &memoryPacks{id: "source"})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target := packedTarget(t, source, "record", "tenant")
	value := bytes.Repeat([]byte("large-value"), PackValueThreshold/10+1)
	if err := source.ApplyRouted(2, "source-write", target, value); err != nil {
		t.Fatal(err)
	}
	prepared, err := source.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	encoded, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return encoded, target, value
}

func assertFileRestoreBlocked(t *testing.T, s *State, target router.Target, artifacts *snapshot.Store) {
	t.Helper()
	if _, _, err := s.Value(target.Keyspace, []byte("item")); err == nil {
		t.Fatal("partial restore served a value")
	}
	if _, _, err := s.Result("source-write"); err == nil {
		t.Fatal("partial restore served a result")
	}
	if _, _, err := s.Range(target.RangeID); err == nil {
		t.Fatal("partial restore served a Range")
	}
	if err := s.Advance(s.AppliedIndex() + 1); err == nil {
		t.Fatal("partial restore accepted apply")
	}
	if _, _, _, err := s.Snapshot(); err == nil {
		t.Fatal("partial restore produced an inline snapshot")
	}
	if prepared, err := s.PrepareSnapshotTo(artifacts); err == nil {
		prepared.Close()
		t.Fatal("partial restore produced a file snapshot")
	}
}

func TestFileSnapshotRestoreAcrossStoreAndInline(t *testing.T) {
	artifacts, _ := fileArtifacts(t)
	encoded, target, value := fileRestoreFixture(t, artifacts)
	for _, packed := range []bool{false, true} {
		root := t.TempDir()
		var backend packStorage
		if packed {
			backend = &memoryPacks{id: "destination"}
		}
		s, err := open(root, "data-1", &packedHandler{}, backend)
		if err != nil {
			t.Fatal(err)
		}
		// The incoming image replaces an unrelated prior Range and results.
		old := packedTarget(t, s, "search", "old")
		if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{}); err != nil {
			s.Close()
			t.Fatal(err)
		}
		if s.AppliedIndex() != 2 {
			t.Fatal(s.AppliedIndex())
		}
		if _, found, err := s.Value(old.Keyspace, []byte("item")); err != nil || found {
			t.Fatal("old scope survived", found, err)
		}
		got, found, err := s.Value(target.Keyspace, []byte("item"))
		if err != nil || !found || !bytes.Equal(got, value) {
			t.Fatal("restored value differs", found, err)
		}
		if _, found, err := s.get(restoreFenceKey); found || err != nil {
			t.Fatal("completed restore retained fence", err)
		}
		if packed && readRef(t, s, "record", "item").Location.StoreID != "destination" {
			t.Fatal("source physical reference copied")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = open(root, "data-1", &packedHandler{}, backend)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := s.PrepareSnapshotTo(artifacts)
		if err != nil {
			t.Fatal(err)
		}
		again, err := prepared.Build(context.Background())
		prepared.Close()
		if err != nil || !bytes.Equal(again, encoded) {
			t.Fatal("restore changed logical image", err)
		}
		if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{}); err == nil {
			t.Fatal("stale snapshot accepted")
		}
		s.Close()
	}
}

func TestFileSnapshotRestoreFailureFenceAndReopen(t *testing.T) {
	for _, stage := range []string{"after-validation", "after-fence", "after-batch", "before-final", "after-final", "pack-failure", "canceled-after-fence"} {
		t.Run(stage, func(t *testing.T) {
			artifacts, _ := fileArtifacts(t)
			encoded, target, value := fileRestoreFixture(t, artifacts)
			root := t.TempDir()
			backend := &memoryPacks{id: "destination"}
			s, err := open(root, "data-1", &packedHandler{}, backend)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.restoreHook = func(at string) error {
				if stage == "canceled-after-fence" && at == "after-fence" {
					cancel()
				}
				if at == stage {
					return errors.New("injected restore failure")
				}
				return nil
			}
			if stage == "pack-failure" {
				backend.failAt = 1
			}
			if err := s.RestoreFrom(ctx, artifacts, 2, encoded, &raftpb.ConfState{}); err == nil {
				t.Fatal("failure was ignored")
			}
			if stage == "after-validation" {
				if err := s.Advance(1); err != nil || backend.calls != 0 {
					t.Fatal("preflight failure modified live state", err)
				}
			} else {
				assertFileRestoreBlocked(t, s, target, artifacts)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if stage != "after-final" && stage != "after-validation" {
				wrong, err := open(root, "data-1", &packedHandler{}, &memoryPacks{id: "wrong-store"})
				if err == nil {
					wrong.Close()
					t.Fatal("interrupted restore reopened on wrong Store")
				}
			}
			s, err = open(root, "data-1", &packedHandler{}, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if stage == "after-final" {
				if s.AppliedIndex() != 2 {
					t.Fatal("lost completed durable restore")
				}
			} else {
				if stage != "after-validation" {
					if s.AppliedIndex() != 0 {
						t.Fatal("pending restore did not force reconciliation")
					}
					assertFileRestoreBlocked(t, s, target, artifacts)
					if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{Voters: []uint64{1}}); err == nil {
						t.Fatal("pending restore accepted wrong membership")
					}
					wrong, _, _ := snapshot.DecodeReference(encoded)
					wrong.SHA256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
					other, _ := wrong.Encode()
					if err := s.RestoreFrom(context.Background(), artifacts, 2, other, &raftpb.ConfState{}); err == nil {
						t.Fatal("pending restore accepted different artifact")
					}
					empty, _ := fileArtifacts(t)
					if err := s.RestoreFrom(context.Background(), empty, 2, encoded, &raftpb.ConfState{}); err == nil {
						t.Fatal("missing durable artifact was treated as empty state")
					}
					assertFileRestoreBlocked(t, s, target, artifacts)
				}
				backend.failAt = 0
				if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{}); err != nil {
					t.Fatal(err)
				}
			}
			got, found, err := s.Value(target.Keyspace, []byte("item"))
			if err != nil || !found || !bytes.Equal(got, value) {
				t.Fatal("recovered image differs", err)
			}
		})
	}
}

func TestFileSnapshotRestoreRejectsInvalidImageBeforePackWrites(t *testing.T) {
	for _, damage := range []string{"missing-fence", "unknown-key", "wrong-format", "wrong-conf", "digest", "canceled"} {
		t.Run(damage, func(t *testing.T) {
			artifacts, artifactPath := fileArtifacts(t)
			encoded, target, _ := fileRestoreFixture(t, artifacts)
			ref, _, _ := snapshot.DecodeReference(encoded)
			reader, err := artifacts.Open(ref)
			if err != nil {
				t.Fatal(err)
			}
			decoder, err := snapshot.NewDecoder(reader, ref.Size)
			if err != nil {
				t.Fatal(err)
			}
			header := decoder.Header()
			var entries []snapshotEntry // Small adversarial fixture, not restore implementation.
			for {
				key, value, err := decoder.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if damage == "missing-fence" && bytes.Equal(key, fenceKeyspaceKey(target.Keyspace)) {
					continue
				}
				entries = append(entries, snapshotEntry{Key: key, Value: value})
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "wrong-format":
				header.Format = "enginestate-v2"
			case "wrong-conf":
				header.Conf, _ = proto.Marshal(&raftpb.ConfState{Voters: []uint64{1}})
			case "unknown-key":
				entries = append(entries, snapshotEntry{Key: []byte("z/unsupported"), Value: []byte("x")})
			}
			ref, err = artifacts.Create(context.Background(), header, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, entry := range entries {
					if err := e.Add(entry.Key, entry.Value); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if damage == "digest" {
				// Valid complete codec, wrong outer reference digest. This specifically
				// tests the reader.Close check after a successful final ReadFull.
				wrong := ref
				wrong.SHA256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
				if err := os.Link(filepath.Join(artifactPath, ref.SHA256+".snap"), filepath.Join(artifactPath, wrong.SHA256+".snap")); err != nil {
					t.Fatal(err)
				}
				ref = wrong
			}
			encoded, err = ref.Encode()
			if err != nil {
				t.Fatal(err)
			}
			backend := &memoryPacks{id: "destination"}
			s, err := open(t.TempDir(), "data-1", &packedHandler{}, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			old := packedTarget(t, s, "search", "old")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if damage == "canceled" {
				cancel()
			}
			if err := s.RestoreFrom(ctx, artifacts, ref.Index, encoded, &raftpb.ConfState{}); err == nil {
				t.Fatal("invalid image accepted")
			}
			if s.AppliedIndex() != 1 || backend.calls != 0 {
				t.Fatal("invalid image mutated destination")
			}
			if _, found, err := s.Range(old.RangeID); err != nil || !found {
				t.Fatal("preflight damaged existing state", err)
			}
			if err := s.Advance(2); err != nil {
				t.Fatal("preflight poisoned destination", err)
			}
		})
	}
}

func TestFileSnapshotRestoreUncertainPublication(t *testing.T) {
	for _, persist := range []bool{false, true} {
		for _, final := range []bool{false, true} {
			artifacts, _ := fileArtifacts(t)
			encoded, target, _ := fileRestoreFixture(t, artifacts)
			root := t.TempDir()
			s, err := Open(root, "data-1", &packedHandler{})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			s.commit = func(batch *pebble.Batch) error {
				calls++
				fail := calls == 1 && !final || calls == 2 && final
				if !fail || persist {
					if err := batch.Commit(pebble.Sync); err != nil {
						return err
					}
				}
				if fail {
					return errors.New("uncertain Sync")
				}
				return nil
			}
			if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{}); err == nil {
				t.Fatal("uncertain commit reported success")
			}
			assertFileRestoreBlocked(t, s, target, artifacts)
			s.Close()
			s, err = Open(root, "data-1", &packedHandler{})
			if err != nil {
				t.Fatal(err)
			}
			if final && persist {
				if s.AppliedIndex() != 2 {
					t.Fatal("durable final publish lost")
				}
			} else if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{}); err != nil {
				t.Fatal(err)
			}
			s.Close()
		}
	}
}

func TestFileSnapshotRestoreCrashHelper(t *testing.T) {
	root := os.Getenv("DUBI_STREAM_RESTORE_ROOT")
	if root == "" {
		return
	}
	artifacts, err := snapshot.OpenStore(filepath.Join(root, "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenWithPacks(filepath.Join(root, "state"), "data-1", &packedHandler{}, pool)
	if err != nil {
		t.Fatal(err)
	}
	s.restoreHook = func(stage string) error {
		if stage == os.Getenv("DUBI_STREAM_RESTORE_POINT") {
			os.Exit(73)
		}
		return nil
	}
	if err := s.RestoreFrom(context.Background(), artifacts, 2, []byte(os.Getenv("DUBI_STREAM_RESTORE_REF")), &raftpb.ConfState{}); err != nil {
		t.Fatal(err)
	}
	t.Fatal("did not reach restore crash point")
}

func TestFileSnapshotRestoreProcessCrash(t *testing.T) {
	for _, stage := range []string{"after-validation", "after-fence", "after-batch", "before-final", "after-final"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			if _, err := identity.Bootstrap(root); err != nil {
				t.Fatal(err)
			}
			volumes, err := volume.Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			volumes.Close()
			path := filepath.Join(root, "artifacts")
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			artifacts, err := snapshot.OpenStore(path)
			if err != nil {
				t.Fatal(err)
			}
			encoded, target, value := fileRestoreFixture(t, artifacts)
			artifacts.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFileSnapshotRestoreCrashHelper$")
			child.Env = append(os.Environ(), "DUBI_STREAM_RESTORE_ROOT="+root, "DUBI_STREAM_RESTORE_POINT="+stage, "DUBI_STREAM_RESTORE_REF="+string(encoded))
			output, err := child.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 73 {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			artifacts, err = snapshot.OpenStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer artifacts.Close()
			volumes, err = volume.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer volumes.Close()
			pool, err := pack.OpenPool(volumes)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			s, err := OpenWithPacks(filepath.Join(root, "state"), "data-1", &packedHandler{}, pool)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if stage == "after-final" {
				if s.AppliedIndex() != 2 {
					t.Fatal("crash lost completed restore")
				}
			} else {
				if stage != "after-validation" {
					assertFileRestoreBlocked(t, s, target, artifacts)
				}
				if err := s.RestoreFrom(context.Background(), artifacts, 2, encoded, &raftpb.ConfState{}); err != nil {
					t.Fatal(err)
				}
			}
			got, found, err := s.Value(target.Keyspace, []byte("item"))
			if err != nil || !found || !bytes.Equal(got, value) {
				t.Fatal("crash recovery did not reconstruct Pack-backed value", err)
			}
			if _, found, err := s.Result("source-write"); err != nil || !found {
				t.Fatal("crash recovery lost request result", err)
			}
		})
	}
}
