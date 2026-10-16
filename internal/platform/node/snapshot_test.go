package node

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeSnapshotRegistrationLossFailsClosed(t *testing.T) {
	for _, damage := range []string{"directory", "registry", "owner"} {
		t.Run(damage, func(t *testing.T) {
			root := t.TempDir()
			id, err := identity.Bootstrap(root)
			if err != nil {
				t.Fatal(err)
			}
			mux, err := engineruntime.NewRegistry().Seal()
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := Open(Options{StoreRoot: root, TickInterval: time.Hour, EngineMux: mux, RootBootstrapPeers: []uint64{id.RaftID}})
			if err != nil {
				t.Fatal(err)
			}
			if runtime.Snapshots == nil {
				t.Fatal("node lacks managed artifact Store")
			}
			ref, err := runtime.Snapshots.Create(context.Background(), snapshot.Header{Version: 1, Format: "opaque-v1", GroupID: "data", Index: 1},
				1024, func(e *snapshot.Encoder) error { return e.Add([]byte("key"), []byte("value")) })
			if err != nil {
				t.Fatal(err)
			}
			reader, err := runtime.OpenSnapshot("data", ref)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.Copy(io.Discard, reader)
			if err := errors.Join(err, reader.Close(), runtime.Close()); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "dubi-snapshots")
			if damage == "registry" {
				path = filepath.Join(root, "DUBI_SNAPSHOT_STORAGE")
			} else if damage == "owner" {
				path = filepath.Join(path, "DUBI_SNAPSHOT_OWNER")
			}
			if err := os.Rename(path, path+".retained"); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(Options{StoreRoot: root, TickInterval: time.Hour, EngineMux: mux}); err == nil {
				opened.Close()
				t.Fatal("node recreated lost registered artifact resource")
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("lost resource was recreated", err)
			}
			lock, err := storelock.Acquire(root)
			if err != nil {
				t.Fatal("failed startup retained Store lock", err)
			}
			lock.Release()
		})
	}
}

func TestRuntimeRejectsFileImportForUnpreparedGroup(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	mux, err := engineruntime.NewRegistry().Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := Open(Options{StoreRoot: root, TickInterval: time.Hour, EngineMux: mux, RootBootstrapPeers: []uint64{id.RaftID}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	stepper := snapshotStepper{Host: runtime.Host, runtime: runtime}
	for _, groupID := range []string{"missing"} {
		ref := snapshot.Reference{Version: 1, GroupID: groupID, Index: 5, Size: 100,
			SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		data, err := ref.Encode()
		if err != nil {
			t.Fatal(err)
		}
		message := &raftpb.Message{Type: raftpb.MessageType_MsgSnap.Enum(), Snapshot: &raftpb.Snapshot{Data: data,
			Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(5)}}}
		reader := &mustNotReadSnapshot{t: t}
		if _, err := stepper.InstallSnapshotStream(context.Background(), groupID, message, reader); err == nil {
			t.Fatal("accepted file import without a prepared file adapter")
		}
	}
}

type mustNotReadSnapshot struct{ t *testing.T }

func (r *mustNotReadSnapshot) Read([]byte) (int, error) {
	r.t.Error("unprepared Group read incoming artifact")
	return 0, io.EOF
}
