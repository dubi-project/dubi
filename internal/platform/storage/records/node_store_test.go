package records

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func TestNodeRecordStoreOwnedCreationAndReopen(t *testing.T) {
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	b, err := r.Resource(volume.ImmutableRecordsResource)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := b.Current()
	if err != nil || intent.State != volume.ResourcePending {
		t.Fatal(intent, err)
	}
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	if s.identity.ResourceID != intent.ResourceID {
		t.Fatal("child generated a separate identity")
	}
	if err := b.CheckActive(); err != nil {
		t.Fatal(err)
	}
	d := descriptor(t, "key", "data")
	put(t, s, d, "data")
	before := location(t, s, d)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	put(t, s, d, "data")
	if location(t, s, d) != before {
		t.Fatal("reopen appended duplicate")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Verify(context.Background(), d); err == nil {
		t.Fatal("closed parent still produced holding")
	}
}

func TestNodeRecordStoreRejectsMissingActiveChild(t *testing.T) {
	for _, part := range []string{"marker", "directory", "index", "whole"} {
		t.Run(part, func(t *testing.T) {
			root := createPoolRoot(t)
			r, p := realPool(t, root)
			s, err := OpenNodeStore(p, r)
			if err != nil {
				t.Fatal(err)
			}
			put(t, s, descriptor(t, "key", "data"), "data")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			paths := []string{}
			switch part {
			case "marker":
				paths = []string{filepath.Join(root, registrationFile)}
			case "directory":
				paths = []string{filepath.Join(root, directory)}
			case "index":
				paths = []string{filepath.Join(root, directory, "index")}
			case "whole":
				paths = []string{filepath.Join(root, registrationFile), filepath.Join(root, directory)}
			}
			for _, path := range paths {
				if err := os.Rename(path, path+".lost"); err != nil {
					t.Fatal(err)
				}
			}
			before := treeDigest(t, root)
			if next, err := OpenNodeStore(p, r); err == nil {
				next.Close()
				t.Fatal("active child recreated")
			}
			if !reflect.DeepEqual(before, treeDigest(t, root)) {
				t.Fatal("rejected reopen modified resources")
			}
		})
	}
}

func TestNodeRecordStoreRejectsForeignChildAndPool(t *testing.T) {
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	foreign, err := open(root, p, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := foreign.Close(); err != nil {
		t.Fatal(err)
	}
	before := treeDigest(t, root)
	if s, err := OpenNodeStore(p, r); err == nil {
		s.Close()
		t.Fatal("unbound child identity adopted")
	}
	if !reflect.DeepEqual(before, treeDigest(t, root)) {
		t.Fatal("foreign child modified")
	}
	otherRoot := createPoolRoot(t)
	other, otherPool := realPool(t, otherRoot)
	if s, err := OpenNodeStore(otherPool, r); err == nil {
		s.Close()
		t.Fatal("foreign pool accepted")
	}
	if s, err := OpenNodeStore(p, other); err == nil {
		s.Close()
		t.Fatal("foreign catalog accepted")
	}
	if _, err := os.Stat(filepath.Join(otherRoot, directory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("mismatched resources created child", err)
	}
}

func TestNodeRecordStoreChecksLiveParent(t *testing.T) {
	for _, name := range []string{"DUBI_VOLUMES", "DUBI_STORAGE_ID", "DUBI_NODE_IDENTITY"} {
		t.Run(name, func(t *testing.T) {
			root := createPoolRoot(t)
			r, p := realPool(t, root)
			s, err := OpenNodeStore(p, r)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			path := filepath.Join(root, name)
			if err := os.Rename(path, path+".lost"); err != nil {
				t.Fatal(err)
			}
			if h, err := s.Verify(context.Background(), d); err == nil || h.RecordID != "" {
				t.Fatal("missing parent produced holding", h, err)
			}
			if h, err := s.Put(context.Background(), d, []byte("data")); err == nil || h.RecordID != "" {
				t.Fatal("duplicate ignored parent loss", h, err)
			}
		})
	}
}

func TestNodeRecordStoreActivationFailuresReleaseChild(t *testing.T) {
	for _, phase := range []string{"before-parent-active", "after-parent-active"} {
		t.Run(phase, func(t *testing.T) {
			root := createPoolRoot(t)
			r, p := realPool(t, root)
			if s, err := openNodeStore(p, r, interruptAt(phase)); s != nil || !errors.Is(err, interrupted) {
				t.Fatal(s, err)
			}
			b, err := r.Resource(volume.ImmutableRecordsResource)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := b.Current()
			if err != nil {
				t.Fatal(err)
			}
			want := volume.ResourcePending
			if phase == "after-parent-active" {
				want = volume.ResourceActive
			}
			if intent.State != want {
				t.Fatal(intent)
			}
			s, err := OpenNodeStore(p, r)
			if err != nil {
				t.Fatal("child lease leaked or retry failed", err)
			}
			defer s.Close()
			put(t, s, descriptor(t, "key", "data"), "data")
		})
	}
}

func TestNodeRecordStoreProcessCrashPublication(t *testing.T) {
	if root := os.Getenv("DUBI_NODE_RECORD_CRASH_ROOT"); root != "" {
		r, p := realPool(t, root)
		stage := os.Getenv("DUBI_NODE_RECORD_CRASH_STAGE")
		_, err := openNodeStore(p, r, func(got string) error {
			if got == stage {
				os.Exit(73)
			}
			return nil
		})
		t.Fatalf("crash stage not reached: %v", err)
	}
	for _, stage := range []string{"after-directory", "after-intent", "after-index-open", "after-index-identity", "after-active", "before-parent-active", "after-parent-active"} {
		t.Run(stage, func(t *testing.T) {
			root := createPoolRoot(t)
			cmd := exec.Command(os.Args[0], "-test.run=^TestNodeRecordStoreProcessCrashPublication$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "DUBI_NODE_RECORD_CRASH_ROOT="+root, "DUBI_NODE_RECORD_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("child did not exit at %s: %v\n%s", stage, err, out)
			}
			r, p := realPool(t, root)
			b, err := r.Resource(volume.ImmutableRecordsResource)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := b.Current()
			if err != nil {
				t.Fatal(err)
			}
			want := volume.ResourcePending
			if stage == "after-parent-active" {
				want = volume.ResourceActive
			}
			if intent.State != want {
				t.Fatal(intent, want)
			}
			s, err := OpenNodeStore(p, r)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.identity.ResourceID != intent.ResourceID {
				t.Fatal("recovery substituted resource identity")
			}
			put(t, s, descriptor(t, "recovered", "data"), "data")
			if err := b.CheckActive(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
