package storelock

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestEnrollmentLockSerializesWorkflowsSeparatelyFromRuntime(t *testing.T) {
	root := t.TempDir()
	workflow, err := AcquireEnrollment(root)
	if err != nil {
		t.Fatal(err)
	}
	defer workflow.Release()
	if other, err := AcquireEnrollment(root); err == nil {
		other.Release()
		t.Fatal("concurrent workflow acquired lease")
	}
	runtime, err := Acquire(root)
	if err != nil {
		t.Fatal("workflow cannot hand off to runtime", err)
	}
	defer runtime.Release()
	if err := workflow.Release(); err != nil {
		t.Fatal(err)
	}
	if other, err := Acquire(root); err == nil {
		other.Release()
		t.Fatal("workflow release also released runtime")
	}
}

func TestStoreLocksRejectSpecialFiles(t *testing.T) {
	for _, name := range []string{Filename, EnrollmentFilename} {
		for _, kind := range []string{"symlink", "fifo", "directory"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				root := t.TempDir()
				path := filepath.Join(root, name)
				var err error
				switch kind {
				case "symlink":
					err = os.Symlink(filepath.Join(root, "absent"), path)
				case "fifo":
					err = unix.Mkfifo(path, 0o600)
				case "directory":
					err = os.Mkdir(path, 0o700)
				}
				if err != nil {
					t.Fatal(err)
				}
				if lease, err := acquire(root, name); err == nil {
					lease.Release()
					t.Fatal("special-file process lock accepted")
				}
			})
		}
	}
}

func TestAcquireIsExclusiveAndReleaseIsIdempotent(t *testing.T) {
	root := t.TempDir()
	first, err := Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(root); err == nil {
		t.Fatal("second process lock unexpectedly acquired")
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}
