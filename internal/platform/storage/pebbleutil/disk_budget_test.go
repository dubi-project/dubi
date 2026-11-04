package pebbleutil

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/dubi-project/dubi/internal/platform/storage/diskbudget"
)

func TestMetadataWALCanWriteWhileOptionalFilesystemBudgetIsReserved(t *testing.T) {
	root := t.TempDir()
	file, err := vfs.Default.Create(filepath.Join(root, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	sample, err := diskbudget.ProbeFD(file.Fd())
	if err != nil {
		t.Fatal(err)
	}
	if sample.Available <= diskbudget.Floor(sample.Total)+(16<<20) {
		t.Fatal("test requires 16 MiB above the filesystem safety floor")
	}
	lease, err := diskbudget.Shared.Reserve(sample, sample.Available-diskbudget.Floor(sample.Total)-(4<<20), diskbudget.Discretionary)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	optional := &budgetFile{File: file, class: diskbudget.Discretionary}
	data := make([]byte, 8<<20)
	if _, err := optional.Write(data); !errors.Is(err, diskbudget.ErrSpace) {
		t.Fatal("optional restore/data writer overbooked another component's reservation", err)
	}
	wal := &budgetFile{File: file, class: diskbudget.Committed}
	if n, err := wal.Write(data); err != nil || n != len(data) {
		t.Fatal("already-committed WAL was blocked by optional admission", n, err)
	}
}

func TestBudgetWrapperPreservesMemoryFilesystemAndUnderlyingRoot(t *testing.T) {
	mem := vfs.NewMem()
	fs := withDiskBudget(mem)
	if vfs.Root(fs) != mem {
		t.Fatal("filesystem wrapper hid the underlying filesystem")
	}
	file, err := fs.Create("wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("memory-backed metadata")); err != nil {
		t.Fatal(err)
	}
	if withDiskBudget(fs) != fs {
		t.Fatal("budget filesystem was double wrapped")
	}
}
