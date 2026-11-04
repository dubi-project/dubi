package pebbleutil

import (
	"github.com/cockroachdb/pebble/vfs"
	"github.com/dubi-project/dubi/internal/platform/storage/diskbudget"
)

type budgetFS struct {
	vfs.FS
	class diskbudget.Class
}
type budgetFile struct {
	vfs.File
	class diskbudget.Class
}

func (f *budgetFS) Unwrap() vfs.FS { return f.FS }

func withDiskBudget(fs vfs.FS) vfs.FS {
	if fs == nil {
		fs = vfs.Default
	}
	if _, already := fs.(*budgetFS); already {
		return fs
	}
	return &budgetFS{FS: fs, class: diskbudget.Committed}
}

// DiscretionaryFS protects the recovery reserve from disposable restore work,
// including the staging database's asynchronous flush and compaction writes.
func DiscretionaryFS(fs vfs.FS) vfs.FS {
	if fs == nil {
		fs = vfs.Default
	}
	return &budgetFS{FS: fs, class: diskbudget.Discretionary}
}

func wrapBudgetFile(class diskbudget.Class, f vfs.File, err error) (vfs.File, error) {
	if err != nil || f.Fd() == vfs.InvalidFd {
		return f, err
	}
	return &budgetFile{File: f, class: class}, nil
}
func (f *budgetFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	return wrapBudgetFile(f.class, file, err)
}
func (f *budgetFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.OpenReadWrite(name, opts...)
	return wrapBudgetFile(f.class, file, err)
}
func (f *budgetFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname)
	return wrapBudgetFile(f.class, file, err)
}

// Short per-write leases cover WAL, SST and manifest IO, including Pebble's
// background goroutines. Already-committed writes can use the safety reserve;
// they are not rejected just because discretionary data admission is paused.
func (f *budgetFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return f.File.Write(p)
	}
	l, err := diskbudget.ReserveFD(f.Fd(), uint64(len(p)), f.class)
	if err != nil {
		return 0, err
	}
	defer l.Release()
	return f.File.Write(p)
}
func (f *budgetFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return f.File.WriteAt(p, off)
	}
	l, err := diskbudget.ReserveFD(f.Fd(), uint64(len(p)), f.class)
	if err != nil {
		return 0, err
	}
	defer l.Release()
	return f.File.WriteAt(p, off)
}
func (f *budgetFile) Preallocate(off, length int64) error {
	if length <= 0 {
		return f.File.Preallocate(off, length)
	}
	l, err := diskbudget.ReserveFD(f.Fd(), uint64(length), f.class)
	if err != nil {
		return err
	}
	defer l.Release()
	return f.File.Preallocate(off, length)
}
