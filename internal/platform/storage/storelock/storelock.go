// Package storelock prevents two DUBI processes from opening the same Store root.
package storelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const Filename = ".DUBI_STORE.lock"

var ErrAlreadyOpen = errors.New("Store is already open by another DUBI process")

// EnrollmentFilename serializes multi-stage offline joining commands. It is
// separate from the runtime Store lease: each publication still takes that
// lease itself, and the final Host takes ownership before this lock is released.
const EnrollmentFilename = ".DUBI_JOIN_WORKFLOW.lock"

type Lock struct {
	mu   sync.Mutex
	file *os.File
}

func Acquire(root string) (*Lock, error) {
	return acquire(root, Filename)
}

func AcquireEnrollment(root string) (*Lock, error) {
	return acquire(root, EnrollmentFilename)
}

func acquire(root, filename string) (*Lock, error) {
	if root == "" {
		return nil, errors.New("Store lock root is empty")
	}
	file, err := os.OpenFile(filepath.Join(root, filename), os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open Store process lock: %w", err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(err, file.Close(), errors.New("Store process lock must be a regular physical file"))
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrAlreadyOpen
		}
		return nil, fmt.Errorf("acquire Store process lock: %w", err)
	}
	return &Lock{file: file}, nil
}

func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := errors.Join(unix.Flock(int(l.file.Fd()), unix.LOCK_UN), l.file.Close())
	l.file = nil
	return err
}
