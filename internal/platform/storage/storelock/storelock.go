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

type Lock struct {
	mu   sync.Mutex
	file *os.File
}

func Acquire(root string) (*Lock, error) {
	if root == "" {
		return nil, errors.New("Store lock root is empty")
	}
	file, err := os.OpenFile(filepath.Join(root, Filename), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open Store process lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errors.New("Store is already open by another DUBI process")
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
