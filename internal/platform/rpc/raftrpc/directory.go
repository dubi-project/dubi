package raftrpc

import (
	"errors"
	"strings"
	"sync"
)

// Directory maps stable Raft identities to current peer RPC addresses.
type Directory interface {
	Resolve(raftID uint64) (string, bool)
}

type StaticDirectory struct {
	mu        sync.RWMutex
	addresses map[uint64]string
}

func NewStaticDirectory() *StaticDirectory {
	return &StaticDirectory{addresses: make(map[uint64]string)}
}

func (d *StaticDirectory) Resolve(raftID uint64) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	address, ok := d.addresses[raftID]
	return address, ok
}

func (d *StaticDirectory) Update(raftID uint64, address string) error {
	address = strings.TrimSpace(address)
	if raftID == 0 || address == "" {
		return errors.New("Raft peer needs a non-zero ID and non-empty address")
	}
	d.mu.Lock()
	d.addresses[raftID] = address
	d.mu.Unlock()
	return nil
}

func (d *StaticDirectory) Remove(raftID uint64) {
	d.mu.Lock()
	delete(d.addresses, raftID)
	d.mu.Unlock()
}
