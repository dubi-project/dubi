package records

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/storage/pack"
)

// SnapshotWithPacks captures locators and sealed inode pins under the same
// Store lock as writes, relocation and GC. Upload may then run without the
// Store lock: pins survive pathname removal during later compaction.
func (s *Store) SnapshotWithPacks(ctx context.Context) ([]pack.SnapshotPack, []byte, error) {
	if ctx == nil {
		return nil, nil, errors.New("nil checkpoint context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, nil, err
	}
	backend, ok := s.packs.(interface {
		SnapshotPacks(context.Context) ([]pack.SnapshotPack, error)
	})
	if !ok {
		return nil, nil, errors.New("Pack backend does not support checkpoints")
	}
	packs, err := backend.SnapshotPacks(ctx)
	if err != nil {
		return nil, nil, err
	}
	image, err := s.snapshotIndexLocked(ctx)
	if err != nil {
		for i := range packs {
			_ = packs[i].Close()
		}
		return nil, nil, err
	}
	return packs, image, nil
}
