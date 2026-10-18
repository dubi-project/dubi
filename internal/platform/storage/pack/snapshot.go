package pack

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SnapshotPack identifies one immutable, sealed Pack file at a local
// checkpoint boundary. The bytes remain in the storage layer; recovery treats
// the path as an opaque source and never decodes Pack records.
type SnapshotPack struct {
	StoreID  string
	VolumeID string
	PackID   string
	Path     string
	Size     int64
	file     *os.File
}

// Open returns a read-only view pinned to the sealed Pack inode. Compaction
// may unlink the pathname after SnapshotPacks returns, but the view remains
// readable until Close is called on the SnapshotPack itself.
func (p SnapshotPack) Open() (io.ReadCloser, error) {
	if p.file == nil {
		return nil, errors.New("Pack snapshot is closed")
	}
	return io.NopCloser(io.NewSectionReader(p.file, 0, p.Size)), nil
}

// Close releases the inode pin held by this snapshot. Callers must close all
// returned SnapshotPacks after their backup worker has uploaded the bytes.
func (p *SnapshotPack) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	err := p.file.Close()
	p.file = nil
	return err
}

func (m *Collection) snapshotPacks(volumePath string) ([]SnapshotPack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil || m.catalog.Pending != "" {
		return nil, errors.New("Pack collection is not at a sealed checkpoint")
	}
	result := make([]SnapshotPack, 0, len(m.catalog.Packs))
	if err := m.readAccess(func(*os.Root) error {
		for _, id := range m.catalog.Packs {
			path := filepath.Join(volumePath, packDirectory, id+".pack")
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("Pack %s is not a regular file", id)
			}
			state, err := os.Stat(filepath.Join(volumePath, packDirectory, id+".state"))
			if err != nil {
				return err
			}
			if !state.Mode().IsRegular() {
				return fmt.Errorf("Pack %s checkpoint is not a regular file", id)
			}
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			result = append(result, SnapshotPack{StoreID: m.catalog.StoreID, VolumeID: m.catalog.VolumeID, PackID: id, Path: path, Size: info.Size(), file: file})
		}
		return nil
	}); err != nil {
		for index := range result {
			_ = result[index].Close()
		}
		return nil, err
	}
	return result, nil
}
