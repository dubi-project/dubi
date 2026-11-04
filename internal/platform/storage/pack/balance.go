package pack

import (
	"errors"
	"os"
	"path/filepath"
)

// PhysicalSize reads only validated Pack metadata. Capacity balancing moves
// an entire sealed Pack's live records only when the Pack fits its plan.
func (p *Pool) PhysicalSize(loc Location) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || loc.StoreID != p.storeID || !idPattern.MatchString(loc.PackID) {
		return 0, errors.New("invalid Pack size lookup")
	}
	var size uint64
	err := p.registry.WithVolume(loc.VolumeID, false, func(root *os.Root, _ string) error {
		info, err := root.Stat(filepath.Join(packDirectory, loc.PackID+".pack"))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return errors.New("invalid Pack file")
		}
		size = uint64(info.Size())
		return nil
	})
	return size, err
}
