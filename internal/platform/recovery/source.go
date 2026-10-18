package recovery

import (
	"context"
	"errors"
	"io"
	"os"
)

// FileSource adapts an already checkpointed local file to the opaque Source
// contract. The caller owns the consistency barrier (for example Pack Pool
// SnapshotPacks); this helper only opens and streams bytes.
func FileSource(name, kind, nodeID, storeID, format, path string) (Source, error) {
	if path == "" {
		return Source{}, errors.New("backup source path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return Source{}, err
	}
	if !info.Mode().IsRegular() {
		return Source{}, errors.New("backup source path is not a regular file")
	}
	source := Source{Name: name, Kind: kind, NodeID: nodeID, StoreID: storeID, Format: format, Size: info.Size()}
	source.Open = func(ctx context.Context) (io.ReadCloser, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return os.Open(path)
	}
	return source, nil
}
