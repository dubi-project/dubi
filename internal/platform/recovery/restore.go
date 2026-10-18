package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

// Target is an engine-neutral restore sink. Apply must write to a temporary
// resource and publish it atomically; recovery never writes directly into a
// live Engine/Module directory.
type Target struct {
	Name   string
	NodeID string
	Apply  func(context.Context, io.Reader, int64) error
}

// Restore verifies a complete Recovery Point and streams each opaque
// artifact to its registered target. It does not recreate Node identities,
// Raft membership or paths; callers perform ordinary Join, Snapshot catch-up
// and Placement/Repair around these resource callbacks.
func Restore(ctx context.Context, store repository.ObjectStore, epoch uint64, targets []Target) (repository.Manifest, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil || epoch == 0 {
		return repository.Manifest{}, errors.New("Repository and Recovery Point are required")
	}
	manifest, err := store.ReadManifest(epoch)
	if err != nil {
		return repository.Manifest{}, err
	}
	byName := make(map[string]Target, len(targets))
	for _, target := range targets {
		if strings.TrimSpace(target.Name) == "" || target.Apply == nil {
			return repository.Manifest{}, errors.New("invalid restore target")
		}
		key := target.NodeID + "\x00" + target.Name
		if _, found := byName[key]; found {
			return repository.Manifest{}, fmt.Errorf("duplicate restore target %q", target.Name)
		}
		byName[key] = target
	}
	// Preflight the complete restore before invoking any target. A missing
	// target or an unavailable object must not leave an earlier artifact
	// already published while a later artifact fails. Target.Apply remains
	// responsible for its own atomic publication once the preflight passes.
	for _, artifact := range manifest.Artifacts {
		if err := ctx.Err(); err != nil {
			return repository.Manifest{}, err
		}
		if _, found := byName[artifact.NodeID+"\x00"+artifact.Name]; !found {
			return repository.Manifest{}, fmt.Errorf("restore target is missing for artifact %q", artifact.Name)
		}
		reader, err := store.OpenObject(artifact.Object.ID)
		if err != nil {
			return repository.Manifest{}, fmt.Errorf("open restore artifact %q: %w", artifact.Name, err)
		}
		if err := reader.Close(); err != nil {
			return repository.Manifest{}, fmt.Errorf("close restore artifact %q: %w", artifact.Name, err)
		}
	}
	for _, artifact := range manifest.Artifacts {
		if err := ctx.Err(); err != nil {
			return repository.Manifest{}, err
		}
		target, found := byName[artifact.NodeID+"\x00"+artifact.Name]
		if !found {
			return repository.Manifest{}, fmt.Errorf("restore target is missing for artifact %q", artifact.Name)
		}
		reader, err := store.OpenObject(artifact.Object.ID)
		if err != nil {
			return repository.Manifest{}, fmt.Errorf("open restore artifact %q: %w", artifact.Name, err)
		}
		stream := &contextReader{ctx: ctx, reader: reader}
		copyErr := target.Apply(ctx, stream, artifact.Object.Size)
		if copyErr == nil && stream.readBytes != artifact.Object.Size {
			copyErr = fmt.Errorf("restore target %q consumed %d bytes, expected %d", artifact.Name, stream.readBytes, artifact.Object.Size)
		}
		closeErr := reader.Close()
		if copyErr != nil || closeErr != nil {
			return repository.Manifest{}, errors.Join(copyErr, closeErr)
		}
	}
	return manifest, nil
}

// RestoreFile is the first concrete backend entry point.
func RestoreFile(ctx context.Context, store *repository.FileStore, epoch uint64, targets []Target) (repository.Manifest, error) {
	if store == nil {
		return repository.Manifest{}, errors.New("File Repository is required")
	}
	return Restore(ctx, store, epoch, targets)
}

type contextReader struct {
	ctx       context.Context
	reader    io.Reader
	readBytes int64
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := r.reader.Read(buffer)
	r.readBytes += int64(count)
	return count, err
}
