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

// ClusterTarget identifies an artifact by the source Node identity recorded
// in a Recovery Point. Apply is free to install it under a new Node/Store
// identity; the source identity is only the immutable lookup key.
type ClusterTarget struct {
	NodeID string
	Name   string
	Apply  func(context.Context, io.Reader, int64) error
}

// NodeStore opens the node-local view of a Repository. For S3 this normally
// returns OpenS3Node; for a file Repository it points at that Node's backup
// disk. The callback is called only after the Recovery Point index is valid.
type NodeStore func(context.Context, string) (repository.ObjectStore, error)

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
	return restoreManifest(ctx, store, manifest, targets)
}

// RestoreCluster restores a complete indexed Recovery Point after validating
// every node Manifest, object and target first. No target Apply is invoked if
// any node store, artifact or target is missing. Apply callbacks should still
// publish each resource atomically into the new cluster.
func RestoreCluster(ctx context.Context, point repository.RecoveryPoint, open NodeStore, targets []ClusterTarget) ([]repository.Manifest, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := point.Validate(); err != nil {
		return nil, err
	}
	if open == nil {
		return nil, errors.New("cluster restore NodeStore opener is required")
	}
	byTarget := make(map[string]Target, len(targets))
	for _, target := range targets {
		if strings.TrimSpace(target.NodeID) != target.NodeID || target.NodeID == "" || strings.TrimSpace(target.Name) != target.Name || target.Name == "" || target.Apply == nil {
			return nil, errors.New("invalid cluster restore target")
		}
		key := target.NodeID + "\x00" + target.Name
		if _, found := byTarget[key]; found {
			return nil, fmt.Errorf("duplicate cluster restore target %q", target.Name)
		}
		byTarget[key] = Target{Name: target.Name, NodeID: target.NodeID, Apply: target.Apply}
	}
	stores := make([]repository.ObjectStore, len(point.Nodes))
	manifests := make([]repository.Manifest, len(point.Nodes))
	for index, node := range point.Nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		store, err := open(ctx, node.NodeID)
		if err != nil {
			return nil, fmt.Errorf("open Recovery Point store for Node %s: %w", node.NodeID, err)
		}
		if store == nil {
			return nil, fmt.Errorf("Recovery Point store for Node %s is nil", node.NodeID)
		}
		manifest, err := store.ReadManifest(point.Epoch)
		if err != nil {
			return nil, fmt.Errorf("read Recovery Point Manifest for Node %s: %w", node.NodeID, err)
		}
		if manifest.Repository != point.Repository || manifest.ClusterID != point.ClusterID || manifest.Epoch != point.Epoch {
			return nil, fmt.Errorf("Recovery Point Manifest for Node %s does not match the cluster index", node.NodeID)
		}
		stores[index], manifests[index] = store, manifest
	}
	for index, manifest := range manifests {
		for _, artifact := range manifest.Artifacts {
			if _, found := byTarget[point.Nodes[index].NodeID+"\x00"+artifact.Name]; !found {
				return nil, fmt.Errorf("cluster restore target is missing for Node %s artifact %q", point.Nodes[index].NodeID, artifact.Name)
			}
			reader, err := stores[index].OpenObject(artifact.Object.ID)
			if err != nil {
				return nil, fmt.Errorf("open cluster restore artifact %q on Node %s: %w", artifact.Name, point.Nodes[index].NodeID, err)
			}
			if err := reader.Close(); err != nil {
				return nil, fmt.Errorf("close cluster restore artifact %q on Node %s: %w", artifact.Name, point.Nodes[index].NodeID, err)
			}
		}
	}
	for index, manifest := range manifests {
		nodeID := point.Nodes[index].NodeID
		localTargets := make([]Target, 0, len(manifest.Artifacts))
		for _, artifact := range manifest.Artifacts {
			localTargets = append(localTargets, byTarget[nodeID+"\x00"+artifact.Name])
		}
		if _, err := restoreManifest(ctx, stores[index], manifest, localTargets); err != nil {
			return nil, err
		}
	}
	return manifests, nil
}

func restoreManifest(ctx context.Context, store repository.ObjectStore, manifest repository.Manifest, targets []Target) (repository.Manifest, error) {
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
