// Package recovery contains the engine-neutral Cluster Backup/Restore
// orchestration primitives. It deliberately accepts opaque artifact streams;
// Engines and Modules register their own checkpointed resources instead of
// teaching the backup layer their data formats.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

// Source is one immutable-at-the-cut artifact supplied by a platform
// component. Open must return the same bytes for the lifetime of this backup
// attempt; the component is responsible for its own WAL/Pack/Checkpoint
// barrier before handing the source to recovery.
type Source struct {
	Name    string
	Kind    string
	NodeID  string
	StoreID string
	Format  string
	Size    int64
	Open    func(context.Context) (io.ReadCloser, error)
}

type Plan struct {
	Repository repository.Repository
	ClusterID  string
	Epoch      uint64
	CreatedAt  time.Time
}

type Result struct {
	Manifest  repository.Manifest
	Bytes     int64
	Objects   int
	Artifacts int
}

// RunBackup uploads each opaque source to an ObjectStore and publishes one
// immutable cluster-level Manifest only after every source has been read,
// hashed and verified. Content-addressing makes a logical full Recovery Point
// physically incremental without a delta-chain dependency.
func RunBackup(ctx context.Context, store repository.ObjectStore, plan Plan, sources []Source) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil {
		return Result{}, errors.New("backup ObjectStore is required")
	}
	if err := plan.Repository.Validate(); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(plan.ClusterID) == "" || plan.Epoch == 0 {
		return Result{}, errors.New("backup ClusterID and Epoch are required")
	}
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Now().UTC()
	}
	if len(sources) == 0 {
		return Result{}, errors.New("backup requires at least one checkpointed artifact")
	}
	artifacts, objectList, total, err := collectSources(ctx, store, sources)
	if err != nil {
		return Result{}, err
	}
	manifest := repository.Manifest{Version: 1, Repository: plan.Repository.Name, ClusterID: plan.ClusterID,
		Epoch: plan.Epoch, CreatedAt: plan.CreatedAt.UTC(), Objects: objectList, Artifacts: artifacts}
	if err := store.CommitManifest(ctx, manifest); err != nil {
		return Result{}, err
	}
	return Result{Manifest: manifest, Bytes: total, Objects: len(objectList), Artifacts: len(artifacts)}, nil
}

// RunFileBackup is the first concrete backend entry point. It enforces that
// the local store matches the cluster Repository path before delegating to the
// backend-neutral planner.
func RunFileBackup(ctx context.Context, store *repository.FileStore, plan Plan, sources []Source) (Result, error) {
	if store == nil {
		return Result{}, errors.New("File Repository is required")
	}
	if !plan.Repository.IsFile() {
		return Result{}, errors.New("file backup requires a file Repository")
	}
	if store.Root() != plan.Repository.Path {
		return Result{}, errors.New("File Repository path does not match backup plan")
	}
	return RunBackup(ctx, store, plan, sources)
}

func collectSources(ctx context.Context, store repository.ObjectStore, sources []Source) ([]repository.Artifact, []repository.ObjectRef, int64, error) {
	seenNames := make(map[string]struct{}, len(sources))
	artifacts := make([]repository.Artifact, 0, len(sources))
	objects := make(map[string]repository.ObjectRef, len(sources))
	var total int64
	for _, source := range sources {
		if err := validateSource(source); err != nil {
			return nil, nil, 0, err
		}
		name := source.NodeID + "\x00" + source.Name
		if _, found := seenNames[name]; found {
			return nil, nil, 0, fmt.Errorf("duplicate backup artifact %q", source.Name)
		}
		seenNames[name] = struct{}{}
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		reader, err := source.Open(ctx)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("open backup artifact %q: %w", source.Name, err)
		}
		object, putErr := store.PutObject(ctx, reader)
		closeErr := reader.Close()
		if putErr != nil || closeErr != nil {
			return nil, nil, 0, errors.Join(putErr, closeErr)
		}
		if source.Size >= 0 && object.Size != source.Size {
			return nil, nil, 0, fmt.Errorf("backup artifact %q size changed: expected %d, got %d", source.Name, source.Size, object.Size)
		}
		objects[object.ID] = object
		artifacts = append(artifacts, repository.Artifact{Name: source.Name, Kind: source.Kind,
			NodeID: source.NodeID, StoreID: source.StoreID, Format: source.Format, Object: object})
		total += object.Size
	}
	objectList := make([]repository.ObjectRef, 0, len(objects))
	for _, object := range objects {
		objectList = append(objectList, object)
	}
	sort.Slice(objectList, func(i, j int) bool { return objectList[i].ID < objectList[j].ID })
	sort.Slice(artifacts, func(i, j int) bool {
		left, right := artifacts[i].NodeID+"\x00"+artifacts[i].Name, artifacts[j].NodeID+"\x00"+artifacts[j].Name
		return left < right
	})
	return artifacts, objectList, total, nil
}

func validateSource(source Source) error {
	if strings.TrimSpace(source.Name) != source.Name || source.Name == "" || len(source.Name) > 512 || strings.ContainsRune(source.Name, '\x00') ||
		strings.TrimSpace(source.Kind) != source.Kind || source.Kind == "" || len(source.Kind) > 128 || strings.ContainsRune(source.Kind, '\x00') || source.Open == nil || source.Size < -1 {
		return errors.New("invalid backup artifact source")
	}
	for _, value := range []string{source.Name, source.Kind, source.NodeID, source.StoreID, source.Format} {
		if strings.ContainsAny(value, "\r\n") {
			return errors.New("backup artifact metadata contains a line break")
		}
	}
	return nil
}

// SortSources is shared by schedulers before they create checkpoint cuts. It
// returns a copy so callers do not observe a backup worker mutating their
// registration order.
func SortSources(sources []Source) []Source {
	result := slices.Clone(sources)
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].NodeID+"\x00"+result[i].Name, result[j].NodeID+"\x00"+result[j].Name
		return left < right
	})
	return result
}
