package recovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

// ShardPlan identifies one node-local worker's contribution to a cluster
// backup. The static NodeID/StoreID identify the assigned worker shard; a
// healthy node may execute that shard after a lease takeover.
type ShardPlan struct {
	Repository repository.Repository
	JobID      string
	ClusterID  string
	Epoch      uint64
	CreatedAt  time.Time
	NodeID     string
	StoreID    string
}

// RunNodeBackup uploads opaque local resources and commits only the immutable
// node shard. It intentionally does not publish a cluster Recovery Point;
// FinalizeShards is the explicit aggregation boundary.
func RunNodeBackup(ctx context.Context, store repository.ShardStore, plan ShardPlan, sources []Source) (Result, repository.Shard, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil {
		return Result{}, repository.Shard{}, errors.New("backup ShardStore is required")
	}
	if err := validateShardPlan(plan); err != nil {
		return Result{}, repository.Shard{}, err
	}
	if len(sources) == 0 {
		return Result{}, repository.Shard{}, errors.New("backup requires at least one checkpointed artifact")
	}
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Now().UTC()
	}
	artifacts, objects, total, err := collectSources(ctx, store, sources)
	if err != nil {
		return Result{}, repository.Shard{}, err
	}
	shard := repository.Shard{Version: 1, JobID: plan.JobID, Repository: plan.Repository.Name,
		ClusterID: plan.ClusterID, Epoch: plan.Epoch, CreatedAt: plan.CreatedAt.UTC(),
		NodeID: plan.NodeID, StoreID: plan.StoreID, Objects: objects, Artifacts: artifacts}
	if err := store.CommitShard(ctx, shard); err != nil {
		return Result{}, repository.Shard{}, err
	}
	return Result{Bytes: total, Objects: len(objects), Artifacts: len(artifacts)}, shard, nil
}

func RunFileNodeBackup(ctx context.Context, store *repository.FileStore, plan ShardPlan, sources []Source) (Result, repository.Shard, error) {
	if store == nil {
		return Result{}, repository.Shard{}, errors.New("file Repository is required")
	}
	if !plan.Repository.IsFile() {
		return Result{}, repository.Shard{}, errors.New("file backup requires a file Repository")
	}
	if store.Root() != plan.Repository.Path {
		return Result{}, repository.Shard{}, errors.New("file Repository path does not match backup plan")
	}
	return RunNodeBackup(ctx, store, plan, sources)
}

func validateShardPlan(plan ShardPlan) error {
	if err := plan.Repository.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(plan.ClusterID) == "" || plan.Epoch == 0 || strings.TrimSpace(plan.JobID) == "" || strings.TrimSpace(plan.NodeID) == "" || strings.TrimSpace(plan.StoreID) == "" {
		return errors.New("backup shard JobID, ClusterID, Epoch, NodeID and StoreID are required")
	}
	return nil
}

// FinalizeShards deterministically merges completed node shards into one
// cluster Recovery Point. The repository verifies that every referenced
// object exists before publishing the final manifest.
func FinalizeShards(ctx context.Context, store repository.ObjectStore, plan Plan, shards []repository.Shard) (Result, error) {
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
	if len(shards) == 0 {
		return Result{}, errors.New("at least one backup shard is required")
	}
	objects := make(map[string]repository.ObjectRef)
	artifacts := make([]repository.Artifact, 0)
	seenNodes := make(map[string]struct{}, len(shards))
	seenArtifacts := make(map[string]struct{})
	createdAt := plan.CreatedAt
	for _, shard := range shards {
		if err := shard.Validate(); err != nil {
			return Result{}, err
		}
		if shard.Repository != plan.Repository.Name || shard.ClusterID != plan.ClusterID || shard.Epoch != plan.Epoch {
			return Result{}, fmt.Errorf("backup shard %s does not match the finalization plan", shard.NodeID)
		}
		if _, found := seenNodes[shard.NodeID]; found {
			return Result{}, fmt.Errorf("duplicate backup shard for Node %s", shard.NodeID)
		}
		seenNodes[shard.NodeID] = struct{}{}
		if createdAt.IsZero() || shard.CreatedAt.After(createdAt) {
			createdAt = shard.CreatedAt
		}
		for _, object := range shard.Objects {
			if existing, found := objects[object.ID]; found && existing.Size != object.Size {
				return Result{}, fmt.Errorf("backup object %s has conflicting sizes", object.ID)
			}
			objects[object.ID] = object
		}
		for _, artifact := range shard.Artifacts {
			key := artifact.NodeID + "\x00" + artifact.Name
			if _, found := seenArtifacts[key]; found {
				return Result{}, fmt.Errorf("duplicate backup artifact %q", artifact.Name)
			}
			seenArtifacts[key] = struct{}{}
			artifacts = append(artifacts, artifact)
		}
	}
	if createdAt.IsZero() {
		return Result{}, errors.New("backup shard creation time is required")
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
	manifest := repository.Manifest{Version: 1, Repository: plan.Repository.Name, ClusterID: plan.ClusterID,
		Epoch: plan.Epoch, CreatedAt: createdAt.UTC(), Objects: objectList, Artifacts: artifacts}
	if err := store.CommitManifest(ctx, manifest); err != nil {
		return Result{}, err
	}
	var total int64
	for _, object := range objectList {
		total += object.Size
	}
	return Result{Manifest: manifest, Bytes: total, Objects: len(objectList), Artifacts: len(artifacts)}, nil
}
