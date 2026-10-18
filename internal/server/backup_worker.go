package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/recovery"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

const (
	backupWorkerInterval = 5 * time.Second
	backupWorkerLease    = 10 * time.Minute
)

// backupWorker executes only the local member's shard. It deliberately does
// not pretend that a node-local file Repository is a shared filesystem: shard
// completion is recorded in Root, while cluster-level manifest aggregation is
// a later coordinator step.
type backupWorker struct {
	runtime *node.Runtime
	report  func(error)
}

func (w *backupWorker) run(ctx context.Context) {
	if w == nil || w.runtime == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(backupWorkerInterval)
	defer ticker.Stop()
	for {
		w.process(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *backupWorker) process(ctx context.Context) {
	jobs, err := w.runtime.Root.BackupJobs()
	if err != nil {
		w.reportError(fmt.Errorf("list Backup jobs: %w", err))
		return
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return
		}
		if err := w.processJob(ctx, job); err != nil {
			w.reportError(fmt.Errorf("Backup job %s: %w", job.JobID, err))
		}
	}
}

func (w *backupWorker) processJob(ctx context.Context, job rootstate.BackupJob) error {
	worker, found := localBackupWorker(job, w.runtime.Identity.RaftID, w.runtime.Identity.NodeID)
	if !found {
		return nil
	}
	repositoryConfig, found, err := w.runtime.Root.Repository(job.Repository)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("Backup Repository disappeared")
	}
	if !repositoryConfig.IsFile() {
		// S3 remains an explicit backend configuration, but its object-store
		// implementation is intentionally not claimed by this local worker yet.
		return fmt.Errorf("Backup Repository type %q is not implemented by the local worker", repositoryConfig.Type)
	}
	if worker.State == rootstate.BackupWorkerCompleted || worker.State == rootstate.BackupWorkerFailed {
		return w.tryFinalize(ctx, job, repositoryConfig)
	}
	now := time.Now().Unix()
	leaseEpoch := worker.LeaseEpoch
	owned := worker.State == rootstate.BackupWorkerRunning && worker.LeaseRaftID == w.runtime.Identity.RaftID && worker.LeaseNodeID == w.runtime.Identity.NodeID && worker.LeaseExpiresUnix > now
	if !owned {
		requestID := backupRequestID("claim", job.JobID, worker.RaftID, uint64(now))
		claim := rootstate.BackupJobClaim{JobID: job.JobID, RaftID: worker.RaftID, NodeID: worker.NodeID,
			ClaimantRaftID: w.runtime.Identity.RaftID, ClaimantNodeID: w.runtime.Identity.NodeID,
			ObservedUnix: now, LeaseExpiresUnix: now + int64(backupWorkerLease/time.Second)}
		if err := w.submit(ctx, requestID, rootstate.Command{Version: rootstate.FormatVersion, RequestID: requestID, Type: rootstate.ClaimBackupJob, BackupJobClaim: &claim}); err != nil {
			// Another current worker may have won the lease. This is a normal
			// polling race, not a reason to fail the cluster backup.
			if strings.Contains(err.Error(), "lease is still active") || strings.Contains(err.Error(), "terminal") {
				return nil
			}
			return err
		}
		latest, ok, err := w.runtime.Root.BackupJob(job.JobID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("Backup job disappeared after claim")
		}
		worker, found = localBackupWorker(latest, worker.RaftID, worker.NodeID)
		if !found || worker.LeaseRaftID != w.runtime.Identity.RaftID || worker.LeaseNodeID != w.runtime.Identity.NodeID {
			return nil
		}
		leaseEpoch = worker.LeaseEpoch
	}
	store, err := repository.OpenFile(repositoryConfig)
	if err != nil {
		return err
	}
	sources, release, err := w.sources(ctx)
	if err != nil {
		return err
	}
	defer release()
	result, _, err := recovery.RunFileNodeBackup(ctx, store, recovery.ShardPlan{Repository: repositoryConfig,
		JobID: job.JobID, ClusterID: job.ClusterID, Epoch: job.Epoch, NodeID: w.runtime.Identity.NodeID,
		StoreID: w.runtime.Packs.StoreID()}, sources)
	if err != nil {
		return err
	}
	completeID := backupRequestID("complete", job.JobID, worker.RaftID, leaseEpoch)
	completion := rootstate.BackupJobCompletion{JobID: job.JobID, RaftID: worker.RaftID, NodeID: worker.NodeID,
		ClaimantRaftID: w.runtime.Identity.RaftID, ClaimantNodeID: w.runtime.Identity.NodeID,
		LeaseEpoch: leaseEpoch, Bytes: result.Bytes, Objects: uint64(result.Objects)}
	if err := w.submit(ctx, completeID, rootstate.Command{Version: rootstate.FormatVersion, RequestID: completeID, Type: rootstate.CompleteBackupJob, BackupJobCompletion: &completion}); err != nil {
		return err
	}
	return w.tryFinalize(ctx, job, repositoryConfig)
}

// tryFinalize is intentionally opportunistic. It succeeds when all shards
// and their objects are visible in the same file namespace (for example a
// shared filesystem), but returns quietly for the documented node-local file
// layout where another Node's objects must first be exchanged by a future
// coordinator.
func (w *backupWorker) tryFinalize(ctx context.Context, job rootstate.BackupJob, config repository.Repository) error {
	store, err := repository.OpenFile(config)
	if err != nil {
		return err
	}
	shards, err := store.ListShards(job.JobID)
	if err != nil || len(shards) != len(job.Workers) {
		return err
	}
	for _, shard := range shards {
		for _, object := range shard.Objects {
			found, err := store.HasObject(object.ID)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
		}
	}
	if _, err := recovery.FinalizeShards(ctx, store, recovery.Plan{Repository: config, ClusterID: job.ClusterID, Epoch: job.Epoch}, shards); err != nil {
		if _, readErr := store.ReadManifest(job.Epoch); readErr != nil {
			return err
		}
	}
	latest, found, err := w.runtime.Root.BackupJob(job.JobID)
	if err != nil || !found {
		return err
	}
	for _, item := range latest.Workers {
		if item.State != rootstate.BackupWorkerCompleted {
			return nil
		}
	}
	finalizeID := backupRequestID("finalize", job.JobID, w.runtime.Identity.RaftID, job.Epoch)
	return w.submit(ctx, finalizeID, rootstate.Command{Version: rootstate.FormatVersion, RequestID: finalizeID,
		Type: rootstate.FinalizeBackupJob, BackupJobReference: &rootstate.BackupJobReference{JobID: job.JobID}})
}

func (w *backupWorker) sources(ctx context.Context) ([]recovery.Source, func(), error) {
	if w.runtime.Packs == nil {
		return nil, func() {}, errors.New("Pack pool is not available")
	}
	packs, err := w.runtime.Packs.SnapshotPacks(ctx)
	if err != nil {
		return nil, func() {}, err
	}
	release := func() {
		for index := range packs {
			_ = packs[index].Close()
		}
	}
	sources := make([]recovery.Source, 0, len(packs)+1)
	for _, item := range packs {
		item := item
		source := recovery.Source{Name: fmt.Sprintf("volume/%s/pack/%s", item.VolumeID, item.PackID), Kind: "pack",
			NodeID: w.runtime.Identity.NodeID, StoreID: item.StoreID, Format: "DUBIPK02", Size: item.Size,
			Open: func(openContext context.Context) (io.ReadCloser, error) {
				if openContext != nil {
					if err := openContext.Err(); err != nil {
						return nil, err
					}
				}
				return item.Open()
			}}
		sources = append(sources, source)
	}
	if w.runtime.Root != nil {
		image, index, _, err := w.runtime.Root.Snapshot()
		if err != nil {
			release()
			return nil, func() {}, fmt.Errorf("snapshot Root state: %w", err)
		}
		rootImage := append([]byte(nil), image...)
		sources = append(sources, recovery.Source{Name: fmt.Sprintf("root/state/%d", index), Kind: "root-state",
			NodeID: w.runtime.Identity.NodeID, StoreID: w.runtime.Packs.StoreID(), Format: "DUBI_ROOT_STATE_V1", Size: int64(len(rootImage)),
			Open: func(openContext context.Context) (io.ReadCloser, error) {
				if openContext != nil {
					if err := openContext.Err(); err != nil {
						return nil, err
					}
				}
				return io.NopCloser(bytes.NewReader(rootImage)), nil
			}})
	}
	return sources, release, nil
}

func localBackupWorker(job rootstate.BackupJob, raftID uint64, nodeID string) (rootstate.BackupWorker, bool) {
	for _, worker := range job.Workers {
		if worker.RaftID == raftID && worker.NodeID == nodeID {
			return worker, true
		}
	}
	return rootstate.BackupWorker{}, false
}

func backupRequestID(kind, jobID string, raftID, epoch uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", kind, jobID, raftID, epoch)))
	return "backup-" + kind + "-" + hex.EncodeToString(digest[:16])
}

func (w *backupWorker) submit(ctx context.Context, requestID string, command rootstate.Command) error {
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := w.runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := w.runtime.ReadRootIndex(ctx); err != nil {
			return err
		}
		result, found, err := w.runtime.Root.ResultForCommand(encoded)
		if err != nil {
			return err
		}
		if found {
			if result.Error != "" {
				return errors.New(result.Error)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *backupWorker) reportError(err error) {
	if err != nil && w.report != nil {
		w.report(err)
	}
}
