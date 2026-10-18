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
	"github.com/dubi-project/dubi/internal/platform/rpc/nodeadminrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

const (
	backupWorkerInterval = 5 * time.Second
	backupWorkerLease    = 10 * time.Minute
)

// backupWorker executes the local member's checkpoint. Each Node writes its
// own Repository objects and Manifest; Root only coordinates the cluster job
// completion. The worker never treats a node-local Repository as shared.
type backupWorker struct {
	runtime   *node.Runtime
	client    *raftrpc.GroupClient
	storeRoot string
	report    func(error)
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
	w.syncRepositoryKey(ctx)
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

// syncRepositoryKey distributes the existing Store-local key over the
// authenticated NodeAdmin channel. A missing local key is left untouched so
// a lost key cannot be silently replaced; an administrator can explicitly
// rotate Repository credentials after that event.
func (w *backupWorker) syncRepositoryKey(ctx context.Context) {
	if w == nil || w.client == nil || w.storeRoot == "" {
		return
	}
	key, err := repository.LoadRepositoryKey(w.storeRoot)
	if err != nil {
		return
	}
	if _, err := w.runtime.ReadRootIndex(ctx); err != nil {
		return
	}
	view, err := w.runtime.Root.RootRoutingView(4096)
	if err != nil {
		return
	}
	for _, target := range view.Nodes {
		if target.RaftID == w.runtime.Identity.RaftID {
			continue
		}
		connection, err := w.client.NodeAdminConnection(target)
		if err != nil {
			continue
		}
		callContext, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = nodeadminrpc.TransferRepositoryKey(callContext, connection, w.runtime.Identity.ClusterID, w.runtime.Identity.RaftID, key)
		cancel()
		_ = connection.Close()
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
	if !repositoryConfig.IsFile() && !repositoryConfig.IsS3() {
		return fmt.Errorf("Backup Repository type %q is not implemented by the local worker", repositoryConfig.Type)
	}
	if worker.State == rootstate.BackupWorkerCompleted || worker.State == rootstate.BackupWorkerFailed {
		return w.finalizeIfComplete(ctx, job)
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
	var store repository.ObjectStore
	if repositoryConfig.IsFile() {
		store, err = repository.OpenFile(repositoryConfig)
	} else {
		key, keyErr := repository.LoadRepositoryKey(w.storeRoot)
		if keyErr != nil {
			return fmt.Errorf("load Repository key for S3 backup: %w", keyErr)
		}
		credentials, credentialErr := repository.OpenS3Credentials(repositoryConfig, key)
		if credentialErr != nil {
			return fmt.Errorf("decrypt S3 Repository credentials: %w", credentialErr)
		}
		store, err = repository.OpenS3Node(repositoryConfig, credentials, w.runtime.Identity.NodeID)
	}
	if err != nil {
		return err
	}
	sources, release, err := w.sources(ctx)
	if err != nil {
		return err
	}
	defer release()
	result, err := recovery.RunBackup(ctx, store, recovery.Plan{Repository: repositoryConfig,
		ClusterID: job.ClusterID, Epoch: job.Epoch}, sources)
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
	return w.finalizeIfComplete(ctx, job)
}

func (w *backupWorker) finalizeIfComplete(ctx context.Context, job rootstate.BackupJob) error {
	latest, found, err := w.runtime.Root.BackupJob(job.JobID)
	if err != nil || !found {
		return err
	}
	if latest.State == rootstate.BackupJobCompleted || latest.State == rootstate.BackupJobFailed {
		return nil
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
	if w.runtime.Engines != nil {
		states, snapshotErr := w.runtime.Engines.SnapshotStates(ctx)
		if snapshotErr != nil {
			release()
			return nil, func() {}, snapshotErr
		}
		for _, state := range states {
			state := state
			sources = append(sources, recovery.Source{Name: fmt.Sprintf("engine/group/%s/state", state.GroupID), Kind: "engine-state",
				NodeID: w.runtime.Identity.NodeID, StoreID: w.runtime.Packs.StoreID(), Format: "DUBI_ENGINE_STATE_V7", Size: int64(len(state.Data)),
				Open: func(openContext context.Context) (io.ReadCloser, error) {
					if openContext != nil {
						if err := openContext.Err(); err != nil {
							return nil, err
						}
					}
					return io.NopCloser(bytes.NewReader(state.Data)), nil
				}})
		}
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
