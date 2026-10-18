package rootstate

import (
	"path/filepath"
	"testing"

	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"go.etcd.io/raft/v3/raftpb"
)

func applyBackupCommand(t *testing.T, state *State, requestID string, command Command) Result {
	t.Helper()
	command.Version = FormatVersion
	command.RequestID = requestID
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.Result(requestID)
	if err != nil || !found {
		t.Fatalf("Root result = %+v, found=%v, err=%v", result, found, err)
	}
	return result
}

func TestBackupJobLifecycleLeaseTakeoverAndSnapshot(t *testing.T) {
	state := rootFileState(t, t.TempDir())
	t.Cleanup(func() { _ = state.Close() })
	if err := state.ApplyMembership(1, testBackupMembership(11)); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, state, Command{Type: RegisterNode, Node: &Node{NodeID: "node_00000000000000000000000000000011", RaftID: 11, RegionID: "west", Address: "127.0.0.1:10011"}})
	applyRootFileCommand(t, state, Command{Type: CreateRepository, Repository: &repository.Repository{Name: "local", Type: repository.TypeFile, Path: "/backup/dubi"}})
	clusterID := "cluster_00000000000000000000000000000011"
	created := applyBackupCommand(t, state, "backup-create", Command{Type: CreateBackupJob, BackupJob: &BackupJob{JobID: "job-1", Repository: "local", ClusterID: clusterID}})
	if created.Error != "" {
		t.Fatal(created.Error)
	}
	job, found, err := state.BackupJob("job-1")
	if err != nil || !found || job.State != BackupJobPending || len(job.Workers) != 1 || job.Workers[0].RaftID != 11 {
		t.Fatalf("created job = %+v, found=%v, err=%v", job, found, err)
	}
	if job.Epoch == 0 || job.Epoch != job.CreatedIndex {
		t.Fatalf("backup epoch is not a stable creation namespace: %+v", job)
	}
	createdEpoch := job.Epoch
	applyBackupCommand(t, state, "backup-claim-1", Command{Type: ClaimBackupJob, BackupJobClaim: &BackupJobClaim{JobID: "job-1", RaftID: 11, NodeID: job.Workers[0].NodeID, ObservedUnix: 100, LeaseExpiresUnix: 200}})
	job, _, _ = state.BackupJob("job-1")
	if job.State != BackupJobRunning || job.Workers[0].LeaseEpoch != 1 {
		t.Fatalf("claimed job = %+v", job)
	}
	if result := applyBackupCommand(t, state, "backup-claim-active", Command{Type: ClaimBackupJob, BackupJobClaim: &BackupJobClaim{JobID: "job-1", RaftID: 11, NodeID: job.Workers[0].NodeID, ObservedUnix: 150, LeaseExpiresUnix: 250}}); result.Error == "" {
		t.Fatal("active lease was unexpectedly taken over")
	}
	applyBackupCommand(t, state, "backup-claim-2", Command{Type: ClaimBackupJob, BackupJobClaim: &BackupJobClaim{JobID: "job-1", RaftID: 11, NodeID: job.Workers[0].NodeID, ObservedUnix: 200, LeaseExpiresUnix: 300}})
	job, _, _ = state.BackupJob("job-1")
	if job.Workers[0].LeaseEpoch != 2 {
		t.Fatalf("takeover did not advance lease epoch: %+v", job)
	}
	if result := applyBackupCommand(t, state, "backup-stale-complete", Command{Type: CompleteBackupJob, BackupJobCompletion: &BackupJobCompletion{JobID: "job-1", RaftID: 11, NodeID: job.Workers[0].NodeID, LeaseEpoch: 1, Bytes: 10, Objects: 1}}); result.Error == "" {
		t.Fatal("stale completion was unexpectedly accepted")
	}
	applyBackupCommand(t, state, "backup-complete", Command{Type: CompleteBackupJob, BackupJobCompletion: &BackupJobCompletion{JobID: "job-1", RaftID: 11, NodeID: job.Workers[0].NodeID, LeaseEpoch: 2, Bytes: 10, Objects: 1}})
	applyBackupCommand(t, state, "backup-finalize", Command{Type: FinalizeBackupJob, BackupJobReference: &BackupJobReference{JobID: "job-1"}})
	job, _, _ = state.BackupJob("job-1")
	if job.State != BackupJobCompleted || job.CompletedIndex == 0 || job.Workers[0].Bytes != 10 {
		t.Fatalf("completed job = %+v", job)
	}
	if job.Epoch != createdEpoch {
		t.Fatalf("backup epoch changed during worker lifecycle: got %d want %d", job.Epoch, createdEpoch)
	}
	image, index, conf, err := state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Open(filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	if restoredJob, found, err := restored.BackupJob("job-1"); err != nil || !found || restoredJob.State != BackupJobCompleted {
		t.Fatalf("restored job = %+v, found=%v, err=%v", restoredJob, found, err)
	}
}

func testBackupMembership(raftID uint64) *raftpb.ConfState {
	return &raftpb.ConfState{Voters: []uint64{raftID}}
}
