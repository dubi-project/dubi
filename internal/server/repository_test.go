package server

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

func TestRepositoryWireRedactsEncryptedEnvelope(t *testing.T) {
	value := repository.Repository{Name: "primary", Type: repository.TypeS3, Bucket: "backup",
		EncryptedCredentials: []byte("ciphertext"), CredentialsNonce: []byte("123456789012"), CredentialsAlgorithm: "aes-256-gcm", CredentialsKeyVersion: 2}
	wire := repositoryToWire(value)
	if !wire.GetCredentialsConfigured() || len(wire.GetEncryptedCredentials()) != 0 || len(wire.GetCredentialsNonce()) != 0 {
		t.Fatalf("encrypted Repository envelope was returned: %+v", wire)
	}
	if wire.GetName() != value.Name || wire.GetType() != string(value.Type) {
		t.Fatalf("Repository metadata changed: %+v", wire)
	}
}

func TestBackupJobWirePreservesOnlyControlState(t *testing.T) {
	value := rootstate.BackupJob{JobID: "job-1", Repository: "local", ClusterID: "cluster_00000000000000000000000000000001", Epoch: 2, State: rootstate.BackupJobRunning, CreatedIndex: 4,
		Workers: []rootstate.BackupWorker{{RaftID: 7, NodeID: "node_00000000000000000000000000000007", State: rootstate.BackupWorkerRunning, LeaseEpoch: 1, LeaseExpiresUnix: 100, LeaseRaftID: 7, LeaseNodeID: "node_00000000000000000000000000000007"}}}
	wire := backupJobToWire(value)
	if wire.GetJobId() != value.JobID || wire.GetState() != rootstate.BackupJobRunning || len(wire.GetWorkers()) != 1 || wire.GetWorkers()[0].GetLeaseEpoch() != 1 {
		t.Fatalf("Backup job wire conversion changed control state: %+v", wire)
	}
}

func TestRepositoryFromWireRejectsClientGeneratedStatus(t *testing.T) {
	_, err := repositoryFromWire(&datawire.RepositoryConfig{Name: "local", Type: "file", Path: "/backup", CredentialsConfigured: true})
	if err == nil {
		t.Fatal("client-controlled credentials_configured was accepted")
	}
}

func TestRepositoryFromWireValidatesStructuredBackends(t *testing.T) {
	if _, err := repositoryFromWire(&datawire.RepositoryConfig{Name: "local", Type: "file", Path: "/backup"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryFromWire(&datawire.RepositoryConfig{Name: "primary", Type: "s3", Bucket: "backup", Endpoint: "file:///tmp"}); err == nil {
		t.Fatal("invalid S3 endpoint was accepted")
	}
}
