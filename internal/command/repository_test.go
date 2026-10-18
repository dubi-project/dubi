package command

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

func TestRepositoryCommandRejectsInvalidConfigurationBeforeConnect(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{"repository", "add", "--store", t.TempDir(), "--server", "127.0.0.1:7443", "--type", "file", "--path", "relative", "local"}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "invalid Repository configuration") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestRepositoryCommandRecognizesDefaultAlias(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{"repository", "default"}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "usage: dubi repository set-default") || strings.Contains(stderr.String(), "unknown repository command") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestBackupCommandUsageAndDispatch(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{"backup", "unknown"}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), `unknown backup command "unknown"`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestRepositoryCLILifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := filepath.Join(t.TempDir(), "node")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, ready := startJoinTestServer(t, process)
	defer stop()
	ready()
	backupPath := filepath.Join(t.TempDir(), "backup")
	var output, diagnostics bytes.Buffer
	add := []string{"repository", "add", "--store", store, "--server", process.LocalAddress(), "--request-id", "repository-add", "--type", "file", "--path", backupPath, "local"}
	if code := Run(ctx, add, &output, &diagnostics); code != 0 {
		t.Fatalf("add Repository: code=%d stderr=%s", code, diagnostics.String())
	}
	var mutation datawire.RepositoryMutationResponse
	if err := json.Unmarshal(output.Bytes(), &mutation); err != nil || mutation.GetRepository().GetName() != "local" {
		t.Fatalf("add response = %q, %v", output.String(), err)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, []string{"repository", "default", "--store", store, "--server", process.LocalAddress(), "--request-id", "repository-default", "local"}, &output, &diagnostics); code != 0 {
		t.Fatalf("set default Repository: code=%d stderr=%s", code, diagnostics.String())
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, []string{"backup", "run", "--store", store, "--server", process.LocalAddress(), "--request-id", "backup-run"}, &output, &diagnostics); code != 0 {
		t.Fatalf("create Backup job: code=%d stderr=%s", code, diagnostics.String())
	}
	var backup datawire.BackupJobResponse
	if err := json.Unmarshal(output.Bytes(), &backup); err != nil || backup.GetJob().GetState() != "pending" || backup.GetJob().GetRepository() != "local" {
		t.Fatalf("Backup run response = %q, %v", output.String(), err)
	}
	output.Reset()
	if code := Run(ctx, []string{"repository", "list", "--store", store, "--server", process.LocalAddress()}, &output, &diagnostics); code != 0 {
		t.Fatalf("list Repositories: code=%d stderr=%s", code, diagnostics.String())
	}
	var listed datawire.ListRepositoriesResponse
	if err := json.Unmarshal(output.Bytes(), &listed); err != nil || len(listed.GetRepositories()) != 1 || listed.GetDefaultName() != "local" {
		t.Fatalf("list response = %q, %v", output.String(), err)
	}
	output.Reset()
	if code := Run(ctx, []string{"backup", "status", "--store", store, "--server", process.LocalAddress(), backup.GetJob().GetJobId()}, &output, &diagnostics); code != 0 {
		t.Fatalf("Backup status: code=%d stderr=%s", code, diagnostics.String())
	}
	var backupStatus datawire.BackupJobResponse
	if err := json.Unmarshal(output.Bytes(), &backupStatus); err != nil || backupStatus.GetJob().GetJobId() != backup.GetJob().GetJobId() {
		t.Fatalf("Backup status response = %q, %v", output.String(), err)
	}
	updatedPath := filepath.Join(t.TempDir(), "backup-v2")
	output.Reset()
	if code := Run(ctx, []string{"repository", "update", "--store", store, "--server", process.LocalAddress(), "--request-id", "repository-update", "--path", updatedPath, "local"}, &output, &diagnostics); code != 0 {
		t.Fatalf("update Repository: code=%d stderr=%s", code, diagnostics.String())
	}
	output.Reset()
	if code := Run(ctx, []string{"repository", "remove", "--store", store, "--server", process.LocalAddress(), "--request-id", "repository-remove", "local"}, &output, &diagnostics); code != 0 {
		t.Fatalf("remove Repository: code=%d stderr=%s", code, diagnostics.String())
	}
	if _, found, err := process.Runtime.Root.Repository("local"); err != nil || found {
		t.Fatalf("Repository remained after removal: found=%v err=%v", found, err)
	}
}
