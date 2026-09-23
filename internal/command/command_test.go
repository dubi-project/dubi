package command

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

func TestInitAndVersionCommands(t *testing.T) {
	storeRoot := filepath.Join(t.TempDir(), "store")
	var stdout, stderr bytes.Buffer
	if code := Run(context.Background(), []string{"init", "--store", storeRoot}, &stdout, &stderr); code != 0 {
		t.Fatalf("init exit = %d, stderr %q", code, stderr.String())
	}
	var initialized identity.Identity
	if err := json.Unmarshal(stdout.Bytes(), &initialized); err != nil || initialized.ClusterID == "" || initialized.RaftID == 0 {
		t.Fatalf("init output = %q, identity %+v, err %v", stdout.String(), initialized, err)
	}
	loaded, err := identity.Load(storeRoot)
	if err != nil || loaded != initialized {
		t.Fatalf("loaded identity = %+v, err %v", loaded, err)
	}
	material, err := nodepki.Load(storeRoot, loaded, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		t.Fatalf("default managed PKI = %+v, err %v", material.Config, err)
	}
	if err := material.Certificate.VerifyHostname(nodepki.DefaultServerName); err != nil {
		t.Fatalf("default managed PKI hostname: %v", err)
	}
	for _, name := range []string{"node-ca.crt", "node.crt", "node.key", "signer-wrap.key", "signer-envelope.json"} {
		if _, err := os.Stat(filepath.Join(storeRoot, "dubi-node-pki", name)); err != nil {
			t.Fatalf("PKI file %s: %v", name, err)
		}
	}
	stdout.Reset()
	stderr.Reset()
	previous := Version
	Version = "test-version"
	t.Cleanup(func() { Version = previous })
	if code := Run(context.Background(), []string{"version"}, &stdout, &stderr); code != 0 ||
		strings.TrimSpace(stdout.String()) != "dubi test-version" {
		t.Fatalf("version exit/output = %d, %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestCommandRejectsUnknownOrIncompleteInput(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"init"},
		{"init", "--store", t.TempDir(), "--server-name", "example.internal"}, {"join"},
		{"join", "prepare"}, {"join", "install"}, {"join", "unknown"}, {"server"}} {
		var stdout, stderr bytes.Buffer
		if code := Run(context.Background(), args, &stdout, &stderr); code == 0 {
			t.Fatalf("args %q unexpectedly succeeded", args)
		}
	}
}

func TestJoinPrepareCommandIsIdempotent(t *testing.T) {
	storeRoot := filepath.Join(t.TempDir(), "joiner")
	args := []string{"join", "prepare", "--store", storeRoot,
		"--region", "region-b", "--advertise", "node-b.example.internal:7443"}
	var first, second, stderr bytes.Buffer
	if code := Run(context.Background(), args, &first, &stderr); code != 0 {
		t.Fatalf("first join prepare exit = %d, stderr %q", code, stderr.String())
	}
	stderr.Reset()
	if code := Run(context.Background(), args, &second, &stderr); code != 0 {
		t.Fatalf("second join prepare exit = %d, stderr %q", code, stderr.String())
	}
	if first.String() != second.String() || !strings.Contains(first.String(), "CERTIFICATE REQUEST") {
		t.Fatalf("join requests differ or lack CSR:\nfirst %q\nsecond %q", first.String(), second.String())
	}
	request, err := identityenrollment.DecodeRequest(first.Bytes())
	if err != nil || request.ServerName != nodepki.DefaultServerName {
		t.Fatalf("join request server name = %q, err %v", request.ServerName, err)
	}
	value, err := identity.Load(storeRoot)
	if err != nil || value.ClusterID != "" {
		t.Fatalf("prepared identity = %+v, err %v", value, err)
	}
}
