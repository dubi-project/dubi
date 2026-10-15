package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
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
	t.Setenv("DUBI_STORE", filepath.Join(t.TempDir(), "uninitialized"))
	for _, args := range [][]string{nil, {"unknown"}, {"init", "--store", ""},
		{"init", "--store", t.TempDir(), "--server-name", "example.internal"}, {"join"},
		{"join", "prepare"}, {"join", "install"}, {"join", "unknown"}, {"server"}} {
		var stdout, stderr bytes.Buffer
		if code := Run(context.Background(), args, &stdout, &stderr); code == 0 {
			t.Fatalf("args %q unexpectedly succeeded", args)
		}
	}
}

func TestManagedServerInitRestartAndRootPersistence(t *testing.T) {
	store := filepath.Join(t.TempDir(), "store")
	t.Setenv("DUBI_STORE", store)
	var output, stderr bytes.Buffer
	if code := Run(context.Background(), []string{"init"}, &output, &stderr); code != 0 {
		t.Fatalf("init: %s", stderr.String())
	}
	original, err := identity.Load(store)
	if err != nil {
		t.Fatal(err)
	}
	var applied uint64
	listenAddress := "127.0.0.1:0"
	learner, err := identity.PrepareJoin(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		// No YAML, IDs, certificates or peer list, including on restart.
		process, err := openServer([]string{"--listen", listenAddress}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result := make(chan error, 1)
		go func() { result <- process.Run(ctx) }()
		select {
		case <-process.Ready():
		case err := <-result:
			cancel()
			t.Fatalf("server stopped before readiness: %v", err)
		case <-ctx.Done():
			cancel()
			_ = process.Close()
			t.Fatal("server did not become ready")
		}
		registered, found, err := process.Runtime.Root.Node(original.RaftID)
		if err != nil || !found || registered.NodeID != original.NodeID || registered.RegionID != "default" {
			cancel()
			_ = process.Close()
			t.Fatal("managed startup did not register its own identity/network", registered, found, err)
		}
		listenAddress = registered.Address
		if attempt == 0 {
			// Expand membership before restart; init must not replace it with
			// the original single-node bootstrap configuration.
			if err := process.Runtime.Host.AddLearner(raft.RootGroupID, learner.RaftID); err != nil {
				cancel()
				_ = process.Close()
				t.Fatal(err)
			}
		}
		index, readErr := process.Runtime.Host.ReadIndex(ctx, raft.RootGroupID)
		conf, confErr := process.Runtime.Host.Membership(raft.RootGroupID)
		if readErr != nil || confErr != nil || index <= applied ||
			len(conf.GetVoters()) != 1 || conf.GetVoters()[0] != original.RaftID ||
			len(conf.GetLearners()) != 1 || conf.GetLearners()[0] != learner.RaftID ||
			process.Runtime.Identity != original {
			cancel()
			_ = process.Close()
			t.Fatalf("Root did not persist: index=%d previous=%d conf=%v errors=%v/%v", index, applied, conf, readErr, confErr)
		}
		applied = index
		cancel()
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		// Repeated init must preserve IDs, certificate and consensus history.
		var repeated bytes.Buffer
		if code := Run(context.Background(), []string{"init"}, &repeated, &stderr); code != 0 {
			t.Fatalf("repeated init: %s", stderr.String())
		}
		if repeated.String() != output.String() {
			t.Fatal("init changed identity or PKI")
		}
	}
}

func TestServerCannotBootstrapUninitializedOrJoiningStore(t *testing.T) {
	for _, state := range []string{"empty", "prepared", "bound"} {
		t.Run(state, func(t *testing.T) {
			store := t.TempDir()
			if state != "empty" {
				if _, err := identity.PrepareJoin(store); err != nil {
					t.Fatal(err)
				}
			}
			if state == "bound" {
				cluster, err := identity.Bootstrap(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := identity.BindCluster(store, cluster.ClusterID); err != nil {
					t.Fatal(err)
				}
			}
			if process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard); err == nil {
				_ = process.Close()
				t.Fatal("server bootstrapped an uninitialized/joining store")
			}
			if _, err := os.Stat(filepath.Join(store, "dubi-raft-groups")); !os.IsNotExist(err) {
				t.Fatalf("rejected startup created consensus state: %v", err)
			}
			if state != "empty" {
				if code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, io.Discard); code == 0 {
					t.Fatal("init accepted a prepared or bound joining identity")
				}
				if _, err := os.Stat(nodepki.StorePaths(store).Directory); !os.IsNotExist(err) {
					t.Fatal("init generated PKI for a joining node")
				}
			}
		})
	}
}

func TestCLIRejectsInternalIdentityAndConfigurationOverrides(t *testing.T) {
	t.Setenv("DUBI_STORE", filepath.Join(t.TempDir(), "store"))
	for _, args := range [][]string{
		{"server", "--config", "dubi.yaml"}, {"server", "--raft-id", "1"},
		{"server", "--node-id", "node-a"}, {"server", "--server-name", "other"},
		{"server", "--peer", "1=localhost:7443"}, {"init", "--cluster-id", "custom"},
	} {
		var stderr bytes.Buffer
		if code := Run(context.Background(), args, io.Discard, &stderr); code == 0 ||
			!strings.Contains(stderr.String(), "flag provided but not defined") {
			t.Fatalf("args %v: exit=%d error=%s", args, code, stderr.String())
		}
	}
}

func TestInstalledJoinCertificateDoesNotBootstrapRoot(t *testing.T) {
	bootstrapStore, joinStore := t.TempDir(), t.TempDir()
	bootstrap, err := identity.Bootstrap(bootstrapStore)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := nodepki.Ensure(bootstrapStore, bootstrap, nodepki.DefaultServerName, now); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(bootstrapStore, bootstrap, now)
	if err != nil {
		t.Fatal(err)
	}
	joining, request, err := identityenrollment.Prepare(joinStore, nodepki.DefaultServerName, "default", "node-b:7443")
	if err != nil {
		t.Fatal(err)
	}
	joining.ClusterID = bootstrap.ClusterID
	certificate, err := signer.IssueNode(joining, nodepki.DefaultServerName, []byte(request.CSRPEM), now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = identityenrollment.Install(joinStore, identityenrollment.Bundle{
		Version: 1, ClusterID: joining.ClusterID, NodeID: joining.NodeID, RaftID: joining.RaftID,
		ServerName: nodepki.DefaultServerName, CACertificatePEM: string(signer.CACertificatePEM()),
		CertificatePEM: string(certificate),
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if process, err := openServer([]string{"--store", joinStore, "--listen", "127.0.0.1:0"}, io.Discard); err == nil {
		_ = process.Close()
		t.Fatal("installed leaf certificate bootstrapped a separate Root")
	}
	if code := Run(context.Background(), []string{"init", "--store", joinStore}, io.Discard, io.Discard); code == 0 {
		t.Fatal("init accepted an enrolled node")
	}
	root, err := rootstate.Open(filepath.Join(joinStore, "dubi-cluster-state"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if root.AppliedIndex() != 0 || len(root.ConfState().GetVoters()) != 0 {
		t.Fatal("joining node acquired its own Root membership")
	}
	if _, err := os.Stat(nodepki.StorePaths(joinStore).Envelope); !os.IsNotExist(err) {
		t.Fatal("joining node acquired a CA signer")
	}
}

func TestLegacyBootstrapResumesWithoutUserProvidedIDs(t *testing.T) {
	for _, hasPKI := range []bool{false, true} {
		t.Run(fmt.Sprint(hasPKI), func(t *testing.T) {
			store := t.TempDir()
			value, err := identity.Bootstrap(store)
			if err != nil {
				t.Fatal(err)
			}
			value.Origin = ""
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(store, "DUBI_NODE_IDENTITY"), encoded, 0o640); err != nil {
				t.Fatal(err)
			}
			if hasPKI {
				if _, err := nodepki.Ensure(store, value, nodepki.DefaultServerName, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, io.Discard)
			if (code == 0) != hasPKI {
				t.Fatalf("legacy init exit=%d hasPKI=%v", code, hasPKI)
			}
			if hasPKI {
				process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				defer process.Close()
				if process.Runtime.Identity != value {
					t.Fatal("legacy identity changed")
				}
			}
		})
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
