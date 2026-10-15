package command

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

func TestLocalContextMakesRoutineCommandsArgumentFree(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DUBI_STORE", "")
	store := filepath.Join(t.TempDir(), "node")
	var output, diagnostics bytes.Buffer
	if code := Run(context.Background(), []string{"init", "--store", store}, &output, &diagnostics); code != 0 {
		t.Fatal("init failed", diagnostics.String())
	}
	path, err := localContextPath()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("local pointer is not private", info, err)
	}
	current, found, err := readLocalContext()
	if err != nil || !found || current.Store != store || current.Server != "" || current.Listen != "" || current.Advertise != "" {
		t.Fatal("init did not select its Store", current, found, err)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(context.Background(), []string{"volume", "list"}, &output, &diagnostics); code != 0 {
		t.Fatal("zero-argument volume list failed", diagnostics.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, []string{"server", "--listen", "127.0.0.1:0"}, io.Discard, io.Discard) }()
	stopped := false
	t.Cleanup(func() {
		cancel()
		if !stopped {
			<-done
		}
	})
	deadline := time.After(15 * time.Second)
	for {
		current, found, err = readLocalContext()
		if err == nil && found && current.Listen != "" && current.Server != "" && current.Advertise != "" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("server did not save its actual local endpoint", current, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if current.Listen == "127.0.0.1:0" || current.Server == defaultLocalServer {
		t.Fatal("port-zero address was not resolved", current)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(context.Background(), []string{"status"}, &output, &diagnostics); code != 0 {
		t.Fatal("zero-argument status failed", diagnostics.String())
	}
	var status struct {
		ObservedNodeID string `json:"observed_node_id"`
	}
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || status.ObservedNodeID != current.NodeID {
		t.Fatal("status reached a different Node", status, err)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(context.Background(), []string{"join", "token"}, &output, &diagnostics); code != 0 || output.Len() == 0 {
		t.Fatal("zero-argument join token failed", diagnostics.String())
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatal("server exited with error", code)
	}
	stopped = true

	ctx, cancelAgain := context.WithCancel(context.Background())
	again := make(chan int, 1)
	go func() { again <- Run(ctx, []string{"server"}, io.Discard, io.Discard) }()
	defer func() { cancelAgain(); <-again }()
	restartDeadline := time.After(15 * time.Second)
	for {
		output.Reset()
		diagnostics.Reset()
		if Run(context.Background(), []string{"status"}, &output, &diagnostics) == 0 {
			break
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-restartDeadline:
			t.Fatal("no-flag restart did not reuse listener", diagnostics.String())
		}
	}
}

func TestLocalContextRejectsStalePointerAndExplicitStoreRepairsIt(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DUBI_STORE", "")
	store := filepath.Join(t.TempDir(), "node")
	if code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, io.Discard); code != 0 {
		t.Fatal("init failed")
	}
	path, err := localContextPath()
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := readLocalContext()
	if err != nil || !found {
		t.Fatal(current, found, err)
	}
	current.NodeID = "wrong-node"
	encoded, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	if code := Run(context.Background(), []string{"volume", "list"}, &output, &diagnostics); code == 0 || output.Len() != 0 {
		t.Fatal("stale pointer was silently accepted")
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(context.Background(), []string{"volume", "list", "--store", store}, &output, &diagnostics); code != 0 {
		t.Fatal("explicit Store could not bypass stale pointer", diagnostics.String())
	}
	if code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, &diagnostics); code != 0 {
		t.Fatal("explicit init could not repair local pointer", diagnostics.String())
	}
	repaired, found, err := readLocalContext()
	value, loadErr := identity.Load(store)
	if err != nil || loadErr != nil || !found || repaired.NodeID != value.NodeID {
		t.Fatal("pointer was not repaired", repaired, err, loadErr)
	}
}

func TestJoinRemembersStoreAndPortForArgumentFreeResume(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DUBI_STORE", "")
	issuerStore, issuerAddress, stopIssuer := joinTestIssuer(t)
	defer stopIssuer()
	_, ticket := joinTestTicket(t, issuerStore, issuerAddress)
	store := filepath.Join(t.TempDir(), "joiner")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	joined, err := openJoiningServer(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	listen := joined.LocalAddress()
	if err := joined.Close(); err != nil {
		t.Fatal(err)
	}
	current, found, err := readLocalContext()
	if err != nil || !found || current.Store != store || current.Listen != listen || current.ClusterID == "" {
		t.Fatal("joined Node selection was not persisted", current, found, err)
	}
	reopened, err := openJoiningServer(ctx, nil, nil, io.Discard)
	if err != nil {
		t.Fatal("argument-free join resume failed", err)
	}
	if reopened.LocalAddress() != listen {
		t.Fatal("join resume changed the bound port")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalStoreSelectionPrecedence(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DUBI_STORE", "")
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	storeFlag := flags.String("store", "", "")
	if err := flags.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if got, err := resolveStore(flags, *storeFlag); err != nil || got != "/var/lib/dubi" {
		t.Fatal(got, err)
	}
	store := filepath.Join(t.TempDir(), "node")
	if code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, io.Discard); code != 0 {
		t.Fatal("init failed")
	}
	if got, err := resolveStore(flags, *storeFlag); err != nil || got != store {
		t.Fatal(got, err)
	}
	t.Setenv("DUBI_STORE", "/env/dubi")
	if got, err := resolveStore(flags, *storeFlag); err != nil || got != "/env/dubi" {
		t.Fatal(got, err)
	}
	explicit := flag.NewFlagSet("explicit", flag.ContinueOnError)
	selection := explicit.String("store", "", "")
	if err := explicit.Parse([]string{"--store", "/flag/dubi"}); err != nil {
		t.Fatal(err)
	}
	if got, err := resolveStore(explicit, *selection); err != nil || got != "/flag/dubi" {
		t.Fatal(got, err)
	}
}
