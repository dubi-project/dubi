package rootjoin

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

type localDiskFixture struct {
	Store   string
	StoreID string
	ID      identity.Identity
	View    View
	Task    rootstate.RootJoin
}

func localDisk(t *testing.T) localDiskFixture {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	authority := t.TempDir()
	issuer, err := identity.Bootstrap(authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodepki.Ensure(authority, issuer, nodepki.DefaultServerName, now); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(authority, issuer, now)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := identityenrollment.NewGrantDraft(0, now)
	if err != nil {
		t.Fatal(err)
	}
	grant := draft.Request()
	leaf, err := signer.IssueEnrollment(grant.GrantID, grant.PublicKeyDER, time.Unix(grant.ExpiresUnix, 0), now)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := draft.Complete(identityenrollment.GrantBundle{ClusterID: issuer.ClusterID, GrantID: grant.GrantID, IssuerAddress: "issuer:7443",
		ExpiresUnix: grant.ExpiresUnix, CACertificatePEM: string(signer.CACertificatePEM()), CertificatePEM: string(leaf)}, now)
	if err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()
	id, request, err := identityenrollment.Prepare(store, nodepki.DefaultServerName, "r2", "joiner:7443")
	if err != nil {
		t.Fatal(err)
	}
	bound := id
	bound.ClusterID = issuer.ClusterID
	certificate, err := signer.IssueNode(bound, nodepki.DefaultServerName, []byte(request.CSRPEM), now)
	if err != nil {
		t.Fatal(err)
	}
	bundle := identityenrollment.Bundle{Version: 1, ClusterID: bound.ClusterID, NodeID: bound.NodeID, RaftID: bound.RaftID, ServerName: nodepki.DefaultServerName,
		CACertificatePEM: string(signer.CACertificatePEM()), CertificatePEM: string(certificate)}
	receipt, err := identityenrollment.SaveReceipt(store, ticket, request, bundle, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := identityenrollment.ResumeReceipt(store, now); err != nil {
		t.Fatal(err)
	}
	registry, err := volume.Initialize(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	report, err := registry.Status()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	view := View{ClusterID: bound.ClusterID, AppliedIndex: 120, Voters: []uint64{issuer.RaftID}, Learners: []uint64{bound.RaftID}, LeaderID: issuer.RaftID,
		Peers: []rootstate.Node{{NodeID: issuer.NodeID, RaftID: issuer.RaftID, RegionID: "r1", Address: "issuer:7443"},
			{NodeID: bound.NodeID, RaftID: bound.RaftID, RegionID: request.RegionID, Address: request.Address}}}
	return localDiskFixture{Store: store, StoreID: report.StoreID, ID: bound, View: view,
		Task: rootstate.RootJoin{GrantID: receipt.GrantID, RequestSHA256: receipt.RequestSHA256, RegistrationSHA256: strings.Repeat("a", 64),
			NodeID: bound.NodeID, RaftID: bound.RaftID, State: rootstate.RootJoinAdmitted, AdmittedIndex: 100}}
}

func openLocalDisk(t *testing.T, f localDiskFixture) *LocalStore {
	t.Helper()
	lock, err := storelock.Acquire(f.Store)
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenLocalStore(f.Store, f.StoreID)
	if err != nil {
		_ = lock.Release()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		if err := lock.Release(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestRootJoinLocalStorePersistsActivationAndMonotonicRoutes(t *testing.T) {
	f := localDisk(t)
	first, err := SaveLocalAdmission(f.Store, f.View, 100)
	if err != nil || first.Phase != LocalCatchingUp {
		t.Fatal(first, err)
	}
	encoded, err := os.ReadFile(filepath.Join(f.Store, LocalStateFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"PRIVATE KEY", "secret", "csr_pem", "certificate_pem"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatal("join state contains identity material", forbidden)
		}
	}
	if _, err := os.Lstat(filepath.Join(f.Store, "dubi-raft-groups")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("admission created Raft state", err)
	}
	s := openLocalDisk(t, f)
	loaded, err := s.Load()
	if err != nil || loaded.Phase != first.Phase || loaded.AdmittedIndex != first.AdmittedIndex {
		t.Fatal(loaded, err)
	}
	active, err := s.Activate(f.View, f.Task)
	if err != nil || active.Phase != LocalActive || active.ActivatedIndex != 120 {
		t.Fatal(active, err)
	}
	newer := f.View.CloneCanonical()
	newer.AppliedIndex = 150
	for i := range newer.Peers {
		if newer.Peers[i].RaftID == f.ID.RaftID {
			newer.Peers[i].Address = "new-joiner:7443"
		}
	}
	if _, err := s.Observe(newer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Observe(f.View); !errors.Is(err, ErrOlderView) {
		t.Fatal("old response reset routes", err)
	}
	retried, err := s.Start(f.View, 100)
	if err != nil || retried.Phase != LocalActive || retried.View.AppliedIndex != 150 || retried.ActivatedIndex != 120 {
		t.Fatal("retry reset progress", retried, err)
	}
	if _, err := s.Start(f.View, 101); err == nil {
		t.Fatal("different admission replaced existing")
	}
	if _, err := SaveLocalAdmission(f.Store, f.View, 100); err == nil {
		t.Fatal("offline writer bypassed live Store lock")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLocalStore(f.Store, f.StoreID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Load()
	if err != nil || got.Phase != LocalActive || got.View.AppliedIndex != 150 || got.ActivatedIndex != 120 {
		t.Fatal("restart lost phase/cache", got, err)
	}
}

func TestRootJoinLocalStoreRefusesCorruptionAndForeignOwners(t *testing.T) {
	for _, scenario := range []string{"mode", "symlink", "truncated", "duplicate-json", "store-id", "node-id", "receipt-hash", "future-fence"} {
		t.Run(scenario, func(t *testing.T) {
			f := localDisk(t)
			state, err := SaveLocalAdmission(f.Store, f.View, 100)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.Store, LocalStateFile)
			switch scenario {
			case "mode":
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".saved", path); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "duplicate-json":
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				b = bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
				if err := os.WriteFile(path, b, 0o600); err != nil {
					t.Fatal(err)
				}
			default:
				switch scenario {
				case "store-id":
					state.StoreID = "store_00000000000000000000000000000001"
				case "node-id":
					state.NodeID = "node_00000000000000000000000000000001"
				case "receipt-hash":
					state.RequestSHA256 = strings.Repeat("b", 64)
				case "future-fence":
					state.ActivatedIndex = 999
				}
				b, err := json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			s := openLocalDisk(t, f)
			if _, err := s.Load(); err == nil {
				t.Fatal("invalid join record loaded")
			}
			if _, err := s.Start(f.View, 100); err == nil {
				t.Fatal("invalid join record overwritten")
			}
		})
	}
	for _, symlink := range []bool{false, true} {
		root := t.TempDir()
		target := root
		if symlink {
			target = filepath.Join(t.TempDir(), "link")
			if err := os.Symlink(root, target); err != nil {
				t.Fatal(err)
			}
			target += string(os.PathSeparator)
		}
		if _, err := SaveLocalAdmission(target, View{}, 0); err == nil {
			t.Fatal("unmanaged Store accepted")
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatal("invalid Store created files", entries, err)
		}
	}
}

func TestRootJoinLocalStoreUncertainPublicationRequiresReopen(t *testing.T) {
	for _, phase := range []string{"after-write", "after-file-sync", "after-publish", "after-directory-sync"} {
		for _, operation := range []string{"start", "activate"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				f := localDisk(t)
				if operation == "activate" {
					if _, err := SaveLocalAdmission(f.Store, f.View, 100); err != nil {
						t.Fatal(err)
					}
				}
				s := openLocalDisk(t, f)
				injected := errors.New("uncertain local join publication")
				s.hook = func(at string) error {
					if at == phase {
						return injected
					}
					return nil
				}
				var err error
				if operation == "start" {
					_, err = s.Start(f.View, 100)
				} else {
					_, err = s.Activate(f.View, f.Task)
				}
				if !errors.Is(err, injected) {
					t.Fatal("fault not reached", err)
				}
				if _, err := s.Load(); !errors.Is(err, injected) {
					t.Fatal("uncertain instance served state", err)
				}
				if _, err := s.Start(f.View, 100); !errors.Is(err, injected) {
					t.Fatal("uncertain instance continued writing", err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenLocalStore(f.Store, f.StoreID)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				state, err := reopened.Start(f.View, 100)
				if err != nil {
					t.Fatal("could not recover exact admission", err)
				}
				want := LocalCatchingUp
				if operation == "activate" && (phase == "after-publish" || phase == "after-directory-sync") {
					want = LocalActive
				}
				if state.Phase != want {
					t.Fatal("recovery ignored visible durable phase", state.Phase, want)
				}
				if _, err := reopened.Activate(f.View, f.Task); err != nil {
					t.Fatal("activation retry failed", err)
				}
			})
		}
	}
}

func TestRootJoinLocalStoreCrashChild(t *testing.T) {
	store := os.Getenv("DUBI_TEST_LOCAL_JOIN_STORE")
	if store == "" {
		return
	}
	encoded, err := os.ReadFile(filepath.Join(store, "test-join-fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f localDiskFixture
	if err := json.Unmarshal(encoded, &f); err != nil {
		t.Fatal(err)
	}
	s := openLocalDisk(t, f)
	s.hook = func(at string) error {
		if at == os.Getenv("DUBI_TEST_LOCAL_JOIN_PHASE") {
			os.Exit(77)
		}
		return nil
	}
	if os.Getenv("DUBI_TEST_LOCAL_JOIN_OPERATION") == "activate" {
		_, err = s.Activate(f.View, f.Task)
	} else {
		_, err = s.Start(f.View, 100)
	}
	t.Fatal("crash hook not reached", err)
}

func TestRootJoinLocalStoreProcessCrashPublication(t *testing.T) {
	for _, phase := range []string{"after-write", "after-file-sync", "after-publish", "after-directory-sync"} {
		for _, operation := range []string{"start", "activate"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				f := localDisk(t)
				if operation == "activate" {
					if _, err := SaveLocalAdmission(f.Store, f.View, 100); err != nil {
						t.Fatal(err)
					}
				}
				encoded, err := json.Marshal(f)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(f.Store, "test-join-fixture.json"), encoded, 0o600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestRootJoinLocalStoreCrashChild$", "-test.count=1")
				cmd.Env = append(os.Environ(), "DUBI_TEST_LOCAL_JOIN_STORE="+f.Store, "DUBI_TEST_LOCAL_JOIN_PHASE="+phase, "DUBI_TEST_LOCAL_JOIN_OPERATION="+operation)
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 77 {
					t.Fatalf("child did not crash at boundary: %v %s", err, output)
				}
				var orphans []string
				entries, err := os.ReadDir(f.Store)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), ".dubi-root-join-") {
						orphans = append(orphans, entry.Name())
					}
				}
				s := openLocalDisk(t, f)
				state, err := s.Start(f.View, 100)
				if err != nil {
					t.Fatal("post-crash recovery failed", err)
				}
				want := LocalCatchingUp
				if operation == "activate" && (phase == "after-publish" || phase == "after-directory-sync") {
					want = LocalActive
				}
				if state.Phase != want {
					t.Fatal("wrong post-crash phase", state.Phase, want)
				}
				if _, err := s.Activate(f.View, f.Task); err != nil {
					t.Fatal(err)
				}
				for _, name := range orphans {
					if _, err := os.Lstat(filepath.Join(f.Store, name)); err != nil {
						t.Fatal("recovery removed unknown crash artifact", name, err)
					}
				}
			})
		}
	}
}

func TestRootJoinLocalStorePinsDirectoryAndRequiresReceipt(t *testing.T) {
	f := localDisk(t)
	if _, err := SaveLocalAdmission(f.Store, f.View, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLocalStore(f.Store, "store_00000000000000000000000000000001"); err == nil {
		t.Fatal("foreign expected StoreID accepted")
	}
	s := openLocalDisk(t, f)
	receipt := filepath.Join(f.Store, identityenrollment.JoinReceiptName)
	if err := os.Rename(receipt, receipt+".saved"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("missing receipt ignored")
	}
	if _, err := s.Start(f.View, 100); err == nil {
		t.Fatal("missing receipt silently recreated")
	}
	if err := os.Rename(receipt+".saved", receipt); err != nil {
		t.Fatal(err)
	}
	moved := f.Store + "-moved"
	if err := os.Rename(f.Store, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(f.Store, 0o750); err != nil {
		_ = os.Rename(moved, f.Store)
		t.Fatal(err)
	}
	defer func() {
		// This test owns the empty replacement directory; restore the original
		// temp directory so normal testing cleanup can reclaim the fixture.
		if err := os.Remove(f.Store); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(moved, f.Store); err != nil {
			t.Error(err)
		}
	}()
	if _, err := s.Load(); err == nil {
		t.Fatal("replaced Store supplied a recovery authorization")
	}
	if _, err := s.Start(f.View, 100); err == nil {
		t.Fatal("replaced Store was written")
	}
	entries, err := os.ReadDir(f.Store)
	if err != nil || len(entries) != 0 {
		t.Fatal("replacement directory was initialized", entries, err)
	}
}
