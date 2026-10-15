package enrollment

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareIsIdempotentAndRejectsMetadataOrKeyChanges(t *testing.T) {
	store := filepath.Join(t.TempDir(), "joiner")
	firstIdentity, first, err := Prepare(store, "node-b.example.internal", "region-b", "node-b.example.internal:7443")
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity, second, err := Prepare(store, "node-b.example.internal", "region-b", "node-b.example.internal:7443")
	if err != nil {
		t.Fatal(err)
	}
	if firstIdentity != secondIdentity || first != second {
		t.Fatal("idempotent preparation replaced identity or CSR")
	}
	if _, _, err := Prepare(store, "node-b.example.internal", "region-c", "node-b.example.internal:7443"); err == nil {
		t.Fatal("different enrollment metadata was accepted")
	}
	if err := os.Chmod(StorePaths(store).PrivateKey, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Prepare(store, "node-b.example.internal", "region-b", "node-b.example.internal:7443"); err == nil {
		t.Fatal("exposed enrollment private key was accepted")
	}
}

func TestLoadPreparedIsReadOnlyAndChecksLocalKeyBinding(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	if _, _, err := LoadPrepared(missing); err == nil {
		t.Fatal("missing request accepted")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("read created store", err)
	}
	store, ticket, request, bundle, now := receiptFixture(t)
	id, loaded, err := LoadPrepared(store)
	if err != nil || loaded != request || id.ClusterID != "" {
		t.Fatal("prepared intent changed", err)
	}
	key, err := os.ReadFile(StorePaths(store).PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	other, _, _, _, _ := receiptFixture(t)
	foreign, err := os.ReadFile(StorePaths(other).PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(StorePaths(store).PrivateKey, foreign, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadPrepared(store); err == nil {
		t.Fatal("foreign CSR key accepted")
	}
	if err := os.WriteFile(StorePaths(store).PrivateKey, key, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveReceipt(store, ticket, request, bundle, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResumeReceipt(store, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadPrepared(store); err == nil {
		t.Fatal("bound node treated as fresh intent")
	}
}

func TestEnrollmentDocumentsAreStrictAndCSRIsAuthenticated(t *testing.T) {
	store := filepath.Join(t.TempDir(), "joiner")
	_, request, err := Prepare(store, "127.0.0.1", "region-b", "127.0.0.1:7443")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	fields["unknown"] = true
	withUnknown, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRequest(withUnknown); err == nil {
		t.Fatal("unknown enrollment request field was accepted")
	}
	tampered := request
	tampered.CSRPEM = string(bytes.Replace([]byte(tampered.CSRPEM), []byte("CERTIFICATE REQUEST"), []byte("BROKEN REQUEST"), 1))
	if _, err := EncodeRequest(tampered); err == nil {
		t.Fatal("tampered enrollment CSR was accepted")
	}
}
