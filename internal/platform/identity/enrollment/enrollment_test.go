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
