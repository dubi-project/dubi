package record

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestReferencePutPayloadRoundTrip(t *testing.T) {
	space := rootstate.Keyspace{TenantID: "tenant", CapabilityID: CapabilityID, DatasetID: "dataset"}
	descriptor, err := records.Describe(space, "group-00000000000000000000000000000001", []byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := ReferencePutPayload(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) < 2 || payload[0] != refPutVersion {
		t.Fatalf("unexpected immutable payload header: %x", payload)
	}
	var got records.Descriptor
	if err := json.Unmarshal(payload[1:], &got); err != nil {
		t.Fatal(err)
	}
	gotID, err := got.ID()
	if err != nil {
		t.Fatal(err)
	}
	wantID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	if gotID != wantID || !bytes.Equal(got.Key, descriptor.Key) {
		t.Fatalf("descriptor changed during payload encoding: got=%+v want=%+v", got, descriptor)
	}
}

func TestReferencePutPayloadRejectsInvalidDescriptor(t *testing.T) {
	if _, err := ReferencePutPayload(records.Descriptor{}); err == nil {
		t.Fatal("invalid descriptor was accepted")
	}
}
