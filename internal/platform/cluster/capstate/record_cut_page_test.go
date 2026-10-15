package capstate

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

func TestVerifiedRecordCutPagesCoverExactManifest(t *testing.T) {
	state, seal, _ := sealedReferenceFixture(t, false)
	artifacts, _ := fileArtifacts(t)
	cut := buildRecordCut(t, state, seal, artifacts)
	var expected []RecordReference
	if err := WalkVerifiedRecordCut(context.Background(), artifacts, cut,
		func(ref RecordReference) error { expected = append(expected, ref); return nil }); err != nil {
		t.Fatal(err)
	}
	var cursor *RecordCutCursor
	var received []RecordReference
	var totalBytes uint64
	for round := 0; round <= len(expected); round++ {
		page, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, cursor, 1,
			func(ref RecordReference) error { received = append(received, ref); return nil })
		if err != nil || page.Count > 1 {
			t.Fatal("verified page failed", round, page, err)
		}
		totalBytes += page.PayloadBytes
		if page.Done {
			if uint64(len(received)) != cut.Count || totalBytes != cut.PayloadBytes || !reflect.DeepEqual(received, expected) {
				t.Fatal("pages omitted or duplicated sealed references", received, expected, totalBytes, cut)
			}
			return
		}
		if page.Cursor == nil || page.Count == 0 {
			t.Fatal("nonfinal page has no advancement", page)
		}
		encoded, err := json.Marshal(page.Cursor)
		if err != nil || json.Unmarshal(encoded, &cursor) != nil {
			t.Fatal("page cursor cannot be serialized", err)
		}
	}
	t.Fatal("page traversal did not finish")
}

func TestVerifiedRecordCutPageRejectsForeignAndForgedCursor(t *testing.T) {
	state, seal, _ := sealedReferenceFixture(t, false)
	artifacts, _ := fileArtifacts(t)
	cut := buildRecordCut(t, state, seal, artifacts)
	first, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, nil, 1, func(RecordReference) error { return nil })
	if err != nil || first.Cursor == nil || first.Done {
		t.Fatal(first, err)
	}
	for _, mutate := range []func(*RecordCutCursor){
		func(c *RecordCutCursor) { c.ArtifactSHA256 = "different" },
		func(c *RecordCutCursor) { c.SetSHA256 = "different" },
		func(c *RecordCutCursor) { c.SealRequestID = "other-seal" },
		func(c *RecordCutCursor) { c.LastKey = append(c.LastKey, 0) },
	} {
		bad := *first.Cursor
		bad.LastKey = append([]byte(nil), bad.LastKey...)
		mutate(&bad)
		called := 0
		if page, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, &bad, 1,
			func(RecordReference) error { called++; return nil }); err == nil || page.Cursor != nil || called != 0 {
			t.Fatal("foreign or forged cursor advanced verified page", page, called, err)
		}
	}
	if _, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, nil, 0, func(RecordReference) error { return nil }); err == nil {
		t.Fatal("unbounded page accepted")
	}
	// The same logical set at a later Owner applied position creates another
	// artifact; a position from the first artifact cannot silently skip it.
	handoffApply(t, state, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &seal.Intent}, true)
	later := buildRecordCut(t, state, seal, artifacts)
	if later.SHA256 != cut.SHA256 || later.Artifact.SHA256 == cut.Artifact.SHA256 {
		t.Fatal("fixture did not create a new artifact of the same sealed set")
	}
	if _, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, later, first.Cursor, 1,
		func(RecordReference) error { t.Fatal("cross-artifact cursor visited a reference"); return nil }); err == nil {
		t.Fatal("old artifact cursor was accepted by a new artifact")
	}
}

func TestVerifiedRecordCutPageFailureReturnsNoProgress(t *testing.T) {
	state, seal, _ := sealedReferenceFixture(t, false)
	artifacts, _ := fileArtifacts(t)
	cut := buildRecordCut(t, state, seal, artifacts)
	stop := errors.New("physical observation failed")
	page, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, nil, 2,
		func(RecordReference) error { return stop })
	if !errors.Is(err, stop) || page.Cursor != nil || page.Count != 0 {
		t.Fatal("failed page returned reusable progress", page, err)
	}
	page, err = WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, nil, 2,
		func(RecordReference) error { return nil })
	if err != nil || page.Count != 2 || page.Cursor == nil {
		t.Fatal("failed page could not restart from the beginning", page, err)
	}
}

func TestVerifiedRecordCutPageHandlesEmptyManifest(t *testing.T) {
	state, seal, _ := sealedReferenceFixture(t, true)
	artifacts, _ := fileArtifacts(t)
	cut := buildRecordCut(t, state, seal, artifacts)
	page, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, nil, 16,
		func(RecordReference) error { t.Fatal("empty manifest invoked visitor"); return nil })
	if err != nil || !page.Done || page.Count != 0 || page.PayloadBytes != 0 || page.Cursor != nil {
		t.Fatal("empty manifest produced a nonempty page", page, err)
	}
}

func TestVerifiedRecordCutCursorAfterArtifactReopenIsOnlyTraversal(t *testing.T) {
	state, seal, _ := sealedReferenceFixture(t, false)
	artifacts, path := fileArtifacts(t)
	cut := buildRecordCut(t, state, seal, artifacts)
	first, err := WalkVerifiedRecordCutPage(context.Background(), artifacts, cut, nil, 1,
		func(RecordReference) error { return nil })
	if err != nil || first.Cursor == nil || first.Done {
		t.Fatal(first, err)
	}
	if err := artifacts.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := snapshot.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	second, err := WalkVerifiedRecordCutPage(context.Background(), reopened, cut, first.Cursor, 1,
		func(RecordReference) error { return nil })
	if err != nil || second.Count != 1 || second.Cursor == nil ||
		reflect.DeepEqual(second.Cursor.LastKey, first.Cursor.LastKey) {
		t.Fatal("reopened artifact did not validate the same traversal position", second, err)
	}
}
