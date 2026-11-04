package records

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/storage/diskbudget"
)

func TestMetadataReservationStopsBeforeAnOtherwiseWritablePackAppend(t *testing.T) {
	ctx := context.Background()
	store, packs := testStore(t)
	existing := descriptor(t, "existing", "payload")
	put(t, store, existing, "payload")
	dir, err := os.Open(store.root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	sample, err := diskbudget.ProbeFD(dir.Fd())
	if err != nil {
		t.Fatal(err)
	}
	if sample.Available <= diskbudget.Floor(sample.Total)+(1<<20) {
		t.Fatal("insufficient test filesystem headroom")
	}
	// Consume admission only, never fill the VPS host disk. The memory Pack
	// backend remains writable, isolating the metadata filesystem boundary.
	lease, err := diskbudget.Shared.Reserve(sample, sample.Available-diskbudget.Floor(sample.Total), diskbudget.Discretionary)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	before := packs.appends
	d := descriptor(t, "new", "payload")
	if _, err := store.Put(ctx, d, []byte("payload")); !errors.Is(err, diskbudget.ErrSpace) {
		t.Fatal("full metadata budget did not reject new data", err)
	}
	if packs.appends != before {
		t.Fatal("data was appended before checking metadata capacity")
	}
	if _, err := store.Put(ctx, existing, []byte("payload")); err != nil {
		t.Fatal("idempotent held data was blocked", err)
	}
	if _, _, err := store.Read(ctx, existing); err != nil {
		t.Fatal("pressure prevented a verified read", err)
	}
	lease.Release()
	if _, err := store.Put(ctx, d, []byte("payload")); err != nil {
		t.Fatal("released metadata capacity did not permit writes", err)
	}
}
