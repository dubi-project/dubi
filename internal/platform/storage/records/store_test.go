package records

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
)

const testStoreID = "store_0123456789abcdef0123456789abcdef"

type memoryPacks struct {
	id                 string
	items              map[pack.Location]pack.Record
	appends, reads     int
	awayAppends        int
	appendErr, readErr error
	readErrors         map[pack.Location]error
	afterAppend        func()
	afterRead          func()
}

func newMemoryPacks() *memoryPacks {
	return &memoryPacks{id: testStoreID, items: make(map[pack.Location]pack.Record)}
}
func (p *memoryPacks) StoreID() string { return p.id }
func (p *memoryPacks) Append(_ context.Context, r pack.Record) (pack.Location, error) {
	p.appends++
	if p.appendErr != nil {
		return pack.Location{}, p.appendErr
	}
	l := pack.Location{Scope: pack.Scope{StoreID: p.id, VolumeID: "volume_0123456789abcdef0123456789abcdef", PackID: "pack_0123456789abcdef0123456789abcdef"}, Offset: int64(p.appends) * 1024, Length: 1024}
	r.Key, r.Payload = bytes.Clone(r.Key), bytes.Clone(r.Payload)
	p.items[l] = r
	if p.afterAppend != nil {
		p.afterAppend()
	}
	return l, nil
}
func (p *memoryPacks) Read(l pack.Location) (pack.Record, error) {
	p.reads++
	if err := p.readErrors[l]; err != nil {
		return pack.Record{}, err
	}
	if p.readErr != nil {
		return pack.Record{}, p.readErr
	}
	r, ok := p.items[l]
	if !ok {
		return pack.Record{}, errors.New("physical record missing")
	}
	r.Key, r.Payload = bytes.Clone(r.Key), bytes.Clone(r.Payload)
	if p.afterRead != nil {
		p.afterRead()
	}
	return r, nil
}

func (p *memoryPacks) AppendAwayFromVolume(_ context.Context, source string, r pack.Record) (pack.Location, error) {
	p.awayAppends++
	l := pack.Location{Scope: pack.Scope{StoreID: p.id, VolumeID: "volume_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", PackID: "pack_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, Offset: int64(p.awayAppends) * 2048, Length: 1024}
	if l.VolumeID == source {
		return pack.Location{}, errors.New("test target equals source")
	}
	r.Key, r.Payload = bytes.Clone(r.Key), bytes.Clone(r.Payload)
	p.items[l] = r
	return l, nil
}

func (p *memoryPacks) Compact(ctx context.Context, live func(pack.Location) bool) (pack.CompactReport, error) {
	var report pack.CompactReport
	if ctx == nil || live == nil {
		return report, errors.New("invalid compact request")
	}
	byPack := make(map[string][]pack.Location)
	for loc := range p.items {
		byPack[loc.PackID] = append(byPack[loc.PackID], loc)
	}
	for _, locations := range byPack {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.ScannedPacks++
		reachable := false
		for _, loc := range locations {
			if live(loc) {
				reachable = true
			}
		}
		if reachable {
			continue
		}
		for _, loc := range locations {
			delete(p.items, loc)
			report.ReclaimedBytes += loc.Length
		}
		report.RemovedPacks++
	}
	return report, nil
}
func testStore(t *testing.T) (*Store, *memoryPacks) {
	t.Helper()
	p := newMemoryPacks()
	s, err := open(t.TempDir(), p, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, p
}
func reopen(t *testing.T, s *Store, p packBackend) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := open(s.root, p, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := next.Close(); err != nil {
			t.Error(err)
		}
	})
	return next
}
func put(t *testing.T, s *Store, d Descriptor, payload string) Holding {
	t.Helper()
	h, err := s.Put(context.Background(), d, []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func location(t *testing.T, s *Store, d Descriptor) pack.Location {
	t.Helper()
	id, _ := d.ID()
	e, found, err := s.lookup(id)
	if err != nil || !found {
		t.Fatal(e, found, err)
	}
	return e.Location
}

func TestRecordImmutableVersionsAndIdempotency(t *testing.T) {
	s, p := testStore(t)
	old := descriptor(t, "same-logical-key", "old")
	latest := descriptor(t, "same-logical-key", "new")
	h := put(t, s, old, "old")
	put(t, s, latest, "new")
	if h.StoreID != testStoreID || h.VolumeID == "" {
		t.Fatal(h)
	}
	h.Descriptor.Key[0] = 'X'
	for n := 0; n < 3; n++ {
		s = reopen(t, s, p)
		for _, d := range []Descriptor{old, latest} {
			value, held, err := s.Read(context.Background(), d)
			if err != nil || !d.matches(value) || !bytes.Equal(held.Descriptor.Key, d.Key) {
				t.Fatal(held, err)
			}
			before := p.reads
			put(t, s, d, string(value))
			if p.reads != before+1 {
				t.Fatal("duplicate was not physically reverified")
			}
		}
	}
	if p.appends != 2 {
		t.Fatal("duplicate appended", p.appends)
	}
	if _, err := s.Verify(context.Background(), descriptor(t, "same-logical-key", "missing")); !errors.Is(err, ErrNotHeld) {
		t.Fatal(err)
	}
	empty := descriptor(t, "empty", "")
	put(t, s, empty, "")
	if _, err := s.Verify(context.Background(), empty); err != nil {
		t.Fatal(err)
	}
}

func TestRecordRejectsInvalidPayloadBeforeAppend(t *testing.T) {
	s, p := testStore(t)
	d := descriptor(t, "key", "data")
	for _, payload := range []string{"wrong-size", "evil"} {
		if _, err := s.Put(context.Background(), d, []byte(payload)); err == nil {
			t.Fatal("mismatch accepted")
		}
	}
	bad := d.clone()
	bad.Key = nil
	if _, err := s.Put(context.Background(), bad, []byte("data")); err == nil {
		t.Fatal("bad descriptor")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(ctx, d, []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Verify(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p.appends != 0 {
		t.Fatal("invalid input reached Pack")
	}
	ctx, cancel = context.WithCancel(context.Background())
	p.afterAppend = cancel
	if _, err := s.Put(ctx, d, []byte("data")); err != nil {
		t.Fatal("known durable success was made ambiguous", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Verify(context.Background(), d); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed read was not classified as unavailable", err)
	}
	if _, err := s.Put(context.Background(), d, []byte("data")); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed write was not classified as unavailable", err)
	}
}

func TestRecordSyncUncertaintyFencesAndRecovers(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(fmt.Sprint(persist), func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			injected := errors.New("lost Sync acknowledgement")
			s.commit = func(b *pebble.Batch) error {
				if persist {
					if err := b.Commit(pebble.Sync); err != nil {
						return err
					}
				}
				return injected
			}
			if _, err := s.Put(context.Background(), d, []byte("data")); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if _, err := s.Verify(context.Background(), d); !errors.Is(err, injected) {
				t.Fatal("uncertain handle remained readable", err)
			}
			if _, err := s.Put(context.Background(), descriptor(t, "another", "data"), []byte("data")); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if p.appends != 1 {
				t.Fatal("fenced handle appended")
			}
			s = reopen(t, s, p)
			_, err := s.Verify(context.Background(), d)
			if persist && err != nil || !persist && !errors.Is(err, ErrNotHeld) {
				t.Fatal("wrong recovered atomic outcome", err)
			}
			put(t, s, d, "data")
			want := 2
			if persist {
				want = 1
			}
			if p.appends != want {
				t.Fatal("wrong retry outcome", p.appends)
			}
		})
	}
}

func TestRecordPackFailureNeverPublishesHolding(t *testing.T) {
	for _, phase := range []string{"append", "read"} {
		t.Run(phase, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			injected := errors.New("Pack failure")
			if phase == "append" {
				p.appendErr = injected
			} else {
				p.readErr = injected
			}
			h, err := s.Put(context.Background(), d, []byte("data"))
			if !errors.Is(err, injected) || h.RecordID != "" {
				t.Fatal(h, err)
			}
			id, _ := d.ID()
			_, found, err := s.lookup(id)
			if err != nil || found != (phase == "read") {
				t.Fatal(found, err)
			}
		})
	}
}

func TestRecordPackReadFailureClassifiesPhysicalHolding(t *testing.T) {
	for _, tc := range []struct {
		name    string
		readErr error
		want    error
	}{
		{"volume-or-pack-access", errors.New("volume marker changed"), ErrUnavailable},
		{"missing-pack", os.ErrNotExist, ErrUnavailable},
		{"corrupt-pack", pack.ErrCorrupt, ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			p.readErr = tc.readErr
			h, err := s.Verify(context.Background(), d)
			if h.RecordID != "" || !errors.Is(err, tc.want) || !errors.Is(err, tc.readErr) {
				t.Fatal("failed read returned a holding or lost its cause", h, err)
			}
			if tc.want == ErrCorrupt && errors.Is(err, ErrUnavailable) {
				t.Fatal("known corruption was masked as temporary unavailability", err)
			}
		})
	}
}

func TestRecordIndexReadFailureCannotBecomeMissingOrHeld(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  error
	}{
		{"index-io", errors.New("index read failed"), ErrUnavailable},
		{"pebble-corruption", pebble.ErrInvalidBatch, ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyIndexRead(tc.cause)
			if !errors.Is(err, tc.cause) || !errors.Is(err, tc.want) || errors.Is(err, ErrNotHeld) {
				t.Fatal("index failure lost its cause or became a missing record", err)
			}
		})
	}
}

func TestRecordRepairExistingRelocatesOnlyAfterVerifiedSync(t *testing.T) {
	s, p := testStore(t)
	d := descriptor(t, "key", "data")
	if _, err := s.RepairExisting(context.Background(), d, []byte("data")); !errors.Is(err, ErrNotHeld) || p.appends != 0 {
		t.Fatal("repair created a missing record", err)
	}
	put(t, s, d, "data")
	old := location(t, s, d)
	p.readErrors = map[pack.Location]error{old: os.ErrNotExist}
	if needed, err := s.RelocationNeeded(context.Background(), d); !needed || err != nil {
		t.Fatal("unreadable old Pack did not require relocation", needed, err)
	}
	if _, err := s.Put(context.Background(), d, []byte("data")); !errors.Is(err, ErrUnavailable) || p.appends != 1 {
		t.Fatal("ordinary Put silently relocated damaged bytes", err)
	}
	if _, err := s.RepairExisting(context.Background(), d, []byte("wrong")); err == nil || p.appends != 1 {
		t.Fatal("invalid repair body reached Pack", err)
	}
	h, err := s.RepairExisting(context.Background(), d, []byte("data"))
	if err != nil || h.RecordID == "" || p.appends != 2 {
		t.Fatal("replacement was not verified and published", h, err)
	}
	next := location(t, s, d)
	if next == old || next.VolumeID != h.VolumeID {
		t.Fatal("index did not switch to new physical location", old, next, h)
	}
	if needed, err := s.RelocationNeeded(context.Background(), d); needed || err != nil {
		t.Fatal("healthy replacement still needs relocation", needed, err)
	}
	if _, err := s.RepairExisting(context.Background(), d, []byte("data")); err != nil || p.appends != 2 {
		t.Fatal("repair retry appended another copy", err)
	}
	s = reopen(t, s, p)
	if _, err := s.Verify(context.Background(), d); err != nil || location(t, s, d) != next {
		t.Fatal("reopened Store lost replacement", err)
	}
}

func TestRecordCompactPreservesPackWithLiveAndObsoleteRecords(t *testing.T) {
	s, p := testStore(t)
	d := descriptor(t, "compact-key", "data")
	put(t, s, d, "data")
	old := location(t, s, d)
	p.readErrors = map[pack.Location]error{old: os.ErrNotExist}
	if _, err := s.RepairExisting(context.Background(), d, []byte("data")); err != nil {
		t.Fatal("prepare obsolete Pack record", err)
	}
	report, err := s.Compact(context.Background())
	if err != nil || report.ScannedPacks != 1 || report.RemovedPacks != 0 {
		t.Fatal("mixed live Pack was incorrectly collected", report, err)
	}
	if _, err := s.Verify(context.Background(), d); err != nil {
		t.Fatal("live index entry was lost during compact", err)
	}
}

func TestVolumeMigrationCursorSurvivesStoreRestart(t *testing.T) {
	s, p := testStore(t)
	const total = volumeMigrationLimit + 1
	descriptors := make([]Descriptor, 0, total)
	for i := 0; i < total; i++ {
		d := descriptor(t, fmt.Sprintf("migration-%03d", i), fmt.Sprintf("payload-%03d", i))
		put(t, s, d, fmt.Sprintf("payload-%03d", i))
		descriptors = append(descriptors, d)
	}
	if err := s.BeginVolumeMigration("volume_0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
	moved, done, err := s.MigrateVolumeStep(context.Background(), "volume_0123456789abcdef0123456789abcdef")
	if err != nil || done || moved != volumeMigrationLimit {
		t.Fatal("first migration page", moved, done, err)
	}
	status, err := s.VolumeMigration()
	if err != nil || !status.Active || status.Migrated != volumeMigrationLimit {
		t.Fatal("durable migration cursor", status, err)
	}
	s = reopen(t, s, p)
	moved, done, err = s.MigrateVolumeStep(context.Background(), "volume_0123456789abcdef0123456789abcdef")
	if err != nil || !done || moved != 1 {
		t.Fatal("resumed migration page", moved, done, err)
	}
	if status, err := s.VolumeMigration(); err != nil || status.Active {
		t.Fatal("completed migration task remained", status, err)
	}
	for _, d := range descriptors {
		if got := location(t, s, d); got.VolumeID != "volume_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Fatal("record remained on source Volume", got)
		}
	}
}

func TestRecordRepairExistingKeepsOldIndexOnPrepublishFailure(t *testing.T) {
	for _, phase := range []string{"append", "preverify"} {
		t.Run(phase, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			old := location(t, s, d)
			p.readErrors = map[pack.Location]error{old: os.ErrNotExist}
			injected := errors.New("replacement failed")
			if phase == "append" {
				p.appendErr = injected
			} else {
				p.afterAppend = func() { p.readErr = injected }
			}
			if h, err := s.RepairExisting(context.Background(), d, []byte("data")); h.RecordID != "" || !errors.Is(err, injected) {
				t.Fatal("failed repair acknowledged holding", h, err)
			}
			if location(t, s, d) != old {
				t.Fatal("failed repair overwrote old index")
			}
			p.appendErr, p.readErr, p.afterAppend = nil, nil, nil
			if _, err := s.RepairExisting(context.Background(), d, []byte("data")); err != nil {
				t.Fatal("retry could not repair after prepublication failure", err)
			}
		})
	}
}

func TestRecordRepairExistingRejectsCorruptIndex(t *testing.T) {
	for _, kind := range []string{"store", "volume", "pack", "offset"} {
		t.Run(kind, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			id, _ := d.ID()
			e, _, _ := s.lookup(id)
			switch kind {
			case "store":
				e.Location.StoreID = "store_foreign"
			case "volume":
				e.Location.VolumeID = "volume_foreign"
			case "pack":
				e.Location.PackID = "pack_foreign"
			case "offset":
				e.Location.Offset = -1
			}
			encoded, _ := json.Marshal(e)
			if err := s.db.Set(indexKey(id), encoded, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if needed, err := s.RelocationNeeded(context.Background(), d); needed || !errors.Is(err, ErrCorrupt) {
				t.Fatal("corrupt index was treated as relocatable", needed, err)
			}
			if _, err := s.RepairExisting(context.Background(), d, []byte("data")); !errors.Is(err, ErrCorrupt) || p.appends != 1 {
				t.Fatal("repair overwrote corrupt index", err)
			}
		})
	}
}

func TestRecordRepairExistingSyncUncertaintyResolvesOnReopen(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			old := location(t, s, d)
			p.readErrors = map[pack.Location]error{old: os.ErrNotExist}
			injected := errors.New("lost relocation Sync acknowledgement")
			s.commit = func(b *pebble.Batch) error {
				if persisted {
					if err := b.Commit(pebble.Sync); err != nil {
						return err
					}
				}
				return injected
			}
			if h, err := s.RepairExisting(context.Background(), d, []byte("data")); h.RecordID != "" || !errors.Is(err, injected) {
				t.Fatal("uncertain replacement was acknowledged", h, err)
			}
			if _, err := s.Verify(context.Background(), d); !errors.Is(err, ErrUnavailable) || !errors.Is(err, injected) {
				t.Fatal("uncertain index stayed usable", err)
			}
			s = reopen(t, s, p)
			_, err := s.Verify(context.Background(), d)
			if persisted && err != nil || !persisted && !errors.Is(err, ErrUnavailable) {
				t.Fatal("reopen did not resolve durable index outcome", persisted, err)
			}
			if persisted && location(t, s, d) == old || !persisted && location(t, s, d) != old {
				t.Fatal("wrong relocation survived uncertain Sync", persisted)
			}
			if _, err := s.RepairExisting(context.Background(), d, []byte("data")); err != nil {
				t.Fatal("resolved state was not idempotently repairable", err)
			}
		})
	}
}

func TestRecordPhysicalVerificationRejectsCorruption(t *testing.T) {
	for _, kind := range []string{"tenant", "engine", "key", "payload", "missing", "crc"} {
		t.Run(kind, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			loc := location(t, s, d)
			r := p.items[loc]
			switch kind {
			case "tenant":
				r.TenantID = "other"
			case "engine":
				r.EngineID = "file"
			case "key":
				r.Key = []byte("wrong")
			case "payload":
				r.Payload = []byte("evil")
			case "missing":
				delete(p.items, loc)
			case "crc":
				p.readErr = pack.ErrCorrupt
			}
			if kind != "missing" {
				p.items[loc] = r
			}
			if h, err := s.Verify(context.Background(), d); err == nil || h.RecordID != "" {
				t.Fatal(h, err)
			}
			if h, err := s.Put(context.Background(), d, []byte("data")); err == nil || h.RecordID != "" {
				t.Fatal("duplicate fabricated holding", h, err)
			}
			if p.appends != 1 {
				t.Fatal("silently overwrote corruption")
			}
		})
	}
}

func TestRecordIndexRejectsCorruption(t *testing.T) {
	for _, kind := range []string{"version", "descriptor", "store", "unknown", "oversized", "trailing", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			id, _ := d.ID()
			e, _, _ := s.lookup(id)
			switch kind {
			case "version":
				e.Version++
			case "descriptor":
				e.Descriptor.Keyspace.DatasetID = "other"
			case "store":
				e.Location.StoreID = "foreign"
			}
			data, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown":
				data = append([]byte(`{"extra":1,`), data[1:]...)
			case "oversized":
				data = []byte(strings.Repeat(" ", 17<<10))
			case "trailing":
				data = append(data, []byte(`{}`)...)
			case "invalid":
				data = []byte(`{`)
			}
			if err := s.db.Set(indexKey(id), data, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Verify(context.Background(), d); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

func TestRecordConcurrentDuplicateHasOneLocation(t *testing.T) {
	s, p := testStore(t)
	d := descriptor(t, "key", "data")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Put(context.Background(), d, []byte("data")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if p.appends != 1 || p.reads != 20 {
		t.Fatal(p.appends, p.reads)
	}
}

func TestRecordRevalidatesResourceAfterPackIO(t *testing.T) {
	for _, phase := range []string{"append", "read"} {
		t.Run(phase, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			if phase == "read" {
				put(t, s, d, "data")
			}
			displace := func() {
				path := filepath.Join(s.root, registrationFile)
				if err := os.Rename(path, path+".lost"); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "append" {
				p.afterAppend = displace
			} else {
				p.afterRead = displace
			}
			h, err := s.Put(context.Background(), d, []byte("data"))
			if err == nil || h.RecordID != "" {
				t.Fatal("resource changed during IO but holding returned", h, err)
			}
			id, _ := d.ID()
			_, found, err := s.lookup(id)
			if err != nil || found != (phase == "read") {
				t.Fatal("wrong index publication", found, err)
			}
		})
	}
}

func TestRecordLiveResourceReplacementFailsClosed(t *testing.T) {
	for _, part := range []string{"root", "directory", "database", "registration"} {
		t.Run(part, func(t *testing.T) {
			s, p := testStore(t)
			d := descriptor(t, "key", "data")
			put(t, s, d, "data")
			path := s.root
			switch part {
			case "directory":
				path = filepath.Join(path, directory)
			case "database":
				path = filepath.Join(path, directory, "index")
			case "registration":
				path = filepath.Join(path, registrationFile)
			}
			if err := os.Rename(path, path+".displaced"); err != nil {
				t.Fatal(err)
			}
			// Restore before Pebble Close and TempDir cleanup.
			t.Cleanup(func() {
				if err := os.Remove(path); err != nil {
					t.Error(err)
				}
				if err := os.Rename(path+".displaced", path); err != nil {
					t.Error(err)
				}
			})
			if part == "registration" {
				if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Verify(context.Background(), d); err == nil {
				t.Fatal("resource replacement accepted")
			}
			if _, err := s.Put(context.Background(), d, []byte("data")); err == nil {
				t.Fatal("resource replacement accepted")
			}
			if p.appends != 1 {
				t.Fatal("replacement touched Pack")
			}
		})
	}
}
