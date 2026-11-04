package logstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestEntriesPersistedRangeAndByteBoundary(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	entries := make([]*raftpb.Entry, 600)
	for i := range entries {
		entries[i] = entry(uint64(i+1), 3, fmt.Sprintf("%04d:%s", i+1, bytes.Repeat([]byte("payload"), 30)))
	}
	if err := s.Save(nil, entries, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name          string
		lo, hi, limit uint64
		count         int
	}{
		{"middle", 250, 360, ^uint64(0), 110},
		{"tail", 599, 601, ^uint64(0), 2},
		{"empty", 300, 300, 0, 0},
		{"zero-limit", 256, 500, 0, 1},
		{"oversize-first", 256, 500, 1, 1},
		{"exact-boundary", 256, 500, uint64(proto.Size(entries[255]) + proto.Size(entries[256])), 2},
		{"below-boundary", 256, 500, uint64(proto.Size(entries[255]) + proto.Size(entries[256]) - 1), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.Entries(tc.lo, tc.hi, tc.limit)
			if err != nil || len(got) != tc.count {
				t.Fatalf("count=%d want=%d error=%v", len(got), tc.count, err)
			}
			for i, value := range got {
				if !proto.Equal(value, entries[int(tc.lo)-1+i]) {
					t.Fatalf("entry %d differs from persisted value", i)
				}
			}
		})
	}
}

func TestEntriesRejectMissingAndCorruptRecords(t *testing.T) {
	for _, kind := range []string{"missing-first", "missing-middle", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			s, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.Save(nil, []*raftpb.Entry{entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 1, "three")}, nil); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing-first":
				err = s.db.Delete(entryKey(1), pebble.Sync)
			case "missing-middle":
				err = s.db.Delete(entryKey(2), pebble.Sync)
			case "corrupt":
				err = s.db.Set(entryKey(2), []byte{0xff}, pebble.Sync)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.Entries(1, 4, ^uint64(0))
			if err == nil || got != nil {
				t.Fatalf("accepted damaged log: %v, %v", got, err)
			}
			if kind != "corrupt" && !errors.Is(err, raft.ErrUnavailable) {
				t.Fatal(err)
			}
		})
	}
}

// This persisted log is larger than the shared cache on a small VPS. Cycling
// through 1 MiB batches models a new learner reading historical log ranges.
func BenchmarkEntriesPersistedCatchup(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	const total = 150000
	for lo := 1; lo <= total; lo += 1000 {
		batch := make([]*raftpb.Entry, 1000)
		for j := range batch {
			payload := make([]byte, 384)
			for k := range payload {
				payload[k] = byte((lo+j)*31 + k*17)
			}
			batch[j] = entry(uint64(lo+j), 3, string(payload))
		}
		if err := s.Save(nil, batch, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.db.Flush(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lo := uint64(1 + (i*2701)%(total-3000))
		got, err := s.Entries(lo, total+1, 1<<20)
		if err != nil || len(got) < 1000 {
			b.Fatalf("entries=%d error=%v", len(got), err)
		}
	}
}
