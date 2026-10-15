package pack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCapabilitiesShareVolumeCatalogAndActivePack(t *testing.T) {
	r, v, m := collection(t, 4096)
	capabilities := []string{"record", "file", "search", "message", "cache", "file", "record"}
	var records []Record
	var locations []Location
	for i, capability := range capabilities {
		record := Record{CapabilityID: capability, TenantID: "shared-tenant", Key: []byte("same-key"), Payload: []byte{byte(i)}}
		loc, err := m.Append(record)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && (loc.PackID != locations[0].PackID || loc.Offset != locations[i-1].Offset+locations[i-1].Length) {
			t.Fatal("Capability change split Pack or broke append order")
		}
		records = append(records, record)
		locations = append(locations, loc)
	}
	if len(m.catalog.Packs) != 1 {
		t.Fatal("created per-Capability Packs")
	}
	entries, err := os.ReadDir(v.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "packs-") || strings.HasPrefix(entry.Name(), collectionMarkerName+"-") {
			t.Fatal("Capability partition remains", entry.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(v.Path, "packs", locations[0].PackID+".pack")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(v.Path, collectionMarkerName)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = OpenCollection(r, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, loc := range locations {
		got, err := m.Read(loc)
		if err != nil || !reflect.DeepEqual(got, records[i]) {
			t.Fatal("lost record attribution", got, err)
		}
	}
	var got []Record
	if err := m.Scan(func(_ Location, record Record) error { got = append(got, record); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatal("mixed Scan changed attribution or order")
	}
	loc, err := m.Append(Record{CapabilityID: "metric", TenantID: "other-tenant", Key: []byte("next")})
	if err != nil {
		t.Fatal(err)
	}
	if loc.PackID != locations[0].PackID {
		t.Fatal("restart/new Capability did not reuse active Pack")
	}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	loc, err = m.Append(Record{CapabilityID: "trace", TenantID: "t", Key: []byte("next")})
	if err != nil {
		t.Fatal(err)
	}
	if loc.PackID == locations[0].PackID || len(m.catalog.Packs) != 2 {
		t.Fatal("shared rollover failed")
	}
}

func TestMixedCapabilitiesRolloverAndCrashTail(t *testing.T) {
	r, v, m := collection(t, headerSize+80)
	var locs []Location
	for i := range 7 {
		capability := []string{"file", "message", "search"}[i%3]
		loc, err := m.Append(Record{CapabilityID: capability, TenantID: "t", Key: []byte("k"), Payload: []byte{byte(i)}})
		if err != nil {
			t.Fatal(err)
		}
		locs = append(locs, loc)
	}
	if locs[0].PackID != locs[1].PackID || locs[0].PackID != locs[2].PackID || locs[3].PackID == locs[2].PackID {
		t.Fatal("rollover based on Capability, not bytes")
	}
	if m.active == nil {
		t.Fatal("expected partially filled final Pack")
	}
	tail, err := encodeRecord(Record{CapabilityID: "log", TenantID: "t", Key: []byte("not-committed")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.active.file.WriteAt(tail, m.active.state.End); err != nil {
		t.Fatal(err)
	}
	if err := m.active.file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = OpenCollection(r, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	count := 0
	if err := m.Scan(func(loc Location, record Record) error {
		if count >= len(locs) || loc != locs[count] || record.CapabilityID != []string{"file", "message", "search"}[count%3] {
			t.Fatal("mixed recovery changed order or retained tail")
		}
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != len(locs) {
		t.Fatal("lost committed mixed records")
	}
}

func TestCapabilityAttributionIsChecksummedAndValidated(t *testing.T) {
	for _, repairCRC := range []bool{false, true} {
		t.Run(map[bool]string{false: "checksum", true: "invalid-identifier"}[repairCRC], func(t *testing.T) {
			r, v := setup(t)
			p := create(t, r, v, 0)
			loc := appendRecord(t, p, "k")
			dataPath := path(v, p, p.dataName())
			b, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			b[loc.Offset+recordHeaderSize] = '/'
			if repairCRC {
				record := b[loc.Offset : loc.Offset+loc.Length]
				binary.BigEndian.PutUint32(record[20:24], crc32.Checksum(record[recordHeaderSize:], crcTable))
				binary.BigEndian.PutUint32(record[24:28], crc32.Checksum(record[:24], crcTable))
			}
			if err := os.WriteFile(dataPath, b, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Read(loc); !errors.Is(err, ErrCorrupt) {
				t.Fatal("read accepted invalid attribution", err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if other, err := Open(r, v.ID, loc.PackID); err == nil {
				_ = other.Close()
				t.Fatal("recovery accepted invalid attribution")
			}
			after, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, after) {
				t.Fatal("corruption was truncated")
			}
		})
	}
}

func TestLegacyCapabilityLayoutIsPreservedAndRejected(t *testing.T) {
	for _, name := range []string{"packs-file", "DUBI_PACKS_ID-record"} {
		t.Run(name, func(t *testing.T) {
			r, v := setup(t)
			legacy := filepath.Join(v.Path, name)
			original := []byte("legacy fixture; must not be rewritten")
			if err := os.WriteFile(legacy, original, 0o600); err != nil {
				t.Fatal(err)
			}
			if m, err := InitializeCollection(r, v.ID, 0); !errors.Is(err, ErrLegacyLayout) {
				if m != nil {
					_ = m.Close()
				}
				t.Fatal(err)
			}
			if p, err := Create(r, v.ID, 0); !errors.Is(err, ErrLegacyLayout) {
				if p != nil {
					_ = p.Close()
				}
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(v.Path, "packs")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("created empty replacement", err)
			}
			after, err := os.ReadFile(legacy)
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("legacy changed", err)
			}
		})
	}
}

func TestOldPackFormatRejectedWithoutModification(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, 0)
	loc := appendRecord(t, p, "old")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	h := p.header
	h.Version = 1
	b, err := encodeDocument("DUBIPK01", headerSize, h)
	if err != nil {
		t.Fatal(err)
	}
	dataPath := path(v, p, p.dataName())
	f, err := os.OpenFile(dataPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(b, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	before, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(r, v.ID, loc.PackID); !errors.Is(err, ErrCorrupt) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal(err)
	}
	after, err := os.ReadFile(dataPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("old format modified", err)
	}
}
