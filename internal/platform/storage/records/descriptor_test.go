package records

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
)

func descriptor(t *testing.T, key, payload string) Descriptor {
	t.Helper()
	d, err := Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "cache", DatasetID: "data"}, "group-records", []byte(key), []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDescriptorIdentityAndOwnership(t *testing.T) {
	d := descriptor(t, "a\x00b", "payload")
	id, err := d.ID()
	if err != nil || len(id) != len("record_")+64 {
		t.Fatal(id, err)
	}
	for name, change := range map[string]func(*Descriptor){
		"tenant":  func(v *Descriptor) { v.Keyspace.TenantID += "2" },
		"engine":  func(v *Descriptor) { v.Keyspace.EngineID += "2" },
		"dataset": func(v *Descriptor) { v.Keyspace.DatasetID += "2" },
		"group":   func(v *Descriptor) { v.GroupID += "2" },
		"key":     func(v *Descriptor) { v.Key = []byte("another") },
		"size":    func(v *Descriptor) { v.Size++ },
		"digest":  func(v *Descriptor) { v.SHA256 = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			other := d.clone()
			change(&other)
			got, err := other.ID()
			if err != nil || got == id {
				t.Fatal(got, err)
			}
		})
	}
	a, b := d.clone(), d.clone()
	a.Keyspace.TenantID, a.Keyspace.EngineID = "ab", "c"
	b.Keyspace.TenantID, b.Keyspace.EngineID = "a", "bc"
	aID, _ := a.ID()
	bID, _ := b.ID()
	if aID == bID {
		t.Fatal("ambiguous concatenation")
	}
	key := []byte("owned")
	own, err := Describe(d.Keyspace, d.GroupID, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	if string(own.Key) != "owned" || !own.matches(nil) {
		t.Fatal("borrowed key or empty payload rejected")
	}
	cloned := own.clone()
	cloned.Key[0] = 'Z'
	if string(own.Key) != "owned" {
		t.Fatal("clone aliases key")
	}
}

func TestDescriptorValidation(t *testing.T) {
	d := descriptor(t, "key", "data")
	for name, change := range map[string]func(*Descriptor){
		"tenant":       func(v *Descriptor) { v.Keyspace.TenantID = "" },
		"engine":       func(v *Descriptor) { v.Keyspace.EngineID = "../file" },
		"dataset":      func(v *Descriptor) { v.Keyspace.DatasetID = "" },
		"group":        func(v *Descriptor) { v.GroupID = "../raft" },
		"root":         func(v *Descriptor) { v.GroupID = raft.RootGroupID },
		"key":          func(v *Descriptor) { v.Key = nil },
		"large-key":    func(v *Descriptor) { v.Key = bytes.Repeat([]byte{1}, MaxKeyBytes+1) },
		"size":         func(v *Descriptor) { v.Size = pack.MaxPayloadSize + 1 },
		"short-digest": func(v *Descriptor) { v.SHA256 = "00" },
		"uppercase":    func(v *Descriptor) { v.SHA256 = strings.Repeat("A", 64) },
		"nonhex":       func(v *Descriptor) { v.SHA256 = strings.Repeat("z", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			v := d.clone()
			change(&v)
			if v.Validate() == nil {
				t.Fatal("accepted invalid descriptor")
			}
			if _, err := v.ID(); err == nil {
				t.Fatal("invalid descriptor acquired identity")
			}
		})
	}
	d.Key = bytes.Repeat([]byte{0}, MaxKeyBytes)
	d.Size = pack.MaxPayloadSize
	if err := d.Validate(); err != nil {
		t.Fatal("valid boundary", err)
	}
}
