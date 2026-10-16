// Package records indexes immutable, module-neutral data in the Node's shared
// Pack pool. A local Holding is a checked physical observation, not authority,
// a Region-generation receipt, a Raft commit, or a durability quorum.
package records

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
)

const MaxKeyBytes = 4096

// GroupID is the originating metadata Group, not a claim that this Store hosts
// that Group or currently owns its Range. A remote Region can hold the bytes
// without running the originating Group. Key is opaque module data.
type Descriptor struct {
	Keyspace rootstate.Keyspace `json:"keyspace"`
	GroupID  string             `json:"group_id"`
	Key      []byte             `json:"key"`
	Size     uint64             `json:"size"`
	SHA256   string             `json:"sha256"`
}

func Describe(keyspace rootstate.Keyspace, group string, key, payload []byte) (Descriptor, error) {
	h := sha256.Sum256(payload)
	d := Descriptor{Keyspace: keyspace, GroupID: group, Key: bytes.Clone(key), Size: uint64(len(payload)), SHA256: hex.EncodeToString(h[:])}
	return d, d.Validate()
}

func (d Descriptor) Validate() error {
	if len(d.SHA256) != sha256.Size*2 {
		return errors.New("invalid immutable record digest length")
	}
	h, err := hex.DecodeString(d.SHA256)
	if d.Keyspace.Validate() != nil || raft.ValidateGroupID(d.GroupID) != nil || d.GroupID == raft.RootGroupID || len(d.Key) == 0 || len(d.Key) > MaxKeyBytes || d.Size > pack.MaxPayloadSize || err != nil || len(h) != sha256.Size || hex.EncodeToString(h) != d.SHA256 {
		return errors.New("invalid immutable record descriptor")
	}
	return nil
}

// ID includes full logical attribution and content. Length prefixes and the
// versioned domain separator prevent ambiguous concatenation and cross-domain
// reuse. Equal content in a different Tenant/Engine/Dataset is NOT deduped.
func (d Descriptor) ID() (string, error) {
	if err := d.Validate(); err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("DUBI immutable record v1\x00"))
	for _, v := range [][]byte{[]byte(d.Keyspace.TenantID), []byte(d.Keyspace.EngineID), []byte(d.Keyspace.DatasetID), []byte(d.GroupID), d.Key} {
		writeField(h, v)
	}
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], d.Size)
	h.Write(n[:])
	digest, _ := hex.DecodeString(d.SHA256)
	h.Write(digest)
	return "record_" + hex.EncodeToString(h.Sum(nil)), nil
}
func writeField(h hash.Hash, v []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(v)))
	h.Write(n[:])
	h.Write(v)
}
func (d Descriptor) clone() Descriptor { d.Key = bytes.Clone(d.Key); return d }
func (d Descriptor) matches(payload []byte) bool {
	h := sha256.Sum256(payload)
	return d.Size == uint64(len(payload)) && d.SHA256 == hex.EncodeToString(h[:])
}

type Holding struct {
	RecordID   string
	Descriptor Descriptor
	StoreID    string
	VolumeID   string // Diagnostic attribution, never another Node vote.
}
