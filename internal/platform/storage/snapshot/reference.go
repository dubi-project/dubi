package snapshot

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

const referencePrefix = "dubi-snapshot-reference-v1\n"
const MaxReferenceBytes = 1024

// Reference contains no local filesystem path. The artifact is addressed only
// by its canonical SHA-256 within a node-owned snapshot Store.
type Reference struct {
	Version int    `json:"version"`
	GroupID string `json:"group_id"`
	Index   uint64 `json:"index"`
	Size    uint64 `json:"size"`
	SHA256  string `json:"sha256"`
}

func (r Reference) Validate() error {
	if r.Version != StreamVersion || !validLabel(r.GroupID) || r.Index == 0 || r.Size == 0 || r.Size > MaxStreamBytes || len(r.SHA256) != 64 {
		return errors.New("invalid snapshot reference")
	}
	if _, err := hex.DecodeString(r.SHA256); err != nil || strings.ToLower(r.SHA256) != r.SHA256 {
		return errors.New("invalid snapshot digest")
	}
	return nil
}

func (r Reference) Encode() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte(referencePrefix), encoded...), nil
}

// DecodeReference distinguishes the new format from legacy inline data. A
// recognized but invalid reference is an error, never an inline fallback.
func DecodeReference(encoded []byte) (Reference, bool, error) {
	if !bytes.HasPrefix(encoded, []byte(referencePrefix)) {
		return Reference{}, false, nil
	}
	if len(encoded) > MaxReferenceBytes {
		return Reference{}, true, errors.New("snapshot reference exceeds bound")
	}
	var reference Reference
	decoder := json.NewDecoder(bytes.NewReader(encoded[len(referencePrefix):]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reference); err != nil {
		return Reference{}, true, err
	}
	canonical, err := reference.Encode()
	if err != nil {
		return Reference{}, true, err
	}
	if !bytes.Equal(canonical, encoded) {
		return Reference{}, true, errors.New("non-canonical snapshot reference")
	}
	return reference, true, nil
}

func (r Reference) filename() string { return r.SHA256 + ".snap" }
