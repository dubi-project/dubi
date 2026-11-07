// Package state implements bounded, versioned state on the shared Runtime.
package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const EngineID = "state"
const MaxValueBytes = 64 << 10
const MaxKeyBytes = 2048

var (
	ErrInvalid       = errors.New("invalid state operation")
	ErrNotFound      = errors.New("state key not found")
	ErrConflict      = errors.New("state version conflict")
	ErrRequestReused = errors.New("state request ID reused with different content")
	ErrUnknown       = errors.New("state operation outcome is unknown; retry the same request ID")
)

type Condition struct {
	IfAbsent bool   `json:"if_absent,omitempty"`
	Version  string `json:"version,omitempty"`
}
type Entry struct {
	Version string `json:"version"`
	Value   []byte `json:"value"`
}
type Result struct {
	RequestID string `json:"request_id"`
	Version   string `json:"version"`
	Deleted   bool   `json:"deleted"`
	Replayed  bool   `json:"replayed"`
}
type operation struct {
	Format    int       `json:"format"`
	RequestID string    `json:"request_id"`
	Key       []byte    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
	Delete    bool      `json:"delete,omitempty"`
	Condition Condition `json:"condition"`
}
type content struct {
	Format  int    `json:"format"`
	Version string `json:"version"`
	Deleted bool   `json:"deleted"`
	Value   []byte `json:"value,omitempty"`
}
type head struct {
	CreatedOrder uint64  `json:"created_order,omitempty"`
	Bytes        *uint64 `json:"bytes,omitempty"`
	Version      string  `json:"version"`
	Deleted      bool    `json:"deleted"`
	RequestID    string  `json:"request_id"`
}
type receipt struct {
	Usage     *rootstate.UsageReport `json:"usage,omitempty"`
	Version   string                 `json:"version"`
	Deleted   bool                   `json:"deleted"`
	Code      string                 `json:"code,omitempty"`
	Confirmed bool                   `json:"confirmed"`
}
type query struct {
	List      *ListRequest `json:"list,omitempty"`
	Format    int          `json:"format"`
	RequestID string       `json:"request_id,omitempty"`
	Version   string       `json:"version,omitempty"`
}
type queryResult struct {
	Usage   *rootstate.UsageReport `json:"usage,omitempty"`
	Head    *head                  `json:"head,omitempty"`
	Receipt *receipt               `json:"receipt,omitempty"`
}

func validVersion(v string) bool {
	b, err := hex.DecodeString(v)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == v
}
func (o operation) validate() error {
	if o.Format != 1 || rootstate.ValidateRequestID(o.RequestID) != nil || len(o.Key) == 0 || len(o.Key) > MaxKeyBytes || len(o.Value) > MaxValueBytes ||
		(o.Delete && (len(o.Value) != 0 || o.Condition.IfAbsent)) || (o.Condition.IfAbsent && o.Condition.Version != "") ||
		(o.Condition.Version != "" && !validVersion(o.Condition.Version)) {
		return ErrInvalid
	}
	return nil
}
func (o operation) version() string {
	b, _ := json.Marshal(o)
	h := sha256.Sum256(append([]byte("dubi-state-operation-v1\x00"), b...))
	return hex.EncodeToString(h[:])
}
func (o operation) body() []byte {
	b, _ := json.Marshal(content{Format: 1, Version: o.version(), Deleted: o.Delete, Value: o.Value})
	return b
}
func dataKey(key []byte) []byte { return append([]byte("v/"), key...) }
func receiptKey(id string) []byte {
	h := sha256.Sum256([]byte(id))
	return []byte("r/" + hex.EncodeToString(h[:]))
}
func decode(b []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrInvalid
	}
	return nil
}
