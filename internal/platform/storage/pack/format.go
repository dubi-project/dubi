// Package pack implements module-neutral local append-only record containers.
// It provides local durability, not Raft commit, authorization, deduplication,
// a logical index, or cross-node durability. Payload bytes belong to modules.
package pack

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"regexp"
)

const (
	DefaultTargetSize int64 = 512 << 20
	MaxTargetSize     int64 = 1 << 30
	MaxPayloadSize          = 64 << 20
	MaxKeySize              = 64 << 10
	MaxTenantSize           = 256
	MaxCapabilitySize       = 128
	headerSize              = 1024
	stateSize               = 256
	recordHeaderSize        = 28
)

var (
	crcTable          = crc32.MakeTable(crc32.Castagnoli)
	capabilityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	idPattern         = regexp.MustCompile(`^pack_[0-9a-f]{32}$`)
	ErrSealed         = errors.New("pack is sealed; create the next pack")
	ErrCorrupt        = errors.New("pack corruption")
)

type Scope struct {
	StoreID  string `json:"store_id"`
	VolumeID string `json:"volume_id"`
	PackID   string `json:"pack_id"`
}

type header struct {
	Scope
	Version    int   `json:"version"`
	TargetSize int64 `json:"target_size"`
}

type checkpoint struct {
	PackID string `json:"pack_id"`
	End    int64  `json:"end"`
	Count  uint64 `json:"count"`
	Sealed bool   `json:"sealed"`
}

// Record carries Capability/Tenant attribution and otherwise opaque module bytes. Keys
// need not be unique; callers own their index, Range mapping and retry policy.
type Record struct {
	CapabilityID string
	TenantID     string
	Key          []byte
	Payload      []byte
}

type Location struct {
	Scope
	Offset int64
	Length int64 // Entire encoded record, including header and attribution.
	CRC32C uint32
}

func encodeDocument(magic string, size int, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > size-16 {
		return nil, errors.New("pack metadata exceeds limit")
	}
	b := make([]byte, size)
	copy(b[:8], magic)
	binary.BigEndian.PutUint32(b[8:12], uint32(len(data)))
	copy(b[12:], data)
	binary.BigEndian.PutUint32(b[size-4:], crc32.Checksum(b[:size-4], crcTable))
	return b, nil
}

func decodeDocument(b []byte, magic string, target any) error {
	if len(b) < 16 || string(b[:8]) != magic || crc32.Checksum(b[:len(b)-4], crcTable) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return ErrCorrupt
	}
	n := int(binary.BigEndian.Uint32(b[8:12]))
	if n < 0 || n > len(b)-16 {
		return ErrCorrupt
	}
	if !bytes.Equal(b[12+n:len(b)-4], make([]byte, len(b)-16-n)) {
		return ErrCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(b[12 : 12+n]))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return errors.Join(ErrCorrupt, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrCorrupt
	}
	return nil
}

func encodeRecord(r Record) ([]byte, error) {
	if err := validateRecord(r); err != nil {
		return nil, err
	}
	b := make([]byte, recordHeaderSize+len(r.CapabilityID)+len(r.TenantID)+len(r.Key)+len(r.Payload))
	copy(b, "DPR2")
	binary.BigEndian.PutUint32(b[4:8], uint32(len(r.CapabilityID)))
	binary.BigEndian.PutUint32(b[8:12], uint32(len(r.TenantID)))
	binary.BigEndian.PutUint32(b[12:16], uint32(len(r.Key)))
	binary.BigEndian.PutUint32(b[16:20], uint32(len(r.Payload)))
	pos := recordHeaderSize
	pos += copy(b[pos:], r.CapabilityID)
	pos += copy(b[pos:], r.TenantID)
	pos += copy(b[pos:], r.Key)
	copy(b[pos:], r.Payload)
	binary.BigEndian.PutUint32(b[20:24], crc32.Checksum(b[recordHeaderSize:], crcTable))
	binary.BigEndian.PutUint32(b[24:28], crc32.Checksum(b[:24], crcTable))
	return b, nil
}

func validateRecord(r Record) error {
	if !capabilityPattern.MatchString(r.CapabilityID) || len(r.TenantID) == 0 || len(r.TenantID) > MaxTenantSize || len(r.Key) == 0 || len(r.Key) > MaxKeySize || len(r.Payload) > MaxPayloadSize {
		return errors.New("invalid pack record capability, tenant, key or payload length")
	}
	return nil
}

func decodeRecordHeader(b []byte) (capability, tenant, key, payload int, checksum uint32, err error) {
	if len(b) != recordHeaderSize || string(b[:4]) != "DPR2" || binary.BigEndian.Uint32(b[24:]) != crc32.Checksum(b[:24], crcTable) {
		err = ErrCorrupt
		return
	}
	capability, tenant, key, payload = int(binary.BigEndian.Uint32(b[4:8])), int(binary.BigEndian.Uint32(b[8:12])), int(binary.BigEndian.Uint32(b[12:16])), int(binary.BigEndian.Uint32(b[16:20]))
	if capability <= 0 || capability > MaxCapabilitySize || tenant <= 0 || tenant > MaxTenantSize || key <= 0 || key > MaxKeySize || payload < 0 || payload > MaxPayloadSize {
		err = ErrCorrupt
		return
	}
	checksum = binary.BigEndian.Uint32(b[20:24])
	return
}
