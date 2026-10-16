// Package snapshot defines engine-neutral bounded streaming snapshot
// primitives. A stream is a logical state image, not a Node backup or a Pack.
package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
)

const (
	StreamVersion         = 1
	MaxHeaderBytes        = 64 << 10
	MaxKeyBytes           = 64 << 10
	MaxValueBytes         = 64 << 20
	MaxStreamBytes uint64 = 1 << 40
	streamMagic           = "DUBISN01"
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// Header binds an opaque ordered-key stream to a state format and Raft cut.
// Conf is an opaque serialized membership value, interpreted by the caller.
// No local path, Pack location or Node identity is transferred here.
type Header struct {
	Version int    `json:"version"`
	Format  string `json:"format"`
	GroupID string `json:"group_id"`
	Index   uint64 `json:"index"`
	Conf    []byte `json:"conf"`
}

func (h Header) Validate() error {
	if h.Version != StreamVersion || h.Index == 0 || !validLabel(h.Format) || !validLabel(h.GroupID) || len(h.Conf) > MaxHeaderBytes {
		return errors.New("invalid snapshot stream header")
	}
	return nil
}

func validLabel(value string) bool {
	if len(value) == 0 || len(value) > 128 || value == "." || value == ".." {
		return false
	}
	for _, c := range value {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("._-", c)) {
			return false
		}
	}
	return true
}

// Encoder retains one previous key, not the complete image. Values are opaque;
// callers must hydrate local references and apply their semantic validation.
// A write error poisons this encoder: a partial record is never retryable in
// place. The owner must discard the unpublished temporary artifact.
type Encoder struct {
	writer   io.Writer
	limit    uint64
	written  uint64
	records  uint64
	previous []byte
	failed   error
	finished bool
}

func NewEncoder(writer io.Writer, header Header, limit uint64) (*Encoder, error) {
	if writer == nil || limit == 0 || limit > MaxStreamBytes {
		return nil, errors.New("invalid snapshot writer or stream bound")
	}
	if err := header.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxHeaderBytes {
		return nil, errors.New("snapshot header exceeds bound")
	}
	e := &Encoder{writer: writer, limit: limit}
	// Reserve framing plus the terminal footer even for an empty image.
	if uint64(len(streamMagic)+4+len(encoded)+4+12) > limit {
		return nil, errors.New("snapshot stream bound is smaller than its header")
	}
	prefix := make([]byte, len(streamMagic)+4)
	copy(prefix, streamMagic)
	binary.BigEndian.PutUint32(prefix[len(streamMagic):], uint32(len(encoded)))
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.Checksum(encoded, checksumTable))
	for _, part := range [][]byte{prefix, encoded, checksum[:]} {
		if err := e.write(part); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *Encoder) Add(key, value []byte) error {
	if err := e.usable(); err != nil {
		return err
	}
	if len(key) == 0 || len(key) > MaxKeyBytes || len(value) > MaxValueBytes ||
		(e.previous != nil && bytes.Compare(e.previous, key) >= 0) {
		e.failed = errors.New("snapshot record is oversized, empty-key, duplicate or unsorted")
		return e.failed
	}
	// Validate the entire record and footer before emitting any part of it.
	if uint64(12+len(key)+len(value)+4+12) > e.limit-e.written {
		e.failed = errors.New("snapshot stream exceeds bound")
		return e.failed
	}
	var lengths [12]byte
	binary.BigEndian.PutUint32(lengths[:4], uint32(len(key)))
	binary.BigEndian.PutUint64(lengths[4:], uint64(len(value)))
	crc := crc32.Update(0, checksumTable, lengths[:])
	crc = crc32.Update(crc, checksumTable, key)
	crc = crc32.Update(crc, checksumTable, value)
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc)
	for _, part := range [][]byte{lengths[:], key, value, checksum[:]} {
		if err := e.write(part); err != nil {
			return err
		}
	}
	e.previous = append(e.previous[:0], key...)
	e.records++
	return nil
}

func (e *Encoder) Finish() error {
	if err := e.usable(); err != nil {
		return err
	}
	var footer [12]byte // Zero key length, followed by exact record count.
	binary.BigEndian.PutUint64(footer[4:], e.records)
	if err := e.write(footer[:]); err != nil {
		return err
	}
	e.finished = true
	return nil
}

func (e *Encoder) Written() uint64 { return e.written }

func (e *Encoder) usable() error {
	if e.failed != nil {
		return e.failed
	}
	if e.finished {
		return errors.New("snapshot encoder is finished")
	}
	return nil
}

func (e *Encoder) write(data []byte) error {
	if uint64(len(data)) > e.limit-e.written {
		e.failed = errors.New("snapshot stream exceeds bound")
		return e.failed
	}
	n, err := e.writer.Write(data)
	if n < 0 || n > len(data) {
		e.failed = errors.New("snapshot writer returned invalid count")
		return e.failed
	}
	e.written += uint64(n)
	if err != nil {
		e.failed = err
	} else if n != len(data) {
		e.failed = io.ErrShortWrite
	}
	return e.failed
}

// Decoder requires the independently authenticated, exact artifact size. It
// validates declared lengths BEFORE allocating and returns at most one record
// per Next call. Consumers must reach io.EOF (verified footer) before publishing
// any restored state, and must separately verify the artifact SHA-256.
type Decoder struct {
	reader           io.Reader
	remaining        uint64
	header           Header
	previous         []byte
	records          uint64
	failed           error
	finished         bool
	maxKey, maxValue uint64
}

func NewDecoder(reader io.Reader, size uint64) (*Decoder, error) {
	return NewBoundedDecoder(reader, size, MaxKeyBytes, MaxValueBytes)
}

// NewBoundedDecoder lets a narrower opaque format reduce allocation bounds.
// It cannot relax the common stream limits, and rejects lengths before reading
// or allocating a record body. This does not alter the on-disk framing.
func NewBoundedDecoder(reader io.Reader, size uint64, maxKey, maxValue int) (*Decoder, error) {
	if maxKey <= 0 || maxKey > MaxKeyBytes || maxValue < 0 || maxValue > MaxValueBytes {
		return nil, errors.New("invalid snapshot record allocation bounds")
	}
	if reader == nil || size == 0 || size > MaxStreamBytes {
		return nil, errors.New("invalid snapshot reader or size")
	}
	d := &Decoder{reader: reader, remaining: size, maxKey: uint64(maxKey), maxValue: uint64(maxValue)}
	prefix := make([]byte, len(streamMagic)+4)
	if err := d.read(prefix); err != nil {
		return nil, err
	}
	if string(prefix[:len(streamMagic)]) != streamMagic {
		return nil, errors.New("invalid snapshot stream magic")
	}
	headerSize := uint64(binary.BigEndian.Uint32(prefix[len(streamMagic):]))
	if headerSize == 0 || headerSize > MaxHeaderBytes || headerSize+4+12 > d.remaining {
		return nil, errors.New("invalid snapshot header length")
	}
	encoded := make([]byte, int(headerSize))
	if err := d.read(encoded); err != nil {
		return nil, err
	}
	var checksum [4]byte
	if err := d.read(checksum[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(checksum[:]) != crc32.Checksum(encoded, checksumTable) {
		return nil, errors.New("snapshot header checksum mismatch")
	}
	jsonDecoder := json.NewDecoder(bytes.NewReader(encoded))
	jsonDecoder.DisallowUnknownFields()
	if err := jsonDecoder.Decode(&d.header); err != nil {
		return nil, fmt.Errorf("decode snapshot header: %w", err)
	}
	canonical, err := json.Marshal(d.header)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, errors.New("non-canonical snapshot header")
	}
	if err := d.header.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Decoder) Header() Header { h := d.header; h.Conf = bytes.Clone(h.Conf); return h }

func (d *Decoder) Next() (key, value []byte, err error) {
	if d.failed != nil {
		return nil, nil, d.failed
	}
	if d.finished {
		return nil, nil, io.EOF
	}
	defer func() {
		if err != nil && !errors.Is(err, io.EOF) {
			d.failed = err
		}
	}()
	var lengths [12]byte
	if err := d.read(lengths[:]); err != nil {
		return nil, nil, err
	}
	keySize, valueSize := uint64(binary.BigEndian.Uint32(lengths[:4])), binary.BigEndian.Uint64(lengths[4:])
	if keySize == 0 {
		if valueSize != d.records || d.remaining != 0 {
			return nil, nil, errors.New("snapshot footer count or length mismatch")
		}
		d.finished = true
		return nil, nil, io.EOF
	}
	// Separately bound each operand before adding to avoid overflow.
	if keySize > d.maxKey || valueSize > d.maxValue || keySize+valueSize+4+12 > d.remaining {
		return nil, nil, errors.New("snapshot record length exceeds bound")
	}
	key, value = make([]byte, int(keySize)), make([]byte, int(valueSize))
	if err := d.read(key); err != nil {
		return nil, nil, err
	}
	if err := d.read(value); err != nil {
		return nil, nil, err
	}
	var checksum [4]byte
	if err := d.read(checksum[:]); err != nil {
		return nil, nil, err
	}
	crc := crc32.Update(0, checksumTable, lengths[:])
	crc = crc32.Update(crc, checksumTable, key)
	crc = crc32.Update(crc, checksumTable, value)
	if binary.BigEndian.Uint32(checksum[:]) != crc {
		return nil, nil, errors.New("snapshot record checksum mismatch")
	}
	if d.previous != nil && bytes.Compare(d.previous, key) >= 0 {
		return nil, nil, errors.New("snapshot records are duplicate or unsorted")
	}
	d.previous = append(d.previous[:0], key...)
	d.records++
	return key, value, nil
}

func (d *Decoder) read(data []byte) error {
	if uint64(len(data)) > d.remaining {
		return io.ErrUnexpectedEOF
	}
	n, err := io.ReadFull(d.reader, data)
	d.remaining -= uint64(n)
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
