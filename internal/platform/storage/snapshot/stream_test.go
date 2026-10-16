package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
)

func streamHeader() Header {
	return Header{Version: StreamVersion, Format: "enginestate-v3", GroupID: "data-1", Index: 42, Conf: []byte("membership")}
}

func streamFixture(t testing.TB) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder, err := NewEncoder(&buffer, streamHeader(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"a", "first"}, {"b", "other"}} {
		if err := encoder.Add([]byte(pair[0]), []byte(pair[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.Finish(); err != nil {
		t.Fatal(err)
	}
	if encoder.Written() != uint64(buffer.Len()) {
		t.Fatal("wrong written size")
	}
	return buffer.Bytes()
}

func consumeStream(encoded []byte) error {
	decoder, err := NewDecoder(bytes.NewReader(encoded), uint64(len(encoded)))
	if err != nil {
		return err
	}
	for {
		_, _, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestStreamRoundTripAndHeaderOwnership(t *testing.T) {
	encoded := streamFixture(t)
	decoder, err := NewDecoder(bytes.NewReader(encoded), uint64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	header := decoder.Header()
	if header.Index != 42 || header.Format != "enginestate-v3" || header.GroupID != "data-1" {
		t.Fatal(header)
	}
	header.Conf[0] = 'x'
	if string(decoder.Header().Conf) != "membership" {
		t.Fatal("header exposes mutable decoder state")
	}
	for _, pair := range [][2]string{{"a", "first"}, {"b", "other"}} {
		key, value, err := decoder.Next()
		if err != nil || string(key) != pair[0] || string(value) != pair[1] {
			t.Fatal(key, value, err)
		}
		key[0] = 'z' // Decoder's monotonicity check owns a separate previous key.
	}
	for range 2 {
		if _, _, err := decoder.Next(); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
}

func TestStreamRejectsTruncationCorruptionAndTrailingBytes(t *testing.T) {
	encoded := streamFixture(t)
	for length := range len(encoded) {
		if err := consumeStream(encoded[:length]); err == nil {
			t.Fatalf("accepted truncation at %d", length)
		}
	}
	// Every byte is covered by structure, header/record checksum, record count
	// or declared-size validation. No truncated prefix may look complete.
	for offset := range len(encoded) {
		copy := bytes.Clone(encoded)
		copy[offset] ^= 1
		if err := consumeStream(copy); err == nil {
			t.Fatalf("accepted corruption at %d", offset)
		}
	}
	if err := consumeStream(append(bytes.Clone(encoded), 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
	headerEnd := len(streamMagic) + 4 + int(binary.BigEndian.Uint32(encoded[len(streamMagic):])) + 4
	recordSize := 12 + 1 + 5 + 4
	unsorted := bytes.Clone(encoded)
	copy(unsorted[headerEnd:headerEnd+recordSize], encoded[headerEnd+recordSize:headerEnd+2*recordSize])
	copy(unsorted[headerEnd+recordSize:headerEnd+2*recordSize], encoded[headerEnd:headerEnd+recordSize])
	if err := consumeStream(unsorted); err == nil {
		t.Fatal("accepted valid-checksum unsorted records")
	}
}

type countedReader struct {
	io.Reader
	read int
}

func (r *countedReader) Read(data []byte) (int, error) {
	n, err := r.Reader.Read(data)
	r.read += n
	return n, err
}

func TestStreamValidatesRecordBoundsBeforeBodyRead(t *testing.T) {
	for _, length := range []uint64{MaxValueBytes + 1, math.MaxUint64} {
		encoded := bytes.Clone(streamFixture(t))
		headerEnd := len(streamMagic) + 4 + int(binary.BigEndian.Uint32(encoded[len(streamMagic):])) + 4
		binary.BigEndian.PutUint64(encoded[headerEnd+4:], length)
		reader := &countedReader{Reader: bytes.NewReader(encoded)}
		decoder, err := NewDecoder(reader, uint64(len(encoded)))
		if err != nil {
			t.Fatal(err)
		}
		_, _, first := decoder.Next()
		if first == nil || reader.read != headerEnd+12 {
			t.Fatal("read body before validating length", reader.read, first)
		}
		if _, _, err := decoder.Next(); err != first {
			t.Fatal("decoder did not retain failure")
		}
	}
}

func TestStreamNarrowAllocationBoundsBeforeReadingBody(t *testing.T) {
	for _, kind := range []string{"key", "value"} {
		encoded := bytes.Clone(streamFixture(t))
		headerEnd := len(streamMagic) + 4 + int(binary.BigEndian.Uint32(encoded[len(streamMagic):])) + 4
		if kind == "key" {
			binary.BigEndian.PutUint32(encoded[headerEnd:], 17)
		} else {
			binary.BigEndian.PutUint64(encoded[headerEnd+4:], 17)
		}
		reader := &countedReader{Reader: bytes.NewReader(encoded)}
		decoder, err := NewBoundedDecoder(reader, uint64(len(encoded)), 16, 16)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := decoder.Next(); err == nil || reader.read != headerEnd+12 {
			t.Fatal("narrow bound read body", kind, reader.read, err)
		}
	}
	for _, limits := range [][2]int{{0, 1}, {-1, 1}, {MaxKeyBytes + 1, 1}, {1, -1}, {1, MaxValueBytes + 1}} {
		if _, err := NewBoundedDecoder(bytes.NewReader(streamFixture(t)), uint64(len(streamFixture(t))), limits[0], limits[1]); err == nil {
			t.Fatal("relaxed global bound", limits)
		}
	}
}

type switchWriter struct {
	bytes.Buffer
	fail bool
}

func (w *switchWriter) Write(data []byte) (int, error) {
	if w.fail {
		return len(data) / 2, nil
	}
	return w.Buffer.Write(data)
}

func TestEncoderFailureIsStickyAndFooterReserved(t *testing.T) {
	for _, damage := range []string{"short-write", "duplicate", "unsorted", "empty", "limit"} {
		t.Run(damage, func(t *testing.T) {
			writer := &switchWriter{}
			encoder, err := NewEncoder(writer, streamHeader(), 256)
			if err != nil {
				t.Fatal(err)
			}
			if err := encoder.Add([]byte("b"), []byte("first")); err != nil {
				t.Fatal(err)
			}
			var first error
			switch damage {
			case "short-write":
				writer.fail = true
				first = encoder.Add([]byte("c"), nil)
			case "duplicate":
				first = encoder.Add([]byte("b"), nil)
			case "unsorted":
				first = encoder.Add([]byte("a"), nil)
			case "empty":
				first = encoder.Add(nil, nil)
			case "limit":
				first = encoder.Add([]byte("c"), make([]byte, 256))
			}
			if first == nil {
				t.Fatal("invalid write accepted")
			}
			size := encoder.Written()
			if err := encoder.Finish(); err != first || encoder.Written() != size {
				t.Fatal("failed encoder wrote footer", err)
			}
		})
	}
	var empty bytes.Buffer
	encoder, err := NewEncoder(&empty, streamHeader(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := consumeStream(empty.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Add([]byte("a"), nil); err == nil {
		t.Fatal("write after finish")
	}
	if _, err := NewEncoder(io.Discard, streamHeader(), uint64(empty.Len()-1)); err == nil {
		t.Fatal("header without reserved footer accepted")
	}
	if _, err := NewDecoder(bytes.NewReader(nil), MaxStreamBytes+1); err == nil {
		t.Fatal("unbounded input accepted")
	}
}

func TestStreamLargerThanInlineLimitWithoutWholeImageBuffer(t *testing.T) {
	const records, payloadSize = 80, 1 << 20
	var empty bytes.Buffer
	e, err := NewEncoder(&empty, streamHeader(), MaxStreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Finish(); err != nil {
		t.Fatal(err)
	}
	size := uint64(empty.Len() + records*(12+8+payloadSize+4))
	if size <= 64<<20 {
		t.Fatal("fixture did not exceed inline limit")
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	result := make(chan error, 1)
	go func() {
		encoder, err := NewEncoder(writer, streamHeader(), size)
		payload := bytes.Repeat([]byte("x"), payloadSize)
		for index := 0; index < records && err == nil; index++ {
			err = encoder.Add([]byte(fmt.Sprintf("%08d", index)), payload)
		}
		if err == nil {
			err = encoder.Finish()
		}
		writer.CloseWithError(err)
		result <- err
	}()
	decoder, err := NewDecoder(reader, size)
	if err != nil {
		t.Fatal(err)
	}
	for index := range records {
		key, value, err := decoder.Next()
		if err != nil || string(key) != fmt.Sprintf("%08d", index) || len(value) != payloadSize || value[0] != 'x' || value[len(value)-1] != 'x' {
			t.Fatal("streamed record mismatch", index, err)
		}
	}
	if _, _, err := decoder.Next(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func FuzzSnapshotStream(f *testing.F) {
	f.Add(streamFixture(f))
	f.Add([]byte("DUBISN01"))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 1<<20 {
			return
		}
		_ = consumeStream(encoded)
	})
}
