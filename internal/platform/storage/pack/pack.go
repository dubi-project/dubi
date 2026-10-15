package pack

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"golang.org/x/sys/unix"
)

// Pack owns one data file and one atomic durable checkpoint. Close it before
// closing its volume registry. Calls are serialized; Scan callbacks must not
// re-enter this Pack or its registry. No disk format compatibility is promised
// yet beyond this version's explicit checks.
type Pack struct {
	mu        sync.Mutex
	registry  *volume.Registry
	header    header
	state     checkpoint
	file      *os.File
	directory os.FileInfo
	failed    error
	closed    bool
}

// Create generates a PackID on an active registered Volume. A zero target uses
// 512 MiB. Smaller targets are useful in tests; the hard target limit is 1 GiB.
// It does not choose a Volume, publish a module index or recover orphan files.
func Create(registry *volume.Registry, volumeID string, target int64) (*Pack, error) {
	if target == 0 {
		target = DefaultTargetSize
	}
	if target <= headerSize || target > MaxTargetSize {
		return nil, errors.New("invalid pack target size")
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	return open(registry, volumeID, id, target, openMode{create: true})
}

// Open requires an active Volume and both existing files. It validates every committed record before
// truncating any uncommitted active tail; it never repairs committed corruption.
func Open(registry *volume.Registry, volumeID, id string) (*Pack, error) {
	return open(registry, volumeID, id, 0, openMode{})
}

type openMode struct {
	create        bool
	managed       bool
	sealedRead    bool
	requireEmpty  bool
	requireSealed bool
}

func open(registry *volume.Registry, volumeID, id string, target int64, mode openMode) (*Pack, error) {
	if registry == nil || !idPattern.MatchString(id) {
		return nil, errors.New("invalid registry or PackID")
	}
	p := &Pack{registry: registry, header: header{Scope: Scope{VolumeID: volumeID, PackID: id}, Version: protocol.PackContainerFormatVersion, TargetSize: target}}
	err := registry.WithVolume(volumeID, !mode.sealedRead, func(root *os.Root, storeID string) error {
		p.header.StoreID = storeID
		if err := rejectLegacyLayout(root); err != nil {
			return err
		}
		if !mode.managed {
			if _, err := root.Lstat(collectionMarkerName); !errors.Is(err, os.ErrNotExist) {
				return errors.Join(err, errors.New("Volume is collection-managed; use Collection"))
			}
		}
		if mode.create {
			if err := root.Mkdir(p.directoryName(), 0o750); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			if err := syncDirectory(root); err != nil {
				return err
			}
		}
		dir, err := openDirectory(root, p.directoryName())
		if err != nil {
			return err
		}
		defer dir.Close()
		if !mode.managed {
			if _, err := dir.Lstat(catalogName); !errors.Is(err, os.ErrNotExist) {
				return errors.Join(err, errors.New("Pack directory is collection-managed; use Collection"))
			}
		}
		p.directory, err = dir.Stat(".")
		if err != nil {
			return err
		}
		flags := os.O_RDWR
		if mode.sealedRead {
			flags = os.O_RDONLY
		}
		if mode.create {
			flags |= os.O_CREATE | os.O_EXCL
		}
		p.file, err = regularFile(dir, p.dataName(), flags)
		if err != nil {
			return err
		}
		if err := unix.Flock(int(p.file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return fmt.Errorf("pack already open: %w", err)
		}
		if mode.create {
			encoded, err := encodeDocument("DUBIPK02", headerSize, p.header)
			if err != nil {
				return err
			}
			if _, err := p.file.WriteAt(encoded, 0); err != nil {
				return err
			}
			if err := p.file.Sync(); err != nil {
				return err
			}
			p.state = checkpoint{PackID: id, End: headerSize}
			return p.publish(dir, p.state, true)
		}
		var saved header
		b := make([]byte, headerSize)
		if _, err := p.file.ReadAt(b, 0); err != nil {
			return errors.Join(ErrCorrupt, err)
		}
		if err := decodeDocument(b, "DUBIPK02", &saved); err != nil {
			return err
		}
		if saved.Scope != p.header.Scope || saved.Version != protocol.PackContainerFormatVersion || saved.TargetSize <= headerSize || saved.TargetSize > MaxTargetSize {
			return ErrCorrupt
		}
		p.header = saved
		p.state, err = p.readCheckpoint(dir)
		if err != nil {
			return err
		}
		if p.state.PackID != id || p.state.End < headerSize || p.state.End > saved.TargetSize+recordHeaderSize+MaxCapabilitySize+MaxTenantSize+MaxKeySize+MaxPayloadSize || p.state.Count > uint64((p.state.End-headerSize)/recordHeaderSize) || (!p.state.Sealed && p.state.End >= saved.TargetSize) {
			return ErrCorrupt
		}
		if mode.requireSealed && !p.state.Sealed {
			return ErrCorrupt
		}
		if mode.requireEmpty {
			info, err := p.file.Stat()
			if err != nil {
				return err
			}
			if p.state.Count != 0 || p.state.End != headerSize || p.state.Sealed || info.Size() != headerSize {
				return ErrCorrupt
			}
		}
		if mode.sealedRead {
			info, err := p.file.Stat()
			if err != nil {
				return err
			}
			if !p.state.Sealed || info.Size() != p.state.End {
				return ErrCorrupt
			}
			return nil // Each requested record is still checksummed by Read/Scan.
		}
		if err := p.recover(); err != nil {
			return err
		}
		// A previous attempt may have renamed its checkpoint and failed before
		// syncing the directory. Confirm it before reusing that visible state.
		return syncDirectory(dir)
	})
	if err != nil {
		if p.file != nil {
			err = errors.Join(err, p.file.Close())
		}
		return nil, err
	}
	return p, nil
}

func (p *Pack) Scope() Scope          { return p.header.Scope }
func (p *Pack) directoryName() string { return packDirectory }
func (p *Pack) dataName() string      { return p.header.PackID + ".pack" }
func (p *Pack) stateName() string     { return p.header.PackID + ".state" }

// with checks both the Volume marker and the identity of the open Pack on every
// operation. A missing mount/file must not turn into a write to a replacement.
func (p *Pack) with(write bool, fn func(*os.Root) error) error {
	if p.closed || p.failed != nil {
		return errors.Join(p.failed, errors.New("pack is closed or requires recovery"))
	}
	return p.registry.WithVolume(p.header.VolumeID, write, func(root *os.Root, storeID string) error {
		if storeID != p.header.StoreID {
			return errors.New("pack Store changed")
		}
		dir, err := openDirectory(root, p.directoryName())
		if err != nil {
			return err
		}
		defer dir.Close()
		current, err := dir.Stat(".")
		if err != nil {
			return err
		}
		if !os.SameFile(current, p.directory) {
			return errors.New("pack directory was replaced")
		}
		current, err = dir.Lstat(p.dataName())
		if err != nil {
			return err
		}
		held, err := p.file.Stat()
		if err != nil {
			return err
		}
		if !current.Mode().IsRegular() || !os.SameFile(held, current) {
			return errors.New("pack file was replaced")
		}
		if held.Size() != p.state.End {
			return errors.Join(ErrCorrupt, errors.New("pack length changed while open"))
		}
		saved, err := p.readCheckpoint(dir)
		if err != nil {
			return err
		}
		if saved != p.state {
			return errors.New("pack checkpoint changed while open")
		}
		return fn(dir)
	})
}

// Append acknowledges only after data fsync, checkpoint fsync/rename, and
// directory fsync. Any IO failure after starting mutation poisons the handle:
// reopen to resolve the uncertain outcome, and retry using a module request ID.
// The record that reaches/exceeds the target is accepted whole and seals Pack.
func (p *Pack) Append(record Record) (Location, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var loc Location
	err := p.with(true, func(dir *os.Root) error {
		if p.state.Sealed {
			return ErrSealed
		}
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		loc = Location{Scope: p.header.Scope, Offset: p.state.End, Length: int64(len(encoded)), CRC32C: crc32.Checksum(encoded[recordHeaderSize:], crcTable)}
		if _, err := p.file.WriteAt(encoded, loc.Offset); err != nil {
			p.failed = err
			return err
		}
		if err := p.file.Sync(); err != nil {
			p.failed = err
			return err
		}
		next := p.state
		next.End += loc.Length
		next.Count++
		next.Sealed = next.End >= p.header.TargetSize
		if err := p.publish(dir, next, false); err != nil {
			p.failed = err
			return err
		}
		p.state = next
		return nil
	})
	if err != nil {
		return Location{}, err
	}
	return loc, nil
}

// Seal durably closes this Pack to new records. It does not delete, compact or
// publish it to a backup catalog. Repeating Seal is harmless.
func (p *Pack) Seal() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.with(true, func(dir *os.Root) error {
		if p.state.Sealed {
			return nil
		}
		next := p.state
		next.Sealed = true
		if err := p.publish(dir, next, false); err != nil {
			p.failed = err
			return err
		}
		p.state = next
		return nil
	})
}

func (p *Pack) Read(loc Location) (Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var record Record
	err := p.with(false, func(_ *os.Root) error {
		if loc.Scope != p.header.Scope {
			return errors.New("location belongs to another Pack")
		}
		actual, decoded, err := p.readRecord(loc.Offset, true)
		if err != nil {
			return err
		}
		if actual != loc {
			return errors.New("location does not match record")
		}
		record = decoded
		return nil
	})
	return record, err
}

// Scan returns the committed physical records in append order. These may
// include obsolete/duplicate module keys; a physical scan is not a live index.
func (p *Pack) Scan(fn func(Location, Record) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.with(false, func(_ *os.Root) error {
		for offset := int64(headerSize); offset < p.state.End; {
			loc, record, err := p.readRecord(offset, true)
			if err != nil {
				return err
			}
			if err := fn(loc, record); err != nil {
				return err
			}
			offset += loc.Length
		}
		return nil
	})
}

func (p *Pack) readRecord(offset int64, materialize bool) (Location, Record, error) {
	var loc Location
	var record Record
	if offset < headerSize || offset > p.state.End-recordHeaderSize {
		return loc, record, ErrCorrupt
	}
	var b [recordHeaderSize]byte
	if _, err := p.file.ReadAt(b[:], offset); err != nil {
		return loc, record, errors.Join(ErrCorrupt, err)
	}
	capability, tenant, key, payload, checksum, err := decodeRecordHeader(b[:])
	if err != nil {
		return loc, record, err
	}
	length := int64(recordHeaderSize + capability + tenant + key + payload)
	if length > p.state.End-offset {
		return loc, record, ErrCorrupt
	}
	reader := io.NewSectionReader(p.file, offset+recordHeaderSize, length-recordHeaderSize)
	if materialize {
		body, err := io.ReadAll(reader)
		if err != nil {
			return loc, record, err
		}
		if int64(len(body)) != length-recordHeaderSize || crc32.Checksum(body, crcTable) != checksum {
			return loc, record, ErrCorrupt
		}
		record = Record{CapabilityID: string(body[:capability]), TenantID: string(body[capability : capability+tenant]), Key: body[capability+tenant : capability+tenant+key], Payload: body[capability+tenant+key:]}
		if !capabilityPattern.MatchString(record.CapabilityID) {
			return loc, Record{}, ErrCorrupt
		}
	} else {
		hash := crc32.New(crcTable)
		attribution := make([]byte, capability)
		if _, err := io.ReadFull(reader, attribution); err != nil {
			return loc, record, errors.Join(ErrCorrupt, err)
		}
		if !capabilityPattern.Match(attribution) {
			return loc, record, ErrCorrupt
		}
		_, _ = hash.Write(attribution)
		n, err := io.Copy(hash, reader)
		n += int64(capability)
		if err != nil {
			return loc, record, err
		}
		if n != length-recordHeaderSize || hash.Sum32() != checksum {
			return loc, record, ErrCorrupt
		}
	}
	return Location{Scope: p.header.Scope, Offset: offset, Length: length, CRC32C: checksum}, record, nil
}

func (p *Pack) recover() error {
	info, err := p.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < p.state.End || (p.state.Sealed && info.Size() != p.state.End) {
		return ErrCorrupt
	}
	var count uint64
	for offset := int64(headerSize); offset < p.state.End; {
		loc, _, err := p.readRecord(offset, false)
		if err != nil {
			return fmt.Errorf("committed record at %d: %w", offset, err)
		}
		offset += loc.Length
		count++
	}
	if count != p.state.Count {
		return ErrCorrupt
	}
	if info.Size() > p.state.End {
		if err := p.file.Truncate(p.state.End); err != nil {
			return err
		}
	}
	// Also sync an already-visible checkpoint's data after an uncertain prior
	// attempt. The parent directory is synced by publish before returning ACK.
	return p.file.Sync()
}

func (p *Pack) publish(dir *os.Root, next checkpoint, exclusive bool) error {
	b, err := encodeDocument("DUBIPC01", stateSize, next)
	if err != nil {
		return err
	}
	return publishBytes(dir, p.stateName(), ".pending-", b, exclusive)
}

func (p *Pack) readCheckpoint(dir *os.Root) (checkpoint, error) {
	var saved checkpoint
	file, err := regularFile(dir, p.stateName(), os.O_RDONLY)
	if err != nil {
		return saved, err
	}
	b, err := io.ReadAll(io.LimitReader(file, stateSize+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return saved, errors.Join(err, closeErr)
	}
	if len(b) != stateSize {
		return saved, ErrCorrupt
	}
	err = decodeDocument(b, "DUBIPC01", &saved)
	return saved, err
}

func (p *Pack) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.file.Close()
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "pack_" + hex.EncodeToString(b[:]), nil
}

func openDirectory(root *os.Root, name string) (*os.Root, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("pack path is not a directory")
	}
	return root.OpenRoot(name)
}

func regularFile(root *os.Root, name string, flags int) (*os.File, error) {
	file, err := root.OpenFile(name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(err, errors.New("pack path is not a regular file"), file.Close())
	}
	return file, nil
}

func syncDirectory(root *os.Root) error {
	file, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
