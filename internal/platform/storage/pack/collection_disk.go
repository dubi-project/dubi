package pack

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/protocol"
	"golang.org/x/sys/unix"
)

const packDirectory = "packs"

var ErrLegacyLayout = errors.New("legacy capability-partitioned Pack layout requires explicit migration")

// Do not hide old development data behind a newly initialized flat catalog.
func rejectLegacyLayout(root *os.Root) error {
	file, err := root.Open(".")
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		entries, err := file.ReadDir(128)
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "packs-") || strings.HasPrefix(entry.Name(), collectionMarkerName+"-") {
				return ErrLegacyLayout
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func readDocument(dir *os.Root, name, magic string, limit int64, target any) error {
	file, err := regularFile(dir, name, os.O_RDONLY)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(io.LimitReader(file, limit+1))
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return ErrCorrupt
	}
	return decodeDocument(b, magic, target)
}

func writeDocument(dir *os.Root, name, magic string, value any, exclusive bool) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(jsonBytes)+16 > maxCatalogSize {
		return errors.New("collection metadata exceeds limit")
	}
	b, err := encodeDocument(magic, len(jsonBytes)+16, value)
	if err != nil {
		return err
	}
	return publishBytes(dir, name, ".catalog-", b, exclusive)
}

func publishBytes(dir *os.Root, name, prefix string, b []byte, exclusive bool) error {
	id, err := randomID()
	if err != nil {
		return err
	}
	temp := prefix + id
	file, err := dir.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer dir.Remove(temp)
	if _, err := file.Write(b); err != nil {
		_ = file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if exclusive {
		err = dir.Link(temp, name)
	} else {
		err = dir.Rename(temp, name)
	}
	if err != nil {
		return err
	}
	return syncDirectory(dir)
}

func checkEmptyCollection(dir *os.Root) error {
	file, err := dir.Open(".")
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		entries, err := file.ReadDir(128)
		for _, entry := range entries {
			name := entry.Name()
			if entry.Type().IsRegular() && (name == collectionLockName || (strings.HasPrefix(name, ".catalog-") && idPattern.MatchString(strings.TrimPrefix(name, ".catalog-")))) {
				continue
			}
			return errors.New("new collection directory contains existing data; refusing adoption")
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (m *Collection) initializePending(dir *os.Root, id string) (result error) {
	if _, err := dir.Lstat(id + ".state"); err == nil {
		return nil // Open must validate both files; never rewrite a published checkpoint.
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := regularFile(dir, id+".pack", os.O_RDWR|os.O_CREATE)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > headerSize {
		return errors.Join(ErrCorrupt, errors.New("pending Pack contains unexpected records"))
	}
	h := header{Scope: Scope{StoreID: m.catalog.StoreID, VolumeID: m.catalog.VolumeID, PackID: id}, Version: protocol.PackContainerFormatVersion, TargetSize: m.catalog.TargetSize}
	if info.Size() == headerSize {
		b := make([]byte, headerSize)
		if _, err := file.ReadAt(b, 0); err != nil {
			return err
		}
		var old header
		if err := decodeDocument(b, "DUBIPK02", &old); err == nil && old != h {
			return errors.New("pending Pack has foreign header")
		}
	}
	b, err := encodeDocument("DUBIPK02", headerSize, h)
	if err != nil {
		return err
	}
	if _, err := file.WriteAt(b, 0); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	p := &Pack{header: h}
	return p.publish(dir, checkpoint{PackID: id, End: headerSize}, true)
}
