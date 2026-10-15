package volume

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	manifestName = "DUBI_VOLUMES"
	storageName  = "DUBI_STORAGE_ID"
	markerName   = "DUBI_VOLUME"
	lockName     = ".DUBI_VOLUMES.lock"
	maxDocument  = 1 << 20
	maxVolumes   = 1024
)

func randomID(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value[:]), nil
}

func validID(value, prefix string) bool {
	if len(value) != len(prefix)+32 || !strings.HasPrefix(value, prefix) {
		return false
	}
	_, err := hex.DecodeString(value[len(prefix):])
	return err == nil && strings.ToLower(value) == value
}

func readJSON(path string, target any) error {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxDocument {
		return errors.New("invalid storage metadata file")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxDocument+1))
	if err != nil || len(encoded) > maxDocument {
		return errors.Join(err, errors.New("storage metadata exceeds limit"))
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing storage metadata")
	}
	return nil
}

func syncDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

// Atomic publication is also used for registration intents. A marker uses
// link rather than rename so an existing owner can never be overwritten.
func writeJSON(path, prefix string, value any, exclusive bool) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if len(encoded)+1 > maxDocument {
		return errors.New("storage metadata exceeds limit")
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, prefix)
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if exclusive {
		err = os.Link(file.Name(), path)
	} else {
		err = os.Rename(file.Name(), path)
	}
	if err != nil {
		return err
	}
	return syncDirectory(dir)
}

func lockFile(path string, create bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if create {
		flags = unix.O_RDWR | unix.O_CREAT | unix.O_NOFOLLOW | unix.O_CLOEXEC
	}
	fd, err := unix.Open(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("storage lock is not a regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("storage is already in use: %w", err)
	}
	return file, nil
}

func directoryIdentity(path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	if !info.IsDir() {
		return 0, 0, errors.New("volume must be a directory, not a symlink")
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return 0, 0, err
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func canonicalDirectory(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("volume path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, _, err := directoryIdentity(absolute); err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(absolute)
}

func within(parent, path string) bool {
	rel, err := filepath.Rel(parent, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func validatePaths(root string, records []Record) error {
	if len(records) == 0 || len(records) > maxVolumes {
		return errors.New("invalid volume count")
	}
	ids := map[string]bool{}
	for i, record := range records {
		if !validID(record.ID, "volume_") || ids[record.ID] || !filepath.IsAbs(record.Path) || filepath.Clean(record.Path) != record.Path {
			return errors.New("invalid or duplicate volume identity/path")
		}
		ids[record.ID] = true
		if within(record.Path, root) || (within(root, record.Path) && record.Path != filepath.Join(root, "volume")) {
			return errors.New("volume overlaps Node metadata; only <store>/volume may be inside StoreRoot")
		}
		for _, other := range records[:i] {
			if within(record.Path, other.Path) || within(other.Path, record.Path) {
				return errors.New("volume paths overlap")
			}
		}
		if record.State != Registered && record.State != Active && record.State != Draining && record.State != Removed {
			return errors.New("invalid volume lifecycle state")
		}
	}
	return nil
}
