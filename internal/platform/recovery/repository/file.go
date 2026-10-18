package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	objectsDirectory   = "objects"
	manifestsDirectory = "manifests"
	maxManifestBytes   = 64 << 20
)

// ObjectRef is the content identity recorded by a Recovery Point Manifest.
// The same object ID may be present on multiple node-local file stores.
type ObjectRef struct {
	ID   string `json:"id"`
	Size int64  `json:"size"`
}

// Artifact is a module-neutral physical resource captured by a Recovery
// Point. Kind/Format are descriptive boundaries owned by the registering
// platform component; the repository never decodes the bytes.
type Artifact struct {
	Name    string    `json:"name"`
	Kind    string    `json:"kind"`
	NodeID  string    `json:"node_id,omitempty"`
	StoreID string    `json:"store_id,omitempty"`
	Format  string    `json:"format,omitempty"`
	Object  ObjectRef `json:"object"`
}

// Manifest is the repository-level immutable commit record. Higher layers
// populate the opaque resource list; this package only persists and verifies
// the manifest envelope.
type Manifest struct {
	Version    int         `json:"version"`
	Repository string      `json:"repository"`
	ClusterID  string      `json:"cluster_id"`
	Epoch      uint64      `json:"epoch"`
	CreatedAt  time.Time   `json:"created_at"`
	Objects    []ObjectRef `json:"objects"`
	Artifacts  []Artifact  `json:"artifacts,omitempty"`
}

func (m Manifest) Validate() error {
	if m.Version != 1 || ValidateName(m.Repository) != nil || m.ClusterID == "" || m.Epoch == 0 || m.CreatedAt.IsZero() {
		return errors.New("invalid Recovery Point Manifest")
	}
	seen := make(map[string]int64, len(m.Objects))
	for _, object := range m.Objects {
		if !validObjectID(object.ID) || object.Size < 0 {
			return errors.New("invalid Recovery Point object")
		}
		if _, found := seen[object.ID]; found {
			return errors.New("duplicate Recovery Point object")
		}
		seen[object.ID] = object.Size
	}
	artifactNames := make(map[string]struct{}, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		if !validArtifactName(artifact.Name) || !validArtifactName(artifact.Kind) || !validObjectID(artifact.Object.ID) || artifact.Object.Size < 0 {
			return errors.New("invalid Recovery Point artifact")
		}
		artifactKey := artifact.NodeID + "\x00" + artifact.Name
		if _, found := artifactNames[artifactKey]; found {
			return errors.New("duplicate Recovery Point artifact")
		}
		artifactNames[artifactKey] = struct{}{}
		if size, found := seen[artifact.Object.ID]; !found || size != artifact.Object.Size {
			return errors.New("Recovery Point artifact references an unknown object")
		}
	}
	return nil
}

func validArtifactName(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 512 || strings.ContainsRune(value, '\x00') {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

type FileStore struct {
	root string
}

// ObjectStore is the backend-neutral contract consumed by recovery workers.
// FileStore is the first implementation; S3 uses the same immutable object
// and manifest semantics without changing the backup planner.
type ObjectStore interface {
	PutObject(context.Context, io.Reader) (ObjectRef, error)
	HasObject(string) (bool, error)
	OpenObject(string) (io.ReadCloser, error)
	CommitManifest(context.Context, Manifest) error
	ReadManifest(uint64) (Manifest, error)
}

var _ ObjectStore = (*FileStore)(nil)

// OpenFile creates or opens a node-local file Repository. The supplied path is
// interpreted on the current Node. A shared filesystem is not required; each
// Node may use the same configured path on its own local backup disk.
func OpenFile(value Repository) (*FileStore, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	if !value.IsFile() {
		return nil, errors.New("file store requires a file Repository")
	}
	for _, directory := range []string{value.Path, filepath.Join(value.Path, objectsDirectory), filepath.Join(value.Path, manifestsDirectory), filepath.Join(value.Path, shardsDirectory)} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return nil, fmt.Errorf("create Repository directory %q: %w", directory, err)
		}
	}
	return &FileStore{root: value.Path}, nil
}

func (s *FileStore) Root() string {
	if s == nil {
		return ""
	}
	return s.root
}

// PutObject writes an immutable content-addressed object. Existing identical
// objects are reused, which gives every Recovery Point logical-full and
// physical-incremental behavior without a delta chain.
func (s *FileStore) PutObject(ctx context.Context, source io.Reader) (ObjectRef, error) {
	if s == nil || s.root == "" || source == nil {
		return ObjectRef{}, errors.New("Repository object store is not ready")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	temporary, err := os.CreateTemp(filepath.Join(s.root, objectsDirectory), ".upload-")
	if err != nil {
		return ObjectRef{}, err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	written, copyErr := copyWithContext(ctx, io.MultiWriter(temporary, hash), source)
	if closeErr := temporary.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return ObjectRef{}, copyErr
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	path := s.objectPath(digest)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return ObjectRef{}, err
	}
	if _, err := os.Stat(path); err == nil {
		return s.reuseObject(digest, written)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ObjectRef{}, err
	}
	if err := syncFile(temporaryName); err != nil {
		return ObjectRef{}, err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return s.reuseObject(digest, written)
		}
		return ObjectRef{}, err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return ObjectRef{}, err
	}
	return ObjectRef{ID: digest, Size: written}, nil
}

func (s *FileStore) reuseObject(id string, expectedSize int64) (ObjectRef, error) {
	info, err := os.Stat(s.objectPath(id))
	if err != nil {
		return ObjectRef{}, err
	}
	if info.Size() != expectedSize {
		return ObjectRef{}, fmt.Errorf("existing Repository object %s size mismatch", id)
	}
	actual, err := s.objectDigest(id)
	if err != nil {
		return ObjectRef{}, err
	}
	if actual != id {
		return ObjectRef{}, fmt.Errorf("existing Repository object %s hash mismatch", id)
	}
	return ObjectRef{ID: id, Size: expectedSize}, nil
}

func (s *FileStore) HasObject(id string) (bool, error) {
	path, err := s.validObjectPath(id)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (s *FileStore) OpenObject(id string) (io.ReadCloser, error) {
	path, err := s.validObjectPath(id)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// CommitManifest validates and atomically publishes one immutable Recovery
// Point. A manifest is not considered usable until the .complete marker is
// synced after the JSON file.
func (s *FileStore) CommitManifest(ctx context.Context, manifest Manifest) error {
	if s == nil || s.root == "" {
		return errors.New("Repository object store is not ready")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	for _, object := range manifest.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		found, err := s.HasObject(object.ID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("Recovery Point object %s is missing", object.ID)
		}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if len(encoded) > maxManifestBytes {
		return errors.New("Recovery Point Manifest is too large")
	}
	base := filepath.Join(s.root, manifestsDirectory, fmt.Sprintf("%020d", manifest.Epoch))
	manifestPath := base + ".json"
	markerPath := base + ".complete"
	if _, err := os.Stat(markerPath); err == nil {
		return errors.New("Recovery Point already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	manifestPresent := false
	if existing, err := os.ReadFile(manifestPath); err == nil {
		manifestPresent = true
		if !bytes.Equal(existing, encoded) {
			return errors.New("Recovery Point has an incomplete different manifest")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !manifestPresent {
		temporary, err := os.CreateTemp(filepath.Join(s.root, manifestsDirectory), ".manifest-")
		if err != nil {
			return err
		}
		temporaryName := temporary.Name()
		defer os.Remove(temporaryName)
		if _, err := temporary.Write(encoded); err != nil {
			_ = temporary.Close()
			return err
		}
		if err := temporary.Sync(); err != nil {
			_ = temporary.Close()
			return err
		}
		if err := temporary.Close(); err != nil {
			return err
		}
		// A hard link publishes the already-synced temporary inode without
		// replacing a manifest another worker may have claimed concurrently.
		if err := os.Link(temporaryName, manifestPath); err != nil {
			if errors.Is(err, os.ErrExist) {
				existing, readErr := os.ReadFile(manifestPath)
				if readErr != nil || !bytes.Equal(existing, encoded) {
					return errors.New("Recovery Point has an incomplete different manifest")
				}
			} else {
				return err
			}
		}
	}
	if err := syncDirectory(filepath.Dir(manifestPath)); err != nil {
		return err
	}
	marker, err := os.OpenFile(markerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("Recovery Point already exists")
		}
		return err
	}
	if _, err := marker.WriteString("complete\n"); err != nil {
		_ = marker.Close()
		return err
	}
	if err := marker.Sync(); err != nil {
		_ = marker.Close()
		return err
	}
	if err := marker.Close(); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(marker.Name()))
}

func (s *FileStore) ReadManifest(epoch uint64) (Manifest, error) {
	if s == nil || s.root == "" || epoch == 0 {
		return Manifest{}, errors.New("invalid Recovery Point")
	}
	base := filepath.Join(s.root, manifestsDirectory, fmt.Sprintf("%020d", epoch))
	if _, err := os.Stat(base + ".complete"); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Manifest{}, errors.New("Recovery Point is not complete")
		}
		return Manifest{}, err
	}
	encoded, err := os.ReadFile(base + ".json")
	if err != nil {
		return Manifest{}, err
	}
	if len(encoded) > maxManifestBytes {
		return Manifest{}, errors.New("Recovery Point Manifest is too large")
	}
	var manifest Manifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return Manifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	for _, object := range manifest.Objects {
		info, err := os.Stat(s.objectPath(object.ID))
		if err != nil {
			return Manifest{}, err
		}
		if info.Size() != object.Size {
			return Manifest{}, fmt.Errorf("Recovery Point object %s size mismatch", object.ID)
		}
		actual, err := s.objectDigest(object.ID)
		if err != nil {
			return Manifest{}, fmt.Errorf("verify Recovery Point object %s: %w", object.ID, err)
		}
		if actual != object.ID {
			return Manifest{}, fmt.Errorf("Recovery Point object %s hash mismatch", object.ID)
		}
	}
	return manifest, nil
}

// ListManifests returns only complete, individually verified Recovery Points
// in creation order. A complete marker with corrupt JSON or objects is an
// integrity error rather than an entry to silently skip.
func (s *FileStore) ListManifests() ([]Manifest, error) {
	if s == nil || s.root == "" {
		return nil, errors.New("Repository object store is not ready")
	}
	entries, err := os.ReadDir(filepath.Join(s.root, manifestsDirectory))
	if err != nil {
		return nil, err
	}
	result := make([]Manifest, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".complete") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".complete")
		if len(base) != 20 {
			return nil, errors.New("invalid Recovery Point manifest name")
		}
		epoch, err := parseDecimalEpoch(base)
		if err != nil {
			return nil, err
		}
		manifest, err := s.ReadManifest(epoch)
		if err != nil {
			return nil, err
		}
		result = append(result, manifest)
	}
	slices.SortFunc(result, func(left, right Manifest) int {
		if left.Epoch < right.Epoch {
			return -1
		}
		if left.Epoch > right.Epoch {
			return 1
		}
		return 0
	})
	return result, nil
}

func (s *FileStore) objectDigest(id string) (string, error) {
	reader, err := s.OpenObject(id)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (s *FileStore) objectPath(id string) string {
	return filepath.Join(s.root, objectsDirectory, id[:2], id)
}

func (s *FileStore) validObjectPath(id string) (string, error) {
	if s == nil || s.root == "" || !validObjectID(id) {
		return "", errors.New("invalid Repository object ID")
	}
	return s.objectPath(id), nil
}

func validObjectID(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && !strings.Contains(value, "..")
}

func parseDecimalEpoch(value string) (uint64, error) {
	epoch, err := strconv.ParseUint(value, 10, 64)
	if err != nil || epoch == 0 {
		return 0, errors.New("invalid Recovery Point epoch")
	}
	return epoch, nil
}

func copyWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 256<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		count, readErr := source.Read(buffer)
		if count > 0 {
			written, writeErr := destination.Write(buffer[:count])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != count {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, readErr
		}
	}
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
