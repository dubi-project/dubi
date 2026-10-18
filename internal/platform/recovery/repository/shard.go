package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"
)

const (
	shardsDirectory = "shards"
	maxShardBytes   = 64 << 20
)

var shardPartPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,255}$`)

// Shard is the immutable output of one node-local backup worker. It records
// which opaque objects that worker uploaded, but is not itself a Recovery
// Point until the cluster coordinator merges all required shards.
type Shard struct {
	Version    int         `json:"version"`
	JobID      string      `json:"job_id"`
	Repository string      `json:"repository"`
	ClusterID  string      `json:"cluster_id"`
	Epoch      uint64      `json:"epoch"`
	CreatedAt  time.Time   `json:"created_at"`
	NodeID     string      `json:"node_id"`
	StoreID    string      `json:"store_id"`
	Objects    []ObjectRef `json:"objects"`
	Artifacts  []Artifact  `json:"artifacts,omitempty"`
}

func (s Shard) Validate() error {
	if s.Version != 1 || !validShardPart(s.JobID) || ValidateName(s.Repository) != nil || s.ClusterID == "" || s.Epoch == 0 || s.CreatedAt.IsZero() || !validShardPart(s.NodeID) || !validShardPart(s.StoreID) {
		return errors.New("invalid backup shard")
	}
	seen := make(map[string]int64, len(s.Objects))
	for _, object := range s.Objects {
		if !validObjectID(object.ID) || object.Size < 0 {
			return errors.New("invalid backup shard object")
		}
		if _, found := seen[object.ID]; found {
			return errors.New("duplicate backup shard object")
		}
		seen[object.ID] = object.Size
	}
	artifactNames := make(map[string]struct{}, len(s.Artifacts))
	for _, artifact := range s.Artifacts {
		if !validArtifactName(artifact.Name) || !validArtifactName(artifact.Kind) || !validObjectID(artifact.Object.ID) || artifact.Object.Size < 0 {
			return errors.New("invalid backup shard artifact")
		}
		key := artifact.NodeID + "\x00" + artifact.Name
		if _, found := artifactNames[key]; found {
			return errors.New("duplicate backup shard artifact")
		}
		artifactNames[key] = struct{}{}
		if size, found := seen[artifact.Object.ID]; !found || size != artifact.Object.Size {
			return errors.New("backup shard artifact references an unknown object")
		}
	}
	return nil
}

// ShardStore extends ObjectStore with the node-worker handoff record. A
// future S3 backend can implement the same contract without changing the
// backup planner or the Root job state machine.
type ShardStore interface {
	ObjectStore
	CommitShard(context.Context, Shard) error
	ReadShard(string, string) (Shard, error)
}

// ListShards returns complete and verified node shards for one job. It is
// useful to a coordinator reading a node-local file Repository after receiving
// the node's authenticated inventory; it never treats an incomplete record as
// a successful worker result.
func (s *FileStore) ListShards(jobID string) ([]Shard, error) {
	if s == nil || s.root == "" || !validShardPart(jobID) {
		return nil, errors.New("invalid backup shard job ID")
	}
	directory := filepath.Join(s.root, shardsDirectory, jobID)
	entries, err := os.ReadDir(directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	result := make([]Shard, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		nodeID := entry.Name()
		if !validShardPart(nodeID) {
			return nil, errors.New("invalid backup shard node directory")
		}
		shard, err := s.ReadShard(jobID, nodeID)
		if err != nil {
			return nil, err
		}
		result = append(result, shard)
	}
	slices.SortFunc(result, func(left, right Shard) int {
		if left.NodeID < right.NodeID {
			return -1
		}
		if left.NodeID > right.NodeID {
			return 1
		}
		return 0
	})
	return result, nil
}

func validShardPart(value string) bool { return shardPartPattern.MatchString(value) }

func (s *FileStore) CommitShard(ctx context.Context, shard Shard) error {
	if s == nil || s.root == "" {
		return errors.New("Repository object store is not ready")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := shard.Validate(); err != nil {
		return err
	}
	for _, object := range shard.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		found, err := s.HasObject(object.ID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("backup shard object %s is missing", object.ID)
		}
	}
	encoded, err := json.Marshal(shard)
	if err != nil {
		return err
	}
	if len(encoded) > maxShardBytes {
		return errors.New("backup shard is too large")
	}
	base, err := s.shardBase(shard.JobID, shard.NodeID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(base), 0o750); err != nil {
		return err
	}
	manifestPath := base + ".json"
	markerPath := base + ".complete"
	if _, err := os.Stat(markerPath); err == nil {
		return errors.New("backup shard already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	present := false
	if existing, err := os.ReadFile(manifestPath); err == nil {
		present = true
		if !bytes.Equal(existing, encoded) {
			return errors.New("backup shard has an incomplete different record")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !present {
		temporary, err := os.CreateTemp(filepath.Dir(manifestPath), ".shard-")
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
		if err := os.Link(temporaryName, manifestPath); err != nil {
			if errors.Is(err, os.ErrExist) {
				existing, readErr := os.ReadFile(manifestPath)
				if readErr != nil || !bytes.Equal(existing, encoded) {
					return errors.New("backup shard has an incomplete different record")
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
			return errors.New("backup shard already exists")
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

func (s *FileStore) ReadShard(jobID, nodeID string) (Shard, error) {
	if s == nil || s.root == "" || !validShardPart(jobID) || !validShardPart(nodeID) {
		return Shard{}, errors.New("invalid backup shard")
	}
	base, err := s.shardBase(jobID, nodeID)
	if err != nil {
		return Shard{}, err
	}
	if _, err := os.Stat(base + ".complete"); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Shard{}, errors.New("backup shard is not complete")
		}
		return Shard{}, err
	}
	encoded, err := os.ReadFile(base + ".json")
	if err != nil {
		return Shard{}, err
	}
	if len(encoded) > maxShardBytes {
		return Shard{}, errors.New("backup shard is too large")
	}
	var shard Shard
	if err := json.Unmarshal(encoded, &shard); err != nil {
		return Shard{}, err
	}
	if err := shard.Validate(); err != nil {
		return Shard{}, err
	}
	if shard.JobID != jobID || shard.NodeID != nodeID {
		return Shard{}, errors.New("backup shard path does not match record")
	}
	for _, object := range shard.Objects {
		found, err := s.HasObject(object.ID)
		if err != nil {
			return Shard{}, err
		}
		if !found {
			return Shard{}, fmt.Errorf("backup shard object %s is missing", object.ID)
		}
	}
	return shard, nil
}

func (s *FileStore) shardBase(jobID, nodeID string) (string, error) {
	if s == nil || s.root == "" || !validShardPart(jobID) || !validShardPart(nodeID) {
		return "", errors.New("invalid backup shard path")
	}
	return filepath.Join(s.root, shardsDirectory, jobID, nodeID), nil
}

var _ ShardStore = (*FileStore)(nil)
