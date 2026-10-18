package recovery

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

// TestRestoreClusterRealAcceptance replays a real multi-node file Repository
// when DUBI_RESTORE_ACCEPTANCE_ROOT is set. It is intentionally opt-in: the
// fixture is an external VPS export, not a repository test fixture. The test
// exercises the same preflight and streaming path used by a new-cluster
// restore, including every node Manifest and every content-addressed object.
func TestRestoreClusterRealAcceptance(t *testing.T) {
	root := strings.TrimSpace(os.Getenv("DUBI_RESTORE_ACCEPTANCE_ROOT"))
	if root == "" {
		t.Skip("set DUBI_RESTORE_ACCEPTANCE_ROOT to run the external Repository acceptance")
	}
	epochValue := strings.TrimSpace(os.Getenv("DUBI_RESTORE_ACCEPTANCE_EPOCH"))
	if epochValue == "" {
		t.Fatal("DUBI_RESTORE_ACCEPTANCE_EPOCH is required with DUBI_RESTORE_ACCEPTANCE_ROOT")
	}
	epoch, err := strconv.ParseUint(epochValue, 10, 64)
	if err != nil || epoch == 0 {
		t.Fatalf("invalid DUBI_RESTORE_ACCEPTANCE_EPOCH %q", epochValue)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	stores := make(map[string]repository.ObjectStore)
	manifests := make(map[string]repository.Manifest)
	var point repository.RecoveryPoint
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		store, openErr := repository.OpenFile(repository.Repository{Name: "acceptance-" + entry.Name(), Type: repository.TypeFile, Path: path})
		if openErr != nil {
			t.Fatalf("open Repository %s: %v", entry.Name(), openErr)
		}
		candidate, readErr := store.ReadRecoveryPoint(epoch)
		if readErr != nil {
			t.Fatalf("read Recovery Point from %s: %v", entry.Name(), readErr)
		}
		manifest, manifestErr := store.ReadManifest(epoch)
		if manifestErr != nil {
			t.Fatalf("read Manifest from %s: %v", entry.Name(), manifestErr)
		}
		if len(manifest.Artifacts) == 0 {
			t.Fatalf("Manifest from %s has no artifacts", entry.Name())
		}
		nodeID := manifest.Artifacts[0].NodeID
		if nodeID == "" {
			t.Fatalf("Manifest from %s has no source Node", entry.Name())
		}
		for _, artifact := range manifest.Artifacts {
			if artifact.NodeID != nodeID {
				t.Fatalf("Manifest from %s mixes source Nodes %q and %q", entry.Name(), nodeID, artifact.NodeID)
			}
		}
		if _, found := stores[nodeID]; found {
			t.Fatalf("source Node %s has more than one Repository directory", nodeID)
		}
		if len(stores) == 0 {
			point = candidate
		} else if !sameRecoveryPointTopology(point, candidate) {
			t.Fatalf("Recovery Point topology differs at Repository %s", entry.Name())
		}
		stores[nodeID] = store
		manifests[nodeID] = manifest
	}
	if len(stores) != len(point.Nodes) {
		t.Fatalf("Recovery Point lists %d Nodes but opened %d local stores", len(point.Nodes), len(stores))
	}

	targets := make([]ClusterTarget, 0)
	for nodeID, manifest := range manifests {
		for _, artifact := range manifest.Artifacts {
			target := artifact
			targets = append(targets, ClusterTarget{NodeID: nodeID, Name: target.Name, Apply: func(ctx context.Context, reader io.Reader, size int64) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				written, err := io.Copy(io.Discard, reader)
				if err != nil {
					return err
				}
				if written != size {
					return fmt.Errorf("artifact consumed %d bytes, expected %d", written, size)
				}
				return nil
			}})
		}
	}
	got, err := RestoreCluster(context.Background(), point, func(_ context.Context, nodeID string) (repository.ObjectStore, error) {
		store, found := stores[nodeID]
		if !found {
			return nil, fmt.Errorf("source Node %s is not present in the acceptance Repository", nodeID)
		}
		return store, nil
	}, targets)
	if err != nil {
		t.Fatal("real multi-node restore preflight: ", err)
	}
	if len(got) != len(point.Nodes) {
		t.Fatalf("restored Manifest count = %d, want %d", len(got), len(point.Nodes))
	}
}

func sameRecoveryPointTopology(left, right repository.RecoveryPoint) bool {
	if left.Version != right.Version || left.Repository != right.Repository || left.ClusterID != right.ClusterID || left.Epoch != right.Epoch || len(left.Nodes) != len(right.Nodes) {
		return false
	}
	for index := range left.Nodes {
		if left.Nodes[index].NodeID != right.Nodes[index].NodeID {
			return false
		}
	}
	return true
}
