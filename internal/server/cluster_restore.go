package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/recovery"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

// RuntimeRestoreTarget binds one new Node identity to its already bootstrapped
// Runtime. The caller must start these Runtimes in the destination cluster
// before invoking RestoreClusterToRuntimes.
type RuntimeRestoreTarget struct {
	NodeID  string
	Runtime *node.Runtime
}

// RestoreClusterToRuntimes performs the first complete new-cluster restore
// orchestration. It intentionally keeps Repository access and topology
// mapping outside the storage packages:
//
//  1. read the indexed Root image and pair Nodes by Region;
//  2. preflight every source Manifest/object through RestoreCluster;
//  3. import Pack and records artifacts under new Store/Volume identities;
//  4. install the rewritten Root snapshot on every destination Node;
//  5. install rewritten Engine snapshots after Root has opened membership.
//
// A failed preflight invokes no target. A failure during installation is
// returned with the destination Node and artifact context; rerunning is safe
// for Pack and records imports and uses the Raft snapshot idempotency checks.
func RestoreClusterToRuntimes(ctx context.Context, point repository.RecoveryPoint, open recovery.NodeStore, targets []RuntimeRestoreTarget) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(targets) == 0 {
		return errors.New("new-cluster restore requires destination Runtimes")
	}
	destinations := make(map[string]*node.Runtime, len(targets))
	var destinationNodes []rootstate.Node
	for _, target := range targets {
		if target.NodeID == "" || target.Runtime == nil || target.Runtime.Identity.NodeID != target.NodeID || target.Runtime.Root == nil || target.Runtime.Host == nil || target.Runtime.Packs == nil {
			return errors.New("invalid destination Runtime restore target")
		}
		if _, exists := destinations[target.NodeID]; exists {
			return fmt.Errorf("duplicate destination Node %s", target.NodeID)
		}
		destinations[target.NodeID] = target.Runtime
		if destinationNodes == nil {
			nodes, err := target.Runtime.Root.Nodes()
			if err != nil {
				return fmt.Errorf("read destination Root Nodes: %w", err)
			}
			destinationNodes = nodes
		}
	}

	rootImage, rootName, err := readIndexedRootArtifact(ctx, point, open)
	if err != nil {
		return err
	}
	if err := validateIndexedRootArtifacts(ctx, point, open); err != nil {
		return err
	}
	identityMap, err := rootstate.BuildRestoreIdentityMap(rootImage, destinationNodes)
	if err != nil {
		return err
	}
	rootIndex, rootTerm, err := snapshotPosition(rootName, "root/state")
	if err != nil {
		return err
	}
	if metadataIndex, _, metadataErr := rootstate.SnapshotMetadata(rootImage); metadataErr != nil || metadataIndex != rootIndex {
		return errors.Join(metadataErr, errors.New("Root snapshot name and image index disagree"))
	}

	// Build source-to-destination placement maps before any target callback can
	// mutate a destination. Volumes are paired deterministically; when the new
	// Node has fewer Volumes, all old Volumes map to the first healthy target.
	placements := make(map[string]node.RestorePlacement, len(point.Nodes))
	for _, source := range point.Nodes {
		destination, found := identityMap.Nodes[source.NodeID]
		if !found {
			return fmt.Errorf("no destination Node for source %s", source.NodeID)
		}
		runtime := destinations[destination.NodeID]
		mapping, err := buildVolumeMapping(runtime, source.NodeID, open, point.Epoch)
		if err != nil {
			return err
		}
		sourceNodeID := source.NodeID
		destinationRuntime := runtime
		volumeMapping := mapping
		placements[source.NodeID] = node.RestorePlacement{
			Volume: func(old string) (string, error) {
				value, ok := volumeMapping[old]
				if !ok {
					return "", fmt.Errorf("source Volume %s has no destination mapping on Node %s", old, sourceNodeID)
				}
				return value, nil
			},
			Location: func(location pack.Location) (pack.Location, error) {
				volumeID, ok := volumeMapping[location.VolumeID]
				if !ok {
					return pack.Location{}, fmt.Errorf("source Volume %s has no destination mapping on Node %s", location.VolumeID, sourceNodeID)
				}
				location.StoreID = destinationRuntime.Packs.StoreID()
				location.VolumeID = volumeID
				return location, nil
			},
		}
	}

	rootInstalled := false
	pendingEngines := make([]pendingEngineRestore, 0)
	pendingRecords := make([]pendingRecordRestore, 0)
	targetsForRecovery := make([]recovery.ClusterTarget, 0)
	for _, source := range point.Nodes {
		destination := identityMap.Nodes[source.NodeID]
		runtime := destinations[destination.NodeID]
		manifestStore, err := open(ctx, source.NodeID)
		if err != nil {
			return err
		}
		manifest, err := manifestStore.ReadManifest(point.Epoch)
		if err != nil {
			return err
		}
		for _, artifact := range manifest.Artifacts {
			artifact := artifact
			sourceID := source.NodeID
			targetsForRecovery = append(targetsForRecovery, recovery.ClusterTarget{NodeID: sourceID, Name: artifact.Name,
				Apply: func(applyCtx context.Context, reader io.Reader, size int64) error {
					switch artifact.Kind {
					case "pack":
						if artifact.Format != "DUBIPK02" {
							return fmt.Errorf("unsupported Pack artifact format %q", artifact.Format)
						}
						return runtime.ImportRestoreArtifact(applyCtx, artifact.Name, artifact.Kind, reader, size, placements[sourceID])
					case "records-index":
						if artifact.Format != "DUBI_RECORD_INDEX_V1" {
							return fmt.Errorf("unsupported records index artifact format %q", artifact.Format)
						}
						image, readErr := io.ReadAll(io.LimitReader(reader, size+1))
						if readErr != nil || int64(len(image)) != size {
							return errors.Join(readErr, errors.New("records index restore artifact size differs from Manifest"))
						}
						pendingRecords = append(pendingRecords, pendingRecordRestore{runtime: runtime, placement: placements[sourceID], data: image})
						return nil
					case "root-state":
						if artifact.Format != "DUBI_ROOT_STATE_V2" {
							return fmt.Errorf("unsupported Root artifact format %q", artifact.Format)
						}
						// Workers capture the same cluster checkpoint at slightly
						// different local Root apply positions. The highest valid Root
						// image selected above is authoritative; the other images were
						// already integrity-checked during preflight and are not
						// installed a second time.
						if artifact.Name != rootName {
							_, drainErr := io.Copy(io.Discard, reader)
							return drainErr
						}
						if rootInstalled {
							_, drainErr := io.Copy(io.Discard, reader)
							return drainErr
						}
						image, readErr := io.ReadAll(io.LimitReader(reader, size+1))
						if readErr != nil || int64(len(image)) != size {
							return errors.Join(readErr, errors.New("Root restore artifact size differs from Manifest"))
						}
						for destinationID, destinationRuntime := range destinations {
							conf, confErr := destinationRuntime.Host.Membership(raft.RootGroupID)
							if confErr != nil {
								return fmt.Errorf("read destination Root membership on Node %s: %w", destinationID, confErr)
							}
							rewritten, rewriteErr := rootstate.RewriteSnapshotForRestore(image, identityMap, conf)
							if rewriteErr != nil {
								return rewriteErr
							}
							if installErr := destinationRuntime.InstallRestoreSnapshot(applyCtx, raft.RootGroupID, rootIndex, rootTerm, rewritten, conf); installErr != nil {
								return fmt.Errorf("install Root snapshot on Node %s: %w", destinationID, installErr)
							}
						}
						rootInstalled = true
						return nil
					case "engine-state":
						if artifact.Format != "DUBI_ENGINE_STATE_V8" {
							return fmt.Errorf("unsupported Engine artifact format %q", artifact.Format)
						}
						image, readErr := io.ReadAll(io.LimitReader(reader, size+1))
						if readErr != nil || int64(len(image)) != size {
							return errors.Join(readErr, errors.New("Engine restore artifact size differs from Manifest"))
						}
						pendingEngines = append(pendingEngines, pendingEngineRestore{runtime: runtime, name: artifact.Name, data: image, identity: identityMap})
						return nil
					default:
						return fmt.Errorf("unsupported restore artifact kind %q", artifact.Kind)
					}
				},
			})
		}
	}
	if _, err := recovery.RestoreCluster(ctx, point, open, targetsForRecovery); err != nil {
		return err
	}
	if !rootInstalled {
		return errors.New("Recovery Point has no Root state artifact")
	}
	for _, pending := range pendingRecords {
		if err := pending.runtime.RestoreRecordIndex(ctx, pending.data, pending.placement.Location); err != nil {
			return fmt.Errorf("restore immutable records index: %w", err)
		}
	}
	for _, pending := range pendingEngines {
		if err := installPendingEngine(ctx, pending); err != nil {
			return err
		}
	}
	return nil
}

type pendingEngineRestore struct {
	runtime  *node.Runtime
	name     string
	data     []byte
	identity rootstate.RestoreIdentityMap
}

type pendingRecordRestore struct {
	runtime   *node.Runtime
	placement node.RestorePlacement
	data      []byte
}

func installPendingEngine(ctx context.Context, pending pendingEngineRestore) error {
	groupID, index, _, err := enginestate.SnapshotMetadata(pending.data)
	if err != nil {
		return err
	}
	parts := strings.Split(pending.name, "/")
	if len(parts) != 6 || parts[0] != "engine" || parts[1] != "group" || parts[3] != "state" {
		return errors.New("invalid Engine restore artifact name")
	}
	term, parseErr := strconv.ParseUint(parts[5], 10, 64)
	if parseErr != nil || parts[2] != groupID || index == 0 || term == 0 {
		return errors.New("Engine restore artifact name and image disagree")
	}
	if !pending.runtime.Host.HasGroup(groupID) {
		if err := pending.runtime.RecoverLocalGroups(ctx); err != nil {
			return fmt.Errorf("open restored Engine Group %s: %w", groupID, err)
		}
	}
	destinationConf, err := pending.runtime.Host.Membership(groupID)
	if err != nil {
		return err
	}
	rewritten, err := enginestate.RewriteSnapshotForRestoreWithIdentity(pending.data, destinationConf, pending.identity)
	if err != nil {
		return err
	}
	if err := pending.runtime.InstallRestoreSnapshot(ctx, groupID, index, term, rewritten, destinationConf); err != nil {
		return fmt.Errorf("install Engine Group %s snapshot: %w", groupID, err)
	}
	return nil
}

func readIndexedRootArtifact(ctx context.Context, point repository.RecoveryPoint, open recovery.NodeStore) ([]byte, string, error) {
	if err := point.Validate(); err != nil {
		return nil, "", err
	}
	if len(point.Nodes) == 0 {
		return nil, "", errors.New("Recovery Point has no source Nodes")
	}
	var selected []byte
	selectedName := ""
	var selectedIndex uint64
	for _, source := range point.Nodes {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		store, err := open(ctx, source.NodeID)
		if err != nil {
			return nil, "", err
		}
		manifest, err := store.ReadManifest(point.Epoch)
		if err != nil {
			return nil, "", err
		}
		found := false
		for _, artifact := range manifest.Artifacts {
			if artifact.Kind != "root-state" {
				continue
			}
			found = true
			index, _, err := snapshotPosition(artifact.Name, "root/state")
			if err != nil {
				return nil, "", err
			}
			reader, err := store.OpenObject(artifact.Object.ID)
			if err != nil {
				return nil, "", err
			}
			image, readErr := io.ReadAll(io.LimitReader(reader, artifact.Object.Size+1))
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || int64(len(image)) != artifact.Object.Size {
				return nil, "", errors.Join(readErr, closeErr, errors.New("Root artifact size differs from Manifest"))
			}
			metadataIndex, _, metadataErr := rootstate.SnapshotMetadata(image)
			if metadataErr != nil || metadataIndex != index {
				return nil, "", errors.Join(metadataErr, errors.New("Root snapshot name and image index disagree"))
			}
			if selected == nil || index > selectedIndex || (index == selectedIndex && artifact.Name > selectedName) {
				selected, selectedName, selectedIndex = image, artifact.Name, index
			}
		}
		if !found {
			return nil, "", fmt.Errorf("source Node %s has no Root state artifact", source.NodeID)
		}
	}
	if selected == nil {
		return nil, "", errors.New("Recovery Point has no Root state artifact")
	}
	return selected, selectedName, nil
}

func validateIndexedRootArtifacts(ctx context.Context, point repository.RecoveryPoint, open recovery.NodeStore) error {
	for _, source := range point.Nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		store, err := open(ctx, source.NodeID)
		if err != nil {
			return err
		}
		manifest, err := store.ReadManifest(point.Epoch)
		if err != nil {
			return err
		}
		found := false
		for _, artifact := range manifest.Artifacts {
			if artifact.Kind != "root-state" {
				continue
			}
			found = true
			index, _, err := snapshotPosition(artifact.Name, "root/state")
			if err != nil {
				return err
			}
			reader, err := store.OpenObject(artifact.Object.ID)
			if err != nil {
				return err
			}
			image, readErr := io.ReadAll(io.LimitReader(reader, artifact.Object.Size+1))
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || int64(len(image)) != artifact.Object.Size {
				return errors.Join(readErr, closeErr, errors.New("Root artifact size differs from Manifest"))
			}
			metadataIndex, _, metadataErr := rootstate.SnapshotMetadata(image)
			if metadataErr != nil || metadataIndex != index {
				return errors.Join(metadataErr, errors.New("Root snapshot name and image index disagree"))
			}
		}
		if !found {
			return fmt.Errorf("source Node %s has no Root state artifact", source.NodeID)
		}
	}
	return nil
}

func snapshotPosition(name, prefix string) (uint64, uint64, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0]+"/"+parts[1] != prefix {
		return 0, 0, fmt.Errorf("invalid %s snapshot artifact name", prefix)
	}
	index, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil || index == 0 {
		return 0, 0, errors.New("invalid snapshot index")
	}
	term, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || term == 0 {
		return 0, 0, errors.New("invalid snapshot term")
	}
	return index, term, nil
}

func buildVolumeMapping(runtime *node.Runtime, sourceNodeID string, open recovery.NodeStore, epoch uint64) (map[string]string, error) {
	if runtime == nil || runtime.Volumes == nil {
		return nil, fmt.Errorf("destination Node %s has no Volume registry", sourceNodeID)
	}
	report, err := runtime.Volumes.Status()
	if err != nil {
		return nil, err
	}
	dest := make([]string, 0, len(report.Volumes))
	for _, item := range report.Volumes {
		if item.State == volume.Active && item.Health == "healthy" {
			dest = append(dest, item.ID)
		}
	}
	if len(dest) == 0 {
		return nil, fmt.Errorf("destination Node %s has no healthy Volume", sourceNodeID)
	}
	sort.Strings(dest)
	// Extract old Volume IDs from the source Manifest. The mapping is only
	// physical placement; all logical keys remain in the records index.
	store, err := open(context.Background(), sourceNodeID)
	if err != nil {
		return nil, err
	}
	manifest, err := store.ReadManifest(epoch)
	if err != nil {
		return nil, err
	}
	oldSet := make(map[string]struct{})
	for _, artifact := range manifest.Artifacts {
		parts := strings.Split(artifact.Name, "/")
		if artifact.Kind == "pack" && len(parts) == 4 && parts[0] == "volume" {
			oldSet[parts[1]] = struct{}{}
		}
	}
	old := make([]string, 0, len(oldSet))
	for id := range oldSet {
		old = append(old, id)
	}
	sort.Strings(old)
	mapping := make(map[string]string, len(old))
	for index, id := range old {
		mapping[id] = dest[index%len(dest)]
	}
	return mapping, nil
}
