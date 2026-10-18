package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type volumePaths []string

func (paths *volumePaths) String() string { return strings.Join(*paths, ",") }
func (paths *volumePaths) Set(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("volume path is empty")
	}
	*paths = append(*paths, path)
	return nil
}

func runVolume(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: dubi volume list | add | drain | remove | compact")
	}
	if args[0] != "list" && args[0] != "add" && args[0] != "drain" && args[0] != "remove" && args[0] != "compact" {
		return fmt.Errorf("unknown volume command %q", args[0])
	}
	flags := flag.NewFlagSet("volume "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "durable Node store root")
	server := flags.String("server", "", "local running Node RPC address (automatically remembered)")
	rewrite := flags.Bool("rewrite", false, "relocate live records before collecting mixed Packs")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if args[0] == "compact" {
		if flags.NArg() != 0 {
			return errors.New("usage: dubi volume compact [--store <path>] [--server <host:port>] [--rewrite]")
		}
		return compactVolume(ctx, *store, *server, *rewrite, encoder)
	}
	if args[0] == "list" {
		if flags.NArg() != 0 || flagWasSet(flags, "server") {
			return errors.New("volume list accepts no positional arguments or --server")
		}
		report, err := volume.Inspect(*store)
		if err != nil {
			return err
		}
		return encoder.Encode(report)
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("usage: dubi volume %s [--store <path>] [--server <host:port>] <volume-id>", args[0])
	}
	if args[0] == "add" {
		path, err := filepath.Abs(flags.Arg(0))
		if err != nil {
			return err
		}
		return mutateVolume(ctx, *store, *server, path, "add", encoder)
	}
	return mutateVolume(ctx, *store, *server, flags.Arg(0), args[0], encoder)
}

func compactVolume(ctx context.Context, store, endpoint string, rewrite bool, encoder *json.Encoder) error {
	lock, err := storelock.Acquire(store)
	if err != nil {
		if !errors.Is(err, storelock.ErrAlreadyOpen) {
			return fmt.Errorf("acquire Store lock for compaction: %w", err)
		}
		if endpoint == "" {
			endpoint, err = resolveServer(&flag.FlagSet{}, "", store)
			if err != nil {
				return fmt.Errorf("resolve local Node server for compaction: %w", err)
			}
		}
		id, conn, err := openVolumeAdminClient(store, endpoint)
		if err != nil {
			return fmt.Errorf("connect to running Node for compaction: %w", err)
		}
		defer conn.Close()
		call, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()
		response, err := datawire.NewDataServiceClient(conn).CompactStore(call, &datawire.CompactStoreRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, Rewrite: rewrite})
		if err != nil {
			return fmt.Errorf("compact Store through running Node: %w", err)
		}
		return encoder.Encode(response)
	}
	registry, err := volume.Open(store)
	if err != nil {
		return errors.Join(err, lock.Release())
	}
	packs, err := pack.OpenPool(registry)
	if err != nil {
		return errors.Join(err, registry.Close(), lock.Release())
	}
	data, err := records.OpenNodeStore(packs, registry)
	if err != nil {
		return errors.Join(err, packs.Close(), registry.Close(), lock.Release())
	}
	var report records.CompactReport
	if rewrite {
		report, err = data.RewriteCompaction(ctx)
	} else {
		report, err = data.Compact(ctx)
	}
	closeErr := errors.Join(data.Close(), packs.Close(), registry.Close(), lock.Release())
	if err != nil {
		return errors.Join(err, closeErr)
	}
	return errors.Join(encoder.Encode(report), closeErr)
}

func mutateVolume(ctx context.Context, store, endpoint, value, action string, encoder *json.Encoder) error {
	// Only the Store owner may mutate a live registry. A stopped Store is
	// handled locally; an owned Store goes through the same Node's authenticated
	// RPC, never by opening a second registry behind its back.
	lock, err := storelock.Acquire(store)
	if err != nil {
		if !errors.Is(err, storelock.ErrAlreadyOpen) {
			return fmt.Errorf("acquire Store lock for Volume mutation: %w", err)
		}
		if endpoint == "" {
			endpoint, err = resolveServer(&flag.FlagSet{}, "", store)
			if err != nil {
				return fmt.Errorf("resolve local Node server for Volume mutation: %w", err)
			}
		}
		if identityenrollment.ValidateNodeAddress(endpoint) != nil {
			return errors.New("local Node address is invalid")
		}
		var record volume.Record
		if value == "" {
			return errors.New("Volume ID or path is required")
		}
		if action == "remove" {
			record, err = removeRunningVolume(ctx, store, endpoint, value)
		} else if action == "drain" {
			record, err = drainRunningVolume(ctx, store, endpoint, value)
		} else {
			record, err = addRunningVolume(ctx, store, endpoint, value)
		}
		if err != nil {
			return fmt.Errorf("ask the running local Node to mutate Volume: %w", err)
		}
		return encoder.Encode(record)
	}
	registry, err := volume.Open(store)
	if err != nil {
		return errors.Join(err, lock.Release())
	}
	var record volume.Record
	if action == "remove" {
		if err := migrateStoppedVolume(ctx, registry, value); err != nil {
			_ = registry.Close()
			_ = lock.Release()
			return err
		}
		record, err = registry.RemoveDrained(value)
	} else if action == "drain" {
		record, err = registry.BeginDrain(value)
		if err == nil {
			err = beginStoppedVolumeMigration(registry, value)
		}
	} else {
		record, err = registry.Add(value)
	}
	if err := errors.Join(err, registry.Close(), lock.Release()); err != nil {
		return err
	}
	return encoder.Encode(record)
}

func beginStoppedVolumeMigration(registry *volume.Registry, volumeID string) error {
	packs, err := pack.OpenPool(registry)
	if err != nil {
		return err
	}
	store, err := records.OpenNodeStore(packs, registry)
	if err != nil {
		return errors.Join(err, packs.Close())
	}
	err = store.BeginVolumeMigration(volumeID)
	return errors.Join(err, store.Close(), packs.Close())
}

// migrateStoppedVolume applies the same safety gate as the running Node
// administration path. A missing/invalid disk is intentionally left for
// cross-Node repair and may be removed after the operator's explicit action;
// readable media must be drained into healthy Volumes first.
func migrateStoppedVolume(ctx context.Context, registry *volume.Registry, volumeID string) error {
	if ctx == nil {
		return errors.New("Volume migration requires a context")
	}
	report, err := registry.Status()
	if err != nil {
		return err
	}
	var selected volume.Status
	var found bool
	for _, item := range report.Volumes {
		if item.ID == volumeID {
			selected, found = item, true
			break
		}
	}
	if !found {
		return errors.New("unknown VolumeID")
	}
	if selected.Health == "missing" || selected.Health == "invalid" {
		// The operator has explicitly chosen to remove permanently lost media;
		// clear only the local migration task, never the logical record debt.
		if _, err := registry.BeginDrain(volumeID); err != nil {
			return err
		}
		packs, openErr := pack.OpenPool(registry)
		if openErr != nil {
			return openErr
		}
		store, openErr := records.OpenNodeStore(packs, registry)
		if openErr != nil {
			_ = packs.Close()
			return openErr
		}
		clearErr := store.AbandonVolumeMigration(volumeID)
		closeErr := errors.Join(store.Close(), packs.Close())
		return errors.Join(clearErr, closeErr)
	}
	packs, err := pack.OpenPool(registry)
	if err != nil {
		return err
	}
	defer packs.Close()
	store, err := records.OpenNodeStore(packs, registry)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.BeginVolumeMigration(volumeID); err != nil {
		return err
	}
	if _, err := store.MigrateVolume(ctx, volumeID); err != nil {
		return fmt.Errorf("migrate Volume data: %w", err)
	}
	if _, err := store.Compact(ctx); err != nil {
		return fmt.Errorf("reclaim migrated Pack data: %w", err)
	}
	return nil
}

func addRunningVolume(ctx context.Context, store, endpoint, path string) (volume.Record, error) {
	if ctx == nil {
		return volume.Record{}, errors.New("Volume administration requires a context")
	}
	id, err := identity.Load(store)
	if err != nil {
		return volume.Record{}, err
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		return volume.Record{}, err
	}
	bound, err := tlscredentials.ForNode(credentials.NewTLS(clientTLS), identity.CertificateIdentity{
		ClusterID: id.ClusterID, NodeID: id.NodeID, RaftID: id.RaftID})
	if err != nil {
		return volume.Record{}, err
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(bound))
	if err != nil {
		return volume.Record{}, err
	}
	defer conn.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).AddVolume(call, &datawire.AddVolumeRequest{
		ClusterId: id.ClusterID, FromRaftId: id.RaftID, Path: path})
	if err != nil {
		return volume.Record{}, err
	}
	return volume.Record{ID: response.GetVolumeId(), Path: response.GetPath(), State: volume.State(response.GetState())}, nil
}

func drainRunningVolume(ctx context.Context, store, endpoint, volumeID string) (volume.Record, error) {
	id, conn, err := openVolumeAdminClient(store, endpoint)
	if err != nil {
		return volume.Record{}, err
	}
	defer conn.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).DrainVolume(call, &datawire.DrainVolumeRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, VolumeId: volumeID})
	if err != nil {
		return volume.Record{}, err
	}
	return volume.Record{ID: response.GetVolumeId(), Path: response.GetPath(), State: volume.State(response.GetState())}, nil
}

func removeRunningVolume(ctx context.Context, store, endpoint, volumeID string) (volume.Record, error) {
	id, conn, err := openVolumeAdminClient(store, endpoint)
	if err != nil {
		return volume.Record{}, err
	}
	defer conn.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).RemoveVolume(call, &datawire.RemoveVolumeRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, VolumeId: volumeID})
	if err != nil {
		return volume.Record{}, err
	}
	return volume.Record{ID: response.GetVolumeId(), Path: response.GetPath(), State: volume.State(response.GetState())}, nil
}

func openVolumeAdminClient(store, endpoint string) (identity.Identity, *grpc.ClientConn, error) {
	id, err := identity.Load(store)
	if err != nil {
		return identity.Identity{}, nil, fmt.Errorf("load Node identity: %w", err)
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		return identity.Identity{}, nil, fmt.Errorf("load Node TLS: %w", err)
	}
	bound, err := tlscredentials.ForNode(credentials.NewTLS(clientTLS), identity.CertificateIdentity{ClusterID: id.ClusterID, NodeID: id.NodeID, RaftID: id.RaftID})
	if err != nil {
		return identity.Identity{}, nil, fmt.Errorf("bind Node TLS identity: %w", err)
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(bound))
	if err != nil {
		return identity.Identity{}, nil, fmt.Errorf("create Node RPC client: %w", err)
	}
	return id, conn, nil
}
