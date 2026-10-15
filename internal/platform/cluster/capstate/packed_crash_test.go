package capstate

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

type exitAfterPack struct{ *pack.Pool }

func (p exitAfterPack) Append(ctx context.Context, record pack.Record) (pack.Location, error) {
	loc, err := p.Pool.Append(ctx, record)
	if err == nil {
		os.Exit(61)
	}
	return loc, err
}

func TestPackedCrashHelper(t *testing.T) {
	root := os.Getenv("DUBI_CAPSTATE_CRASH_ROOT")
	if root == "" {
		return
	}
	volumes, err := volume.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	p, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenWithPacks(filepath.Join(root, "state"), "data-1", &packedHandler{}, p)
	if err != nil {
		t.Fatal(err)
	}
	target := packedTarget(t, s, "record", "t")
	if os.Getenv("DUBI_CAPSTATE_CRASH_POINT") == "after-pack" {
		s.packs = exitAfterPack{p}
	} else {
		s.commit = func(batch *pebble.Batch) error {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			os.Exit(62)
			return nil
		}
	}
	if err := s.ApplyRouted(2, "write", target, bytes.Repeat([]byte("c"), PackValueThreshold)); err != nil {
		t.Fatal(err)
	}
	t.Fatal("did not reach crash point")
}

func TestPackedCrashRecoveryPublicationBoundaries(t *testing.T) {
	for _, point := range []string{"after-pack", "after-index"} {
		t.Run(point, func(t *testing.T) {
			root := t.TempDir()
			if _, err := identity.Bootstrap(root); err != nil {
				t.Fatal(err)
			}
			volumes, err := volume.Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := volumes.Reserve(8<<20, nil)
			if errors.Is(err, volume.ErrInsufficientSpace) {
				volumes.Close()
				t.Skip("requires disk headroom")
			}
			if err != nil {
				volumes.Close()
				t.Fatal(err)
			}
			lease.Release()
			if err := volumes.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPackedCrashHelper$")
			command.Env = append(os.Environ(), "DUBI_CAPSTATE_CRASH_ROOT="+root, "DUBI_CAPSTATE_CRASH_POINT="+point)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			wantExit := 61
			if point == "after-index" {
				wantExit = 62
			}
			if !errors.As(err, &exit) || exit.ExitCode() != wantExit {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			volumes, err = volume.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer volumes.Close()
			p, err := pack.OpenPool(volumes)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			h := &packedHandler{}
			s, err := OpenWithPacks(filepath.Join(root, "state"), "data-1", h, p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			wantIndex, wantRecords := uint64(1), 2
			if point == "after-index" {
				wantIndex, wantRecords = 2, 1
			}
			if s.AppliedIndex() != wantIndex {
				t.Fatal("wrong recovered applied index", s.AppliedIndex())
			}
			_, found, err := s.Result("write")
			if err != nil || found != (point == "after-index") {
				t.Fatal("wrong durable request result", found, err)
			}
			descriptor, found, err := s.Range("range-1")
			if err != nil || !found {
				t.Fatal("lost Range fence", err)
			}
			target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
				OwnerGroupID: descriptor.OwnerGroupID, Epoch: descriptor.Epoch, RoutingKey: []byte("item")}
			large := bytes.Repeat([]byte("c"), PackValueThreshold)
			if err := s.ApplyRouted(wantIndex+1, "write", target, large); err != nil {
				t.Fatal(err)
			}
			requireValue(t, s, "record", "item", large)
			if h.calls != wantRecords-1 {
				t.Fatal("recovery repeated a published mutation")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			report, err := volumes.Status()
			if err != nil {
				t.Fatal(err)
			}
			collection, err := pack.OpenCollection(volumes, report.Volumes[0].ID)
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			count := 0
			if err := collection.Scan(func(_ pack.Location, _ pack.Record) error { count++; return nil }); err != nil {
				t.Fatal(err)
			}
			if count != wantRecords {
				t.Fatal("orphan retention / deduplication", count, wantRecords)
			}
		})
	}
}
