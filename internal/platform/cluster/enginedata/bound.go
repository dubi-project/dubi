package enginedata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangeplacement"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// Binding is assembled by trusted server code, never decoded from a public request.
// Each Module receives its own Engine binding even when implementations are shared.
type Binding struct {
	service        *Service
	module, engine string
}
type Bound struct {
	binding Binding
	space   rootstate.Keyspace
}
type BoundWrite struct {
	RequestID string
	Key, Body []byte
	Publish   Publication
}

func (s *Service) Bind(module, engine string) (Binding, error) {
	if s == nil || s.runtime == nil || (rootstate.Keyspace{TenantID: "validation", ModuleID: module, EngineID: engine}).Validate() != nil {
		return Binding{}, errors.New("invalid server Engine binding")
	}
	return Binding{service: s, module: module, engine: engine}, nil
}

// ForTenant must be called after authentication and authorization. It cannot
// change the Module/Engine chosen at assembly. Neither method creates resources.
func (b Binding) ForTenant(tenant string) (*Bound, error) {
	space := rootstate.Keyspace{TenantID: tenant, ModuleID: b.module, EngineID: b.engine}
	if b.service == nil || space.Validate() != nil {
		return nil, errors.New("invalid authorized Tenant binding")
	}
	return &Bound{binding: b, space: space}, nil
}

func (b *Bound) Write(ctx context.Context, r BoundWrite) (WriteResult, error) {
	if b == nil || b.binding.service == nil || ctx == nil || rootstate.ValidateRequestID(r.RequestID) != nil || len(r.Key) == 0 || len(r.Key) > 4096 || r.Publish == nil {
		return WriteResult{State: WriteUnknown}, errors.New("invalid bound Engine write")
	}
	started := time.Now()
	err := b.binding.service.ensureRange(ctx, b.space)
	recordwrite.Trace(ctx, "engine.write.ensure_range", started)
	if err != nil {
		return WriteResult{State: WriteUnknown}, fmt.Errorf("prepare bound Range: %w", err)
	}
	requestID, _ := b.space.BindRequestID(r.RequestID)
	return b.binding.service.Write(ctx, WriteRequest{RequestID: requestID, Keyspace: b.space, Key: r.Key, Body: r.Body, Publish: r.Publish})
}

func (b *Bound) Read(ctx context.Context, key []byte) (recordwrite.LocalRead, error) {
	if b == nil || b.binding.service == nil {
		return recordwrite.LocalRead{}, errors.New("uninitialized Engine binding")
	}
	return b.binding.service.Read(ctx, b.space, key)
}

// Bootstrap host uses the Runtime Root barrier, including non-voter routing.
type bootstrapHost struct{ service *Service }

func (h bootstrapHost) Status(group string) (raft.GroupStatus, error) {
	return h.service.runtime.Host.Status(group)
}

func (s *Service) prepareInitialPlacement(ctx context.Context, rangeID string) error {
	c, err := rangeplacement.New(bootstrapHost{s}, s.runtime.Root)
	if err != nil {
		return err
	}
	return c.EnsureInitial(ctx, rangeID)
}

func (h bootstrapHost) Submit(group string, data []byte) error {
	return h.service.runtime.Host.Submit(group, data)
}
func (h bootstrapHost) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if group == raft.RootGroupID {
		return h.service.runtime.ReadRootIndex(ctx)
	}
	return h.service.runtime.Host.ReadIndex(ctx, group)
}

// ensureRange uses only a local directory lookup on the existing active path.
// Missing/Pending ranges are prepared by durable Root/Owner operations. The
// ensuing write still validates current routing and fences; this is not authority.
func (s *Service) ensureRange(ctx context.Context, space rootstate.Keyspace) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	descriptor, found, err := s.runtime.Root.InitialRange(space)
	if err != nil {
		return err
	}
	if found && descriptor.State == rootstate.RangeStateActive {
		return s.waitInitialPlacement(ctx, descriptor)
	}
	host := bootstrapHost{s}
	prepareStarted := time.Now()
	if !found {
		if _, err = host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return err
		}
		descriptor, found, err = s.runtime.Root.InitialRange(space)
		if err != nil {
			return err
		}
		if !found {
			// Do not persist a deterministic preparation rejection for a Tenant
			// that may legitimately be registered later. Registration is monotonic;
			// this check follows the confirmed Root cut above.
			if _, exists, err := s.runtime.Root.Tenant(space.TenantID); err != nil {
				return err
			} else if !exists {
				return errors.New("Tenant is not registered")
			}
			member, ok, err := s.runtime.Root.RootMember(s.runtime.Identity.RaftID)
			if err != nil || !ok {
				return errors.New("local Node has no confirmed membership")
			}
			owner := regiongroups.GroupID(member.RegionID)
			progress, ok, err := s.runtime.Root.RegionProgress(owner)
			if err != nil || !ok || !progress.Eligible || progress.Pending != nil {
				return errors.New("local Region has no confirmed data Group")
			}
			encoded, _ := json.Marshal(space)
			digest := sha256.Sum256(append([]byte("dubi bound range v2\x00"), encoded...))
			seed := rootstate.RangeSeed{TenantID: space.TenantID, ModuleID: space.ModuleID, EngineID: space.EngineID, RangeID: "range-" + hex.EncodeToString(digest[:16]), OwnerGroupID: owner}
			// Region is included in request identity: concurrent first writes from
			// different Regions may propose different Owners. Root elects one Range;
			// losers must load that descriptor, not overwrite it or reuse a command ID.
			commandHash := sha256.Sum256([]byte(seed.RangeID + "\x00" + owner))
			cmd, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, RequestID: "prepare-range-" + hex.EncodeToString(commandHash[:]), Type: rootstate.PrepareRange, InitialRange: &seed})
			if err != nil {
				return err
			}
			if err = host.Submit(raft.RootGroupID, cmd); err != nil {
				return err
			}
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				if _, err = host.ReadIndex(ctx, raft.RootGroupID); err != nil {
					return err
				}
				result, done, err := s.runtime.Root.ResultForCommand(cmd)
				if err != nil {
					return err
				}
				descriptor, found, err = s.runtime.Root.InitialRange(space)
				if err != nil {
					return err
				}
				if found {
					break
				}
				if done {
					return fmt.Errorf("prepare initial Range: %s", result.Error)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	}
	recordwrite.Trace(ctx, "engine.range.prepare", prepareStarted)
	bootstrapStarted := time.Now()
	bootstrap, err := rangebootstrap.New(host, s.runtime.Root, func(group string) (rangebootstrap.OwnerControl, bool) {
		if state, ok := s.runtime.Engines.Resolve(group); ok {
			owner, err := rangebootstrap.NewLocalOwner(host, state)
			return owner, err == nil
		}
		for _, id := range s.client.GroupCandidates(group) {
			if id != s.runtime.Identity.RaftID {
				owner, err := s.client.Owner(id, group)
				if err == nil {
					return owner, true
				}
			}
		}
		return nil, false
	})
	if err != nil {
		return err
	}
	err = bootstrap.Reconcile(ctx, descriptor.RangeID)
	recordwrite.Trace(ctx, "engine.range.activate", bootstrapStarted)
	if err != nil {
		return err
	}
	return s.waitInitialPlacement(ctx, descriptor)
}

// Activation publishes the Owner fence before placement. First writers help
// publish missing placement through the same Root command as the background
// coordinator, then verify it. Formation/growth can still require a bounded
// background wait. Established writes stay local here; normal routing retains
// its authority/fence checks.
func (s *Service) waitInitialPlacement(ctx context.Context, descriptor rootstate.Range) error {
	started := time.Now()
	defer recordwrite.Trace(ctx, "engine.range.wait_placement", started)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var ticker *time.Ticker
	attempted := false
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for initial Range placement: %w", err)
		}
		placement, found, err := s.runtime.Root.RangePlacement(descriptor.RangeID)
		if err != nil {
			return err
		}
		if found {
			if placement.RangeEpoch != descriptor.Epoch || placement.OwnerGroupID != descriptor.OwnerGroupID {
				return errors.New("initial Range placement differs from its Owner or epoch")
			}
			return nil
		}
		if !attempted && s.initialPlacement != nil {
			attempted = true
			if err := s.initialPlacement(ctx, descriptor.RangeID); err != nil {
				return fmt.Errorf("publish initial Range placement: %w", err)
			}
			continue // Verify persisted placement; callback success is not proof.
		}
		if ticker == nil {
			ticker = time.NewTicker(50 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for initial Range placement: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// EnsureRange implements Runtime's trusted RangePreparer; it never creates Tenants.
func (s *Service) EnsureRange(ctx context.Context, space rootstate.Keyspace) error {
	if s == nil || ctx == nil || space.Validate() != nil {
		return errors.New("invalid initial Range binding")
	}
	return s.ensureRange(ctx, space)
}
