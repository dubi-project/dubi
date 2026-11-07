package enginedata

import (
	"bytes"
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/tenantquota"
)

var ErrMutationUnknown = errors.New("Engine mutation outcome is unknown; retry the same request ID")

// MutationView describes an Engine's existing business read. Current is the
// Runtime-owned usage metadata returned with the current record reference.
// Receipt is opaque to Runtime. A durable rejection or confirmed receipt is
// terminal; an unconfirmed publication must still finish data confirmation.
type MutationView struct {
	Exists    bool
	Current   *rootstate.UsageReport
	Published *rootstate.UsageReport
	Receipt   []byte
	Confirmed bool
	Rejected  bool
}

// Mutation supplies business semantics, never quota policy or lifecycle hooks.
// Inspect must use the caller's context and validate request identity/content.
// It reuses the normal Engine read; Runtime adds no quota-specific RPC.
type Mutation struct {
	Write   BoundWrite
	Value   enginestate.LogicalValue
	Inspect func(context.Context) (MutationView, error)
}

type MutationResult struct {
	Write    WriteResult
	Receipt  []byte
	Replayed bool
}

// Mutate is the common logical write path for all bound Engines. Admission,
// replay handling, pending estimates and uncertain outcomes belong to Runtime.
func (b *Bound) Mutate(ctx context.Context, m Mutation) (MutationResult, error) {
	if b == nil || b.binding.service == nil || b.binding.service.runtime == nil || ctx == nil ||
		m.Inspect == nil || m.Value.Validate() != nil || validateBoundWrite(m.Write) != nil {
		return MutationResult{}, errors.New("invalid logical Engine mutation")
	}
	if err := ctx.Err(); err != nil {
		return MutationResult{}, err
	}
	q := b.binding.service.runtime.TenantQuota
	if q == nil {
		return MutationResult{}, errors.New("Runtime quota service is unavailable")
	}
	return runMutation(ctx, b.space, q, m, func(ctx context.Context, r BoundWrite) (WriteResult, error) {
		return b.write(ctx, r, true)
	})
}

func runMutation(ctx context.Context, space rootstate.Keyspace, q *tenantquota.Service, m Mutation,
	write func(context.Context, BoundWrite) (WriteResult, error)) (MutationResult, error) {
	observe := func(v MutationView) error {
		for _, r := range []*rootstate.UsageReport{v.Current, v.Published} {
			if r != nil && (r.Validate() != nil || r.Space != space || !bytes.Equal(r.Key, m.Value.Key)) {
				return errors.New("Engine observation differs from logical mutation")
			}
		}
		if v.Published != nil && v.Published.RequestID != m.Write.RequestID {
			return errors.New("Engine publication differs from request identity")
		}
		if (v.Confirmed || v.Rejected) && len(v.Receipt) == 0 || v.Rejected && v.Published != nil {
			return errors.New("invalid Engine mutation outcome")
		}
		q.Observe(space, m.Write.RequestID, v.Published, v.Rejected)
		return nil
	}
	prior, err := m.Inspect(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	if err = observe(prior); err != nil {
		return MutationResult{}, err
	}
	if prior.Confirmed || prior.Rejected {
		return MutationResult{Receipt: prior.Receipt, Replayed: true}, nil
	}
	var admission *tenantquota.Admission
	if prior.Published == nil {
		oldSize := uint64(0)
		if prior.Current != nil {
			oldSize = prior.Current.Bytes
		} else if prior.Exists {
			// Previously metered development data may predate usage references.
			// This is a local metadata read, never a Root barrier or migration.
			k, e := q.Root.UsageKey(space, m.Value.Key)
			if e != nil {
				return MutationResult{}, e
			}
			oldSize = k.Bytes
		}
		admission, err = q.Admit(space, m.Value.Key, m.Write.RequestID, oldSize, m.Value.Bytes)
		if err != nil {
			return MutationResult{}, err
		}
	}
	written, writeErr := write(ctx, m.Write)
	if written.Local.Reference.Usage != nil {
		if err := observe(MutationView{Published: written.Local.Reference.Usage}); err != nil {
			return MutationResult{}, errors.Join(ErrMutationUnknown, err)
		}
		q.Published(admission, written.Local.Reference.Usage)
	}
	if writeErr == nil && (written.State == WriteLocalReady || written.State == WriteGeoReady) {
		if u := written.Local.Reference.Usage; u == nil || u.Bytes != m.Value.Bytes {
			return MutationResult{}, errors.Join(ErrMutationUnknown, errors.New("publication lacks matching logical size"))
		}
		return MutationResult{Write: written}, nil
	}
	// Failed transport is not a durable rejection. Preserve pending growth
	// unless the Engine's ordinary outcome read proves rejection/publication.
	after, err := m.Inspect(ctx)
	if err == nil {
		err = observe(after)
		if err == nil && (after.Confirmed || after.Rejected) {
			return MutationResult{Write: written, Receipt: after.Receipt, Replayed: true}, nil
		}
	}
	return MutationResult{Write: written}, errors.Join(ErrMutationUnknown, writeErr, err)
}
