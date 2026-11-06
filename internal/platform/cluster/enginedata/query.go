package enginedata

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
)

// Query runs an Engine's bounded read handler at a fenced Owner read cut.
// It is metadata access, not evidence that referenced Pack bytes are held.
func (b *Bound) Query(ctx context.Context, key, payload []byte) (enginestate.Outcome, error) {
	if b == nil || b.binding.service == nil || ctx == nil || len(key) == 0 || len(key) > 4096 || len(payload) == 0 || len(payload) > 256<<10 {
		return enginestate.Outcome{}, errors.New("invalid bound Engine query")
	}
	s := b.binding.service
	target, err := s.resolveTarget(ctx, b.space, key)
	if err != nil {
		return enginestate.Outcome{}, err
	}
	var last error
	if local, ok := s.runtime.Engines.Resolve(target.OwnerGroupID); ok {
		out, _, err := local.QueryRouted(ctx, s.runtime, target, payload)
		if err == nil {
			return out, nil
		}
		last = err
	}
	remote, err := s.client.RoutedEngine(target.OwnerGroupID)
	if err != nil {
		return enginestate.Outcome{}, errors.Join(last, err)
	}
	out, _, err := remote.Read(ctx, target, payload)
	if err != nil {
		return enginestate.Outcome{}, errors.Join(last, err)
	}
	return out, nil
}
