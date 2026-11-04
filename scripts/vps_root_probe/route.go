package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
)

type attempt struct {
	To      uint64  `json:"to"`
	Leader  uint64  `json:"leader_hint"`
	Index   uint64  `json:"confirmed_index"`
	MS      float64 `json:"rpc_ms"`
	Outcome string  `json:"outcome"`
	Error   string  `json:"error,omitempty"`
}

type rootProbe struct {
	// The input view is a routing allowlist, never quorum evidence. Unknown
	// identities require a new probe with a freshly validated membership view.
	targets   map[uint64]identity.CertificateIdentity
	preferred atomic.Uint64
	query     func(context.Context, uint64) (identity.CertificateIdentity, *raftwire.GroupReadIndexResponse, error)
}

func (p *rootProbe) confirm(ctx context.Context) (uint64, []attempt, error) {
	target := p.preferred.Load()
	seen := make(map[uint64]bool)
	var attempts []attempt
	for len(seen) < len(p.targets) {
		if err := ctx.Err(); err != nil {
			return 0, attempts, err
		}
		want, ok := p.targets[target]
		if !ok || seen[target] {
			return 0, attempts, errors.New("unknown voter or routing loop")
		}
		seen[target] = true
		start := time.Now()
		remote, reply, err := p.query(ctx, target)
		a := attempt{To: target, MS: float64(time.Since(start).Microseconds()) / 1000,
			Leader: reply.GetLeaderRaftId(), Index: reply.GetConfirmedIndex()}
		if err == nil && remote != want {
			err = errors.New("peer certificate identity mismatch")
		}
		if err == nil {
			if _, known := p.targets[a.Leader]; !known {
				err = errors.New("response leader is not a known voter")
			}
		}
		if err == nil && a.Index != 0 && a.Leader != target {
			err = errors.New("confirmed Root response did not come from its leader")
		}
		if err == nil && a.Index == 0 && a.Leader == target {
			err = errors.New("leader response has no quorum-confirmed index")
		}
		if err != nil {
			a.Outcome, a.Error = "failed", err.Error()
			attempts = append(attempts, a)
			// Preserve actual timeouts and authorization/proof failures. Only an
			// authenticated routing hint is followed, under the original deadline.
			return 0, attempts, err
		}
		if a.Index != 0 {
			a.Outcome = "confirmed"
			attempts = append(attempts, a)
			p.preferred.Store(target)
			return a.Index, attempts, nil
		}
		a.Outcome = "redirect"
		attempts = append(attempts, a)
		target = a.Leader
	}
	return 0, attempts, fmt.Errorf("Root routing exhausted %d known voters", len(p.targets))
}
