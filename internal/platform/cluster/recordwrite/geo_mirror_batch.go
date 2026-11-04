package recordwrite

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GeoMetadataMirrorBatchTransport interface {
	MirrorGeoBatch(context.Context, uint64, string, [][]byte) ([]raftrpc.GeoMirrorBatchResult, error)
}

const geoMirrorBatchAttempts = 3

type geoMirrorCall struct {
	ctx      context.Context
	target   uint64
	group    string
	command  []byte
	attempts int
	done     chan raftrpc.GeoMirrorBatchResult
}

// This pool lives for one bounded confirmation page. At most 32 publication
// workers supply metadata; the existing durable ledger owns unfinished work.
// Failed rows move behind newly waiting rows, with three attempts per selected
// destination before handing the error to the existing candidate/repair path.
type geoMirrorPagePool struct {
	GeoMetadataMirrorTransport
	batch  GeoMetadataMirrorBatchTransport
	ctx    context.Context
	cancel context.CancelFunc
	waits  chan *geoMirrorCall
	done   chan struct{}
}

func newGeoMirrorPagePool(ctx context.Context, transport GeoMetadataMirrorTransport) (GeoMetadataMirrorTransport, func()) {
	batch, ok := transport.(GeoMetadataMirrorBatchTransport)
	if !ok {
		return transport, func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &geoMirrorPagePool{GeoMetadataMirrorTransport: transport, batch: batch, ctx: ctx, cancel: cancel,
		waits: make(chan *geoMirrorCall, GeoConfirmationRepairPageLimit), done: make(chan struct{})}
	go p.run()
	return p, func() { cancel(); <-p.done }
}

func (p *geoMirrorPagePool) MirrorGeo(ctx context.Context, target uint64, group string, command []byte) (enginestate.Result, error) {
	// Preserve support for an individually valid command larger than a batch.
	if len(command) > raftrpc.GeoMirrorBatchBytes {
		return p.GeoMetadataMirrorTransport.MirrorGeo(ctx, target, group, command)
	}
	w := &geoMirrorCall{ctx: ctx, target: target, group: group, command: command, done: make(chan raftrpc.GeoMirrorBatchResult, 1)}
	select {
	case <-ctx.Done():
		return enginestate.Result{}, ctx.Err()
	case <-p.ctx.Done():
		return enginestate.Result{}, p.ctx.Err()
	case p.waits <- w:
	}
	select {
	case <-ctx.Done():
		return enginestate.Result{}, ctx.Err()
	case <-p.ctx.Done():
		return enginestate.Result{}, p.ctx.Err()
	case r := <-w.done:
		return r.Result, r.Err
	}
}

func (p *geoMirrorPagePool) run() {
	defer close(p.done)
	var pending []*geoMirrorCall
	for {
		if p.ctx.Err() != nil {
			return
		}
		if len(pending) == 0 {
			select {
			case <-p.ctx.Done():
				return
			case w := <-p.waits:
				pending = append(pending, w)
			}
		}
		pending = p.drain(pending)
		var batch, rest []*geoMirrorCall
		total := 0
		seen := make(map[string]bool)
		for _, w := range pending {
			if w.ctx.Err() != nil {
				continue
			}
			if len(batch) == 0 || (w.target == batch[0].target && w.group == batch[0].group && len(batch) < raftrpc.GeoMirrorBatchLimit && len(w.command) <= raftrpc.GeoMirrorBatchBytes-total && !seen[string(w.command)]) {
				batch = append(batch, w)
				total += len(w.command)
				seen[string(w.command)] = true
			} else {
				rest = append(rest, w)
			}
		}
		pending = rest
		if len(batch) == 0 {
			continue
		}
		encoded := make([][]byte, len(batch))
		for i, w := range batch {
			encoded[i] = w.command
			w.attempts++
		}
		ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
		results, err := p.batch.MirrorGeoBatch(ctx, batch[0].target, batch[0].group, encoded)
		// Mixed-version compatibility is explicit, never a fallback on a bad
		// response, denied request or durable rejection.
		if status.Code(err) == codes.Unimplemented {
			results = make([]raftrpc.GeoMirrorBatchResult, len(batch))
			err = nil
			for i, w := range batch {
				results[i].Result, results[i].Err = p.GeoMetadataMirrorTransport.MirrorGeo(ctx, w.target, w.group, w.command)
			}
		}
		cancel()
		if err == nil && len(results) != len(batch) {
			err = status.Error(codes.DataLoss, "Geo mirror batch omitted results")
		}
		// Arrivals during this RPC take precedence over its retained failures.
		pending = p.drain(pending)
		for i, w := range batch {
			r := raftrpc.GeoMirrorBatchResult{Err: err}
			if err == nil {
				r = results[i]
			}
			if w.ctx.Err() != nil {
				continue
			}
			if r.Err != nil && (retryable(r.Err) || errors.Is(r.Err, context.DeadlineExceeded)) && w.attempts < geoMirrorBatchAttempts {
				pending = append(pending, w)
			} else {
				w.done <- r
			}
		}
	}
}

func (p *geoMirrorPagePool) drain(pending []*geoMirrorCall) []*geoMirrorCall {
	for len(pending) < GeoConfirmationRepairPageLimit {
		select {
		case w := <-p.waits:
			pending = append(pending, w)
		default:
			return pending
		}
	}
	return pending
}
