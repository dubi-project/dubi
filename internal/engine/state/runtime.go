package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// Engine fixes the Module and Engine identities at server assembly.
type Engine struct {
	binding enginedata.Binding
}

type Client struct {
	backend *enginedata.Bound
}

func New(runtime *enginedata.Service, module string) (*Engine, error) {
	b, err := runtime.Bind(module, EngineID)
	if err != nil {
		return nil, err
	}
	return &Engine{binding: b}, nil
}

// ForTenant accepts an already authorized tenant, never an unverified API field.
func (e *Engine) ForTenant(tenant string) (*Client, error) {
	if e == nil {
		return nil, ErrInvalid
	}
	b, err := e.binding.ForTenant(tenant)
	if err != nil {
		return nil, err
	}
	return &Client{backend: b}, nil
}

func (c *Client) lookup(ctx context.Context, key []byte, q query) (queryResult, error) {
	payload, _ := json.Marshal(q)
	out, err := c.backend.Query(ctx, dataKey(key), payload)
	if errors.Is(err, router.ErrNoRoute) {
		return queryResult{}, nil
	}
	if err != nil {
		return queryResult{}, err
	}
	if out.Rejection == ErrRequestReused.Error() {
		return queryResult{}, ErrRequestReused
	}
	if out.Rejection != "" {
		return queryResult{}, errors.New(out.Rejection)
	}
	var result queryResult
	if err := decode(out.Value, &result); err != nil {
		return queryResult{}, err
	}
	return result, nil
}

func (c *Client) Get(ctx context.Context, key []byte) (Entry, error) {
	if c == nil || c.backend == nil || ctx == nil || len(key) == 0 || len(key) > MaxKeyBytes {
		return Entry{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Entry{}, err
	}
	q, err := c.lookup(ctx, key, query{Format: 1})
	if err != nil {
		return Entry{}, err
	}
	if q.Head == nil {
		return Entry{}, ErrNotFound
	}
	// The value and version come from the SAME physically verified publication,
	// never from a separate head read that can race with an overwrite.
	read, err := c.backend.Read(ctx, dataKey(key))
	if err != nil {
		return Entry{}, err
	}
	var body content
	if len(read.Body) > 96<<10 || decode(read.Body, &body) != nil || body.Format != 1 || !validVersion(body.Version) || len(body.Value) > MaxValueBytes || (body.Deleted && len(body.Value) != 0) {
		return Entry{}, errors.New("invalid State content")
	}
	if body.Deleted {
		return Entry{}, ErrNotFound
	}
	return Entry{Version: body.Version, Value: bytes.Clone(body.Value)}, nil
}

func (c *Client) Put(ctx context.Context, key, value []byte, requestID string, condition Condition) (Result, error) {
	return c.mutate(ctx, operation{Format: 1, RequestID: requestID, Key: bytes.Clone(key), Value: bytes.Clone(value), Condition: condition})
}
func (c *Client) Delete(ctx context.Context, key []byte, requestID string, condition Condition) (Result, error) {
	return c.mutate(ctx, operation{Format: 1, RequestID: requestID, Key: bytes.Clone(key), Delete: true, Condition: condition})
}

func operationResult(o operation, r *receipt, replayed bool) (Result, error) {
	if r == nil {
		return Result{}, ErrUnknown
	}
	if r.Version != o.version() {
		return Result{}, ErrRequestReused
	}
	if r.Code == "cancelled" {
		return Result{}, ErrCancelled
	}
	if r.Code == "conflict" {
		return Result{}, ErrConflict
	}
	if r.Code != "" || !r.Confirmed {
		return Result{}, ErrUnknown
	}
	return Result{RequestID: o.RequestID, Version: r.Version, Deleted: r.Deleted, Replayed: replayed}, nil
}

func (c *Client) mutate(ctx context.Context, o operation) (Result, error) {
	if c == nil || c.backend == nil || ctx == nil || o.validate() != nil {
		return Result{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	q := query{Format: 1, RequestID: o.RequestID, Version: o.version()}
	result, err := c.backend.Mutate(ctx, enginedata.Mutation{
		Value: o.logicalValue(),
		Write: enginedata.BoundWrite{RequestID: o.RequestID, Key: dataKey(o.Key), Body: o.body(),
			Publish: func(d records.Descriptor) ([]byte, error) {
				return json.Marshal(publication{Operation: o, Descriptor: d})
			}},
		Inspect: func(ctx context.Context) (enginedata.MutationView, error) {
			got, err := c.lookup(ctx, o.Key, q)
			if err != nil {
				return enginedata.MutationView{}, err
			}
			view := enginedata.MutationView{Current: got.Usage, Exists: got.Head != nil && !got.Head.Deleted}
			if got.Receipt != nil {
				view.Receipt, _ = json.Marshal(got.Receipt)
				view.Published = got.Receipt.Usage
				view.Confirmed = got.Receipt.Confirmed
				view.Rejected = got.Receipt.Code != ""
			}
			return view, nil
		},
	})
	if err != nil {
		if errors.Is(err, ErrRequestReused) {
			return Result{}, ErrRequestReused
		}
		if errors.Is(err, enginedata.ErrMutationUnknown) {
			return Result{}, errors.Join(ErrUnknown, err)
		}
		return Result{}, err
	}
	if len(result.Receipt) != 0 {
		var r receipt
		if err := decode(result.Receipt, &r); err != nil {
			return Result{}, errors.Join(ErrUnknown, err)
		}
		return operationResult(o, &r, result.Replayed)
	}
	return Result{RequestID: o.RequestID, Version: o.version(), Deleted: o.Delete}, nil
}

func (o operation) logicalValue() enginestate.LogicalValue {
	size := uint64(0)
	if !o.Delete {
		size = uint64(len(o.Key) + len(o.Value))
	}
	return enginestate.LogicalValue{Key: o.Key, Bytes: size}
}
