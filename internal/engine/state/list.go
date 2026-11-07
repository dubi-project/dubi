package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// The current Runtime binds each Keyspace to one complete Range. This index
// lives in the same replicated transaction and snapshots as its State heads.
var listCounterKey = []byte("c/list-order")

func listIndexKey(order uint64) []byte { return []byte(fmt.Sprintf("i/%016x", ^order)) }
func updateListIndex(tx *enginestate.Transaction, key []byte, old head, live bool, next *head) error {
	next.CreatedOrder = old.CreatedOrder
	if next.Deleted {
		if old.CreatedOrder != 0 {
			return tx.Delete(listIndexKey(old.CreatedOrder))
		}
		return nil
	}
	if !live {
		b, ok, err := tx.Get(listCounterKey)
		if err != nil {
			return err
		}
		var seq uint64
		if ok && decode(b, &seq) != nil {
			return fmt.Errorf("invalid State list counter")
		}
		if seq == math.MaxUint64 {
			return fmt.Errorf("State list counter exhausted")
		}
		seq++
		b, _ = json.Marshal(seq)
		if err = tx.Set(listCounterKey, b); err != nil {
			return err
		}
		next.CreatedOrder = seq
	}
	if next.CreatedOrder != 0 {
		return tx.Set(listIndexKey(next.CreatedOrder), bytes.Clone(key))
	}
	// Existing live heads without recorded creation order keep their unknown age.
	return nil
}

type ListPosition struct {
	Phase string `json:"phase"`
	After []byte `json:"after,omitempty"`
}
type ListRequest struct {
	Search   string       `json:"search,omitempty"`
	Limit    int          `json:"limit"`
	Position ListPosition `json:"position"`
}
type ListItem struct {
	Key          []byte  `json:"key"`
	Version      string  `json:"version"`
	ValueBytes   *uint64 `json:"value_bytes"`
	CreatedOrder uint64  `json:"created_order"`
	Confirmed    bool    `json:"confirmed"`
}
type ListPage struct {
	Items []ListItem    `json:"items"`
	Next  *ListPosition `json:"next,omitempty"`
}

func (r ListRequest) validate() error {
	if r.Limit < 1 || r.Limit > 100 || len(r.Search) > 256 || len(r.Position.After) > MaxKeyBytes+2 {
		return ErrInvalid
	}
	if r.Position.Phase != "" && r.Position.Phase != "index" && r.Position.Phase != "legacy" {
		return ErrInvalid
	}
	prefix := []byte("i/")
	if r.Position.Phase == "legacy" {
		prefix = []byte("v/")
	}
	if len(r.Position.After) > 0 && !bytes.HasPrefix(r.Position.After, prefix) {
		return ErrInvalid
	}
	return nil
}
func readList(view enginestate.ReadView, r ListRequest) (enginestate.Outcome, error) {
	if r.validate() != nil {
		return enginestate.Outcome{Rejection: ErrInvalid.Error()}, nil
	}
	result := ListPage{Items: []ListItem{}}
	position := r.Position
	if position.Phase == "" {
		position.Phase = "index"
	}
	budget := 512
	search := strings.ToLower(r.Search)
	for budget > 0 {
		prefix := []byte("i/")
		if position.Phase == "legacy" {
			prefix = []byte("v/")
		}
		rows, more, err := view.ScanInline(prefix, position.After, min(budget, 128))
		if err != nil {
			return enginestate.Outcome{}, err
		}
		for _, row := range rows {
			budget--
			previous := position
			position.After = row.Key
			var h head
			var key []byte
			if position.Phase == "index" {
				key = row.Value
				b, found, e := view.Get(dataKey(key))
				if e != nil {
					return enginestate.Outcome{}, e
				}
				if !found {
					continue
				}
				if decode(b, &h) != nil {
					return enginestate.Outcome{}, fmt.Errorf("invalid State list head")
				}
				if h.CreatedOrder == 0 || !bytes.Equal(listIndexKey(h.CreatedOrder), row.Key) {
					return enginestate.Outcome{}, fmt.Errorf("State list index mismatch")
				}
			} else {
				key = row.Key[2:]
				if decode(row.Value, &h) != nil {
					return enginestate.Outcome{}, fmt.Errorf("invalid State head")
				}
				if h.CreatedOrder != 0 {
					continue
				}
			}
			if h.Deleted || !strings.Contains(strings.ToLower(string(key)), search) {
				continue
			}
			if len(result.Items) == r.Limit {
				result.Next = &previous
				return listOutcome(result)
			}
			confirmed, e := view.PublicationConfirmed(dataKey(key))
			if e != nil {
				return enginestate.Outcome{}, e
			}
			var n *uint64
			if h.Bytes != nil {
				if *h.Bytes < uint64(len(key)) {
					return enginestate.Outcome{}, fmt.Errorf("invalid State size")
				}
				v := *h.Bytes - uint64(len(key))
				n = &v
			}
			result.Items = append(result.Items, ListItem{Key: bytes.Clone(key), Version: h.Version, ValueBytes: n, CreatedOrder: h.CreatedOrder, Confirmed: confirmed})
		}
		if !more {
			if position.Phase == "legacy" {
				return listOutcome(result)
			}
			position = ListPosition{Phase: "legacy"}
		}
	}
	result.Next = &position
	return listOutcome(result)
}
func listOutcome(p ListPage) (enginestate.Outcome, error) {
	b, e := json.Marshal(p)
	return enginestate.Outcome{Value: b}, e
}
func (c *Client) List(ctx context.Context, r ListRequest) (ListPage, error) {
	if c == nil || c.backend == nil || ctx == nil || r.validate() != nil {
		return ListPage{}, ErrInvalid
	}
	// No bootstrap on reads. An empty tenant has no Range and returns no items.
	q, err := c.listQuery(ctx, r)
	return q, err
}
func (c *Client) listQuery(ctx context.Context, r ListRequest) (ListPage, error) {
	b, _ := json.Marshal(query{Format: 1, List: &r})
	out, err := c.backend.Query(ctx, []byte("v/"), b)
	if errors.Is(err, router.ErrNoRoute) {
		return ListPage{Items: []ListItem{}}, nil
	}
	if err != nil {
		return ListPage{}, err
	}
	if out.Rejection != "" {
		return ListPage{}, fmt.Errorf("State list: %s", out.Rejection)
	}
	var page ListPage
	if err = decode(out.Value, &page); err != nil {
		return ListPage{}, err
	}
	return page, nil
}
